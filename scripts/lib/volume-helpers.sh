#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# volume-helpers.sh — Shared Hetzner Cloud Volume helpers (fleet-degradation f1)
#
# SOURCED, not executed:
#
#   source "$(dirname "${BASH_SOURCE[0]}")/lib/volume-helpers.sh"
#
# The create/attach/record flow lives in vm-provision.sh; the mountpoint
# pre-flight lives in sut-deploy.sh; the sysfs yank gate lives in
# volume-sysfs-gate.sh. This file owns the parts that must behave
# identically in all three:
#
#   * device discovery by stable identity (never enumeration order),
#   * idempotent format + mount + fstab persistence,
#   * mountpoint assertions that cannot silently fall back to local disk.
#
# Two execution contexts exist:
#
#   1. Local  — the caller sources this file and calls the functions
#      directly (e.g. on the laptop, or inside a `bash -s` heredoc that
#      already runs on the node).
#   2. Remote — the caller uses `volume_run_remote <ssh-target> <fn> args...`
#      which re-emits the needed function bodies over SSH. No copy of this
#      file ever needs to exist on the node (the SUT has no repo checkout).
#
# All functions are silent on success unless they compute a value (which is
# printed to stdout); errors go to stderr with a non-zero return.
# ---------------------------------------------------------------------------

# Stable device identity: Hetzner attaches volumes over virtio-scsi with the
# serial `0HC_Volume_<id>`, which udev exposes as
# /dev/disk/by-id/scsi-0HC_Volume_<id>. The volume *id* is the only stable
# handle (device letters shuffle across attach/rescan/reboot); this path is
# how every redeploy and the sysfs-gate rescan re-find the same device.
#
# NOTE: volume_by_id_path inlines its literals (no shared constants) so the
# function body is self-contained when volume_run_remote streams it to a
# node — only functions travel, not file-level variables.

# Mount root for every role volume: /mnt/oceanfs-<role>. Used by the
# callers (vm-provision.sh, sut-deploy.sh) as well as this file.
# shellcheck disable=SC2034
readonly VOLUME_MOUNT_BASE="/mnt"

# Sanity floors (GB) for the deploy pre-flight. These are deliberately
# generous fractions of the provisioning defaults (data 120, wal 20,
# meta 20, hints 10): the assertion's job is to catch a wrong/absent
# volume (root fs, tiny loop device, a stray small disk), not to validate
# exact sizing.
volume_min_gb_for_role() {
    case "${1:-}" in
        data) echo 50 ;;
        wal) echo 8 ;;
        meta) echo 8 ;;
        hints) echo 4 ;;
        spare) echo 20 ;;
        *) echo 4 ;;
    esac
}

# Echo the stable by-id path for a Hetzner volume id.
volume_by_id_path() {
    printf '%s' "/dev/disk/by-id/scsi-0HC_Volume_${1:-}"
}

# is_mountpoint PATH — true when PATH is a mountpoint.
is_mountpoint() {
    [ -n "${1:-}" ] && mountpoint -q "$1" 2>/dev/null
}

# device_fs_type DEVICE — existing filesystem type or empty (no signature).
device_fs_type() {
    blkid -o value -s TYPE "${1:-}" 2>/dev/null || true
}

# volume_device_by_serial VOLUME_ID [RETRIES] — wait for the by-id symlink to
# appear and echo the resolved /dev node. RETRIES defaults to 30 (1s steps):
# attachment is asynchronous (the API returns before the guest sees the
# device), and the rescan after a sysfs yank re-registers it asynchronously
# too.
volume_device_by_serial() {
    local volume_id="${1:-}"
    local retries="${2:-30}"
    local by_id
    by_id="$(volume_by_id_path "$volume_id")"
    local i=0
    while [ "$i" -lt "$retries" ]; do
        if [ -e "$by_id" ]; then
            readlink -f "$by_id"
            return 0
        fi
        i=$((i + 1))
        sleep 1
    done
    echo "volume_device_by_serial: ${by_id} did not appear within ${retries}s" >&2
    return 1
}

# fstab_upsert DEVICE MOUNT FS — add a UUID-based nofail fstab entry once.
# UUID= (not /dev/sdX) so a device-letter shuffle across rescan/reboot cannot
# mount the wrong volume. `nofail` + a short device timeout keep boot from
# hanging if a volume is detached for a failure scenario; a missing device
# simply does not mount, and sut-deploy's pre-flight fails loudly instead.
fstab_upsert() {
    local device="${1:-}" mount="${2:-}" fs="${3:-ext4}"
    local uuid
    uuid="$(blkid -o value -s UUID "$device" 2>/dev/null || true)"
    if [ -z "$uuid" ]; then
        echo "fstab_upsert: no UUID on ${device}" >&2
        return 1
    fi
    if awk -v m="$mount" '$2 == m { found = 1 } END { exit !found }' /etc/fstab 2>/dev/null; then
        return 0
    fi
    printf 'UUID=%s %s %s noatime,nofail,x-systemd.device-timeout=10 0 2\n' \
        "$uuid" "$mount" "$fs" >> /etc/fstab
}

# ensure_mount DEVICE MOUNT [FS] — idempotent format + mount + fstab.
#
#   * refuses to format a device that already carries a filesystem
#     ("never formats an already-marked volume"); an unexpected existing
#     filesystem is an error, not a format target;
#   * skips format when a signature is present, mounts with noatime;
#   * if MOUNT is already mounted, verifies the source is DEVICE;
#   * records the fstab entry so a VM reboot remounts it.
#
# Must run as root (mkfs/mount/fstab are root operations).
ensure_mount() {
    local device="${1:-}" mount="${2:-}" fs="${3:-ext4}"

    if [ "$(id -u)" != "0" ]; then
        echo "ensure_mount: must run as root (device=${device} mount=${mount})" >&2
        return 1
    fi
    if [ -z "$device" ] || [ ! -b "$device" ]; then
        echo "ensure_mount: not a block device: '${device}'" >&2
        return 1
    fi
    if [ -z "$mount" ]; then
        echo "ensure_mount: empty mount path" >&2
        return 1
    fi

    mkdir -p "$mount"

    local existing_fs
    existing_fs="$(device_fs_type "$device")"
    if [ -n "$existing_fs" ] && [ "$existing_fs" != "$fs" ]; then
        echo "ensure_mount: refusing to format ${device}: existing filesystem '${existing_fs}' != requested '${fs}'" >&2
        return 1
    fi

    if is_mountpoint "$mount"; then
        local source
        source="$(findmnt -rno SOURCE --target "$mount" 2>/dev/null || true)"
        if [ -n "$source" ] && [ "$(readlink -f "$source")" != "$(readlink -f "$device")" ]; then
            echo "ensure_mount: ${mount} is mounted from ${source}, not ${device}" >&2
            return 1
        fi
    else
        if [ -z "$existing_fs" ]; then
            # shellcheck disable=SC2154 # fs is validated by the caller
            mkfs."$fs" -q "$device" >&2
        fi
        mount -o noatime "$device" "$mount"
    fi

    fstab_upsert "$device" "$mount" "$fs"
}

# assert_volume_mount MOUNT DATA_DIR MIN_GB — fail unless MOUNT is a real,
# writable, block-device mount of at least MIN_GB that is disjoint from the
# node's local data_dir and not the root filesystem. This is the guard that
# makes a silent fallback onto the node's local disk impossible: a plain
# directory fails `is_mountpoint`, a bind mount of / fails the root-source
# check, and a missing/ro-mounted volume fails the write probe.
assert_volume_mount() {
    local mount="${1:-}" data_dir="${2:-}" min_gb="${3:-4}"

    if [ -z "$mount" ]; then
        echo "assert_volume_mount: empty mount path" >&2
        return 1
    fi
    if ! is_mountpoint "$mount"; then
        echo "assert_volume_mount: ${mount} is not a mountpoint (volume not attached/mounted?)" >&2
        return 1
    fi

    local source source_resolved root_source root_resolved
    source="$(findmnt -rno SOURCE --target "$mount" 2>/dev/null || true)"
    source_resolved="$(readlink -f "$source" 2>/dev/null || true)"
    if [ -z "$source_resolved" ] || [ ! -b "$source_resolved" ]; then
        echo "assert_volume_mount: ${mount} source '${source}' is not a block device" >&2
        return 1
    fi

    root_source="$(findmnt -rno SOURCE --target / 2>/dev/null || true)"
    root_resolved="$(readlink -f "$root_source" 2>/dev/null || true)"
    if [ "$source_resolved" = "$root_resolved" ]; then
        echo "assert_volume_mount: ${mount} is on the root filesystem (${source_resolved}) — refusing the local-disk fallback" >&2
        return 1
    fi

    if [ -n "$data_dir" ]; then
        case "$mount" in
            "$data_dir" | "$data_dir"/*)
                echo "assert_volume_mount: ${mount} is nested under data_dir (${data_dir}) — pool roots must be disjoint" >&2
                return 1
                ;;
        esac
        case "$data_dir" in
            "$mount" | "$mount"/*)
                echo "assert_volume_mount: data_dir (${data_dir}) is nested under ${mount} — pool roots must be disjoint" >&2
                return 1
                ;;
        esac
    fi

    if ! touch "${mount}/.oceanfs-write-probe" 2>/dev/null; then
        echo "assert_volume_mount: ${mount} is not writable" >&2
        return 1
    fi
    rm -f "${mount}/.oceanfs-write-probe"

    local size_gb
    size_gb="$(df -BG --output=size "$mount" 2>/dev/null | tail -n 1 | tr -dc '0-9')"
    if [ -z "$size_gb" ] || [ "$size_gb" -lt "$min_gb" ]; then
        echo "assert_volume_mount: ${mount} is ${size_gb:-?}G, expected at least ${min_gb}G (wrong volume mounted?)" >&2
        return 1
    fi
    return 0
}

# volume_remote_payload — emit the function bodies needed to run one of the
# remote-capable helpers inside `bash -s` on a node (no file copy needed).
volume_remote_payload() {
    local fn
    for fn in is_mountpoint volume_by_id_path device_fs_type fstab_upsert \
        ensure_mount assert_volume_mount volume_device_by_serial; do
        declare -f "$fn"
        echo
    done
}

# volume_run_remote SSH_TARGET FN [ARGS...] — run FN (with ARGS) on the
# target node over SSH, streaming the helper payload over stdin. stdout and
# the exit status pass through.
volume_run_remote() {
    local target="${1:-}"
    local fn="${2:-}"
    if [ -z "$target" ] || [ -z "$fn" ]; then
        echo "volume_run_remote: usage: volume_run_remote <target> <fn> [args...]" >&2
        return 1
    fi
    shift 2
    {
        echo 'set -euo pipefail'
        volume_remote_payload
        printf '%s "$@"\n' "$fn"
    } | ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=10 -o BatchMode=yes "$target" bash -s -- "$@"
}
