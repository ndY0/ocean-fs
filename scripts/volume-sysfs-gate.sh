#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# volume-sysfs-gate.sh — Validate sysfs device yank/replug on a real Hetzner
# Cloud Volume (fleet-degradation f1, blocking gate for f2/f3/f4).
#
# The epic's hard-failure injectors depend on the device disappearing and
# reappearing under the node's feet. Before any scenario relies on it, this
# gate proves the full cycle on ONE real, attached, ext4-mounted volume:
#
#   1. resolve the device by stable serial (by-id scsi-0HC_Volume_<id>);
#   2. echo 1 > /sys/block/<dev>/device/delete   (device disappears);
#   3. SCSI host rescan re-adds it — same serial/volume id;
#   4. the filesystem re-mounts and previously written data is readable;
#   5. the Hetzner API still reports the volume attached (the API was
#      never told; only the guest lost the device).
#
# A failing gate does NOT block on its own: it decides the fallback. If
# sysfs yank is unreliable, f2 must use `hcloud volume detach`/`attach`,
# which requires a scoped Hetzner token on the Harness VM (recorded as a
# Deviations decision, not silently implemented).
#
# Usage:
#   ./scripts/volume-sysfs-gate.sh --sut root@HOST --volume-id ID \
#       [--mount /mnt/oceanfs-data] [--out local-results/sysfs-gate.json]
#
# Exit: 0 = gate PASS, 1 = gate FAIL (artifact still written).
# ---------------------------------------------------------------------------
set -euo pipefail

# Load .hetzner/.env (HCLOUD_TOKEN) + ssh-agent/key setup, same as the
# other lifecycle scripts (no-op when run from a machine without .hetzner/).
# shellcheck source=lib/env-hetzner.sh
_ENV_HETZNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/env-hetzner.sh"
# shellcheck disable=SC1090
[ -f "$_ENV_HETZNER" ] && . "$_ENV_HETZNER"
unset _ENV_HETZNER

SUT=""
VOLUME_ID=""
MOUNT="/mnt/oceanfs-data"
OUT=""

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --sut) SUT="${2:-}"; shift 2 ;;
        --volume-id) VOLUME_ID="${2:-}"; shift 2 ;;
        --mount) MOUNT="${2:-}"; shift 2 ;;
        --out) OUT="${2:-}"; shift 2 ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 2 ;;
    esac
done

[ -n "$SUT" ] || { log_error "--sut TARGET is required (e.g. root@1.2.3.4)."; exit 2; }
[ -n "$VOLUME_ID" ] || { log_error "--volume-id is required (hcloud volume id)."; exit 2; }

BY_ID="/dev/disk/by-id/scsi-0HC_Volume_${VOLUME_ID}"

# ── Local pre-check: does the cloud still know the volume? ─────────────────
API_BEFORE="$(hcloud volume describe "$VOLUME_ID" --output json 2>/dev/null || true)"
if [ -z "$API_BEFORE" ]; then
    log_error "hcloud cannot describe volume id=${VOLUME_ID} (wrong id or unavailable API)."
    exit 2
fi
API_SERVER_BEFORE="$(echo "$API_BEFORE" | jq -r '.server // ""')"
log_info "Volume ${VOLUME_ID} (${BY_ID}) attached to server id ${API_SERVER_BEFORE:-none}; running gate on ${SUT} mount ${MOUNT}."

# ── Remote sequence. Never aborts on a failed step: every step is reported
#    as "STEP <name> <PASS|WARN|FAIL> <detail>" so the artifact records the
#    exact failure mode instead of an opaque SSH error. ────────────────────
REMOTE_OUT="$(
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=yes \
    "$SUT" bash -s -- "$VOLUME_ID" "$BY_ID" "$MOUNT" <<'GATE_SETUP' 2>&1
set -uo pipefail
VOLUME_ID="$1"; BY_ID="$2"; MOUNT="$3"

step() { printf 'STEP %s %s %s\n' "$1" "$2" "${3:-}"; }

# Step 1 — resolve the device by stable serial.
if [ ! -e "$BY_ID" ]; then
    step resolve FAIL "stable device path ${BY_ID} not present"
    exit 0
fi
DEV="$(readlink -f "$BY_ID")"
BASE="$(basename "$DEV")"
step resolve PASS "${DEV} (by-id=${BY_ID##*/})"

# The yank mechanism is the sysfs SCSI delete attribute; virtio-blk devices
# have none, which is itself a gate finding.
if [ ! -w "/sys/block/${BASE}/device/delete" ]; then
    step delete_attribute FAIL "/sys/block/${BASE}/device/delete missing or not writable"
    exit 0
fi
step delete_attribute PASS "/sys/block/${BASE}/device/delete"

# Step 2 — the filesystem is mounted from this device, and carries a marker.
if ! mountpoint -q "$MOUNT"; then
    step mount_before FAIL "${MOUNT} is not a mountpoint"
    exit 0
fi
SRC="$(findmnt -rno SOURCE --target "$MOUNT")"
step mount_before PASS "${MOUNT} <- ${SRC}"
MARKER="sysfs-gate-$(date -u +%Y%m%dT%H%M%SZ)"
printf '%s' "$MARKER" > "${MOUNT}/.sysfs-gate-marker"
sync
step marker_write PASS "$MARKER"

# Step 3 — yank.
if printf '1' > "/sys/block/${BASE}/device/delete" 2>/dev/null; then
    step yank PASS "deleted ${BASE}"
else
    step yank FAIL "write to /sys/block/${BASE}/device/delete failed"
    exit 0
fi
sleep 2
if [ -e "$DEV" ] || [ -e "$BY_ID" ]; then
    step yank_confirm WARN "device still visible 2s after delete"
else
    step yank_confirm PASS "${BASE} and ${BY_ID} gone"
fi

# Step 4 — rescan all SCSI hosts and wait for the same volume to reappear.
for h in /sys/class/scsi_host/host*/scan; do
    [ -w "$h" ] && printf -- '- - -' > "$h" 2>/dev/null || true
done
NEW=""
for _ in $(seq 1 30); do
    if [ -e "$BY_ID" ]; then
        NEW="$(readlink -f "$BY_ID")"
        break
    fi
    sleep 1
done
if [ -z "$NEW" ]; then
    step rescan FAIL "device did not reappear within 30s"
    exit 0
fi
step rescan PASS "${NEW}"

# Identity: the reappeared device must still be the same volume id.
NEW_SERIAL="$(udevadm info --query=property --name="$NEW" 2>/dev/null | sed -n 's/^ID_SERIAL=//p' || true)"
case "$NEW_SERIAL" in
    *"$VOLUME_ID"*) step serial PASS "ID_SERIAL=${NEW_SERIAL}" ;;
    *) step serial WARN "ID_SERIAL='${NEW_SERIAL:-unknown}' does not contain volume id ${VOLUME_ID}" ;;
esac

# Step 5 — clear the stale mount and re-mount the returned device; the
# marker written before the yank must still be readable (the volume is
# network storage; no I/O happened during the gap).
umount -l "$MOUNT" 2>/dev/null || umount -f "$MOUNT" 2>/dev/null || umount "$MOUNT" 2>/dev/null || true
if ! mount -o noatime "$BY_ID" "$MOUNT" 2>/dev/null; then
    step remount FAIL "mount ${BY_ID} ${MOUNT} failed"
    exit 0
fi
step remount PASS "${MOUNT} remounted from ${BY_ID}"

if [ -f "${MOUNT}/.sysfs-gate-marker" ]; then
    READ_BACK="$(cat "${MOUNT}/.sysfs-gate-marker")"
    if [ "$READ_BACK" = "$MARKER" ]; then
        step marker_read PASS "$READ_BACK"
    else
        step marker_read FAIL "marker changed: '${READ_BACK}' != '${MARKER}'"
    fi
    rm -f "${MOUNT}/.sysfs-gate-marker"
else
    step marker_read FAIL "marker missing after remount"
fi
GATE_SETUP
)"

# ── Local post-check: the API still sees the volume attached. ──────────────
API_AFTER="$(hcloud volume describe "$VOLUME_ID" --output json 2>/dev/null || true)"
API_SERVER_AFTER="$(echo "$API_AFTER" | jq -r '.server // ""' 2>/dev/null || echo "")"

# ── Build the artifact. ────────────────────────────────────────────────────
STEPS_JSON="$(printf '%s\n' "$REMOTE_OUT" | sed -n 's/^STEP //p' | jq -R -s '
    split("\n")
    | map(select(length > 0) | split(" ") | { step: .[0], status: .[1], detail: (.[2:] | join(" ")) })
')"

API_STEP_STATUS="PASS"
API_STEP_DETAIL="volume ${VOLUME_ID} attached to server ${API_SERVER_AFTER}"
if [ -z "$API_SERVER_AFTER" ]; then
    API_STEP_STATUS="FAIL"
    API_STEP_DETAIL="volume ${VOLUME_ID} no longer reports a server after the guest-side yank"
fi
STEPS_JSON="$(echo "$STEPS_JSON" | jq --arg s "$API_STEP_STATUS" --arg d "$API_STEP_DETAIL" '. + [{step: "api_attached", status: $s, detail: $d}]')"

# Counts are derived AFTER the API step is appended so they always match
# the step list.
FAIL_COUNT="$(echo "$STEPS_JSON" | jq '[.[] | select(.status == "FAIL")] | length')"
PASS_COUNT="$(echo "$STEPS_JSON" | jq '[.[] | select(.status == "PASS")] | length')"

RESULT="pass"
[ "$FAIL_COUNT" -eq 0 ] || RESULT="fail"

ARTIFACT="$(jq -n \
    --arg gate "sysfs-yank-replug" \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg sut "$SUT" \
    --argjson volume_id "$VOLUME_ID" \
    --arg by_id "$BY_ID" \
    --arg mount "$MOUNT" \
    --arg result "$RESULT" \
    --argjson steps "$STEPS_JSON" \
    --argjson pass_count "$PASS_COUNT" \
    --argjson fail_count "$FAIL_COUNT" \
    --arg server_before "$API_SERVER_BEFORE" \
    --arg server_after "$API_SERVER_AFTER" \
    '{
        gate: $gate, timestamp: $ts, sut: $sut,
        volume_id: $volume_id, by_id: $by_id, mount: $mount,
        server_before: $server_before, server_after: $server_after,
        result: $result, pass_count: $pass_count, fail_count: $fail_count,
        steps: $steps
    }')"

if [ -n "$OUT" ]; then
    mkdir -p "$(dirname "$OUT")"
    printf '%s\n' "$ARTIFACT" > "$OUT"
    log_info "Gate artifact written to ${OUT}"
fi
printf '%s\n' "$ARTIFACT"

if [ "$RESULT" = "pass" ]; then
    log_info "sysfs yank/replug gate PASS — f2/f3/f4 may rely on the sysfs mechanism."
    exit 0
fi
log_error "sysfs yank/replug gate FAIL — record the fallback decision (hcloud detach/attach + scoped token on the Harness) before f2."
exit 1
