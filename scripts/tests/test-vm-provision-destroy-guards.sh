#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# test-vm-provision-destroy-guards.sh — Failure-injection tests for the
# `--destroy` volume guarantees (fleet-degradation f1).
#
# The contract: a destroy that cannot prove a volume is gone must NOT exit 0
# or print completion — volumes bill while they exist, detached included.
# These tests run the real destroy code against a mock `hcloud` on PATH
# (no cloud calls):
#
#   A. delete fails (inventory still lists the volume) -> exit 1, no
#      "Destroy complete", MANUAL CLEANUP REQUIRED
#   B. a recorded volume + delete fails + inventory unavailable (absence
#      unprovable) -> exit 1, "absence could not be confirmed"
#   C. no record and the inventory call fails -> exit 1, "Cannot enumerate"
#   D. control: prefix-scan finds volumes, deletes succeed -> exit 0 and
#      every volume removed from the mock state
#
# Usage: ./scripts/tests/test-vm-provision-destroy-guards.sh
# Exit: 0 = all pass, 1 = at least one failure.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVISION="${SCRIPT_DIR}/../vm-provision.sh"
# Scenario B uses a real (temporary) provisioning record so the destroy
# runs the recorded-id path instead of the prefix scan.
B_PREFIX="tstb-vol-guard-$$"
B_RECORD="${SCRIPT_DIR}/../../.hetzner/provision-${B_PREFIX}.json"

PASS=0
FAIL=0

check() {
    local name="$1" condition="$2"
    if [ "$condition" = "0" ]; then
        echo "ok   - ${name}"
        PASS=$((PASS + 1))
    else
        echo "FAIL - ${name}"
        FAIL=$((FAIL + 1))
    fi
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"; rm -f "$B_RECORD"' EXIT
mkdir -p "$TMP/bin"

# Mock hcloud: state is a file of "name id" lines; MOCK_FAIL is a
# space-separated list of operations to fail (list, describe, delete).
cat > "$TMP/bin/hcloud" <<'MOCK'
#!/usr/bin/env bash
set -uo pipefail
STATE="${MOCK_STATE:?MOCK_STATE required}"
FAILS=" ${MOCK_FAIL:-} "
fail() { case "$FAILS" in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

sub="$1 ${2:-}"
case "$sub" in
    "volume list")
        if fail list; then exit 1; fi
        printf '['
        first=1
        while read -r v_name v_id; do
            [ -n "$v_name" ] || continue
            [ "$first" = 1 ] || printf ','
            first=0
            printf '{"id":%s,"name":"%s","size":10,"server":null}' "$v_id" "$v_name"
        done < "$STATE"
        printf ']\n'
        ;;
    "volume describe")
        ref="${3:-}"
        if fail describe; then exit 1; fi
        found=1
        while read -r v_name v_id; do
            if [ "$v_name" = "$ref" ] || [ "$v_id" = "$ref" ]; then
                printf '{"id":%s,"name":"%s","size":10,"server":null}\n' "$v_id" "$v_name"
                found=0
            fi
        done < "$STATE"
        exit "$found"
        ;;
    "volume detach") exit 0 ;;
    "volume delete")
        ref="${3:-}"
        if fail delete; then exit 1; fi
        tmp="$STATE.tmp"
        : > "$tmp"
        while read -r v_name v_id; do
            if [ "$v_name" = "$ref" ] || [ "$v_id" = "$ref" ]; then continue; fi
            printf '%s %s\n' "$v_name" "$v_id" >> "$tmp"
        done < "$STATE"
        mv "$tmp" "$STATE"
        ;;
    "server list") printf '[]\n' ;;
    "server describe") exit 1 ;;
    "server delete") exit 0 ;;
    *) echo "mock hcloud: unhandled: $*" >&2; exit 1 ;;
esac
MOCK
chmod +x "$TMP/bin/hcloud"

# Run one destroy against a fresh mock state. Args: label expect_exit
# mock_fail prefix.
run_destroy() {
    local label="$1" expect_exit="$2" mock_fail="$3" prefix="$4"
    printf '%s-sut-vol-data 1001\n%s-sut-vol-wal 1002\n' "$prefix" "$prefix" > "$TMP/state-$label"
    set +e
    PATH="$TMP/bin:$PATH" MOCK_STATE="$TMP/state-$label" MOCK_FAIL="$mock_fail" \
        "$PROVISION" --destroy "$prefix" >"$TMP/out-$label" 2>"$TMP/err-$label"
    local rc=$?
    set -e
    if [ "$rc" -eq "$expect_exit" ]; then
        check "destroy ${label}: exit ${expect_exit}" 0
    else
        check "destroy ${label}: exit ${expect_exit} (got ${rc})" 1
        tail -3 "$TMP/err-$label" >&2
    fi
}

# A: delete fails; inventory still lists the volume -> must fail loudly.
run_destroy "delete-fails" 1 "delete describe" "tsta"
case "$(cat "$TMP/err-delete-fails")" in
    *"MANUAL CLEANUP REQUIRED"*) check "delete-fails: reports MANUAL CLEANUP" 0 ;;
    *) check "delete-fails: reports MANUAL CLEANUP" 1 ;;
esac
case "$(cat "$TMP/err-delete-fails")" in
    *"Destroy complete"*) check "delete-fails: does not claim completion" 1 ;;
    *) check "delete-fails: does not claim completion" 0 ;;
esac

# B: recorded volume + delete fails + inventory unavailable -> the absence
# of the volume is unprovable, so the destroy must fail loudly.
printf '%s-sut-vol-data 1001\n%s-sut-vol-wal 1002\n' "$B_PREFIX" "$B_PREFIX" > "$TMP/state-inventory-down"
jq -n --arg node "${B_PREFIX}-sut" \
    '{sut: {name: $node, volumes: [
        {role: "data", name: ($node + "-vol-data"), id: 1001, device: "/dev/sdz",
         device_id: "/dev/disk/by-id/scsi-0HC_Volume_1001", mount: "/mnt/oceanfs-data", size_gb: 10},
        {role: "wal", name: ($node + "-vol-wal"), id: 1002, device: "/dev/sdy",
         device_id: "/dev/disk/by-id/scsi-0HC_Volume_1002", mount: "/mnt/oceanfs-wal", size_gb: 10}
    ]}}' > "$B_RECORD"
set +e
PATH="$TMP/bin:$PATH" MOCK_STATE="$TMP/state-inventory-down" MOCK_FAIL="delete list" \
    "$PROVISION" --destroy "$B_PREFIX" >"$TMP/out-inventory-down" 2>"$TMP/err-inventory-down"
RC_INV=$?
set -e
if [ "$RC_INV" -eq 1 ]; then
    check "inventory-down: exit 1" 0
else
    check "inventory-down: exit 1 (got ${RC_INV})" 1
fi
case "$(cat "$TMP/err-inventory-down")" in
    *"absence could not be confirmed"*) check "inventory-down: unprovable absence reported" 0 ;;
    *) check "inventory-down: unprovable absence reported" 1 ;;
esac
rm -f "$B_RECORD"

# C: no record and inventory fails -> refuse to enumerate, exit 1.
printf '' > "$TMP/state-enum-down"
set +e
PATH="$TMP/bin:$PATH" MOCK_STATE="$TMP/state-enum-down" MOCK_FAIL="list" \
    "$PROVISION" --destroy "tstc" >"$TMP/out-enum-down" 2>"$TMP/err-enum-down"
RC_ENUM=$?
set -e
if [ "$RC_ENUM" -eq 1 ]; then
    check "enum-down: exit 1" 0
else
    check "enum-down: exit 1 (got ${RC_ENUM})" 1
fi
case "$(cat "$TMP/err-enum-down")" in
    *"Cannot enumerate volumes"*) check "enum-down: refuses to enumerate" 0 ;;
    *) check "enum-down: refuses to enumerate" 1 ;;
esac

# D: control — scan finds both volumes, deletes succeed, state is drained.
run_destroy "control" 0 "" "tstd"
case "$(cat "$TMP/err-control")" in
    *"Destroy complete"*) check "control: claims completion" 0 ;;
    *) check "control: claims completion" 1 ;;
esac
if [ -s "$TMP/state-control" ]; then
    check "control: mock state drained" 1
else
    check "control: mock state drained" 0
fi

echo
echo "passed: ${PASS}, failed: ${FAIL}"
[ "$FAIL" -eq 0 ]
