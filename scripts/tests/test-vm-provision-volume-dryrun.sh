#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# test-vm-provision-volume-dryrun.sh — Script-level tests for the volume-pools
# provisioning surface (fleet-degradation f1). These run WITHOUT cloud calls:
# every case uses --dry-run, which exercises argument parsing, the quota
# guard, the per-role volume plan, and the provisioning-record schema.
#
# Usage: ./scripts/tests/test-vm-provision-volume-dryrun.sh
# Exit: 0 = all pass, 1 = at least one failure.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVISION="${SCRIPT_DIR}/../vm-provision.sh"
DEPLOY="${SCRIPT_DIR}/../sut-deploy.sh"

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

jqtest() {
    local name="$1" json="$2" expr="$3"
    if printf '%s' "$json" | jq -e "$expr" >/dev/null 2>&1; then
        check "$name" 0
    else
        check "$name" 1
        printf '       json: %s\n' "$(printf '%s' "$json" | jq -c . 2>/dev/null || echo '<invalid JSON>')" >&2
    fi
}

# ── Phase 2: 5 volumes, one per role, record schema ────────────────────────
P2="$(LOAD_TEST_VOLUME_QUOTA_GB=1024 "$PROVISION" --phase 2 --name t2 --volume-pools --dry-run 2>/dev/null)"
jqtest "phase2: sut.volumes has 5 entries" "$P2" '.sut.volumes | length == 5'
jqtest "phase2: roles are data/wal/meta/hints/spare" "$P2" \
    '[.sut.volumes[].role] == ["data","wal","meta","hints","spare"]'
jqtest "phase2: mounts are /mnt/oceanfs-<role>" "$P2" \
    '[.sut.volumes[].mount] == ["/mnt/oceanfs-data","/mnt/oceanfs-wal","/mnt/oceanfs-meta","/mnt/oceanfs-hints","/mnt/oceanfs-spare"]'
jqtest "phase2: default sizes 120/20/20/10/60" "$P2" \
    '[.sut.volumes[].size_gb] == [120,20,20,10,60]'
jqtest "phase2: norm schema fields present" "$P2" \
    'all(.sut.volumes[]; has("role") and has("name") and has("id") and has("device") and has("mount") and has("size_gb"))'
jqtest "phase2: volume names are {node}-vol-{role}" "$P2" \
    '.sut.volumes | all(.name | startswith("t2-sut-vol-"))'

# ── Phase 3: fleet shape, 5 volumes per node ───────────────────────────────
P3="$(LOAD_TEST_VOLUME_QUOTA_GB=1024 "$PROVISION" --phase 3 --nodes 3 --name t3 --volume-pools --dry-run 2>/dev/null)"
jqtest "phase3: sut_nodes has 3 nodes" "$P3" '.sut_nodes | length == 3'
jqtest "phase3: every node has 5 volumes" "$P3" 'all(.sut_nodes[]; .volumes | length == 5)'
jqtest "phase3: node volume names are per-node" "$P3" \
    '.sut_nodes[1].volumes[0].name == "t3-sut-1-vol-data"'

# ── Quota guard ────────────────────────────────────────────────────────────
if LOAD_TEST_VOLUME_QUOTA_GB=100 "$PROVISION" --phase 2 --name t2q --volume-pools --dry-run >/dev/null 2>&1; then
    check "quota: over-cap request is refused" 1
else
    check "quota: over-cap request is refused" 0
fi
if LOAD_TEST_VOLUME_QUOTA_GB=1024 "$PROVISION" --phase 3 --nodes 5 --name t3q --volume-pools --dry-run >/dev/null 2>&1; then
    check "quota: 5-node fleet (1150 GB) is refused at 1024 GB cap" 1
else
    check "quota: 5-node fleet (1150 GB) is refused at 1024 GB cap" 0
fi
P2CUSTOM="$(LOAD_TEST_VOLUME_QUOTA_GB=200 "$PROVISION" --phase 2 --name t2c --volume-pools --dry-run \
    --volume-data-gb 10 --volume-wal-gb 20 --volume-meta-gb 20 --volume-hints-gb 10 --volume-spare-gb 60 2>/dev/null)"
jqtest "quota: 120 GB custom plan fits a 200 GB cap" "$P2CUSTOM" '[.sut.volumes[].size_gb] == [10,20,20,10,60]'
if LOAD_TEST_VOLUME_QUOTA_GB=abc "$PROVISION" --phase 2 --name t2n --volume-pools --dry-run >/dev/null 2>&1; then
    check "quota: non-numeric cap is refused (guard cannot be disabled)" 1
else
    check "quota: non-numeric cap is refused (guard cannot be disabled)" 0
fi
QUOTA_LOG="$(LOAD_TEST_VOLUME_QUOTA_GB=1024 "$PROVISION" --phase 2 --name t2cost --volume-pools --dry-run 2>&1 >/dev/null)"
case "$QUOTA_LOG" in
    *"230 GB"*"volume cost ~"*) check "quota: output includes volume GB and estimated cost" 0 ;;
    *) check "quota: output includes volume GB and estimated cost" 1 ;;
esac

# ── Local-disk mode stays volume-free ──────────────────────────────────────
P2LOCAL="$("$PROVISION" --phase 2 --name t2l --dry-run 2>/dev/null)"
jqtest "local mode: no volumes planned" "$P2LOCAL" '.sut.volumes | length == 0'

# ── sut-deploy.sh --pools-on-mounts dry-run ────────────────────────────────
TMP_BIN="$(mktemp)"
trap 'rm -f "$TMP_BIN"' EXIT
printf '#!/bin/sh\nexit 0\n' > "$TMP_BIN"
chmod +x "$TMP_BIN"
DEPLOY_OUT="$("$DEPLOY" --sut root@192.0.2.1 --pools-on-mounts --dry-run --binary "$TMP_BIN" 2>&1)"
case "$DEPLOY_OUT" in
    *"[DRY-RUN] Assert /mnt/oceanfs-data"*) check "deploy: mount pre-flight is planned" 0 ;;
    *) check "deploy: mount pre-flight is planned" 1 ;;
esac
case "$DEPLOY_OUT" in
    *"[DRY-RUN] Assert /mnt/oceanfs-hints"*) check "deploy: all four role mounts asserted" 0 ;;
    *) check "deploy: all four role mounts asserted" 1 ;;
esac
DEPLOY_LOCAL="$("$DEPLOY" --sut root@192.0.2.1 --dry-run --binary "$TMP_BIN" 2>&1)"
case "$DEPLOY_LOCAL" in
    *"[DRY-RUN] Assert /mnt/oceanfs"*) check "deploy: local mode has no mount assertions" 1 ;;
    *) check "deploy: local mode has no mount assertions" 0 ;;
esac

echo
echo "passed: ${PASS}, failed: ${FAIL}"
[ "$FAIL" -eq 0 ]
