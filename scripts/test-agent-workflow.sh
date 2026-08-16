#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# test-agent-workflow.sh — End-to-end agent workflow validation (two-VM).
#
# Manual integration test for the whole load-test infrastructure pipeline,
# exactly as an agent would drive it via the vm-* skills:
#
#   1. PROVISION  scripts/vm-provision.sh --phase 2        (SUT + Harness, cx23)
#   2. DEPLOY     scripts/setup-harness.sh                 (build on Harness,
#                  deploy oceanfs+systemd+observability to SUT, verify health)
#   3. RUN        scripts/run-phase2.sh --harness --quick  (Phase 2 sustained
#                  load on the Harness VM targeting the SUT over the internal
#                  network; report fetched back to the laptop)
#   4. ASSERT     LoadReport JSON: result == "pass", manifest.mismatches == 0
#   5. TEARDOWN   scripts/vm-provision.sh --destroy        (both VMs)
#
# NOT a CI test: it provisions real Hetzner VMs and runs a real (quick-mode)
# load test. Run it before declaring the test infrastructure ready, or after
# changing any script in scripts/ that the vm-* skills depend on.
#
# Cost: two CX23 VMs for the workflow duration (TTL default 2h), plus the
# build time on the Harness. Auto-teardown is attempted on ANY failure
# unless WORKFLOW_KEEP_VMS=true.
#
# Usage:
#   ./scripts/test-agent-workflow.sh [OPTIONS]
#
# Options:
#   --phase N            Load test phase to run (default: 2; only 2 is
#                        supported by run-phase2.sh today).
#   --branch BRANCH      Branch to build on the Harness (default: main).
#   --seed N             Deterministic seed (default: 42).
#   --duration-secs N    Sustained-load duration (default: 300 = quick mode).
#   --name PREFIX        VM name prefix (default: oceanfs-wf-<YYYYmmddHHMMSS>).
#   --ttl HOURS          Auto-shutdown TTL for the VMs (default: 2).
#   --dry-run            Print the steps without provisioning anything.
#   -h, --help           Show this help.
#
# Environment:
#   HCLOUD_TOKEN         Hetzner API token (required).
#   WORKFLOW_KEEP_VMS    "true" keeps VMs on failure for inspection.
#   WORKFLOW_REPORT_DIR  Local report dir (default: /tmp/oceanfs-reports-wf).
#
# Exit code: 0 on full pipeline pass, 1 on any failure.
# ---------------------------------------------------------------------------
set -euo pipefail

# Load .hetzner/.env (HCLOUD_TOKEN), ensure ssh-agent + the Hetzner key,
# and set the default provisioning key. No-op without .hetzner/.
# shellcheck source=lib/env-hetzner.sh
_ENV_HETZNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/env-hetzner.sh"
[ -f "$_ENV_HETZNER" ] && . "$_ENV_HETZNER"
unset _ENV_HETZNER

PHASE="2"
BRANCH="main"
SEED="42"
DURATION_SECS="300"
NAME_PREFIX=""
TTL_HOURS="2"
DRY_RUN=false
KEEP_VMS="${WORKFLOW_KEEP_VMS:-false}"
REPORT_DIR="${WORKFLOW_REPORT_DIR:-/tmp/oceanfs-reports-wf}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVISION_FILE=""

log_info()  { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }
log_ok()    { echo "[OK]    $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --phase) PHASE="${2:-}"; shift 2 ;;
        --branch) BRANCH="${2:-}"; shift 2 ;;
        --seed) SEED="${2:-}"; shift 2 ;;
        --duration-secs) DURATION_SECS="${2:-}"; shift 2 ;;
        --name) NAME_PREFIX="${2:-}"; shift 2 ;;
        --ttl) TTL_HOURS="${2:-}"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

[ "$PHASE" = "2" ] || { log_error "Only --phase 2 is supported by the workflow today (run-phase2.sh implements Phase 2)."; exit 1; }
[ "$DRY_RUN" = false ] || { log_info "DRY-RUN — steps below would execute:"; }

STEP_START=0
declare -A STEP_TIMES
step_start() { STEP_START=$(date +%s); }
step_end() { STEP_TIMES[$1]=$(( $(date +%s) - STEP_START )); }

# Best-effort teardown on any failure (unless the user wants to keep the VMs).
fail() {
    log_error "WORKFLOW FAILED: $*"
    if [ "$KEEP_VMS" = "true" ]; then
        log_error "WORKFLOW_KEEP_VMS=true — leaving VMs in place:"
        [ -n "$PROVISION_FILE" ] && [ -f "$PROVISION_FILE" ] \
            && log_error "  record: $PROVISION_FILE" || true
    elif [ -n "$PROVISION_FILE" ] && [ -f "$PROVISION_FILE" ]; then
        local prefix
        # The record carries no top-level name — derive from the filename.
        prefix=$(basename "$PROVISION_FILE" | sed 's/^provision-//; s/\.json$//')
        if [ -n "$prefix" ]; then
            log_info "Best-effort teardown of '${prefix}'..."
            "${SCRIPT_DIR}/vm-provision.sh" --destroy "$prefix" || log_error "Teardown failed — destroy manually: hcloud server list"
            rm -f "$PROVISION_FILE"
        fi
    fi
    echo "{\"overall\": \"fail\", \"error\": \"$*\"}"
    exit 1
}

# ── Preflight ──────────────────────────────────────────────────────────────
if [ "$DRY_RUN" = false ]; then
    for tool in hcloud jq ssh scp; do
        command -v "$tool" >/dev/null || fail "missing required tool: $tool"
    done
    { [ -n "${HCLOUD_TOKEN:-}" ] || hcloud server list --output json >/dev/null 2>&1; } \
        || fail "HCLOUD_TOKEN not set and hcloud is not configured"
fi

NAME_PREFIX="${NAME_PREFIX:-oceanfs-wf-$(date +%Y%m%d%H%M%S)}"
mkdir -p "$REPORT_DIR"

log_info "test-agent-workflow: phase=$PHASE branch=$BRANCH seed=$SEED duration=${DURATION_SECS}s prefix=$NAME_PREFIX ttl=${TTL_HOURS}h"

# ── STEP 1: PROVISION ──────────────────────────────────────────────────────
log_info "STEP 1/5: provisioning two VMs (SUT + Harness) on Hetzner..."
step_start
if [ "$DRY_RUN" = false ]; then
    PROVISION_OUTPUT=$("${SCRIPT_DIR}/vm-provision.sh" \
        --phase "$PHASE" --branch "$BRANCH" --name "$NAME_PREFIX" --ttl "$TTL_HOURS") \
        || fail "vm-provision.sh exited non-zero"
    PROVISION_FILE=".hetzner/provision-${NAME_PREFIX}.json"
    [ -f "$PROVISION_FILE" ] || fail "provisioning record not written: $PROVISION_FILE"
    SUT_PUB=$(jq -r '.sut.public_ip // empty' "$PROVISION_FILE")
    SUT_INT=$(jq -r '.sut.internal_ip // empty' "$PROVISION_FILE")
    HARNESS_PUB=$(jq -r '.harness.public_ip // empty' "$PROVISION_FILE")
    { [ -n "$SUT_PUB" ] && [ -n "$SUT_INT" ] && [ -n "$HARNESS_PUB" ]; } \
        || fail "provisioning record missing IPs"
    log_ok "provisioned: sut=$SUT_PUB (internal $SUT_INT), harness=$HARNESS_PUB"
else
    log_info "[DRY-RUN] vm-provision.sh --phase $PHASE --branch $BRANCH --name $NAME_PREFIX --ttl $TTL_HOURS"
fi
step_end provision

# ── STEP 2: DEPLOY ─────────────────────────────────────────────────────────
log_info "STEP 2/5: building on the Harness and deploying to the SUT (setup-harness.sh)..."
step_start
if [ "$DRY_RUN" = false ]; then
    "${SCRIPT_DIR}/setup-harness.sh" --provision-file "$PROVISION_FILE" \
        || fail "setup-harness.sh failed (build/deploy/observability/health)"
    log_ok "deployed: oceanfs + systemd unit + observability on the SUT"
else
    log_info "[DRY-RUN] setup-harness.sh --provision-file $PROVISION_FILE"
fi
step_end deploy

# ── STEP 3: RUN ────────────────────────────────────────────────────────────
log_info "STEP 3/5: running Phase 2 quick-mode load test from the Harness VM..."
step_start
if [ "$DRY_RUN" = false ]; then
    # run-phase2.sh reads LOAD_TEST_DURATION_SECS for both modes; --quick
    # stays the mode, the env var overrides the duration.
    LOAD_TEST_DURATION_SECS="$DURATION_SECS" \
    "${SCRIPT_DIR}/run-phase2.sh" \
        --harness "root@${HARNESS_PUB}" \
        --quick \
        --sut "${SUT_INT}:9000" \
        --ssh "root@${SUT_INT}" \
        --service oceanfs \
        --seed "$SEED" \
        --report-dir "$REPORT_DIR" \
        || fail "run-phase2.sh exited non-zero (test assertions failed or harness error)"
    log_ok "phase 2 quick run completed"
else
    log_info "[DRY-RUN] LOAD_TEST_DURATION_SECS=$DURATION_SECS run-phase2.sh --harness root@${HARNESS_PUB:-<harness-ip>} --quick --sut ${SUT_INT:-<sut-ip>}:9000 --ssh root@${SUT_INT:-<sut-ip>} --seed $SEED"
fi
step_end run

# ── STEP 4: ASSERT REPORT ──────────────────────────────────────────────────
log_info "STEP 4/5: validating the LoadReport..."
step_start
if [ "$DRY_RUN" = false ]; then
    LATEST_REPORT=$(ls -t "${REPORT_DIR}"/2_load_sustained_*.json 2>/dev/null | head -1) \
        || LATEST_REPORT=""
    [ -n "$LATEST_REPORT" ] || fail "no LoadReport JSON in $REPORT_DIR"
    RESULT=$(jq -r '.result // empty' "$LATEST_REPORT")
    MISMATCHES=$(jq -r '.manifest.mismatches // -1' "$LATEST_REPORT")
    OBJECTS=$(jq -r '.manifest.objects_written // 0' "$LATEST_REPORT")
    log_ok "report: $LATEST_REPORT (result=$RESULT, objects=$OBJECTS, mismatches=$MISMATCHES)"
    [ "$RESULT" = "pass" ] || fail "report.result is '$RESULT', expected 'pass'"
    [ "$MISMATCHES" = "0" ] || fail "manifest.mismatches=$MISMATCHES, expected 0"
else
    log_info "[DRY-RUN] assert result==pass and manifest.mismatches==0 in $REPORT_DIR/2_load_sustained_*.json"
fi
step_end assert

# ── STEP 5: TEARDOWN ───────────────────────────────────────────────────────
log_info "STEP 5/5: tearing down both VMs..."
step_start
if [ "$DRY_RUN" = false ]; then
    "${SCRIPT_DIR}/vm-provision.sh" --destroy "$NAME_PREFIX" \
        || fail "vm-provision.sh --destroy $NAME_PREFIX failed"
    rm -f "$PROVISION_FILE"
    log_ok "VMs destroyed, record removed"
else
    log_info "[DRY-RUN] vm-provision.sh --destroy $NAME_PREFIX"
fi
step_end teardown

# ── SUMMARY ────────────────────────────────────────────────────────────────
SUMMARY=$(jq -n \
    --arg name "$NAME_PREFIX" \
    --arg phase "$PHASE" \
    --arg branch "$BRANCH" \
    --arg seed "$SEED" \
    --arg report "${LATEST_REPORT:-}" \
    --argjson provision "${STEP_TIMES[provision]:-0}" \
    --argjson deploy "${STEP_TIMES[deploy]:-0}" \
    --argjson run "${STEP_TIMES[run]:-0}" \
    --argjson assert "${STEP_TIMES[assert]:-0}" \
        --argjson teardown "${STEP_TIMES[teardown]:-0}" \
    '{
        overall: "pass",
        name: $name,
        phase: ($phase | tonumber),
        branch: $branch,
        seed: ($seed | tonumber),
        report: (if $report == "" then null else $report end),
        steps_secs: {
            provision: $provision,
            deploy: $deploy,
            run: $run,
            assert: $assert,
            teardown: $teardown
        }
    }')

echo "$SUMMARY"
log_ok "WORKFLOW PASS — the full agent pipeline (provision → deploy → run → assert → teardown) works end-to-end."
exit 0
