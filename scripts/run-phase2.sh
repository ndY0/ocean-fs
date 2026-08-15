#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run-phase2.sh — Run the Phase 2 sustained-load test.
#
# Two modes:
#   --harness HOST   Run the payload ON the harness VM (the load must be
#                    generated there: the SUT firewall only accepts :9000
#                    from the internal network). SSHes to the harness,
#                    executes the local-spawn flow there, then fetches the
#                    LoadReport back to $REPORT_DIR.
#   (no --harness)   Run locally (on the harness itself, or in CI quick
#                    mode with a locally spawned node).
#
# Targets an already-running OceanFS SUT (deployed via sut-deploy.sh /
# setup-harness.sh) in remote-target mode (ADR-0019 two-VM topology), or
# spawns a local node when no --sut is given.
#
# Usage:
#   ./scripts/run-phase2.sh [--quick|--full] [OPTIONS]
#
# Options:
#   --quick            Quick mode: 300s sustained load (default).
#   --full             Full mode: 3600s sustained load.
#   --harness HOST     Harness VM to run the payload on (user@host or an
#                      alias like oceanfs-harness).
#   --sut HOST:PORT    Remote SUT endpoint as seen from the harness
#                      (e.g. 10.0.0.2:9000). When unset, the test spawns
#                      a local node (CI quick mode).
#   --ssh TARGET       SSH target for crash control from the harness
#                      (e.g. root@10.0.0.2). Required with --sut; without
#                      it the crash-recovery phase is skipped.
#   --service NAME     systemd unit name on the SUT (default: oceanfs).
#   --seed N           Deterministic seed (default: 42).
#   --report-dir DIR   Report output dir (on the harness in --harness
#                      mode, fetched back here afterwards; default:
#                      /tmp/oceanfs-reports — tmpfs per ADR-0019).
#   -h, --help         Show this help.
#
# Environment: all options can also be passed via LOAD_TEST_SEED /
# LOAD_TEST_DURATION_SECS / TARGET_HOST / TARGET_HOST_SSH / TARGET_SERVICE /
# LOAD_TEST_REPORT_DIR.
#
# Examples:
#   ./scripts/run-phase2.sh --harness oceanfs-harness --quick --sut 10.0.0.2:9000 --ssh root@10.0.0.2
#   ./scripts/run-phase2.sh --harness oceanfs-harness --full --seed 7
#   ./scripts/run-phase2.sh --quick                      # local spawn
# ---------------------------------------------------------------------------
set -euo pipefail

MODE="quick"
HARNESS=""
SUT=""
SSH_TARGET=""
SERVICE="${TARGET_SERVICE:-oceanfs}"
SEED="${LOAD_TEST_SEED:-42}"
REPORT_DIR="${LOAD_TEST_REPORT_DIR:-/tmp/oceanfs-reports}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --quick) MODE="quick"; shift ;;
        --full) MODE="full"; shift ;;
        --harness) HARNESS="${2:-}"; shift 2 ;;
        --sut) SUT="${2:-}"; shift 2 ;;
        --ssh) SSH_TARGET="${2:-}"; shift 2 ;;
        --service) SERVICE="${2:-}"; shift 2 ;;
        --seed) SEED="${2:-}"; shift 2 ;;
        --report-dir) REPORT_DIR="${2:-}"; shift 2 ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

if [ "$MODE" = "full" ]; then
    DURATION="${LOAD_TEST_DURATION_SECS:-3600}"
else
    DURATION="${LOAD_TEST_DURATION_SECS:-300}"
fi

# ---------------------------------------------------------------------------
# Harness mode: run the payload on the harness VM (load must originate on
# the internal network), then fetch the report back.
# ---------------------------------------------------------------------------
if [ -n "$HARNESS" ]; then
    log_info "Phase 2 $MODE mode on harness ${HARNESS} (sut=${SUT:-local}, seed=${SEED})..."

    # The remote invocation drops --harness and runs the local flow.
    ssh $SSH_OPTS -o BatchMode=yes "$HARNESS" \
        "cd /root/ocean-fs && ./scripts/run-phase2.sh --${MODE} ${SUT:+--sut $SUT} ${SSH_TARGET:+--ssh $SSH_TARGET} --service ${SERVICE} --seed ${SEED} --report-dir ${REPORT_DIR}"
    local_exit=$?

    # Push the load-test textfile into the SUT's Prometheus textfile
    # collector (best-effort: only when observability is installed).
    if [ -n "$SUT" ]; then
        local sut_ip="${SUT%%:*}"
        ssh $SSH_OPTS -o BatchMode=yes "$HARNESS" \
            "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${REPORT_DIR}/load_test.prom root@${sut_ip}:/var/lib/prometheus/textfile/ 2>/dev/null || true" \
            || true
    fi

    mkdir -p "$REPORT_DIR"
    scp $SSH_OPTS "${HARNESS}:${REPORT_DIR}/2_load_sustained_*.json" "$REPORT_DIR/" 2>/dev/null \
        && log_info "Report fetched to ${REPORT_DIR}/" || log_info "No report fetched (check ${HARNESS}:${REPORT_DIR})."
    exit $local_exit
fi

# ---------------------------------------------------------------------------
# Local flow (runs on the harness itself, or in CI with local spawn).
# ---------------------------------------------------------------------------
if [ -n "$SUT" ]; then
    [ -n "$SSH_TARGET" ] || log_error "--ssh is required with --sut (crash control needs it)."
    log_info "Phase 2 $MODE mode: remote SUT ${SUT} (crash control via ${SSH_TARGET})"
else
    log_info "Phase 2 $MODE mode: local spawn (no TARGET_HOST)"
fi

log_info "Building release e2e harness..."
cargo build --release -p e2e

log_info "Running load_sustained (${DURATION}s, seed ${SEED})..."
env \
    LOAD_TEST_SEED="$SEED" \
    LOAD_TEST_DURATION_SECS="$DURATION" \
    LOAD_TEST_REPORT_DIR="$REPORT_DIR" \
    TARGET_HOST="${SUT:-}" \
    TARGET_HOST_SSH="${SSH_TARGET:-}" \
    TARGET_SERVICE="$SERVICE" \
    cargo test -p e2e --release --test load_sustained -- --test-threads=1

log_info "Report: ${REPORT_DIR}/2_load_sustained_*.json"
