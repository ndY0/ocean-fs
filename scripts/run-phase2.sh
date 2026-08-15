#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run-phase2.sh — Run the Phase 2 sustained-load test from the Harness VM.
#
# Targets an already-running OceanFS SUT (deployed via sut-deploy.sh) in
# remote-target mode (ADR-0019 two-VM topology) or spawns a local node
# when no --sut is given. Builds the release e2e harness, runs
# load_sustained, and reports the LoadReport path.
#
# Usage:
#   ./scripts/run-phase2.sh [--quick|--full] [OPTIONS]
#
# Options:
#   --quick            Quick mode: 300s sustained load (default).
#   --full             Full mode: 3600s sustained load.
#   --sut HOST:PORT    Remote SUT endpoint (e.g. 10.0.0.5:9000). When
#                      unset, the test spawns a local node (CI quick mode).
#   --ssh TARGET       SSH target for crash control (e.g. root@10.0.0.5 or
#                      an alias like oceanfs-sut). Required with --sut;
#                      without it the crash-recovery phase is skipped.
#   --service NAME     systemd unit name on the SUT (default: oceanfs).
#   --seed N           Deterministic seed (default: 42).
#   --report-dir DIR   Report output dir on the harness (default:
#                      /tmp/oceanfs-reports — tmpfs per ADR-0019).
#   -h, --help         Show this help.
#
# Environment: all options can also be passed via LOAD_TEST_SEED /
# LOAD_TEST_DURATION_SECS / TARGET_HOST / TARGET_HOST_SSH / TARGET_SERVICE /
# LOAD_TEST_REPORT_DIR.
#
# Examples:
#   ./scripts/run-phase2.sh --quick --sut 10.0.0.5:9000 --ssh oceanfs-sut
#   ./scripts/run-phase2.sh --full --sut 10.0.0.5:9000 --ssh oceanfs-sut --seed 7
#   ./scripts/run-phase2.sh --quick                      # local spawn
# ---------------------------------------------------------------------------
set -euo pipefail

MODE="quick"
SUT=""
SSH_TARGET=""
SERVICE="${TARGET_SERVICE:-oceanfs}"
SEED="${LOAD_TEST_SEED:-42}"
REPORT_DIR="${LOAD_TEST_REPORT_DIR:-/tmp/oceanfs-reports}"

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --quick) MODE="quick"; shift ;;
        --full) MODE="full"; shift ;;
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
