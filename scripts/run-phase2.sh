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
# LOAD_TEST_REPORT_DIR / LOAD_TEST_COMPRESSION (1 = opt the load-test
# bucket into per-bucket zstd compression) / LOAD_TEST_COMPRESSIBLE
# (1 = compressible payloads so compression actually shrinks data).
#
# Examples:
#   ./scripts/run-phase2.sh --harness oceanfs-harness --quick --sut 10.0.0.2:9000 --ssh root@10.0.0.2
#   ./scripts/run-phase2.sh --harness oceanfs-harness --full --seed 7
#   ./scripts/run-phase2.sh --quick                      # local spawn
# ---------------------------------------------------------------------------
set -euo pipefail

# Load .hetzner/.env, ensure ssh-agent + the Hetzner key (no-op without
# .hetzner/, e.g. when this script runs on the Harness VM).
# shellcheck source=lib/env-hetzner.sh
_ENV_HETZNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/env-hetzner.sh"
[ -f "$_ENV_HETZNER" ] && . "$_ENV_HETZNER"
unset _ENV_HETZNER

MODE="quick"
HARNESS=""
SUT=""
SSH_TARGET=""
SERVICE="${TARGET_SERVICE:-oceanfs}"
SEED="${LOAD_TEST_SEED:-42}"
REPORT_DIR="${LOAD_TEST_REPORT_DIR:-/tmp/oceanfs-reports}"
COMPRESSION="${LOAD_TEST_COMPRESSION:-0}"
COMPRESSIBLE="${LOAD_TEST_COMPRESSIBLE:-0}"

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

    # Ensure the observe.sh tunnel so the persistent laptop Prometheus
    # (mcps/docker-compose.yml, service "prometheus") can federate this
    # run's metrics — the durable copy that survives VM teardown.
    # Best-effort: needs a provisioning record; a missing tunnel only
    # means the run is not archived to the laptop store.
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if ! "${SCRIPT_DIR}/observe.sh" >/dev/null 2>&1; then
        log_info "observe.sh tunnel not up — this run's metrics will not be archived to the laptop Prometheus (start it with: ./scripts/observe.sh)"
    else
        log_info "observe.sh tunnel up — run metrics will be federated to the persistent laptop Prometheus (localhost:9091)."
    fi

    # The remote invocation drops --harness and runs the local flow.
    # Forward LOAD_TEST_DURATION_SECS so a duration override (e.g. from
    # test-agent-workflow.sh --duration-secs) reaches the harness-side
    # run, plus the compression switches — without them the harness-side
    # script defaults to 0 and the load never opts the bucket into
    # per-bucket compression (the accel compress counters stay zero).
    ssh $SSH_OPTS -o BatchMode=yes "$HARNESS" \
        "cd /root/ocean-fs && ${LOAD_TEST_DURATION_SECS:+LOAD_TEST_DURATION_SECS=$LOAD_TEST_DURATION_SECS }${LOAD_TEST_COMPRESSION:+LOAD_TEST_COMPRESSION=$LOAD_TEST_COMPRESSION }${LOAD_TEST_COMPRESSIBLE:+LOAD_TEST_COMPRESSIBLE=$LOAD_TEST_COMPRESSIBLE }./scripts/run-phase2.sh --${MODE} ${SUT:+--sut $SUT} ${SSH_TARGET:+--ssh $SSH_TARGET} --service ${SERVICE} --seed ${SEED} --report-dir ${REPORT_DIR}"
    # NOTE: this is the top-level script body, not a function — `local`
    # would fail and silently lose the run's exit code.
    local_exit=$?

    # Push the load-test textfile into the SUT's Prometheus textfile
    # collector (best-effort: only when observability is installed).
    if [ -n "$SUT" ]; then
        sut_ip="${SUT%%:*}"
        ssh $SSH_OPTS -o BatchMode=yes "$HARNESS" \
            "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${REPORT_DIR}/load_test.prom root@${sut_ip}:/var/lib/prometheus/textfile/ 2>/dev/null || true" \
            || true
    fi

    mkdir -p "$REPORT_DIR"
    scp $SSH_OPTS "${HARNESS}:${REPORT_DIR}/2_load_sustained_*.json" "$REPORT_DIR/" 2>/dev/null \
        && log_info "Report fetched to ${REPORT_DIR}/" || log_info "No report fetched (check ${HARNESS}:${REPORT_DIR})."

    # Archive the just-finished run's metrics into the observability
    # backup (best-effort: the persistent laptop Prometheus must be
    # running). Guards against a later destructive volume command wiping
    # the archived run history.
    "${SCRIPT_DIR}/backup-observability.sh" --quiet >/dev/null 2>&1 \
        && log_info "Observability backup taken (scripts/backup-observability.sh)" \
        || log_info "Observability backup skipped (start the laptop stack: docker compose -f mcps/docker-compose.yml up -d prometheus)"

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
# The Harness VM installs Rust via rustup; non-interactive ssh shells do
# not source ~/.cargo/env, so make cargo available explicitly. No-op when
# cargo is already on PATH (laptop / CI).
if ! command -v cargo >/dev/null 2>&1 && [ -f /root/.cargo/env ]; then
    # shellcheck disable=SC1091
    . /root/.cargo/env
fi
cargo build --release -p e2e

log_info "Running load_sustained (${DURATION}s, seed ${SEED})..."
env \
    LOAD_TEST_SEED="$SEED" \
    LOAD_TEST_DURATION_SECS="$DURATION" \
    LOAD_TEST_REPORT_DIR="$REPORT_DIR" \
    LOAD_TEST_COMPRESSION="$COMPRESSION" \
    LOAD_TEST_COMPRESSIBLE="$COMPRESSIBLE" \
    TARGET_HOST="${SUT:-}" \
    TARGET_HOST_SSH="${SSH_TARGET:-}" \
    TARGET_SERVICE="$SERVICE" \
    cargo test -p e2e --release --test load_sustained -- --test-threads=1

log_info "Report: ${REPORT_DIR}/2_load_sustained_*.json"
