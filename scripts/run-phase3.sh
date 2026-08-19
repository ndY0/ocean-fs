#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run-phase3.sh — Run the Phase 3 cluster-churn test.
#
# Targets the already-running 3-5 node OceanFS fleet (deployed via
# sut-deploy.sh --cluster per ADR-0026) in remote-target mode, or spawns
# a local cluster when no --nodes is given (CI quick mode).
#
# Two modes:
#   --harness HOST   Run the payload ON the harness VM (the load must be
#                    generated there: the node firewall only accepts :9000
#                    from the internal network). SSHes to the harness,
#                    executes the local flow there, then fetches the
#                    LoadReport back to $REPORT_DIR.
#   (no --harness)   Run locally (on the harness itself, or in CI quick
#                    mode with a locally spawned cluster).
#
# Fleet topology (ADR-0026): one oceanfs node per SUT VM, all on the same
# ports (:9000/:9001); nodes differ by internal IP. Node 0 is the
# bootstrap; nodes 1..N-1 seed to node 0's gRPC address. TARGET_HOSTS is
# the comma-separated node:9000 list as seen from the harness; churn crash
# control maps node index i to the i-th entry of TARGET_HOST_SSH.
#
# Usage:
#   ./scripts/run-phase3.sh [--quick|--full] [OPTIONS]
#
# Options:
#   --quick            Quick mode: 120s cluster churn (default).
#   --full             Full mode: 300s cluster churn.
#   --harness HOST     Harness VM to run the payload on (user@host or an
#                      alias like oceanfs-harness).
#   --nodes IPLIST     Comma-separated internal IPs of the SUT fleet
#                      (e.g. 10.0.0.2,10.0.0.3,10.0.0.4). TARGET_HOSTS is
#                      derived as <ip>:9000 per node.
#   --ssh LIST         Comma-separated SSH targets for churn crash control,
#                      one per node (e.g.
#                      root@10.0.0.2,root@10.0.0.3,root@10.0.0.4).
#                      Required with --nodes; without it the crash-recovery
#                      phase of churn is skipped.
#   --service NAME     systemd unit name on every node (default: oceanfs).
#   --seed N           Deterministic seed (default: 42).
#   --report-dir DIR   Report output dir (on the harness in --harness
#                      mode, fetched back here afterwards; default:
#                      /tmp/oceanfs-reports — tmpfs per ADR-0019).
#   -h, --help         Show this help.
#
# Environment: all options can also be passed via LOAD_TEST_SEED /
# LOAD_TEST_DURATION_SECS / TARGET_HOSTS / TARGET_HOST_SSH /
# TARGET_SERVICE / LOAD_TEST_REPORT_DIR / LOAD_TEST_COMPRESSION /
# LOAD_TEST_COMPRESSIBLE.
#
# Examples:
#   ./scripts/run-phase3.sh --harness oceanfs-harness --quick --nodes 10.0.0.2,10.0.0.3,10.0.0.4 --ssh root@10.0.0.2,root@10.0.0.3,root@10.0.0.4
#   ./scripts/run-phase3.sh --harness oceanfs-harness --full --seed 7
#   ./scripts/run-phase3.sh --quick                      # local spawn
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
NODES=""
SSH_LIST=""
SERVICE="${TARGET_SERVICE:-oceanfs}"
SEED="${LOAD_TEST_SEED:-42}"
REPORT_DIR="${LOAD_TEST_REPORT_DIR:-/tmp/oceanfs-reports}"
COMPRESSION="${LOAD_TEST_COMPRESSION:-0}"
COMPRESSIBLE="${LOAD_TEST_COMPRESSIBLE:-0}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,52p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --quick) MODE="quick"; shift ;;
        --full) MODE="full"; shift ;;
        --harness) HARNESS="${2:-}"; shift 2 ;;
        --nodes) NODES="${2:-}"; shift 2 ;;
        --ssh) SSH_LIST="${2:-}"; shift 2 ;;
        --service) SERVICE="${2:-}"; shift 2 ;;
        --seed) SEED="${2:-}"; shift 2 ;;
        --report-dir) REPORT_DIR="${2:-}"; shift 2 ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

if [ "$MODE" = "full" ]; then
    DURATION="${LOAD_TEST_DURATION_SECS:-300}"
else
    DURATION="${LOAD_TEST_DURATION_SECS:-120}"
fi

# Derive TARGET_HOSTS (<ip>:9000 per node) and validate the fleet.
TARGET_HOSTS="${TARGET_HOSTS:-}"
if [ -n "$NODES" ]; then
    TARGET_HOSTS=""
    IFS=',' read -ra NODE_LIST <<< "$NODES"
    for ip in "${NODE_LIST[@]}"; do
        ip="$(echo "$ip" | tr -d '[:space:]')"
        [ -n "$ip" ] || continue
        [ -z "$TARGET_HOSTS" ] || TARGET_HOSTS="${TARGET_HOSTS},"
        TARGET_HOSTS="${TARGET_HOSTS}${ip}:9000"
    done
fi
if [ -n "$TARGET_HOSTS" ]; then
    n_targets=$(echo "$TARGET_HOSTS" | tr ',' '\n' | grep -c . || true)
    [ "$n_targets" -ge 3 ] || { log_error "Phase 3 needs at least 3 nodes (quorum semantics); got: $TARGET_HOSTS"; exit 1; }
    log_info "Fleet: $TARGET_HOSTS ($n_targets nodes)"
fi

# ---------------------------------------------------------------------------
# Harness mode: run the payload on the harness VM (load must originate on
# the internal network), then fetch the report back.
# ---------------------------------------------------------------------------
if [ -n "$HARNESS" ]; then
    log_info "Phase 3 $MODE mode on harness ${HARNESS} (nodes=${TARGET_HOSTS:-local}, seed=${SEED})..."

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
    # Forward LOAD_TEST_DURATION_SECS plus the compression switches.
    ssh $SSH_OPTS -o BatchMode=yes "$HARNESS" \
        "cd /root/ocean-fs && ${LOAD_TEST_DURATION_SECS:+LOAD_TEST_DURATION_SECS=$LOAD_TEST_DURATION_SECS }${LOAD_TEST_COMPRESSION:+LOAD_TEST_COMPRESSION=$LOAD_TEST_COMPRESSION }${LOAD_TEST_COMPRESSIBLE:+LOAD_TEST_COMPRESSIBLE=$LOAD_TEST_COMPRESSIBLE }./scripts/run-phase3.sh --${MODE} ${TARGET_HOSTS:+--nodes $TARGET_HOSTS} ${SSH_LIST:+--ssh $SSH_LIST} --service ${SERVICE} --seed ${SEED} --report-dir ${REPORT_DIR}"
    # NOTE: this is the top-level script body, not a function — `local`
    # would fail and silently lose the run's exit code.
    local_exit=$?

    # Push the load-test textfile into node 0's Prometheus textfile
    # collector (best-effort: only when observability is installed).
    if [ -n "$TARGET_HOSTS" ]; then
        node0_ip="${TARGET_HOSTS%%,*}"
        node0_ip="${node0_ip%%:*}"
        ssh $SSH_OPTS -o BatchMode=yes "$HARNESS" \
            "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${REPORT_DIR}/load_test.prom root@${node0_ip}:/var/lib/prometheus/textfile/ 2>/dev/null || true" \
            || true
    fi

    mkdir -p "$REPORT_DIR"
    scp $SSH_OPTS "${HARNESS}:${REPORT_DIR}/3_load_cluster_churn_*.json" "$REPORT_DIR/" 2>/dev/null \
        && log_info "Report fetched to ${REPORT_DIR}/" || log_info "No report fetched (check ${HARNESS}:${REPORT_DIR})."

    # Archive the just-finished run's metrics into the observability
    # backup (best-effort: the persistent laptop Prometheus must be
    # running).
    "${SCRIPT_DIR}/backup-observability.sh" --quiet >/dev/null 2>&1 \
        && log_info "Observability backup taken (scripts/backup-observability.sh)" \
        || log_info "Observability backup skipped (start the laptop stack: docker compose -f mcps/docker-compose.yml up -d prometheus)"

    exit $local_exit
fi

# ---------------------------------------------------------------------------
# Local flow (runs on the harness itself, or in CI with local spawn).
# ---------------------------------------------------------------------------
if [ -n "$TARGET_HOSTS" ]; then
    if [ -n "$SSH_LIST" ]; then
        n_ssh=$(echo "$SSH_LIST" | tr ',' '\n' | grep -c . || true)
        [ "$n_ssh" -eq "$n_targets" ] || { log_error "--ssh must have one target per node ($n_targets nodes, got $n_ssh)."; exit 1; }
    else
        log_warn "--ssh not set — churn will skip crash/recovery cycles (TARGET_HOST_SSH unset)."
    fi
    log_info "Phase 3 $MODE mode: remote fleet ${TARGET_HOSTS} (churn crash control via ${SSH_LIST:-none})"
else
    log_info "Phase 3 $MODE mode: local spawn (no TARGET_HOSTS)"
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

log_info "Running load_cluster_churn (${DURATION}s, seed ${SEED})..."
env \
    LOAD_TEST_SEED="$SEED" \
    LOAD_TEST_DURATION_SECS="$DURATION" \
    LOAD_TEST_REPORT_DIR="$REPORT_DIR" \
    LOAD_TEST_COMPRESSION="$COMPRESSION" \
    LOAD_TEST_COMPRESSIBLE="$COMPRESSIBLE" \
    TARGET_HOSTS="${TARGET_HOSTS:-}" \
    TARGET_HOST_SSH="${SSH_LIST:-}" \
    TARGET_SERVICE="$SERVICE" \
    cargo test -p e2e --release --test load_cluster_churn -- --test-threads=1

log_info "Report: ${REPORT_DIR}/3_load_cluster_churn_*.json"
