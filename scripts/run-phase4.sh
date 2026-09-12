#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run-phase4.sh — Run the Phase 4 degraded-mode test on the ADR-0026 fleet.
#
# Targets the already-running N-node OceanFS fleet (deployed via
# sut-deploy.sh --cluster) in remote-target mode, or spawns a local 3-node
# cluster when no fleet is given (dev/CI quick mode, no volume injectors).
#
# Phase 4 runs the four degraded-mode scenarios under load (load_degraded):
#   1. mid-write SIGKILL of one node's oceanfs unit (over SSH),
#   2. tc netem +500ms on the node's internal interface,
#   3. data-volume fill (real Hetzner volume, 95%),
#   4. remote segment corruption + POST /admin/scrub + read-back heal.
#
# Fleet injection is SSH black-box (f2 injectors) and needs:
#   - the f1 provisioning record (volumes per node + internal_ip), and
#   - per-node SSH targets (--ssh or the record's internal_ip).
# The record is copied to the harness and passed as LOAD_TEST_RECORD_FILE
# (or use LOAD_TEST_VOLUMES_JSON inline for gate/debug runs).
#
# Two modes:
#   --harness HOST   Run the payload ON the harness VM (the load must be
#                    generated there: the node firewall only accepts :9000
#                    from the internal network). SSHes to the harness,
#                    copies the record, executes the local flow there, then
#                    fetches the LoadReport back to $REPORT_DIR.
#   (no --harness)   Run locally (on the harness itself, or in CI quick mode
#                    with a locally spawned cluster).
#
# Usage:
#   ./scripts/run-phase4.sh [--quick|--full] [OPTIONS]
#
# Options:
#   --quick            Quick mode: 120s background load (default).
#   --full             Full mode: 300s background load.
#   --harness HOST     Harness VM to run the payload on (user@host or an
#                      alias like oceanfs-harness).
#   --nodes IPLIST     Comma-separated internal IPs of the SUT fleet
#                      (e.g. 10.0.0.2,10.0.0.3,10.0.0.4). TARGET_HOSTS is
#                      derived as <ip>:9000 per node.
#   --ssh LIST         Comma-separated SSH targets for fault injection, one
#                      per node (e.g. root@10.0.0.2,root@10.0.0.3,...).
#                      Required for a full run; without it the test fails
#                      fast (no silent skip of impossible scenarios).
#   --service NAME     systemd unit name on every node (default: oceanfs).
#   --seed N           Deterministic seed (default: 42).
#   --report-dir DIR   Report output dir (on the harness in --harness
#                      mode, fetched back here afterwards; default:
#                      /tmp/oceanfs-reports — tmpfs per ADR-0019).
#   --record PATH      f1 provisioning record (volumes/mounts/internal_ip).
#                      Copied to the harness in --harness mode. Also
#                      accepted as --report PATH (the f3 doc's name).
#                      Env: LOAD_TEST_RECORD_FILE. Without it, volume
#                      injectors cannot run and a full run fails fast
#                      (unless LOAD_TEST_VOLUMES_JSON is set inline).
#   --no-injections    Control run: background load + manifest verification
#                      only, zero injections (no record/SSH required).
#   --allow-local-fallback
#                      In --harness mode, allow a local 3-node spawn when
#                      no fleet is given (NOT recommended; debugging only).
#   -h, --help         Show this help.
#
# Environment: all options can also be passed via LOAD_TEST_SEED /
# LOAD_TEST_DURATION_SECS / TARGET_HOSTS / TARGET_HOST_SSH /
# TARGET_SERVICE / LOAD_TEST_REPORT_DIR / LOAD_TEST_RECORD_FILE /
# LOAD_TEST_VOLUMES_JSON / LOAD_TEST_NO_INJECTIONS / LOAD_TEST_LATENCY_IFACE /
# LOAD_TEST_CONCURRENCY.
#
# Examples:
#   ./scripts/run-phase4.sh --harness oceanfs-harness --full \
#     --nodes 10.0.0.2,10.0.0.3,10.0.0.4 \
#     --ssh root@10.0.0.2,root@10.0.0.3,root@10.0.0.4 \
#     --record .hetzner/provision-oceanfs-loadtest-4.json
#   ./scripts/run-phase4.sh --harness oceanfs-harness --quick --no-injections
#   ./scripts/run-phase4.sh --quick                      # local spawn smoke
# ---------------------------------------------------------------------------
set -euo pipefail

# Load .hetzner/.env, ensure ssh-agent + the Hetzner key (no-op without
# .hetzner/, e.g. when this script runs on the Harness VM).
_ENV_HETZNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/env-hetzner.sh"
if [ -f "$_ENV_HETZNER" ]; then
    # shellcheck source=/dev/null  # dynamic path: guarded by the [ -f ] above
    . "$_ENV_HETZNER"
fi
unset _ENV_HETZNER

MODE="quick"
HARNESS=""
NODES=""
SSH_LIST=""
SERVICE="${TARGET_SERVICE:-oceanfs}"
SEED="${LOAD_TEST_SEED:-42}"
REPORT_DIR="${LOAD_TEST_REPORT_DIR:-/tmp/oceanfs-reports}"
RECORD="${LOAD_TEST_RECORD_FILE:-}"
NO_INJECTIONS="${LOAD_TEST_NO_INJECTIONS:-0}"
ALLOW_LOCAL_FALLBACK=0

# The harness-side record path (fixed, so the remote env assignment is safe).
HARNESS_RECORD="/root/oceanfs-phase4-record.json"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10)

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,76p' "$0" | sed 's/^# \{0,1\}//'
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
        --record|--report) RECORD="${2:-}"; shift 2 ;;
        --no-injections) NO_INJECTIONS=1; shift ;;
        --allow-local-fallback) ALLOW_LOCAL_FALLBACK=1; shift ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

if [ "$MODE" = "full" ]; then
    DURATION="${LOAD_TEST_DURATION_SECS:-300}"
else
    DURATION="${LOAD_TEST_DURATION_SECS:-120}"
fi

case "$NO_INJECTIONS" in 1|true|yes) NO_INJECTIONS=1 ;; *) NO_INJECTIONS=0 ;; esac

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

n_targets=0
if [ -n "$TARGET_HOSTS" ]; then
    n_targets=$(echo "$TARGET_HOSTS" | tr ',' '\n' | grep -c . || true)
    [ "$n_targets" -ge 3 ] || { log_error "Phase 4 needs at least 3 nodes (quorum semantics); got: $TARGET_HOSTS"; exit 1; }
    log_info "Fleet: $TARGET_HOSTS ($n_targets nodes)"
fi

if [ -n "$TARGET_HOSTS" ] && [ -n "$SSH_LIST" ]; then
    n_ssh=$(echo "$SSH_LIST" | tr ',' '\n' | grep -c . || true)
    [ "$n_ssh" -eq "$n_targets" ] || { log_error "--ssh must have one target per node ($n_targets nodes, got $n_ssh)."; exit 1; }
fi

# ---------------------------------------------------------------------------
# Fail fast on missing injection prerequisites (record / SSH).
#
# A full run needs the f1 record (volumes) and per-node SSH (kill/latency);
# the test itself fails the run when they are missing, but catching it here
# avoids burning a build + load window on an impossible run.
# ---------------------------------------------------------------------------
has_inline_volumes=0
[ -n "${LOAD_TEST_VOLUMES_JSON:-}" ] && has_inline_volumes=1

if [ "$NO_INJECTIONS" -eq 0 ]; then
    if [ -n "$TARGET_HOSTS" ]; then
        if [ "$has_inline_volumes" -eq 0 ] && [ -z "$RECORD" ]; then
            log_error "Phase 4 fleet mode needs the provisioning record (--record .hetzner/provision-*.json) or LOAD_TEST_VOLUMES_JSON."
            log_error "Use --no-injections for a control run (background load only)."
            exit 1
        fi
        if [ -z "$SSH_LIST" ] && [ -z "${TARGET_HOST_SSH:-}" ] && [ -z "$RECORD" ] && [ "$has_inline_volumes" -eq 0 ]; then
            log_error "Phase 4 fault injection needs per-node SSH (--ssh or TARGET_HOST_SSH)."
            exit 1
        fi
        if [ -n "$RECORD" ] && [ "$has_inline_volumes" -eq 0 ]; then
            if [ ! -f "$RECORD" ]; then
                log_error "Provisioning record not found: $RECORD"
                exit 1
            fi
            if command -v jq >/dev/null 2>&1; then
                volume_count=$(jq -r '((.sut_nodes // []) + (if .sut then [.sut] else [] end)) | map(.volumes // [] | length) | add // 0' "$RECORD" 2>/dev/null || echo 0)
                if [ "${volume_count:-0}" -eq 0 ]; then
                    log_error "Record $RECORD has no volume data — was the fleet provisioned with --volume-pools?"
                    log_error "Re-provision with: ./scripts/vm-provision.sh --phase 4 --nodes N --volume-pools"
                    exit 1
                fi
                log_info "Record volumes: $volume_count across the fleet."
            else
                log_info "jq not found — skipping the record volume pre-check (the test fails fast itself)."
            fi
        fi
    fi
fi

# ---------------------------------------------------------------------------
# Harness mode: run the payload on the harness VM (load must originate on
# the internal network), then fetch the report back.
# ---------------------------------------------------------------------------
if [ -n "$HARNESS" ]; then
    if [ -z "$TARGET_HOSTS" ] && [ "$ALLOW_LOCAL_FALLBACK" -ne 1 ]; then
        log_error "Phase 4 harness mode requires a fleet (--nodes/TARGET_HOSTS);"
        log_error "pass --allow-local-fallback to explicitly spawn locally on the harness instead."
        exit 1
    fi
    log_info "Phase 4 $MODE mode on harness ${HARNESS} (nodes=${TARGET_HOSTS:-local}, seed=${SEED}, injections=$([ "$NO_INJECTIONS" -eq 1 ] && echo no || echo yes))..."

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if ! "${SCRIPT_DIR}/observe.sh" >/dev/null 2>&1; then
        log_info "observe.sh tunnel not up — this run's metrics will not be archived to the laptop Prometheus (start it with: ./scripts/observe.sh)"
    else
        log_info "observe.sh tunnel up — run metrics will be federated to the persistent laptop Prometheus (localhost:9091)."
    fi

    # Copy the provisioning record to the harness so the injectors can
    # resolve volumes/mounts there (the harness cannot reach the laptop).
    REMOTE_ENV_PREFIX=""
    RECORD_ARG=""
    if [ -n "$RECORD" ]; then
        scp "${SSH_OPTS[@]}" "$RECORD" "${HARNESS}:${HARNESS_RECORD}" >/dev/null
        REMOTE_ENV_PREFIX="LOAD_TEST_RECORD_FILE=${HARNESS_RECORD} "
        RECORD_ARG="--record ${HARNESS_RECORD}"
        log_info "Provisioning record copied to ${HARNESS}:${HARNESS_RECORD}"
    fi

    if [ -n "$NODES" ]; then
        NODES_FWD="$NODES"
    elif [ -n "$TARGET_HOSTS" ]; then
        # TARGET_HOSTS-env fallback: strip any :port suffix.
        # shellcheck disable=SC2001  # bash pattern replacement is greedy here
        NODES_FWD="$(echo "$TARGET_HOSTS" | sed 's/:[0-9]*//g')"
    else
        NODES_FWD=""
    fi

    local_exit=0
    ssh "${SSH_OPTS[@]}" -o BatchMode=yes "$HARNESS" \
        "cd /root/ocean-fs && ${REMOTE_ENV_PREFIX}${TARGET_HOST_SSH:+TARGET_HOST_SSH=$TARGET_HOST_SSH }LOAD_TEST_DURATION_SECS=$DURATION ${LOAD_TEST_CONCURRENCY:+LOAD_TEST_CONCURRENCY=$LOAD_TEST_CONCURRENCY }${LOAD_TEST_LATENCY_IFACE:+LOAD_TEST_LATENCY_IFACE=$LOAD_TEST_LATENCY_IFACE }${LOAD_TEST_VOLUMES_JSON:+LOAD_TEST_VOLUMES_JSON=${LOAD_TEST_VOLUMES_JSON} }LOAD_TEST_NO_INJECTIONS=$NO_INJECTIONS ./scripts/run-phase4.sh --${MODE} ${NODES_FWD:+--nodes $NODES_FWD} ${SSH_LIST:+--ssh $SSH_LIST} --service ${SERVICE} --seed ${SEED} --report-dir ${REPORT_DIR} ${RECORD_ARG}" \
        || local_exit=$?

    # Push the load-test textfile into node 0's Prometheus textfile
    # collector (best-effort: only when observability is installed).
    if [ -n "$TARGET_HOSTS" ]; then
        node0_ip="${TARGET_HOSTS%%,*}"
        node0_ip="${node0_ip%%:*}"
        ssh "${SSH_OPTS[@]}" -o BatchMode=yes "$HARNESS" \
            "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${REPORT_DIR}/load_test.prom root@${node0_ip}:/var/lib/prometheus/textfile/ 2>/dev/null || true" \
            || true
    fi

    mkdir -p "$REPORT_DIR"
    if scp "${SSH_OPTS[@]}" "${HARNESS}:${REPORT_DIR}/4_load_degraded_*.json" "$REPORT_DIR/" 2>/dev/null; then
        log_info "Report fetched to ${REPORT_DIR}/"
    else
        log_info "No report fetched (check ${HARNESS}:${REPORT_DIR})."
    fi

    if "${SCRIPT_DIR}/backup-observability.sh" --quiet >/dev/null 2>&1; then
        log_info "Observability backup taken (scripts/backup-observability.sh)"
    else
        log_info "Observability backup skipped (start the laptop stack: docker compose -f mcps/docker-compose.yml up -d prometheus)"
    fi

    exit $local_exit
fi

# ---------------------------------------------------------------------------
# Local flow (runs on the harness itself, or in CI with local spawn).
# ---------------------------------------------------------------------------
if [ -n "$TARGET_HOSTS" ]; then
    if [ "$NO_INJECTIONS" -eq 0 ] && [ -z "$SSH_LIST" ] && [ -z "${TARGET_HOST_SSH:-}" ]; then
        log_error "Phase 4 injection needs per-node SSH (--ssh/TARGET_HOST_SSH); use --no-injections for a control run."
        exit 1
    fi
    log_info "Phase 4 $MODE mode: remote fleet ${TARGET_HOSTS} (injections=$([ "$NO_INJECTIONS" -eq 1 ] && echo no || echo yes))"
else
    log_info "Phase 4 $MODE mode: local spawn (no TARGET_HOSTS)"
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

log_info "Running load_degraded (${DURATION}s background load, seed ${SEED})..."
env \
    LOAD_TEST_SEED="$SEED" \
    LOAD_TEST_DURATION_SECS="$DURATION" \
    LOAD_TEST_REPORT_DIR="$REPORT_DIR" \
    LOAD_TEST_NO_INJECTIONS="$NO_INJECTIONS" \
    TARGET_HOSTS="${TARGET_HOSTS:-}" \
    TARGET_HOST_SSH="${TARGET_HOST_SSH:-${SSH_LIST:-}}" \
    TARGET_SERVICE="$SERVICE" \
    ${RECORD:+LOAD_TEST_RECORD_FILE="$RECORD"} \
    ${LOAD_TEST_VOLUMES_JSON:+LOAD_TEST_VOLUMES_JSON="$LOAD_TEST_VOLUMES_JSON"} \
    ${LOAD_TEST_LATENCY_IFACE:+LOAD_TEST_LATENCY_IFACE="$LOAD_TEST_LATENCY_IFACE"} \
    ${LOAD_TEST_CONCURRENCY:+LOAD_TEST_CONCURRENCY="$LOAD_TEST_CONCURRENCY"} \
    cargo test -p e2e --release --test load_degraded -- --test-threads=1 --nocapture

log_info "Report: ${REPORT_DIR}/4_load_degraded_*.json"
