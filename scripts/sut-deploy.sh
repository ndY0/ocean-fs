#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# sut-deploy.sh — Deploy OceanFS to the SUT VM for load-test phases.
#
# Closes the vm-provision.sh gap: provisioning creates the SUT VM but never
# deploys OceanFS. This script installs the binary, writes the Phase 2
# sustained-load config, and creates the systemd unit the harness's SSH
# crash-control (TARGET_HOST_SSH) depends on.
#
# The unit MUST be Restart=no: the harness SIGKILLs the service
# (systemctl kill -s KILL) to exercise WAL crash recovery and needs the
# process to stay down until it issues `systemctl restart`.
#
# Usage:
#   ./scripts/sut-deploy.sh --sut root@10.0.0.5 [OPTIONS]
#
# Options:
#   --sut TARGET       SSH target for the SUT VM (required): user@host or
#                      a ~/.ssh/config alias like oceanfs-sut.
#   --binary PATH      Local oceanfs binary to deploy
#                      (default: ./target/release/oceanfs).
#   --port N           SUT HTTP port (default: 9000).
#   --data-dir DIR     Persistent data directory on the SUT (default:
#                      /var/lib/oceanfs). Survives restarts — WAL crash
#                      recovery depends on it.
#   --config-dir DIR   Config directory on the SUT (default: /etc/oceanfs).
#   --service NAME     systemd unit name (default: oceanfs).
#   --dry-run          Print actions without executing.
#   -h, --help         Show this help.
#
# Examples:
#   ./scripts/sut-deploy.sh --sut oceanfs-sut
#   ./scripts/sut-deploy.sh --sut root@10.0.0.5 --port 9000
# ---------------------------------------------------------------------------
set -euo pipefail

SUT=""
BINARY="${OCEANFS_BIN:-./target/release/oceanfs}"
PORT=9000
DATA_DIR="/var/lib/oceanfs"
CONFIG_DIR="/etc/oceanfs"
SERVICE="oceanfs"
DRY_RUN=false

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,34p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --sut) SUT="${2:-}"; shift 2 ;;
        --binary) BINARY="${2:-}"; shift 2 ;;
        --port) PORT="${2:-}"; shift 2 ;;
        --data-dir) DATA_DIR="${2:-}"; shift 2 ;;
        --config-dir) CONFIG_DIR="${2:-}"; shift 2 ;;
        --service) SERVICE="${2:-}"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

[ -n "$SUT" ] || { log_error "--sut is required (e.g. root@10.0.0.5 or oceanfs-sut)."; exit 1; }
[ -f "$BINARY" ] || { log_error "binary not found at $BINARY — build it first: cargo build --release -p oceanfs"; exit 1; }

run() {
    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] $*"
    else
        "$@"
    fi
}

log_info "Deploying OceanFS to $SUT (port $PORT, service $SERVICE)"

# 1. Binary.
if [ "$DRY_RUN" = false ]; then
    scp -o StrictHostKeyChecking=accept-new "$BINARY" "${SUT}:/usr/local/bin/oceanfs" \
        || { log_error "scp failed to ${SUT}:/usr/local/bin/oceanfs"; exit 1; }
    ssh -o StrictHostKeyChecking=accept-new "$SUT" "chmod +x /usr/local/bin/oceanfs"
else
    log_info "[DRY-RUN] scp $BINARY ${SUT}:/usr/local/bin/oceanfs && chmod +x"
fi

# 2. Config + systemd unit (written via a single SSH heredoc).
if [ "$DRY_RUN" = true ]; then
    log_info "[DRY-RUN] write ${CONFIG_DIR}/oceanfs.toml + /etc/systemd/system/${SERVICE}.service on $SUT"
    exit 0
fi

ssh -o StrictHostKeyChecking=accept-new "$SUT" bash -s -- "$PORT" "$DATA_DIR" "$CONFIG_DIR" "$SERVICE" <<'SUT_SETUP'
set -euo pipefail
PORT="$1"; DATA_DIR="$2"; CONFIG_DIR="$3"; SERVICE="$4"

mkdir -p "$DATA_DIR" "$CONFIG_DIR"

cat > "${CONFIG_DIR}/oceanfs.toml" <<CONFIG
node_id = "sut"
listen_addr = "0.0.0.0:${PORT}"
grpc_listen_addr = "0.0.0.0:$((${PORT} + 1))"
data_dir = "${DATA_DIR}"
log_level = "info"
max_body_size = 16777216
gc_interval_sec = 10
tombstone_ttl_sec = 5
ae_interval_sec = 10
scrub_interval_sec = 60
object_cache_size_bytes = 268435456
object_cache_max_blob_size = 16777216
object_cache_ttl_ms = 0
CONFIG

cat > "/etc/systemd/system/${SERVICE}.service" <<UNIT
[Unit]
Description=OceanFS node (load-test SUT)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/oceanfs --config ${CONFIG_DIR}/oceanfs.toml --log-level info
# Restart=no is REQUIRED: the harness SIGKILLs this unit to exercise
# WAL crash recovery and must control the restart itself.
Restart=no
WorkingDirectory=${DATA_DIR}

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now "$SERVICE"
SUT_SETUP

# 3. Health check.
log_info "Waiting for ${SERVICE} health on ${SUT}:${PORT}..."
for _ in $(seq 1 30); do
    if ssh -o StrictHostKeyChecking=accept-new "$SUT" \
        "curl -sf http://localhost:${PORT}/admin/health >/dev/null 2>&1"; then
        log_info "SUT healthy: http://${SUT}:${PORT}/admin/health"
        exit 0
    fi
    sleep 1
done
log_error "SUT did not become healthy within 30s. Check: systemctl status ${SERVICE} on ${SUT}"
exit 1
