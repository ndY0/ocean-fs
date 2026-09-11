#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# sut-deploy.sh — Deploy OceanFS to the SUT VM(s) for load-test phases.
#
# Closes the vm-provision.sh gap: provisioning creates the SUT VM(s) but
# never deploys OceanFS. This script installs the binary, writes the load-
# test config, and creates the systemd unit the harness's SSH crash-control
# (TARGET_HOST_SSH) depends on.
#
# The unit MUST be Restart=no: the harness SIGKILLs the service
# (systemctl kill -s KILL) to exercise WAL crash recovery and needs the
# process to stay down until it issues `systemctl restart`.
#
# Two modes:
#   Single-SUT (Phase 2):   --sut root@10.0.0.5 [--port 9000]
#   Cluster fleet (Phase 3+, per ADR-0026):
#       --cluster "root@10.0.0.2,root@10.0.0.3,root@10.0.0.4"
#     Deploys to every node. Node 0 is the bootstrap (no seed_nodes);
#     nodes 1..N-1 get [gossip] seed_nodes = ["<node0>:9002"] (the
#     membership plane port, ADR-0028 D1) and the
#     phase-3 fast-gossip profile (1s gossip, 3s suspicion, 8s failure).
#     Every node listens on :9000/:9001/:9002 — nodes differ by IP, no port
#     juggling (9002 = membership plane, ADR-0028). node_id = oceanfs-node-{i} per node.
#
# Usage:
#   ./scripts/sut-deploy.sh --sut root@10.0.0.5 [OPTIONS]
#   ./scripts/sut-deploy.sh --cluster "root@10.0.0.2,root@10.0.0.3,root@10.0.0.4" [OPTIONS]
#
# Options:
#   --sut TARGET       SSH target for the SUT VM (required in single mode):
#                      user@host or a ~/.ssh/config alias like oceanfs-sut.
#   --cluster LIST     Comma-separated SSH targets for the node fleet
#                      (phase 3+, ADR-0026). Mutually exclusive with --sut.
#   --pools-on-mounts  Root the four mandatory pools on the Hetzner volume
#                      mounts (/mnt/oceanfs-{data,wal,meta,hints}) instead
#                      of sibling directories of data_dir. Pre-flights every
#                      mount on every node: must be a writable block-device
#                      mount of the expected size class, disjoint from
#                      data_dir, and not the root filesystem — a silent
#                      local-disk fallback fails the deploy. Requires
#                      vm-provision.sh --volume-pools.
#   --volume-pools     Alias of --pools-on-mounts.
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
# Volume-backed runs are correctness/degradation tests, NOT performance
# measurements — network block storage numbers are not comparable to
# local-disk runs.
#
# Examples:
#   ./scripts/sut-deploy.sh --sut oceanfs-sut
#   ./scripts/sut-deploy.sh --sut root@10.0.0.5 --port 9000
#   ./scripts/sut-deploy.sh --sut oceanfs-sut --pools-on-mounts
#   ./scripts/sut-deploy.sh --cluster "root@10.0.0.2,root@10.0.0.3,root@10.0.0.4" --pools-on-mounts
# ---------------------------------------------------------------------------
set -euo pipefail

# Load .hetzner/.env, ensure ssh-agent + the Hetzner key (no-op without
# .hetzner/, e.g. on the Harness VM).
# shellcheck source=lib/env-hetzner.sh
_ENV_HETZNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/env-hetzner.sh"
# shellcheck disable=SC1090
[ -f "$_ENV_HETZNER" ] && . "$_ENV_HETZNER"
unset _ENV_HETZNER

# Shared volume helpers: the mount pre-flight runs on the node via
# volume_run_remote (no copy of the helper file is needed there).
# shellcheck source=lib/volume-helpers.sh
_ENV_VOLUME_HELPERS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/volume-helpers.sh"
# shellcheck disable=SC1090
[ -f "$_ENV_VOLUME_HELPERS" ] && . "$_ENV_VOLUME_HELPERS"
unset _ENV_VOLUME_HELPERS

SUT=""
CLUSTER=""
BINARY="${OCEANFS_BIN:-./target/release/oceanfs}"
PORT=9000
DATA_DIR="/var/lib/oceanfs"
CONFIG_DIR="/etc/oceanfs"
SERVICE="oceanfs"
DRY_RUN=false
POOLS_ON_MOUNTS=false

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,63p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --sut) SUT="${2:-}"; shift 2 ;;
        --cluster) CLUSTER="${2:-}"; shift 2 ;;
        --binary) BINARY="${2:-}"; shift 2 ;;
        --port) PORT="${2:-}"; shift 2 ;;
        --data-dir) DATA_DIR="${2:-}"; shift 2 ;;
        --config-dir) CONFIG_DIR="${2:-}"; shift 2 ;;
        --service) SERVICE="${2:-}"; shift 2 ;;
        --pools-on-mounts|--volume-pools) POOLS_ON_MOUNTS=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

if [ -n "$CLUSTER" ] && [ -n "$SUT" ]; then
    log_error "--sut and --cluster are mutually exclusive."; exit 1
fi
if [ -z "$CLUSTER" ] && [ -z "$SUT" ]; then
    log_error "--sut (single) or --cluster (fleet) is required."; exit 1
fi
[ -f "$BINARY" ] || { log_error "binary not found at $BINARY — build it first: cargo build --release -p oceanfs"; exit 1; }

# Cluster mode: expand the fleet into per-node deploy targets. Node 0 is
# the bootstrap (no seeds); nodes 1..N-1 seed to node 0's gRPC address.
FLEET_TARGETS=()
FLEET_SEEDS=()
if [ -n "$CLUSTER" ]; then
    IFS=',' read -ra FLEET_TARGETS <<< "$CLUSTER"
    [ "${#FLEET_TARGETS[@]}" -ge 3 ] || { log_error "--cluster needs at least 3 nodes (quorum semantics, ADR-0026)."; exit 1; }
    # Resolve node 0's internal MEMBERSHIP endpoint for seed_nodes
    # (ADR-0028 D1: gossip lives on its own port — PORT+2; the data
    # plane at PORT+1 no longer serves GossipRpc). Accept either
    # user@host or user@host:port forms.
    local_node0="${FLEET_TARGETS[0]}"
    seed_host="${local_node0##*@}"
    FLEET_SEEDS[0]=""
    for ((i = 1; i < ${#FLEET_TARGETS[@]}; i++)); do
        FLEET_SEEDS[i]="${seed_host}:$((PORT + 2))"
    done
    log_info "Cluster deploy: ${#FLEET_TARGETS[@]} nodes, seed=${FLEET_SEEDS[1]:-none} (node 0 is bootstrap)"
fi

run() {
    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] $*"
    else
        "$@"
    fi
}

# Deploy one node: binary + config + unit + health. Arguments:
#   $1 target (SSH), $2 node name (node_id; "sut" for single mode),
#   $3 seed ("" for bootstrap)
deploy_node() {
    local target="$1"
    local node_name="$2"
    local seed="$3"

    log_info "Deploying OceanFS to ${target} (node ${node_name}, port ${PORT}, service ${SERVICE})"

    # Volume pools mode: assert every required mount is a real, writable,
    # block-device mount of the expected size class BEFORE touching the
    # binary or the service. A plain directory, a root-fs path or a tiny
    # wrong device fails the deploy — silent local-disk fallback is the
    # failure mode this guard exists for.
    local pool_mode="local"
    if [ "$POOLS_ON_MOUNTS" = true ]; then
        pool_mode="mounts"
        local role mount min_gb
        for role in data wal meta hints; do
            mount="${VOLUME_MOUNT_BASE}/oceanfs-${role}"
            min_gb="$(volume_min_gb_for_role "$role")"
            if [ "$DRY_RUN" = true ]; then
                log_info "[DRY-RUN] Assert ${mount} on ${target} is a block-device mount >= ${min_gb}G, writable, disjoint from ${DATA_DIR}"
                continue
            fi
            if ! volume_run_remote "$target" assert_volume_mount "$mount" "$DATA_DIR" "$min_gb"; then
                log_error "Volume mount pre-flight failed: ${mount} on ${target} is not a writable block-device mount (>= ${min_gb}G) disjoint from ${DATA_DIR}."
                log_error "Provision volumes first (./scripts/vm-provision.sh --phase <N> --volume-pools), then check 'findmnt ${mount}' on the node."
                exit 1
            fi
        done
        log_info "Volume mount pre-flight OK on ${target} (data/wal/meta/hints)."
    fi

    # 1. Binary — staged via a temp name + atomic rename: the running
    #    service holds the old inode, and Linux refuses to open an executing
    #    file for writing (ETXTBSY / "Text file busy"), which makes direct
    #    overwrite fail on every redeploy while the service is up.
    if [ "$DRY_RUN" = false ]; then
        scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "$BINARY" "${target}:/usr/local/bin/oceanfs.new" \
            || { log_error "scp failed to ${target}:/usr/local/bin/oceanfs.new"; exit 1; }
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "$target" \
            "mv -f /usr/local/bin/oceanfs.new /usr/local/bin/oceanfs && chmod +x /usr/local/bin/oceanfs" \
            || { log_error "install (rename) failed on $target"; exit 1; }
    else
        log_info "[DRY-RUN] scp $BINARY ${target}:/usr/local/bin/oceanfs.new && mv + chmod +x"
    fi

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY-RUN] write ${CONFIG_DIR}/oceanfs.toml + /etc/systemd/system/${SERVICE}.service on $target"
        return 0
    fi

    # NB: ssh JOINS the remote command line with spaces, so an empty
    # "$seed" would vanish and shift every positional — pass a
    # sentinel and normalize it on the remote side (phase-2 single
    # node: seed must be empty for `seed_nodes = []`). The comment MUST
    # precede the ssh command — a comment on a `\`-continued line
    # swallows the whole remote command.
    # NODE_IP: the node's reachable address for the ADVERTISED gRPC
    # address. `grpc_listen_addr = "0.0.0.0:9001"` makes every node
    # advertise 0.0.0.0 — peers then connect to THEIR OWN grpc port
    # (Unimplemented) and gossip dies beyond the explicit seed path
    # (the fleet never converges: observed node-1 stuck at 2/3).
    node_ip="${target##*@}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "$target" \
        bash -s -- "$PORT" "$DATA_DIR" "$CONFIG_DIR" "$SERVICE" "$node_name" "$node_ip" "${seed:-NONE}" "$pool_mode" <<'SUT_SETUP'
set -euo pipefail
PORT="$1"; DATA_DIR="$2"; CONFIG_DIR="$3"; SERVICE="$4"; NODE_NAME="$5"; NODE_IP="$6"; SEED="$7"; POOL_MODE="$8"
[ "$SEED" = "NONE" ] && SEED=""
# Pool roots live on a SIBLING base of data_dir in local mode (ADR-0031/
# ADR-0029: a pool root must stay disjoint from data_dir — the validate
# rule rejects a root nested inside or equal to it). In mounts mode the
# roots are the Hetzner volume mounts asserted by the caller.
if [ "$POOL_MODE" = "mounts" ]; then
    POOL_ROOT_DATA="/mnt/oceanfs-data"
    POOL_ROOT_WAL="/mnt/oceanfs-wal"
    POOL_ROOT_META="/mnt/oceanfs-meta"
    POOL_ROOT_HINTS="/mnt/oceanfs-hints"
else
    POOL_BASE="${DATA_DIR}-pools"
    POOL_ROOT_DATA="${POOL_BASE}/pool-data"
    POOL_ROOT_WAL="${POOL_BASE}/pool-wal"
    POOL_ROOT_META="${POOL_BASE}/pool-meta"
    POOL_ROOT_HINTS="${POOL_BASE}/pool-hints"
fi

mkdir -p "$DATA_DIR" "$CONFIG_DIR"

# OOM safety net: a 2 GiB swapfile so memory crests degrade latency
# instead of letting the kernel OOM-kill the node mid-run. The harness
# crash-control (SIGKILL/restart) is unaffected.
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
    grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

cat > "${CONFIG_DIR}/oceanfs.toml" <<CONFIG
node_id = "${NODE_NAME}"
listen_addr = "0.0.0.0:${PORT}"
# The ADVERTISED gRPC address must be the node's own reachable IP
# (peers dial it directly): 0.0.0.0 here made every node dial ITSELF
# and gossip never converged past the seed path.
grpc_listen_addr = "${NODE_IP}:$((${PORT} + 1))"
# ADR-0028 D1: the membership plane (gossip + SWIM probes) listens on
# its own port (9002 for PORT=9000), isolated from the data plane.
membership_listen_addr = "${NODE_IP}:$((${PORT} + 2))"
data_dir = "${DATA_DIR}"
log_level = "info"
# ── CX33 (8 GB RAM) — generous memory profile ──────────────────────────────
# The earlier CX23 calibration (4 MiB bodies, 96-128 MiB caches) made
# memory the constraint. On the 8 GB SUT, restore production-like values
# so CPU (hashing, EC encode) is the bottleneck; the streaming read-path
# fix removed the multi-GB burst behavior that forced the small profile.
max_body_size = 16777216
object_cache_size_bytes = 268435456
object_cache_max_blob_size = 16777216
metadata_cache_size_bytes = 1073741824
block_cache_size = 134217728
objects_write_buffer_mb = 64
segments_write_buffer_mb = 256
deletions_write_buffer_mb = 16
# Bound fd-per-SST spikes (RocksDB default max_open_files=-1 opens one fd
# per SST during compaction bursts — the fds_stable root cause).
max_open_files = 256
# ── End of memory profile ──────────────────────────────────────────────────
gc_interval_sec = 10
tombstone_ttl_sec = 5
ae_interval_sec = 10
scrub_interval_sec = 60
# Orphan reaper must match the GC cadence: with the default 3600s the
# reaper never fires during a load-test run, so orphaned segments (from
# the delete-heavy workload) accumulate until the disk fills and the SUT
# OOM-kills (observed: ~31 GB of orphans swept only at crash-recovery
# restart). A 10s interval reclaims them continuously.
orphan_reaper_interval_sec = 10
object_cache_ttl_ms = 0
# ── Cluster semantics (phase 3+, ADR-0026) ──────────────────────────────────
# 3-node quorum: every write must reach 2 nodes, reads must consult 2.
# Matches the local-spawn profile (config_cluster_churn in the e2e
# harness) so remote runs exercise the same durability semantics.
write_quorum = 2
read_quorum = 2
replication_factor = 3
[gossip]
interval_ms = 1000
# Loosened from 3000/8000: under load-test CPU contention the gossip
# push (which carries the SWIM pings, DK-007) lags and the tight
# windows produced FALSE suspects — a live node marked suspect/dead
# during the settle and convergence never recovered (local churn47
# + fleet churn runs 4-6). Matches the local churn profile.
suspicion_timeout_ms = 6000
failure_timeout_ms = 15000
indirect_ping_count = 3
seed_nodes = [${SEED:+$(printf '"%s"' "$SEED")}]
CONFIG

# ADR-0031 (f1): storage pools are mandatory at boot — a node without a
# role-complete [storage.pools] block refuses to start. One pool per role,
# roots disjoint from data_dir. Local mode: sibling directories (the e2e
# harness injects the same layout for local spawns), created by the startup
# probe. Mounts mode: the Hetzner volume mounts, pre-flighted above.
cat >> "${CONFIG_DIR}/oceanfs.toml" <<POOLS

# ── Storage pools (ADR-0031: mandatory at boot) ─────────────────────────
[storage]
missing_root_policy = "fatal"
# f0: the hints role ignores this and always boots Degraded when its root is
# missing (hints are debt, not data — the admission gate enforces honesty).

[[storage.pools]]
name = "data-0"
role = "data"
root = "${POOL_ROOT_DATA}"

[[storage.pools]]
name = "wal-0"
role = "wal"
root = "${POOL_ROOT_WAL}"

[[storage.pools]]
name = "meta-0"
role = "metadata"
root = "${POOL_ROOT_META}"

[[storage.pools]]
name = "hints-0"
role = "hints"
root = "${POOL_ROOT_HINTS}"
POOLS

cat > "/etc/systemd/system/${SERVICE}.service" <<UNIT
[Unit]
Description=OceanFS node ${NODE_NAME} (load-test SUT)
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
systemctl enable "$SERVICE"
# restart (not enable --now): a redeploy must apply the new config even
# when the service is already running.
systemctl restart "$SERVICE"
SUT_SETUP

    # 3. Health check.
    log_info "Waiting for ${SERVICE} health on ${target}:${PORT}..."
    for _ in $(seq 1 30); do
        if ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "$target" \
            "curl -sf http://localhost:${PORT}/admin/health >/dev/null 2>&1"; then
            log_info "Node healthy: http://${target}:${PORT}/admin/health"
            return 0
        fi
        sleep 1
    done
    log_error "Node did not become healthy within 30s. Check: systemctl status ${SERVICE} on ${target}"
    exit 1
}

# ── Entry ──────────────────────────────────────────────────────────────────
if [ -n "$CLUSTER" ]; then
    for ((i = 0; i < ${#FLEET_TARGETS[@]}; i++)); do
        deploy_node "${FLEET_TARGETS[$i]}" "oceanfs-node-${i}" "${FLEET_SEEDS[$i]}"
    done
    log_info "Cluster deploy complete: ${#FLEET_TARGETS[@]} nodes healthy."
else
    # Phase 2 single-SUT: keep the historical node_id "sut".
    deploy_node "$SUT" "sut" ""
fi
