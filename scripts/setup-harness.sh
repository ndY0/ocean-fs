#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# setup-harness.sh — wire the Harness VM to the SUT after provisioning.
#
# Reads the provisioning record that vm-provision.sh persists under
# .hetzner/provision-<prefix>.json and:
#   1. seeds the harness's SSH identity (private key) so it can reach the
#      SUT over the internal network — required for the harness's crash
#      control (TARGET_HOST_SSH / systemctl kill+restart)
#   2. ensures the correct repo/branch/commit is on the harness (the
#      provisioner's clone is only as fresh as provisioning time) and
#      builds the release oceanfs + e2e binaries
#   3. verifies the SUT is healthy over the internal network
#
# Usage:
#   ./scripts/setup-harness.sh [OPTIONS]
#
# Options:
#   --provision-file PATH  Provisioning record to use (default: the
#                          newest .hetzner/provision-*.json).
#   --ssh-key PUB          Public key path (default: from the record).
#   --ssh-private-key PRIV Private key to seed on the harness (default:
#                          the --ssh-key path with a trailing .pub
#                          stripped).
#   --repo URL             Override the repo recorded at provisioning.
#   --branch B             Override the branch (default: from record).
#   --commit SHA           Override the commit to check out.
#   --dry-run              Print actions without executing.
#   -h, --help             Show this help.
#
# Examples:
#   ./scripts/setup-harness.sh
#   ./scripts/setup-harness.sh --provision-file .hetzner/provision-oceanfs-loadtest-2.json
# ---------------------------------------------------------------------------
set -euo pipefail

# Load .hetzner/.env, ensure ssh-agent + the Hetzner key (no-op without
# .hetzner/, e.g. on the Harness VM).
# shellcheck source=lib/env-hetzner.sh
_ENV_HETZNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/env-hetzner.sh"
[ -f "$_ENV_HETZNER" ] && . "$_ENV_HETZNER"
unset _ENV_HETZNER

PROVISION_FILE=""
SSH_KEY=""
SSH_PRIVATE_KEY=""
REPO=""
BRANCH=""
COMMIT=""
DRY_RUN=false

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }
log_warn() { echo "[WARN]  $(date '+%H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --provision-file) PROVISION_FILE="${2:-}"; shift 2 ;;
        --ssh-key) SSH_KEY="${2:-}"; shift 2 ;;
        --ssh-private-key) SSH_PRIVATE_KEY="${2:-}"; shift 2 ;;
        --repo) REPO="${2:-}"; shift 2 ;;
        --branch) BRANCH="${2:-}"; shift 2 ;;
        --commit) COMMIT="${2:-}"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

# Resolve the provisioning record: explicit, else the newest one.
if [ -z "$PROVISION_FILE" ]; then
    PROVISION_FILE=$(ls -t .hetzner/provision-*.json 2>/dev/null | head -1 || true)
fi
if [ -z "$PROVISION_FILE" ] || [ ! -f "$PROVISION_FILE" ]; then
    log_error "No provisioning record found. Re-run vm-provision.sh (it writes .hetzner/provision-<prefix>.json), or pass --provision-file."
    exit 1
fi
log_info "Using provisioning record: $PROVISION_FILE"

# Extract the topology + repo settings from the record.
get() { jq -r "$1 // empty" "$PROVISION_FILE" 2>/dev/null || true; }
HARNESS_PUBLIC_IP=$(get '.harness.public_ip')
SUT_INTERNAL_IP=$(get '.sut.internal_ip')
SUT_PUBLIC_IP=$(get '.sut.public_ip')
HARNESS_NAME=$(get '.harness.name')
SUT_NAME=$(get '.sut.name')
RECORD_REPO=$(get '.repo')
RECORD_BRANCH=$(get '.branch')
RECORD_COMMIT=$(get '.commit')
RECORD_SSH_KEY=$(get '.ssh_key')

[ -n "$HARNESS_PUBLIC_IP" ] && [ -n "$SUT_INTERNAL_IP" ] \
    || { log_error "Record is missing harness/sut IPs — is it a vm-provision.sh output?"; exit 1; }
log_info "Harness ${HARNESS_PUBLIC_IP} -> SUT ${SUT_INTERNAL_IP} (internal)"

# Resolve the SSH keys.
SSH_KEY="${SSH_KEY:-$RECORD_SSH_KEY}"
if [ -z "$SSH_PRIVATE_KEY" ]; then
    if [ -n "$SSH_KEY" ]; then
        SSH_PRIVATE_KEY="${SSH_KEY%.pub}"
    else
        SSH_PRIVATE_KEY="${HOME}/.ssh/id_ed25519"
    fi
fi
[ -f "$SSH_KEY" ] || log_error "Public key not found at $SSH_KEY (use --ssh-key)."
[ -f "$SSH_PRIVATE_KEY" ] || { log_error "Private key not found at $SSH_PRIVATE_KEY (use --ssh-private-key)."; exit 1; }
chmod 600 "$SSH_PRIVATE_KEY" 2>/dev/null || true
log_info "SSH identity: $SSH_PRIVATE_KEY (public: $SSH_KEY)"

# 1. Seed the harness's SSH identity (harness -> SUT over internal net).
log_info "Seeding harness SSH identity..."
if [ "$DRY_RUN" = false ]; then
    scp $SSH_OPTS "$SSH_PRIVATE_KEY" "root@${HARNESS_PUBLIC_IP}:~/.ssh/id_ed25519"
    ssh $SSH_OPTS "root@${HARNESS_PUBLIC_IP}" "chmod 600 ~/.ssh/id_ed25519"
    if ! ssh $SSH_OPTS -o BatchMode=yes "root@${HARNESS_PUBLIC_IP}" \
        "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=5 root@${SUT_INTERNAL_IP} echo ok" >/dev/null 2>&1; then
        log_error "Harness cannot reach the SUT at ${SUT_INTERNAL_IP} over the internal network."
        exit 1
    fi
    log_info "Harness -> SUT (${SUT_INTERNAL_IP}) SSH verified."
else
    log_info "[DRY-RUN] scp $SSH_PRIVATE_KEY -> root@${HARNESS_PUBLIC_IP}:~/.ssh/id_ed25519 + verify harness -> root@${SUT_INTERNAL_IP}"
fi

# 2. Ensure the right repo/branch/commit on the harness, then build.
REPO="${REPO:-$RECORD_REPO}"
if [ -z "$REPO" ]; then
    REPO="https://github.com/ndY0/ocean-fs.git"
fi
BRANCH="${BRANCH:-$RECORD_BRANCH}"
[ -n "$BRANCH" ] || BRANCH="main"
log_info "Ensuring repo ${REPO} (branch=${BRANCH}${COMMIT:+, commit=${COMMIT}}) on the harness..."

if [ "$DRY_RUN" = false ]; then
    ssh $SSH_OPTS "root@${HARNESS_PUBLIC_IP}" bash -s -- "$REPO" "$BRANCH" "$COMMIT" <<'HARNESS_REPO'
set -euo pipefail
REPO="$1"; BRANCH="$2"; COMMIT="${3:-}"
# Note: ${3:-} — ssh joins remote-command arguments with spaces, so an
# empty $COMMIT is DROPPED and the remote bash only sees $1 and $2.

if [ -d /root/ocean-fs/.git ]; then
    git -C /root/ocean-fs fetch origin "$BRANCH" || true
    git -C /root/ocean-fs checkout "$BRANCH" || true
    git -C /root/ocean-fs pull --ff-only origin "$BRANCH" || git -C /root/ocean-fs reset --hard "origin/$BRANCH"
else
    rm -rf /root/ocean-fs
    git clone "$REPO" --branch "$BRANCH" /root/ocean-fs
fi
if [ -n "$COMMIT" ]; then
    git -C /root/ocean-fs checkout "$COMMIT"
fi
source /root/.cargo/env
cd /root/ocean-fs
cargo build --release -p oceanfs -p e2e
HARNESS_REPO
    log_info "Repo synced and release binaries built on the harness."
else
    log_info "[DRY-RUN] clone/pull ${REPO} on the harness + cargo build --release -p oceanfs -p e2e"
fi

# 3. Deploy to the SUT from the harness (the harness is the build machine,
# per ADR-0019: build on the Harness VM, scp to the SUT VM). Reuses
# sut-deploy.sh over the internal network.
log_info "Deploying oceanfs to the SUT (${SUT_INTERNAL_IP}) from the harness..."
if [ "$DRY_RUN" = false ]; then
    if ! ssh $SSH_OPTS -o BatchMode=yes "root@${HARNESS_PUBLIC_IP}" \
        "cd /root/ocean-fs && ./scripts/sut-deploy.sh --sut root@${SUT_INTERNAL_IP} --port 9000 --binary target/release/oceanfs"; then
        log_error "SUT deploy failed. Check: ssh root@${SUT_PUBLIC_IP} 'systemctl status oceanfs'"
        exit 1
    fi
    log_info "SUT deployed from the harness."
else
    log_info "[DRY-RUN] on harness: ./scripts/sut-deploy.sh --sut root@${SUT_INTERNAL_IP} --port 9000"
fi

# 4. Ensure the observability stack on the SUT (idempotent; covers VMs
# provisioned before the provisioner installed it by default).
log_info "Ensuring observability stack on the SUT (${SUT_INTERNAL_IP})..."
if [ "$DRY_RUN" = false ]; then
    if ! ssh $SSH_OPTS -o BatchMode=yes "root@${HARNESS_PUBLIC_IP}" \
        "cd /root/ocean-fs && scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null scripts/setup-observability.sh root@${SUT_INTERNAL_IP}:/root/ && ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@${SUT_INTERNAL_IP} 'bash /root/setup-observability.sh'"; then
        log_warn "Observability setup failed on the SUT (non-fatal — the harness scrape still covers the run)."
    else
        log_info "Observability stack ensured on the SUT."
    fi
else
    log_info "[DRY-RUN] on harness: scp setup-observability.sh -> root@${SUT_INTERNAL_IP} && run it"
fi

# 5. Verify SUT health over the internal network (the path the harness
# will actually use for the payload).
log_info "Verifying SUT health over the internal network..."
if [ "$DRY_RUN" = false ]; then
    if ! ssh $SSH_OPTS -o BatchMode=yes "root@${HARNESS_PUBLIC_IP}" \
        "curl -sf --max-time 10 http://${SUT_INTERNAL_IP}:9000/admin/health" >/dev/null 2>&1; then
        log_error "SUT ${SUT_INTERNAL_IP}:9000 not healthy. Check: ssh root@${SUT_PUBLIC_IP} 'systemctl status oceanfs'"
        exit 1
    fi
    log_info "SUT healthy at http://${SUT_INTERNAL_IP}:9000/admin/health."
else
    log_info "[DRY-RUN] curl http://${SUT_INTERNAL_IP}:9000/admin/health from the harness"
fi

cat <<READY

Setup complete. Run the Phase 2 payload from your laptop:
  ./scripts/run-phase2.sh --harness root@${HARNESS_PUBLIC_IP} --quick --sut ${SUT_INTERNAL_IP}:9000 --ssh root@${SUT_INTERNAL_IP} --seed 42
  ./scripts/run-phase2.sh --harness root@${HARNESS_PUBLIC_IP} --full  --sut ${SUT_INTERNAL_IP}:9000 --ssh root@${SUT_INTERNAL_IP} --seed 7
READY
