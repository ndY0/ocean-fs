#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# observe.sh — ensure the Prometheus tunnel to the SUT is up.
#
# Idempotent: checks whether localhost:9090 already answers; if not, opens
# `ssh -L 9090:localhost:9090` to the SUT (read from the provisioning
# record under .hetzner/, or --sut). Safe to call repeatedly from a
# terminal OR an agent session — it never stacks duplicate tunnels.
#
# The SUT firewall keeps :9090 closed to the internet by design; this
# tunnel is the only way in, and it terminates on loopback.
#
# Usage:
#   ./scripts/observe.sh [OPTIONS]
#
# Options:
#   --sut HOST          SUT public IP/host (default: from the newest
#                       .hetzner/provision-*.json).
#   --port N            Local tunnel port (default: 9090).
#   --kill              Close the tunnel instead of opening it.
#   --url-only          Print http://localhost:PORT and exit without
#                       checking/opening (for scripts).
#   -h, --help          Show this help.
# ---------------------------------------------------------------------------
set -euo pipefail

# Load .hetzner/.env, ensure ssh-agent + the Hetzner key (no-op without
# .hetzner/, e.g. on the Harness VM).
# shellcheck source=lib/env-hetzner.sh
_ENV_HETZNER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/env-hetzner.sh"
[ -f "$_ENV_HETZNER" ] && . "$_ENV_HETZNER"
unset _ENV_HETZNER

SUT=""
PORT=9090
KILL=false
URL_ONLY=false

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

log_info() { echo "[INFO]  $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --sut) SUT="${2:-}"; shift 2 ;;
        --port) PORT="${2:-}"; shift 2 ;;
        --kill) KILL=true; shift ;;
        --url-only) URL_ONLY=true; shift ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

# Resolve the SUT from the provisioning record unless overridden.
if [ -z "$SUT" ]; then
    local_record=$(ls -t .hetzner/provision-*.json 2>/dev/null | head -1 || true)
    if [ -n "$local_record" ]; then
        SUT=$(jq -r '.sut.public_ip // empty' "$local_record" 2>/dev/null || true)
    fi
fi
[ -n "$SUT" ] || { log_info "No SUT known — pass --sut HOST or re-run vm-provision.sh first."; exit 1; }

URL="http://localhost:${PORT}"

if [ "$URL_ONLY" = true ]; then
    printf '%s\n' "$URL"
    exit 0
fi

if [ "$KILL" = true ]; then
    pkill -f "9090:localhost:9090" 2>/dev/null && log_info "Tunnel closed." || log_info "No tunnel was running."
    exit 0
fi

# Already up? (both the local port answering AND our ssh holding it)
if curl -sf --max-time 2 "${URL}/-/healthy" >/dev/null 2>&1; then
    log_info "Prometheus already reachable at ${URL}."
    exit 0
fi

log_info "Opening SSH tunnel ${SUT}:9090 -> ${URL}..."
# -f backgrounds after auth; -N no remote command; -L forwards.
ssh $SSH_OPTS -f -N -L "${PORT}:localhost:9090" "root@${SUT}"

# Give the forward a moment, then verify.
for _ in $(seq 1 10); do
    if curl -sf --max-time 2 "${URL}/-/healthy" >/dev/null 2>&1; then
        log_info "Prometheus up: ${URL}"
        log_info "Try:  curl '${URL}/api/v1/query?query=rate(process_resident_memory_bytes[5m])'"
        exit 0
    fi
    sleep 1
done
log_info "Tunnel opened but Prometheus not answering yet — is it installed on the SUT?"
log_info "Check: ssh root@${SUT} 'systemctl status prometheus'"
exit 1
