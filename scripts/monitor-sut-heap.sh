#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# monitor-sut-heap.sh — 1s heap/address-space sampler for the SUT oceanfs.
#
# Samples /proc/<pid>/smaps_rollup + /proc/<pid>/status on the SUT VM every
# second and writes a CSV. Use during a load-test run to attribute memory
# bursts: the smaps_rollup "Heap" line covers the brk heap, while large
# malloc/mmap allocations appear as anonymous Private_Dirty (mmap) — the
# split tells you whether a burst is heap growth or big mmap'd buffers.
#
# Usage (from the laptop, while a run is in progress):
#   ./scripts/monitor-sut-heap.sh [--seconds N] [--sut HOST] [--out FILE]
#
# Options:
#   --seconds N   Sample for N seconds (default: 420 = one quick run)
#   --sut HOST    SSH target for the SUT (default: oceanfs-sut alias,
#                 or the public IP from the newest provisioning record)
#   --out FILE    CSV output (default: /tmp/sut-heap.csv)
#   -h, --help    Show this help.
#
# CSV columns:
#   ts, rss_bytes, pss_bytes, private_dirty_bytes, heap_bytes, stack_bytes,
#   swap_bytes, open_fds, pid
#
# Author: OceanFS
# Date: 2026-08-16
# ---------------------------------------------------------------------------
set -euo pipefail

SECONDS_N=420
SUT="oceanfs-sut"
OUT="/tmp/sut-heap.csv"
SSH_OPTS="-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

usage() {
    sed -n '2,27p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --seconds) SECONDS_N="${2:-420}"; shift 2 ;;
        --sut) SUT="${2:-}"; shift 2 ;;
        --out) OUT="${2:-}"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# Resolve the SUT from the provisioning record if no alias is usable.
if ! ssh $SSH_OPTS -o ConnectTimeout=5 "$SUT" true 2>/dev/null; then
    RECORD=$(ls -t .hetzner/provision-*.json 2>/dev/null | head -1 || true)
    if [ -n "$RECORD" ]; then
        SUT="root@$(jq -r '.sut.public_ip' "$RECORD")"
    fi
fi
echo "SUT: $SUT  (sampling ${SECONDS_N}s -> $OUT)"

# The remote sampler: locate the oceanfs pid once, then read the proc files.
ssh $SSH_OPTS "$SUT" bash -s -- "$SECONDS_N" <<'REMOTE' > "$OUT"
set -euo pipefail
N="$1"
PID="$(pgrep -f '/usr/local/bin/oceanfs' | head -1 || true)"
[ -n "$PID" ] || { echo "no oceanfs process on the SUT" >&2; exit 1; }
# smaps_rollup has no Heap/Stack lines (those exist only in full smaps);
# the anonymous signal is Private_Dirty (brk heap + mmap'd anon buffers).
echo "ts,rss_bytes,pss_bytes,private_dirty_bytes,swap_bytes,open_fds,pid"
for i in $(seq 1 "$N"); do
    ROLLUP=$(awk '
        /^Rss:/{r=$2} /^Pss:/{p=$2} /^Private_Dirty:/{pd=$2} /^Swap:/{sw=$2}
        END{printf "%d,%d,%d,%d", r*1024, p*1024, pd*1024, sw*1024}
    ' "/proc/$PID/smaps_rollup" 2>/dev/null || true)
    FDS=$(ls /proc/$PID/fd 2>/dev/null | wc -l)
    echo "$(date -u +%H:%M:%S),${ROLLUP},$FDS,$PID"
    sleep 1
done
REMOTE

echo "done: $OUT"
