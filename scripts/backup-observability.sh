#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# backup-observability.sh — Backup the persistent laptop observability stack.
#
# The prometheus-storage volume holds EVERY archived load-test run (365-day
# retention — "information we pay for"). A careless
# `docker compose down --volumes` (or a docker prune) would destroy it
# silently. This script creates consistent, restorable backups:
#
#   1. Prometheus TSDB snapshot via the official admin API
#      (POST /api/v1/admin/tsdb/snapshot — consistent even while running),
#      copied out of the container as prometheus-<timestamp>.tar.gz
#   2. Grafana state (dashboards, annotations, users) as
#      grafana-<timestamp>.tar.gz  [--no-grafana to skip]
#
# Rotation: keeps the --keep newest backups, deletes the rest.
#
# Restore:
#   docker compose -f mcps/docker-compose.yml down prometheus
#   # replace the volume contents:
#   docker run --rm -v mcps_prometheus-storage:/prometheus \
#     -v "$(pwd)/<backup>/prometheus-<ts>.tar.gz":/backup.tar.gz:ro \
#     alpine sh -c 'rm -rf /prometheus/* && tar xzf /backup.tar.gz -C /prometheus'
#   docker compose -f mcps/docker-compose.yml up -d prometheus
#
# Usage:
#   ./scripts/backup-observability.sh [OPTIONS]
#
# Options:
#   --backup-dir DIR   Backup root (default: ./local-results/observability-backups)
#   --keep N           Number of backups to retain (default: 7)
#   --no-grafana       Skip the grafana-storage backup
#   --quiet            Only print errors (for automated hooks)
#   --dry-run          Print actions without executing
#   -h, --help         Show this help.
#
# Auto-hook: run-phase2.sh invokes this (best-effort, --quiet) after every
# remote run, so each finished run is archived before anything can touch
# the volumes. Agents and humans can also run it any time.
# ---------------------------------------------------------------------------
set -euo pipefail

BACKUP_DIR="local-results/observability-backups"
KEEP=7
WITH_GRAFANA=true
QUIET=false
DRY_RUN=false

log_info()  { [ "$QUIET" = false ] && echo "[INFO]  $(date '+%H:%M:%S') $*" >&2 || true; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*" >&2; }

usage() {
    sed -n '2,44p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --backup-dir) BACKUP_DIR="${2:-}"; shift 2 ;;
        --keep) KEEP="${2:-7}"; shift 2 ;;
        --no-grafana) WITH_GRAFANA=false; shift ;;
        --quiet) QUIET=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) usage ;;
        *) log_error "Unknown option: $1. Use --help."; exit 1 ;;
    esac
done

PROM_CONTAINER="oceanfs-prometheus"
GRAFANA_CONTAINER="oceanfs-grafana"
PROM_URL="http://127.0.0.1:9091"

if [ "$DRY_RUN" = true ]; then
    log_info "[DRY-RUN] would snapshot $PROM_URL TSDB and archive to $BACKUP_DIR (keep=$KEEP)"
    exit 0
fi

command -v docker >/dev/null || { log_error "docker not found — the laptop stack is required."; exit 1; }
docker ps --format '{{.Names}}' | grep -qx "$PROM_CONTAINER" \
    || { log_error "container $PROM_CONTAINER is not running (start it: docker compose -f mcps/docker-compose.yml up -d prometheus)."; exit 1; }

mkdir -p "$BACKUP_DIR"
TS="$(date -u +%Y%m%dT%H%M%SZ)"

# ── 1. Prometheus TSDB snapshot (consistent, live-safe) ──────────────────
log_info "Snapshotting Prometheus TSDB ($PROM_URL)..."
SNAP_JSON="$(curl -sf -XPOST --max-time 120 "${PROM_URL}/api/v1/admin/tsdb/snapshot" \
    || { log_error "snapshot API failed — is --web.enable-admin-api set on the prometheus service?"; exit 1; })"
SNAP_NAME="$(printf '%s' "$SNAP_JSON" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p')"
[ -n "$SNAP_NAME" ] || { log_error "snapshot response did not contain a name: $SNAP_JSON"; exit 1; }
log_info "Snapshot: $SNAP_NAME"

PROM_ARCHIVE="${BACKUP_DIR}/prometheus-${TS}.tar.gz"
log_info "Copying snapshot out of the container -> $PROM_ARCHIVE"
docker exec "$PROM_CONTAINER" tar czf - -C /prometheus/snapshots "$SNAP_NAME" \
    > "$PROM_ARCHIVE" \
    || { log_error "docker exec tar failed"; exit 1; }

# ── 2. Grafana state ──────────────────────────────────────────────────────
GRAFANA_ARCHIVE=""
if [ "$WITH_GRAFANA" = true ]; then
    VOL_NAME="$(docker inspect "$GRAFANA_CONTAINER" --format '{{range .Mounts}}{{if eq .Destination "/var/lib/grafana"}}{{.Name}}{{end}}{{end}}' 2>/dev/null || true)"
    if [ -n "$VOL_NAME" ]; then
        GRAFANA_ARCHIVE="${BACKUP_DIR}/grafana-${TS}.tar.gz"
        log_info "Archiving grafana volume ($VOL_NAME) -> $GRAFANA_ARCHIVE"
        docker run --rm \
            -v "${VOL_NAME}:/data:ro" \
            -v "$(pwd)/${BACKUP_DIR}:/backup" \
            alpine tar czf "/backup/grafana-${TS}.tar.gz" -C /data . \
            || log_error "grafana backup failed (non-fatal)."
    else
        log_warn "grafana volume not found — skipping grafana backup."
    fi
fi

# ── 3. Prune the in-container snapshot (already archived) ─────────────────
docker exec "$PROM_CONTAINER" rm -rf "/prometheus/snapshots/${SNAP_NAME}" \
    || log_error "could not prune in-container snapshot (non-fatal)."

# ── 4. Rotation ───────────────────────────────────────────────────────────
if [ "$KEEP" -gt 0 ]; then
    STALE="$(ls -1t "${BACKUP_DIR}"/prometheus-*.tar.gz 2>/dev/null | tail -n +$((KEEP + 1)))"
    if [ -n "$STALE" ]; then
        log_info "Rotating: removing backups older than the newest ${KEEP}"
        while IFS= read -r f; do
            [ -n "$f" ] && rm -f "$f"
            # also drop the matching grafana archive
            g="${f/prometheus-/grafana-}"
            [ -f "$g" ] && rm -f "$g"
        done <<< "$STALE"
    fi
fi

if [ "$QUIET" = true ]; then
    echo "[backup] prometheus=$PROM_ARCHIVE grafana=${GRAFANA_ARCHIVE:-skipped}"
else
    echo "Backup complete:"
    echo "  $PROM_ARCHIVE"
    [ -n "$GRAFANA_ARCHIVE" ] && echo "  $GRAFANA_ARCHIVE"
    echo "  (in $BACKUP_DIR, keeping $KEEP)"
fi
