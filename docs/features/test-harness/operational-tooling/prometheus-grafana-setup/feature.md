---
feature: "Prometheus & Grafana Setup — Observability Stack for Load Test VM"
epic: "operational-tooling"
status: proposed
priority: high
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-05
---

# Prometheus & Grafana Setup — Observability Stack for Load Test VM

## Summary

Create `scripts/setup-observability.sh` and a Grafana dashboard JSON to provision
the observability stack on the load test VM. Installs and configures Prometheus as
a systemd service scraping OceanFS nodes at `:9000/admin/metrics` every 15s and
a textfile collector for harness events. Creates a Grafana dashboard JSON
(`scripts/dashboards/load-test.json`) committed to the repo with panels for all
key load test metrics: RSS, fd count, RocksDB level-0 files, cache hit rates,
gossip message rates, heal request rates, accel fallback count, and test phase
marker. Documents the SSH tunnel for laptop Grafana access.

## Scope

### In Scope

- `scripts/setup-observability.sh` — idempotent setup script
  - Installs Prometheus via `apt-get` or downloads the binary
  - Creates `prometheus.yml` with scrape configs:
    - Job `oceanfs`: scrapes `localhost:9000/admin/metrics` every 15s
    - Job `load_test`: textfile collector from `/var/lib/prometheus/textfile/*.prom`
  - Configures 7-day retention (`--storage.tsdb.retention.time=7d`)
  - Creates systemd unit `prometheus.service` (enabled, started)
  - Creates textfile directory `/var/lib/prometheus/textfile/` with correct permissions
  - Outputs success/failure for each step
- `scripts/dashboards/load-test.json` — Grafana dashboard JSON
  - Panels:
    1. **RSS over time** (line chart): `process_resident_memory_bytes`
    2. **Open FDs over time** (line chart): `process_open_fds`
    3. **RocksDB Level-0 SST files** (line chart): `rocksdb_num_files_at_level_0`
    4. **Cache Hit Rates** (stat/gauge): `cache_hits_total / (cache_hits_total + cache_misses_total)` per tier
    5. **Gossip Message Rate** (line chart): `rate(gossip_messages_sent_total[1m])`
    6. **Heal Request Rate** (line chart): `rate(heal_requests_total[1m])`
    7. **Accel Fallback Count** (stat): `accel_fallback_total`
    8. **Test Phase Marker** (state timeline): `load_test_phase` from textfile
    9. **S3 Request Latency p50/p99** (line chart): histogram quantiles
    10. **WAL Bytes Written** (line chart): `wal_bytes_written_total`
  - Variables: `$phase` (from `load_test_phase`), `$test` (from textfile label)
  - Datasource: Prometheus (configured by user, variable `$datasource`)
- README documentation in script header or `docs/observability-setup.md`:
  - How to SSH tunnel: `ssh -L 9090:localhost:9090 -N vm`
  - How to import dashboard into Grafana
  - How to find dashboard: `http://localhost:3000/d/load-test`

### Out of Scope

- Grafana installation and configuration on the laptop (user-installed separately)
- Grafana provisioning (automatic dashboard import) — manual JSON import is sufficient
- Mimir/Loki setup (deferred to Phase 5 scale testing)
- Prometheus alerting rules (load tests check assertions in-harness; alerts would duplicate)
- Docker/Podman containerization of the observability stack

## Crate Impact

| Crate | Change |
|---|---|
| (none) | All deliverables are scripts and JSON files outside the Cargo workspace. |

## Interface (Public API)

No Rust code. The script interface:

```
Usage: ./scripts/setup-observability.sh [--textfile-dir /var/lib/prometheus/textfile] [--retention-days 7]
```

## Data Flow

```
Setup (run once):
  ./scripts/setup-observability.sh
    → apt install prometheus (or download)
    → write /etc/prometheus/prometheus.yml
    → systemctl enable --now prometheus
    → mkdir -p /var/lib/prometheus/textfile
    → chown prometheus:prometheus /var/lib/prometheus/textfile

During test:
  Harness writes → /var/lib/prometheus/textfile/load_test.prom (atomic)
  Prometheus scrapes → :9090 (API)
  Laptop Grafana → SSH tunnel → localhost:9090 → dashboards
```

## Definition of Done

- [ ] **Script:** `scripts/setup-observability.sh` is executable and idempotent (re-running is safe)
- [ ] **Script:** On a fresh Ubuntu VM, running the script results in a working Prometheus at `:9090`
- [ ] **Config:** `prometheus.yml` scrape configs correctly target OceanFS and textfile collector
- [ ] **Systemd:** `systemctl status prometheus` shows `active (running)` after setup
- [ ] **Textfile:** `/var/lib/prometheus/textfile/` exists with correct permissions (writable by test harness user)
- [ ] **Dashboard:** `scripts/dashboards/load-test.json` is valid Grafana JSON (importable via Grafana UI)
- [ ] **Dashboard:** All panels use correct PromQL queries matching the metrics catalog
- [ ] **Dashboard:** Test phase marker panel correctly reads `load_test_phase` metric
- [ ] **Docs:** Script header documents all steps; observability setup guide explains SSH tunnel
- [ ] **Integration:** After setup, `curl localhost:9090/api/v1/query?query=up` returns `up{job="oceanfs"}` with value 1
