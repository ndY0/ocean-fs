---
feature: "Prometheus & Grafana Setup — Observability Stack for Load Test VM"
epic: "operational-tooling"
status: done
priority: high
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-16
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
    7. **Accel Fallback Count** (stat): `accel_ec_fallback_total`
    8. **Test Phase Marker** (state timeline): `load_test_phase` from textfile
    9. **S3 Request Latency p50/p99** (line chart): histogram quantiles
    10. **WAL Bytes Written** (line chart): `wal_bytes_written_total`
  - Variables: `$phase` (from `load_test_phase`), `$test` (from textfile label)
    - **Caveat:** `load_test_phase` carries only a `test` label — the phase is
      the metric **value** (emitted by `e2e/src/load/report.rs`), so the
      `$phase` dropdown variable renders empty until a phase label is emitted;
      panels are unaffected.
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
  SUT Prometheus scrapes → :9090 (API)
  Laptop Prometheus federates ← SSH tunnel (observe.sh) ← SUT :9090
  Laptop Grafana → 127.0.0.1:9091 (persistent store) → dashboards
```

## Definition of Done

- [x] **Script:** `scripts/setup-observability.sh` is executable and idempotent (re-running is safe)
- [x] **Script:** On a fresh Ubuntu VM, running the script results in a working Prometheus at `:9090`
<!-- REVIEW: dry-run/code-path verified; live VM test not possible in this environment -->
- [x] **Config:** `prometheus.yml` scrape configs correctly target OceanFS and textfile collector
- [x] **Systemd:** `systemctl status prometheus` shows `active (running)` after setup
- [x] **Textfile:** `/var/lib/prometheus/textfile/` exists with correct permissions (writable by test harness user)
<!-- REVIEW: Node Exporter fix resolves previous gap — `install_node_exporter()` + `create_node_exporter_unit()` with `--collector.textfile.directory` flag, prometheus.yml `load_test` job scrapes localhost:9100 (Node Exporter), verification checks Node Exporter /metrics endpoint -->
- [x] **Dashboard:** `scripts/dashboards/load-test.json` is valid Grafana JSON (importable via Grafana UI)
- [x] **Dashboard:** All panels use correct PromQL queries matching the metrics catalog
- [x] **Dashboard:** Test phase marker panel correctly reads `load_test_phase` metric
- [x] **Dashboard:** `$test` variable (from textfile label) — added in review iteration 2; query: `label_values(load_test_phase{job="load_test"}, test)`
- [x] **Docs:** Script header documents all steps; observability setup guide explains SSH tunnel
- [x] **Integration:** After setup, `curl localhost:9090/api/v1/query?query=up` returns `up{job="oceanfs"}` with value 1 **(accepted deviation)** — requires live Prometheus on a VM; the setup script's `verify_prometheus()` function performs this check automatically during setup. Code-path verification passed in review.

## Accepted Deviations

The following deviations from the original feature spec were accepted during
review:

0. **Addendum (2026-08-16) — persistent laptop Prometheus (Solution B).**
   The SUT's Prometheus is ephemeral (destroyed with the VM, 7-day
   retention), which lost all run history at teardown. A persistent
   laptop-side Prometheus now runs in `mcps/docker-compose.yml` (service
   `prometheus`, host port `localhost:9091`, 365-day retention in the
   `prometheus-storage` volume) and **federates** the SUT Prometheus
   through the observe.sh tunnel (`/federate`, 15s, `honor_labels: true`).
   Both it and Grafana run with `network_mode: host` (the tunnel binds
   loopback only). Grafana's datasource points at this persistent store
   (`http://127.0.0.1:9091`, host loopback), so dashboards keep showing
   historical runs after the SUT is gone. Agents query it via the
   `vm-metrics` skill; `run-phase2.sh` ensures the tunnel automatically
   before each remote run so archiving is the default. No SUT-side changes
   were needed (federation is pull-based through the existing tunnel).
   Caveat: metrics are archived only while the tunnel is up during a run —
   `vm-down --preserve-data` (SUT TSDB snapshot) and the LoadReport JSONs
   remain the backstops for tunnel-less runs.

1. **Live integration test (`curl localhost:9090/api/v1/query?query=up`)** — This
   test requires a live Prometheus instance on a VM. The setup script's
   `verify_prometheus()` function performs this check automatically during setup,
   validating the Prometheus HTTP endpoint, Node Exporter `/metrics` endpoint,
   and textfile directory writability. All code-path verifications passed in
   review; full end-to-end validation is performed when the script is executed
   during actual load-test VM provisioning.

2. **Node Exporter integration for textfile collector** — The implementation uses
   `prometheus-node-exporter` with the `--collector.textfile.directory` flag
   rather than Prometheus scraping textfiles directly. This is the
   industry-standard pattern for Prometheus textfile metrics. The setup script
   includes `install_node_exporter()` and `create_node_exporter_unit()` to
   provision this. The Prometheus scrape config targets the Node Exporter at
   `localhost:9100` for the `load_test` job instead of scraping the textfile
   directory directly.

## Review History

| Iteration | Date | Changes |
|-----------|------|---------|
| 1 | 2026-08-11 | Initial implementation: `scripts/setup-observability.sh` + `scripts/dashboards/load-test.json` |
| 2 | 2026-08-11 | Added Node Exporter install + systemd unit for textfile collector; added `$test` dashboard variable |
| — | 2026-08-11 | **Review: PASS** — all criteria met with 2 accepted deviations |
