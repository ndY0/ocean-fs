---
name: vm-metrics
description: "Query Prometheus metrics for the OceanFS load-test runs. Use when the user asks for resource metrics (RSS, fds, RocksDB, cache hit rates), wants to run a PromQL query against OceanFS metrics — live OR historical — or needs metric evidence to compare test runs. Triggers: \"vm-metrics\", \"query prometheus\", \"check the metrics\", \"SUT memory usage\", \"RSS\", \"rocksdb metrics\", \"compare runs\"."
---

# vm-metrics

Execute PromQL queries against OceanFS load-test metrics.

**Primary endpoint: the persistent laptop Prometheus** (`http://localhost:9091`)
— a long-lived store (365-day retention, `prometheus-storage` volume in
`mcps/docker-compose.yml`) that **federates the SUT VM's Prometheus through
the observe.sh tunnel**. It contains ALL archived runs — including runs from
VMs that have since been destroyed — so this is the endpoint for comparing
test runs over time.

**Live/full-fidelity endpoint: the SUT's Prometheus directly** via the
observe.sh tunnel (`http://localhost:9090`) or direct SSH. Use when you need
the current run's data at full SUT-side fidelity (the laptop store lags by
the 15s federation scrape and only exists while the tunnel is up).

## Data flow

```
SUT Prometheus :9090  --observe.sh tunnel-->  localhost:9090 (host loopback)
   -> laptop Prometheus /federate (15s)  ->  prometheus-storage (365d)
   -> http://localhost:9091  <- agents query here
   -> http://127.0.0.1:9091  <- Grafana reads here (host networking)
```

Both the persistent Prometheus and Grafana run with `network_mode: host`
(mcps/docker-compose.yml) because the tunnel binds loopback only — a
bridge-mode container could not reach it. `run-phase2.sh` ensures the tunnel
automatically before every remote run, so archived runs are the default. The
SUT's own Prometheus keeps 7 days as a local buffer; the laptop store is the
durable copy.

## Prerequisites

- `curl` and `jq`
- Laptop Prometheus running: `docker compose -f mcps/docker-compose.yml up -d prometheus`

## Procedure

1. **Historical/archived metrics (default)** — query the persistent store:

   ```bash
   curl -sG 'http://localhost:9091/api/v1/query' \
     --data-urlencode "query=${PROMQL}"
   ```

   Time range (matrix) for trends:

   ```bash
   curl -sG 'http://localhost:9091/api/v1/query_range' \
     --data-urlencode "query=${PROMQL}" \
     --data-urlencode "start=$(date -d '30 days ago' +%s)" \
     --data-urlencode "end=$(date +%s)" \
     --data-urlencode "step=60"
   ```

2. **Live metrics (current run)** — ensure the tunnel, then query the SUT
   directly:

   ```bash
   ./scripts/observe.sh          # idempotent; opens ssh -L 9090 to the SUT
   curl -sG 'http://localhost:9090/api/v1/query' \
     --data-urlencode "query=${PROMQL}"
   ```

   Fallback without a tunnel (firewalled env): direct SSH curl on the SUT:

   ```bash
   SUT_PUB=$(jq -r '.sut.public_ip' "$(ls -t .hetzner/provision-*.json | head -1)")
   ssh -o BatchMode=yes "root@${SUT_PUB}" \
     "curl -sG 'http://localhost:9090/api/v1/query' --data-urlencode 'query=${PROMQL}'"
   ```

3. **Always URL-encode the query** with `--data-urlencode` (PromQL is full
   of `{`, `}`, `,`, `=` which break inline URLs).

4. **Parse the response** — return the `data` object
   (`{resultType: "vector"|"matrix", result: [...]}`) plus a human summary
   of the first few series. Do not dump raw JSON without interpretation.

## Common queries (actual metric names)

| Question | PromQL |
|---|---|
| Current RSS of the oceanfs process | `process_resident_memory_bytes` |
| Open fd count | `process_open_fds` |
| WAL file count | `wal_file_count` |
| RocksDB level-0 SST files (write-stall indicator) | `rocksdb_num_files_at_level_0` |
| Segment seal errors | `segment_seal_errors_total` |
| EC acceleration fallbacks | `accel_ec_fallback_total` |
| Active segments in the pool | `segment_active_count` |
| L1 object cache hit rate | `sum(rate(cache_hits_total{tier="l1"}[5m])) / (sum(rate(cache_hits_total{tier="l1"}[5m])) + sum(rate(cache_misses_total{tier="l1"}[5m])))` |
| Run markers (test name per run) | `load_test_phase` |

## Returns

```json
{
  "query": "process_resident_memory_bytes",
  "endpoint": "http://localhost:9091 (persistent laptop Prometheus)",
  "resultType": "vector",
  "result": [
    { "metric": { "__name__": "process_resident_memory_bytes", "instance": "oceanfs-sut", "job": "oceanfs" },
      "value": [1722859200, "870000000"] }
  ],
  "summary": "RSS = 830 MiB at 2026-08-16T10:00:00Z"
}
```

## Cross-run comparison

- The persistent store keeps the `job="oceanfs"` and `job="load_test"`
  series of every archived run. Use `query_range` over weeks with
  `step=60` to overlay runs; `load_test_phase` marks each run's window.
- For structured per-run data (assertions, manifest, 10s snapshots), use
  **vm-results** on the LoadReport JSONs — the two complement each other:
  reports for the verdict, Prometheus for the raw signal.

## Notes

- The SUT firewall keeps :9090 closed to the internet — only the tunnel or
  direct SSH can reach the live SUT Prometheus.
- `./scripts/observe.sh --kill` closes the tunnel when done. The persistent
  store keeps working (it only needs the tunnel during runs).
- Grafana shows the same data (`http://localhost:3000/d/oceanfs-load-test`,
  backed by the persistent store) — use it for visual inspection, this
  skill for programmatic queries.
