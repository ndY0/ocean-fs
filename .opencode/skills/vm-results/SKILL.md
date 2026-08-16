---
name: vm-results
description: "Fetch and summarize the OceanFS load test results (LoadReport JSON). Use when the user asks whether the last load test passed, wants the load report, the assertion outcomes, or the manifest verification summary. Triggers: \"vm-results\", \"get the results\", \"did the test pass\", \"load report\", \"show the assertions\", \"fetch the report\"."
---

# vm-results

Fetch and summarize the `LoadReport` JSON produced by a load test run.
Reports are written to `/tmp/oceanfs-reports` (tmpfs, per ADR-0019
Decision 4 — disk-fill tests can never block report output) on the machine
that ran the payload: the Harness VM for cloud runs, the CI runner for
local runs. `run-phase2.sh --harness` already fetches the report to the
laptop — this skill reads it, or fetches it if it is missing.

## Parameters

| Parameter | Meaning | Default |
|---|---|---|
| `report-dir` | Local report directory | `/tmp/oceanfs-reports` (or `LOAD_TEST_REPORT_DIR`) |
| `test` | Test name pattern | `2_load_sustained` |

## Procedure

1. **Look for the report locally first**:

   ```bash
   REPORT_DIR="${REPORT_DIR:-/tmp/oceanfs-reports}"
   LATEST=$(ls -t "${REPORT_DIR}"/${TEST:-2_load_sustained}_*.json 2>/dev/null | head -1)
   ```

2. **Fetch from the harness if missing** (only when VMs are up — read the
   `.hetzner/provision-*.json` record for `.harness.public_ip`):

   ```bash
   rsync -avz "root@${HARNESS_PUB}:/tmp/oceanfs-reports/" "${REPORT_DIR}/" \
     || echo "no reports on the harness"
   LATEST=$(ls -t "${REPORT_DIR}"/${TEST:-2_load_sustained}_*.json 2>/dev/null | head -1)
   ```

   If still no report: the run may never have happened, or the test died
   before writing — return the empty result below, do NOT invent data.

3. **Parse the report with `jq`**:

   ```bash
   jq '{ phase, test, seed, duration_secs, result,
         failed_assertions: [.assertions[] | select(.passed == false)],
         assertions: [.assertions[] | {name, passed}],
         manifest, harness_metrics }' "$LATEST"
   ```

## Returns

```json
{
  "report_path": "/tmp/oceanfs-reports/2_load_sustained_20260816T101500.json",
  "phase": 2,
  "test": "load_sustained",
  "seed": 42,
  "result": "pass",
  "duration_secs": 300.0,
  "assertions": [
    { "name": "memory_bounded", "passed": true },
    { "name": "fds_stable", "passed": true },
    { "name": "rocksdb_no_write_stall", "passed": true },
    { "name": "segment_seal_no_errors", "passed": true },
    { "name": "accel_fallback_zero", "passed": true },
    { "name": "wal_not_unbounded", "passed": true },
    { "name": "cache_reasonable", "passed": true },
    { "name": "segment_active_count", "passed": true },
    { "name": "crash_recovery", "passed": true }
  ],
  "failed_assertions": [],
  "manifest": {
    "objects_written": 8234,
    "objects_verified": 8234,
    "mismatches": 0
  },
  "harness_metrics": { "process_resident_memory_bytes": 118000000, "process_open_fds": 42 }
}
```

When no report exists:

```json
{ "result": "no_report", "report_path": null, "reason": "no 2_load_sustained_*.json in /tmp/oceanfs-reports (locally or on the harness)" }
```

## Notes

- Report filenames embed the run timestamp:
  `{phase}_{test}_{YYYYMMDD}T{HHMMSS}.json` — the latest by `ls -t` is the
  newest run.
- `result` is `"pass"` or `"fail"` (ReportResult serialization). A
  non-zero test exit code usually corresponds to `"fail"`.
- Key fields for analysis: `metric_snapshots` (10s resource time-series),
  `worker_stats` (ops, errors, latency), `manifest` (data integrity),
  `harness_metrics` (harness self-monitoring, metadata only).
