---
feature: "Test Execution Skills — Agent Commands for Running Tests & Fetching Results"
epic: "agent-skills"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: test-phase-implementations/phase1-concurrency-test
    reason: Need Phase 1 test to invoke
  - epic: test-phase-implementations/phase2-sustained-load-test
    reason: Need Phase 2 test to invoke
  - epic: test-phase-implementations/phase3-cluster-churn-test
    reason: Need Phase 3 test to invoke
  - epic: test-phase-implementations/phase4-degraded-mode-test
    reason: Need Phase 4 test to invoke
  - epic: test-harness-extensions/load-report
    reason: Need LoadReport format for vm-results parsing
  - epic: operational-tooling/prometheus-grafana-setup
    reason: Need Prometheus on VM for vm-metrics PromQL queries
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-05
---

# Test Execution Skills — Agent Commands for Running Tests & Fetching Results

## Summary

Create four OpenCode skill files under `.opencode/skills/` for executing load
tests and consuming results: `vm-test-phase`, `vm-results`, `vm-metrics`, and
`vm-logs`. `vm-test-phase` runs `cargo test -p e2e -- load_phase{N}` on the
VM with configurable seed and duration. `vm-results` rsyncs the latest
LoadReport JSON from VM to laptop and returns a structured summary. `vm-metrics`
executes a PromQL query against the VM's Prometheus via SSH tunnel and returns
parsed results. `vm-logs` fetches journald logs from the VM for a given time
window. These skills close the loop from test execution to result consumption.

## Scope

### In Scope

#### `vm-test-phase.md`
- SSH to VM: `cargo test -p e2e -- load_phase{N} --nocapture`
- Accepts: `--phase N` (required), `--seed SEED` (optional), `--duration-secs N` (optional — sets `LOAD_TEST_DURATION_SECS`)
- Sets environment variables on the VM before running: `LOAD_TEST_SEED`, `LOAD_TEST_DURATION_SECS`
- Returns: `{phase: N, exit_code: 0|1, report_path: "...", grafana_url: "http://localhost:3000/d/load-test?var-phase=N", duration_secs: N}`
- On failure: includes last 20 lines of stderr in the response
- The `report_path` is the path on the VM, not the laptop (use `vm-results` to fetch)

#### `vm-results.md`
- SSH to VM: find the latest LoadReport JSON in `~/ocean-fs/target/load-reports/`
- Rsync the report to laptop: `rsync oceanfs-vm:~/ocean-fs/target/load-reports/{latest} ./local-results/`
- Parse report JSON, return structured summary:
  ```json
  {
    "phase": 2,
    "test": "load_sustained",
    "result": "pass" | "fail" | "timeout",
    "duration_secs": 1800,
    "key_metrics": {
      "objects_written": 8234,
      "objects_verified": 8234,
      "mismatches": 0,
      "avg_put_latency_ms": 12.3,
      "avg_get_latency_ms": 4.1
    },
    "failed_assertions": [],
    "failures": []
  }
  ```
- Accepts: `--phase N` (to find the latest report for that phase)

#### `vm-metrics.md`
- Execute PromQL query against VM's Prometheus: `ssh oceanfs-vm "curl -s 'http://localhost:9090/api/v1/query?query={url_encoded_query}'"`
- Accepts: `--query "promql_expression"` (required)
- Returns: parsed JSON result with `{resultType: "vector"|"matrix", result: [...]}`
- Common queries are documented in the skill file as examples:
  - `process_resident_memory_bytes` — current RSS
  - `rate(accel_fallback_total[1m])` — fallback rate
  - `rocksdb_num_files_at_level_0` — write stall indicator
  - `load_test_result` — last test result

#### `vm-logs.md`
- SSH to VM: `journalctl -u oceanfs --since "{since}" --no-pager`
- Accepts: `--since "10 min ago"` (default), `--phase N` (to scope to most recent Phase N run)
- Returns: array of log lines: `[{timestamp: "...", message: "..."}, ...]`
- Optionally filter by severity: `--level error` filters `grep ERROR`

### Out of Scope

- Grafana screenshot generation (use the laptop's Grafana UI for visual inspection)
- Log streaming (tail -f) — one-shot queries only
- Metric alerting (Prometheus AlertManager is not configured on the VM)
- Multi-node log correlation (single-node VM for Phases 1-4)

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Skill files under `.opencode/skills/`. |

## Interface (Public API)

Each skill is a Markdown file with command templates and return schemas.

### vm-test-phase
```markdown
# vm-test-phase

Run a load test phase on the VM.

## Parameters
- `--phase N` (required): 1, 2, 3, or 4
- `--seed SEED`: deterministic seed (default: random)
- `--duration-secs N`: override test duration

## Returns
{
  "phase": 2,
  "exit_code": 0,
  "report_path": "~/ocean-fs/target/load-reports/2_load_sustained_20260805T100000.json",
  "grafana_url": "http://localhost:3000/d/load-test?var-phase=2",
  "duration_secs": 1800
}
```

## Data Flow

```
Agent: vm-test-phase --phase 2 --seed 42 --duration-secs 300

  → ssh oceanfs-vm "cd ocean-fs && LOAD_TEST_SEED=42 LOAD_TEST_DURATION_SECS=300 cargo test -p e2e -- load_sustained --nocapture"
  → Test runs on VM; writes LoadReport to target/load-reports/
  → SSH command returns exit code 0
  → Returns { phase: 2, exit_code: 0, report_path: "...", grafana_url: "...", duration_secs: 300 }

Agent: vm-results --phase 2

  → ssh oceanfs-vm "ls -t ~/ocean-fs/target/load-reports/2_* | head -1"
  → rsync oceanfs-vm:~/ocean-fs/target/load-reports/2_load_sustained_20260805T100000.json ./local-results/
  → Parse JSON locally
  → Returns structured summary

Agent: vm-metrics --query "process_resident_memory_bytes"

  → ssh oceanfs-vm "curl -s 'http://localhost:9090/api/v1/query?query=process_resident_memory_bytes'"
  → Parse Prometheus API JSON response
  → Returns { resultType: "vector", result: [{ metric: {}, value: [1722859200, "87000000"] }] }

Agent: vm-logs --since "10 min ago" --level error

  → ssh oceanfs-vm "journalctl -u oceanfs --since '10 min ago' --no-pager | grep ERROR"
  → Returns [{ timestamp: "...", message: "..." }, ...]
```

## Definition of Done

- [ ] **Files:** `.opencode/skills/vm-test-phase.md` exists with parameters and return schema
- [ ] **Files:** `.opencode/skills/vm-results.md` exists with parameters and return schema
- [ ] **Files:** `.opencode/skills/vm-metrics.md` exists with parameters, return schema, and common query examples
- [ ] **Files:** `.opencode/skills/vm-logs.md` exists with parameters and return schema
- [ ] **Validation:** `vm-test-phase` skill correctly constructs the SSH command with env vars
- [ ] **Validation:** `vm-results` skill correctly handles the case of no reports found (returns empty result, not error)
- [ ] **Validation:** `vm-metrics` skill correctly URL-encodes the PromQL query and parses the Prometheus API response
- [ ] **Validation:** `vm-logs` skill correctly formats `--since` argument for journalctl
- [ ] **Docs:** Each skill file includes example invocations with expected outputs
- [ ] **Integration:** Agent can: vm-test-phase 2 → vm-results 2 → vm-metrics --query "process_resident_memory_bytes" → analyze all results in a single workflow
