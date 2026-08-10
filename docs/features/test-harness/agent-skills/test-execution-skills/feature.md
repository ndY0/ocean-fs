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
    reason: Need Prometheus on SUT VM for vm-metrics PromQL queries
adr:
  - 0019-test-harness-topology-cost-guardrails
perf: []
created: 2026-08-05
updated: 2026-08-10
---

# Test Execution Skills — Agent Commands for Running Tests & Fetching Results

## Summary

Create four OpenCode skill files under `.opencode/skills/` for executing load
tests and consuming results in the two-VM topology (per ADR-0019):
`vm-test-phase`, `vm-results`, `vm-metrics`, and `vm-logs`. `vm-test-phase` runs
the harness on the **Harness VM** with `TARGET_HOST` (Phase 2) or `TARGET_HOSTS`
(Phase 3-4) env vars pointing to the SUT VM's internal IP — the harness connects
to already-running OceanFS processes instead of spawning them locally.
`vm-results` rsyncs the LoadReport JSON from Harness VM's `/tmp` to the laptop.
`vm-metrics` executes PromQL queries against the **SUT VM's** Prometheus via SSH
tunnel. `vm-logs` fetches journald logs from the **SUT VM**.

## Scope

### In Scope

#### `vm-test-phase.md`
- SSH to **Harness VM**: run the e2e test with `TARGET_HOST`/`TARGET_HOSTS` env vars pointing to SUT VM
- Command template:
  - Phase 2 (single-node): `ssh oceanfs-harness "cd ~/ocean-fs && TARGET_HOST=<sut-ip>:9000 LOAD_TEST_SEED=<seed> LOAD_TEST_DURATION_SECS=<secs> cargo test -p e2e -- load_sustained --nocapture"`
  - Phase 3-4 (multi-node): `ssh oceanfs-harness "cd ~/ocean-fs && TARGET_HOSTS=<sut-ip>:9000,<sut-ip>:9001,<sut-ip>:9002 LOAD_TEST_SEED=<seed> cargo test -p e2e -- load_cluster_churn --nocapture"`
  - Phase 1 (CI-only): `ssh oceanfs-harness "cd ~/ocean-fs && cargo test -p e2e -- load_concurrency --nocapture"` (no TARGET_HOST needed, local spawn)
- Accepts: `--phase N` (required), `--seed SEED` (optional), `--duration-secs N` (optional)
- Sets environment variables on the Harness VM before running: `TARGET_HOST`, `TARGET_HOSTS`, `LOAD_TEST_SEED`, `LOAD_TEST_DURATION_SECS`
- For `--single-vm` mode (harness and SUT co-located), configures relaxed gossip parameters:
  - `GOSSIP_INTERVAL_MS=3000`, `SUSPICION_TIMEOUT_MS=10000`, `FAILURE_TIMEOUT_MS=30000`
  - Passed as environment variables read by the test harness
- For Phase 3-4 in `--single-vm` mode, prints WARNING banner before running (per ADR-0019 Decision 4)
- Returns: `{phase: N, exit_code: 0|1, report_path: "/tmp/2_load_sustained_20260810T100000.json", grafana_url: "http://localhost:3000/d/load-test?var-phase=N", duration_secs: N, topology: "two-vm"|"single-vm"}`
- On failure: includes last 20 lines of stderr in the response
- The `report_path` is the path on the **Harness VM** (always in `/tmp` per ADR-0019)

#### `vm-results.md`
- SSH to **Harness VM**: find the latest LoadReport JSON in `/tmp/`
- Rsync the report to laptop: `rsync oceanfs-harness:/tmp/{latest} ./local-results/`
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
- Execute PromQL query against **SUT VM's** Prometheus via SSH:
  `ssh oceanfs-sut "curl -s 'http://localhost:9090/api/v1/query?query={url_encoded_query}'"`
- Note: Prometheus runs on the SUT VM (scraping localhost:9000/admin/metrics), NOT on the Harness VM
- Accepts: `--query "promql_expression"` (required)
- Returns: parsed JSON result with `{resultType: "vector"|"matrix", result: [...]}`
- Common queries documented in the skill file as examples:
  - `process_resident_memory_bytes` — current RSS
  - `rate(accel_fallback_total[1m])` — fallback rate
  - `rocksdb_num_files_at_level_0` — write stall indicator
  - `load_test_result` — last test result

#### `vm-logs.md`
- SSH to **SUT VM**: `journalctl -u oceanfs --since "{since}" --no-pager`
- Accepts: `--since "10 min ago"` (default), `--phase N` (to scope to most recent Phase N run)
- Returns: array of log lines: `[{timestamp: "...", message: "..."}, ...]`
- Optionally filter by severity: `--level error` filters `grep ERROR`

### Out of Scope

- Grafana screenshot generation (use the laptop's Grafana UI for visual inspection)
- Log streaming (tail -f) — one-shot queries only
- Metric alerting (Prometheus AlertManager is not configured on the VM)
- Multi-node log correlation across SUT VM processes (Phase 3-4 runs multiple oceanfs processes on the SUT VM; journald unit naming handles this)

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Skill files under `.opencode/skills/`. |

## Interface (Public API)

Each skill is a Markdown file with command templates and return schemas.

### vm-test-phase
```markdown
# vm-test-phase

Run a load test phase on the two-VM topology (Harness VM targets SUT VM).

## Parameters
- `--phase N` (required): 1, 2, 3, or 4
- `--seed SEED`: deterministic seed (default: random)
- `--duration-secs N`: override test duration
- `--single-vm`: use single-VM co-located mode with relaxed gossip (Phase 2 only, Phase 3-4 warns)

## Topology
- Phase 1: local spawn on Harness VM (or CI runner; no TARGET_HOST)
- Phase 2: TARGET_HOST=<sut-ip>:9000 (single OceanFS process on SUT VM)
- Phase 3-4: TARGET_HOSTS=<sut-ip>:9000,<sut-ip>:9001,... (multiple OceanFS processes on SUT VM)

## Returns
{
  "phase": 2,
  "exit_code": 0,
  "report_path": "/tmp/2_load_sustained_20260810T100000.json",
  "grafana_url": "http://localhost:3000/d/load-test?var-phase=2",
  "duration_secs": 1800,
  "topology": "two-vm"
}
```

## Data Flow

```
Agent: vm-test-phase --phase 2 --seed 42 --duration-secs 300

  # Resolve SUT VM internal IP (from vm-status or stored state)
  → SUT_IP=10.0.0.5

  # Run harness on Harness VM, targeting SUT VM:
  → ssh oceanfs-harness "cd ~/ocean-fs && TARGET_HOST=10.0.0.5:9000 LOAD_TEST_SEED=42 LOAD_TEST_DURATION_SECS=300 cargo test -p e2e -- load_sustained --nocapture"
  → Test runs harness on Harness VM; harness connects to oceanfs at 10.0.0.5:9000
  → Harness writes LoadReport to /tmp/ on Harness VM
  → SSH command returns exit code 0
  → Returns { phase: 2, exit_code: 0, report_path: "/tmp/2_load_sustained_...", ... }

Agent: vm-test-phase --phase 3 --seed 42 (multi-node cluster churn)

  → SUT_IP=10.0.0.5
  → ssh oceanfs-harness "cd ~/ocean-fs && TARGET_HOSTS=10.0.0.5:9000,10.0.0.5:9001,10.0.0.5:9002 LOAD_TEST_SEED=42 cargo test -p e2e -- load_cluster_churn --nocapture"
  → Harness connects to 3 oceanfs processes on SUT VM

Agent: vm-results --phase 2

  → ssh oceanfs-harness "ls -t /tmp/2_* | head -1"
  → rsync oceanfs-harness:/tmp/2_load_sustained_20260810T100000.json ./local-results/
  → Parse JSON locally
  → Returns structured summary

Agent: vm-metrics --query "process_resident_memory_bytes"

  → ssh oceanfs-sut "curl -s 'http://localhost:9090/api/v1/query?query=process_resident_memory_bytes'"
  → Parse Prometheus API JSON response
  → Returns { resultType: "vector", result: [{ metric: {}, value: [1722859200, "87000000"] }] }

Agent: vm-logs --since "10 min ago" --level error

  → ssh oceanfs-sut "journalctl -u oceanfs --since '10 min ago' --no-pager | grep ERROR"
  → Returns [{ timestamp: "...", message: "..." }, ...]
```

## Definition of Done

- [ ] **Files:** `.opencode/skills/vm-test-phase.md` exists with parameters, topology description, and return schema
- [ ] **Files:** `.opencode/skills/vm-results.md` exists with parameters and return schema (fetches from Harness VM `/tmp`)
- [ ] **Files:** `.opencode/skills/vm-metrics.md` exists with parameters, return schema, and common query examples (targets SUT VM Prometheus)
- [ ] **Files:** `.opencode/skills/vm-logs.md` exists with parameters and return schema (targets SUT VM journald)
- [ ] **Validation:** `vm-test-phase` skill correctly constructs the SSH command with `TARGET_HOST` env var for Phase 2
- [ ] **Validation:** `vm-test-phase` skill correctly constructs the SSH command with `TARGET_HOSTS` env var for Phase 3-4
- [ ] **Validation:** `vm-test-phase` skill correctly handles Phase 1 (runs local spawn on Harness VM, no TARGET_HOST)
- [ ] **Validation:** `vm-test-phase` skill in `--single-vm` mode passes relaxed gossip env vars (`GOSSIP_INTERVAL_MS=3000`, etc.)
- [ ] **Validation:** `vm-results` skill correctly handles the case of no reports found (returns empty result, not error)
- [ ] **Validation:** `vm-metrics` skill correctly URL-encodes the PromQL query and parses the Prometheus API response
- [ ] **Validation:** `vm-logs` skill correctly formats `--since` argument for journalctl
- [ ] **Docs:** Each skill file includes example invocations with expected outputs
- [ ] **Docs:** `vm-test-phase` skill documents the topology difference between phases (Phase 1 local, Phase 2 TARGET_HOST, Phase 3-4 TARGET_HOSTS)
- [ ] **Integration:** Agent can: vm-test-phase 2 → vm-results 2 → vm-metrics --query "process_resident_memory_bytes" → analyze all results in a single workflow
