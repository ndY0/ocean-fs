---
feature: "Test Execution Skills — Agent Commands for Running Tests & Fetching Results"
epic: "agent-skills"
status: done
priority: high
owner: ""
dependencies:
  - epic: test-phase-implementations/phase1-concurrency-test
    reason: Need Phase 1 test to invoke
  - epic: test-phase-implementations/phase2-sustained-load-test
    reason: Need Phase 2 test to invoke
  - epic: test-phase-implementations/phase3-cluster-churn-test
    reason: Need Phase 3 test to invoke (pending)
  - epic: test-phase-implementations/phase4-degraded-mode-test
    reason: Need Phase 4 test to invoke (pending)
  - epic: test-harness-extensions/load-report
    reason: Need LoadReport format for vm-results parsing
  - epic: operational-tooling/prometheus-grafana-setup
    reason: Need Prometheus on SUT VM for vm-metrics PromQL queries
adr:
  - 0019-test-harness-topology-cost-guardrails
perf: []
created: 2026-08-05
updated: 2026-08-16
---

# Test Execution Skills — Agent Commands for Running Tests & Fetching Results

## Summary

Create four OpenCode skills under `.opencode/skills/` for executing load
tests and consuming results in the two-VM topology (per ADR-0019):
`vm-test-phase`, `vm-results`, `vm-metrics`, and `vm-logs`. `vm-test-phase`
runs the harness on the **Harness VM** with `TARGET_HOST` pointing at the
SUT VM's internal IP via `scripts/run-phase2.sh --harness` — the harness
connects to the already-running OceanFS process instead of spawning it
locally, and the report is fetched back to the laptop automatically.
`vm-results` reads the fetched LoadReport JSON (or rsyncs it from the
Harness VM's `/tmp/oceanfs-reports`). `vm-metrics` executes PromQL queries
against the **persistent laptop Prometheus** (`localhost:9091` — federates
the SUT VM's Prometheus through the observe.sh tunnel, 365-day retention)
with the live SUT path (tunnel or direct SSH) as fallback. `vm-logs` fetches
journald logs from the **SUT VM**.

**Gap closure (2026-08-16):** The skills are built against the actual
script interfaces after the 2026-08-15/16 script refactor:

| Skill | Backing script(s) | Notes vs. original design |
|---|---|---|
| `vm-test-phase` | `scripts/run-phase2.sh --harness HOST --sut INT:9000 --ssh root@INT` | Replaces hand-rolled `ssh harness "TARGET_HOST=... cargo test ..."`: the script builds, runs with the right env (`TARGET_HOST`, `TARGET_HOST_SSH`, `TARGET_SERVICE`, seed, duration), pushes `load_test.prom` into the SUT Prometheus, and fetches the report back. Only Phase 2 is implemented today (phases 3-4 pending) |
| `vm-results` | `LoadReport` at `/tmp/oceanfs-reports` (tmpfs) | Report filenames `{phase}_{test}_{timestamp}.json`; local-first, rsync fallback from the harness; no-report returns an explicit empty result |
| `vm-metrics` | persistent laptop Prometheus `curl localhost:9091/api/v1/query` (historical, 365d) + live path via the observe.sh tunnel `localhost:9090` or direct SSH curl on the SUT | URL-encoding via `--data-urlencode`; documents the actual metric names (`accel_ec_fallback_total`, `segment_active_count`, `cache_hits_total{tier="l1"}`, …) |
| `vm-logs` | `journalctl -u oceanfs` on the SUT | unchanged in spirit; `--level error` filter, `--since` window |

## Scope

### In Scope

#### `vm-test-phase` (`.opencode/skills/vm-test-phase/SKILL.md`)
- Resolve topology from the provisioning record (SUT internal IP, Harness
  public IP); verify VMs are running first
- **Phase 2** (implemented): run via
  `scripts/run-phase2.sh --harness root@<harness-public> --quick|--full
  --sut <sut-internal>:9000 --ssh root@<sut-internal> --service oceanfs
  --seed N [--report-dir DIR]`
  - `--quick` = 300s, `--full` = 3600s; the duration is overridable via the
    `LOAD_TEST_DURATION_SECS` env var (exported by the skill; run-phase2.sh
    forwards it to the harness-side invocation)
  - Crash-recovery runs over SSH (SIGKILL → systemctl restart; the SUT unit
    is `Restart=no` by design)
  - Pushes the `load_test.prom` textfile to the SUT Prometheus (best-effort)
  - Fetches `2_load_sustained_*.json` back to the laptop report dir
- **Phase 1**: runs in CI (local spawn) — no VM invocation
- **Phases 3-4**: not yet implemented (no `run-phase3.sh`/`load_cluster_churn`
  remote support yet) — the skill says so instead of faking a run
- Returns: `{phase, mode, seed, duration_secs, topology: "two-vm", exit_code, report_path, harness_report_path, grafana_url}`

#### `vm-results` (`.opencode/skills/vm-results/SKILL.md`)
- Look for the report locally first (`/tmp/oceanfs-reports` or
  `--report-dir`); rsync from the Harness if missing
- Parse the LoadReport JSON with `jq` and return a structured summary:
  phase, test, seed, result (`pass`/`fail`), duration, per-assertion
  outcomes, failed assertions, manifest summary
  (`objects_written`, `objects_verified`, `mismatches`), harness
  self-metrics
- No reports found → explicit `{result: "no_report"}` — not an error

#### `vm-metrics` (`.opencode/skills/vm-metrics/SKILL.md`)
- Query the **persistent laptop Prometheus** (default; historical across
  destroyed VMs): `curl -sG 'http://localhost:9091/api/v1/query' --data-urlencode "query=..."`
  (365-day retention; the SUT Prometheus is federated into it through the
  observe.sh tunnel)
- Live/full-fidelity path: ensure the observe.sh tunnel (idempotent), then
  `curl -sG 'http://localhost:9090/api/v1/query' --data-urlencode "query=..."`
  (direct SSH curl on the SUT as fallback)
- Support `query_range` for time windows (start/end/step)
- Document common queries with the actual metric names:
  `process_resident_memory_bytes`, `process_open_fds`, `wal_file_count`,
  `rocksdb_num_files_at_level_0`, `segment_seal_errors_total`,
  `accel_ec_fallback_total`, `segment_active_count`, L1 cache hit-rate
  PromQL, `load_test_phase`
- Returns: parsed `{resultType, result}` + human summary

#### `vm-logs` (`.opencode/skills/vm-logs/SKILL.md`)
- SSH to the SUT: `journalctl -u oceanfs --since "{since}" --no-pager -n {lines}`
- Accepts: `--since "10 min ago"` (default), `--level error`
  (grep ERROR/FATAL/PANIC), `--lines N` (default 200)
- Returns: `[{timestamp, message}, ...]` + line count

### Out of Scope

- Grafana screenshot generation (laptop Grafana UI for visual inspection)
- Log streaming (tail -f) — one-shot queries only
- Metric alerting (Prometheus AlertManager is not configured on the VM)
- Multi-node log correlation (Phase 3-4 multi-process SUT — pending)

## Crate Impact

| Crate | Change |
|---|---|
| (none) | Skill files under `.opencode/skills/<name>/SKILL.md`. |

## Interface (Public API)

Each skill is an OpenCode skill: `.opencode/skills/<name>/SKILL.md` with
frontmatter (`name`, `description`) and a markdown body of exact commands,
return schemas, error conditions, and examples.

### Return schemas

```
vm-test-phase → { phase: 2, mode: "quick"|"full", seed, duration_secs, topology: "two-vm",
                  exit_code, report_path, harness_report_path, grafana_url, stderr_tail }
vm-results    → { report_path, phase, test, seed, result: "pass"|"fail"|"no_report",
                  duration_secs, assertions: [{name, passed}], failed_assertions,
                  manifest: {objects_written, objects_verified, mismatches}, harness_metrics }
vm-metrics    → { query, endpoint, resultType: "vector"|"matrix", result: [...], summary }
vm-logs       → { service, sut, since, lines, logs: [{timestamp, message}] }
```

## Data Flow

```
Agent: vm-test-phase --phase 2 --seed 42 (quick)
  → record: SUT_INT=10.0.0.5, HARNESS_PUB=1.2.3.5
  → ./scripts/run-phase2.sh --harness root@1.2.3.5 --quick
        --sut 10.0.0.5:9000 --ssh root@10.0.0.5 --service oceanfs --seed 42
  → harness: TARGET_HOST=10.0.0.5:9000 TARGET_HOST_SSH=root@10.0.0.5 cargo test -p e2e --test load_sustained
  → harness: writes /tmp/oceanfs-reports/2_load_sustained_*.json + load_test.prom
  → harness → SUT: scp load_test.prom → /var/lib/prometheus/textfile/
  → laptop: report fetched to /tmp/oceanfs-reports/
  → returns { phase: 2, exit_code: 0, report_path: "...", ... }

Agent: vm-results --phase 2
  → ls -t /tmp/oceanfs-reports/2_load_sustained_*.json | head -1
  → jq parse → returns structured summary (result, assertions, manifest)

Agent: vm-metrics --query "process_resident_memory_bytes"
  → curl -sG http://localhost:9091/api/v1/query --data-urlencode 'query=...'   (persistent store)
  → returns { resultType: "vector", result: [...], summary: "RSS = 830 MiB" }

Agent: vm-logs --since "10 min ago" --level error
  → ssh root@<sut> "journalctl -u oceanfs --since '10 min ago' --no-pager -n 200 | grep -iE 'error|fatal|panic'"
  → returns [{ timestamp, message }, ...]
```

## Definition of Done

- [x] **Files:** `.opencode/skills/vm-test-phase/SKILL.md` exists with parameters, topology description, and return schema
- [x] **Files:** `.opencode/skills/vm-results/SKILL.md` exists with parameters and return schema (local-first, rsync fallback from Harness `/tmp`)
- [x] **Files:** `.opencode/skills/vm-metrics/SKILL.md` exists with parameters, return schema, and common query examples (targets SUT VM Prometheus)
- [x] **Files:** `.opencode/skills/vm-logs/SKILL.md` exists with parameters and return schema (targets SUT VM journald)
- [x] **Validation:** `vm-test-phase` skill constructs the invocation via `run-phase2.sh --harness` with `TARGET_HOST` env (SUT internal IP) for Phase 2
- [x] **Validation:** `vm-test-phase` skill correctly handles Phase 1 (CI local spawn, no VMs) and reports phases 3-4 as not yet implemented
- [x] **Validation:** `vm-results` skill handles the no-reports case (returns `no_report`, not error)
- [x] **Validation:** `vm-metrics` skill URL-encodes the PromQL query (`--data-urlencode`) and parses the Prometheus API response
- [x] **Validation:** `vm-logs` skill formats `--since` for journalctl and supports `--level error`
- [x] **Docs:** Each skill file includes example invocations with expected outputs
- [x] **Docs:** `vm-test-phase` skill documents the topology difference between phases (1 = CI local, 2 = TARGET_HOST, 3-4 = TARGET_HOSTS pending)
- [x] **Integration:** Agent can: vm-test-phase 2 → vm-results 2 → vm-metrics --query "process_resident_memory_bytes" → analyze all results in a single workflow

## Accepted Deviations (gap closure)

1. **`vm-test-phase` delegates to `run-phase2.sh`.** The original
   hand-rolled `ssh harness "TARGET_HOST=... cargo test ..."` recipe is
   replaced by the script that handles env wiring, textfile pushing, and
   report fetching. Only Phase 2 is implementable today — phases 3-4 are
   declared pending rather than emulated.
2. **Single-VM gossip relaxation is not in the skill.** The original spec
   passed `GOSSIP_INTERVAL_MS`/`SUSPICION_TIMEOUT_MS`/`FAILURE_TIMEOUT_MS`
   for `--single-vm` mode; the current Phase 2 implementation is
   single-node with gossip disabled (phase2 feature, deviation note), so
   no gossip env vars exist to pass. Revisit when Phase 3-4 land.
3. **`vm-metrics` prefers the persistent laptop Prometheus** (localhost:9091,
   365-day retention, federates the SUT through the tunnel — the same
   datasource Grafana consumes, and it works after the SUT is destroyed).
   The tunnel (localhost:9090) and direct SSH remain the live/full-fidelity
   paths.
4. **Report dir.** `/tmp/oceanfs-reports` (subdir of tmpfs,
   `LOAD_TEST_REPORT_DIR`-overridable) — ADR-0019 Decision 4 compliant.
