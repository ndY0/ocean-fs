---
feature: "load_degraded — Fleet-Ready Phase 4 Degraded Mode Under Load"
epic: "fleet-degradation"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: f1-volume-backed-fleet-topology
    reason: Scenarios target real volumes/mounts; the sysfs-yank verdict defines the hard-failure mechanism
  - feature: f2-remote-fault-injectors
    reason: Needs SSH yank/replug, per-volume fill, remote segment corruption, internal-interface tc, and POST-with-body
  - epic: test-harness-extensions
    reason: Manifest, Worker/orchestrator, MetricsSnapshot and LoadReport infrastructure (Epic 1) are consumed unchanged
adr:
  - 0026-phase3-dedicated-node-vms
  - 0029-storage-pools-disk-resilience
  - 0030-re-replication-target-pull
  - 0035-replicated-segment-lifecycle-state
  - 0036-phase-c-scale-ops-drain
  - 0019-test-harness-topology-cost-guardrails
perf: []
created: 2026-09-11
updated: 2026-09-11
---

# load_degraded — Fleet-Ready Phase 4 Degraded Mode Under Load

## Summary

Implement `e2e/tests/load_degraded.rs` for the **ADR-0026 fleet topology**
(N dedicated node VMs + CX43 harness, real volumes per pool role) and add
`scripts/run-phase4.sh` following the `run-phase3.sh` pattern (harness-mode
SSH execution, `TARGET_HOSTS`/`TARGET_HOST_SSH`, report fetch,
`observe.sh`/`backup-observability.sh`). The test keeps the **four original
degraded-mode scenarios** — mid-write kill, slow node, disk-full,
corruption+heal — re-grounded on the fleet:

- kill = **VM-level service kill** over SSH on a dedicated node VM;
- slow node = `tc netem` on the node's **internal interface** (f2), not `lo`;
- disk-full = fill the node's **data volume mount** (f2), not the VM root;
- corruption = remote `.dat` corruption on the target role volume (f2),
  followed by the real heal/scrub path;
- all constructs that scanned a local `data_dir` or assumed co-location are
  dropped.

**This feature supersedes
`docs/features/test-harness/test-phase-implementations/phase4-degraded-mode-test/feature.md`**
(written for ADR-0019's 3-co-located-processes-on-one-SUT topology, never
implemented). The old doc is marked superseded/cancelled in place with a
pointer to this feature — no two conflicting specs remain.

**Correctness only — no performance claims.** Volume-backed runs cross
network block storage; latency/throughput results are **not comparable** to
local-disk runs and **no scenario asserts a latency or throughput bound**.
The slow-node scenario's original p99 < 600 ms assertion is **dropped** (it
would measure the injection, not the system); soft-degradation *behavior*
(health classification under `dm-delay`, Degraded vs Dead) is deferred to a
follow-up feature.

## Scope

### In Scope

- `#[tokio::test]` `e2e/tests/load_degraded.rs`:
  - Two target modes:
    - **Fleet (primary, Phase 4):** `TARGET_HOSTS` = N node endpoints,
      `TARGET_HOST_SSH` = N SSH targets; connects to the already-running
      fleet; all injections go through the **f2 injectors** (SSH black-box);
      report always written to `/tmp` (tmpfs) on the Harness VM (ADR-0019
      Decision 4 rationale retained).
    - **Local spawn (dev/CI quick mode):** a local cluster where feasible;
      device/network injectors skip with a recorded warning (never a silent
      success). The test still runs disk-fill and corruption on local role
      roots.
  - Duration: `LOAD_TEST_DURATION_SECS` (default 300s; quick mode 120s).
  - Background load: PUT 40% / GET 50% / DELETE 10%, moderate concurrency;
    manifest verification at the end.
  - Scenario 1 — **Mid-Write Kill (VM-level):**
    - Write a known blob while `systemctl kill -s KILL --kill-who=main oceanfs`
      on one **dedicated node VM** (existing
      `kill_and_restart_node_via_ssh` semantics; the VM and its data
      directory persist).
    - Assert: the write completes (quorum satisfied by the remaining nodes);
      the killed node is eventually seen as down then rejoins; hints/repair
      reconverge; the object is readable with correct bytes from every node
      after restart; manifest verifies.
    - **Grounding-required before implementation:** confirm the hinted-handoff
      metric set on a live node. The code registers
      `hinted_handoff_hints_stored_total` / `..._delivered_total` (the old doc
      dropped the `_total`), but the deployed `/admin/metrics` output is the
      authority. The old doc's trigger endpoint
      `POST /admin/trigger-anti-entropy` **does not exist** in the current
      admin surface (the existing local `corrupt_and_verify_heal` already
      falls back when it 404s). Use `POST /admin/scrub` (202) and/or wait for
      the configured AE interval (`ae_interval_sec`).
  - Scenario 2 — **Slow Node (degradation is survivable):**
    - `inject_latency(node, internal_iface, 500ms)` via f2; run concurrent
      reads/writes for a bounded window; assert (a) the cluster keeps serving
      with correct bytes, (b) the slow node is **not** evicted from the ring
      for latency alone (membership shows it Alive when probes complete under
      the configured suspicion window), (c) after `remove_latency` the node is
      a normal peer again.
    - **No latency bound asserted.** If the node's scheduler cannot complete
      SWIM probes under the injection and it is legitimately suspected, the
      scenario records the observed membership timeline as evidence and
      asserts only the no-data-loss/no-cascade properties — the health
      classifier's soft-degradation semantics are the follow-up feature.
  - Scenario 3 — **Disk-Full (on the data volume):**
    - `fill_volume(node, "data", 95)` (f2) — targets `/mnt/oceanfs-data`
      only; the node root fs and the harness report path are unaffected.
    - Run writes to that node for a bounded window.
    - Assert: writes fail gracefully (HTTP 503/507 or equivalent, **no
      panic/crash** — process stays up and `/admin/health` keeps answering);
      reads of data also stored on that node still serve from other replicas
      (or fail over) with correct bytes; no OOM spike beyond the recorded
      pre-fill baseline (recorded as evidence, not an absolute threshold);
      GC/reaper do not delete live data during ENOSPC (this is the
      `4c0d216` bug class — verified via manifest/read checks and reaper
      counters).
    - Cleanup: `remove_fill`; assert free space returns; the node accepts
      writes again (recovery).
  - Scenario 4 — **Corruption + Heal (remote segment):**
    - Write a known blob with RF ≥ 2; discover a live segment id on the
      target node's data volume (f2 `list_segment_ids`); corrupt 64 random
      bytes (f2); trigger `POST /admin/scrub` and/or wait for the AE cycle;
      poll the corrupted node's read until the original bytes are returned.
    - Assert: the corruption is detected and repaired from surviving
      replicas; a second verification pass finds no further mismatch; the
      blob hash matches the manifest; no other key is lost (the repair is
      surgical).
    - **Grounding-required before implementation:** the exact
      mismatch/heal metric names. The old doc cited unsuffixed names; the code
      registers `hinted_handoff_hints_stored_total` / `..._delivered_total`
      (durability), `heal_requests_total` / `heal_completed_total` /
      `heal_failed_total` (core), and `ae_mismatches_found_total`
      (durability). Re-verify each is actually registered on a deployed node
      (`/admin/metrics`) before freezing the assertion; the current admin
      surface also exposes no `trigger-anti-entropy` route, so use
      `POST /admin/scrub` and/or the AE interval. If a metric is not present
      in the deployed build, assert via read-back and `/admin/segments`
      counts instead and record the finding.
  - **Cross-cutting assertions:**
    - manifest integrity across the whole run (0 mismatches on verified
      keys);
    - every injection produced a `FailureInjectionRecord` with the correct
      node/role attribution; failed/skipped injections fail the
      `all_injections_succeeded` assertion unless the scenario is
      explicitly platform-skipped and marked as such;
    - no cascading failure: surviving nodes stay healthy, no unhandled
      panic in any node log (`grep_logs`-style scan);
    - harness unaffected: each scenario's report data is complete
      (written to the Harness VM `/tmp`);
    - **no perf assertion anywhere** (reviewer-checkable: the report may
      contain latency *observations*, never threshold assertions).
- `scripts/run-phase4.sh` (patterned on `run-phase3.sh`):
  - Modes `--quick`/`--full`; `--harness HOST` harness-mode SSH payload
    execution; `--nodes IPs`, `--ssh LIST`, `--service`, `--seed`,
    `--report-dir`;
  - Forwards the provisioning record path (or `LOAD_TEST_VOLUMES_JSON`) so
    the f2 injectors can resolve devices/mounts; fails fast with a clear
    message when `--volume-pools` data is absent for a scenario that needs
    it;
  - `observe.sh` tunnel best-effort + textfile push to node-0 Prometheus +
    report `scp` fetch + `backup-observability.sh` (unchanged Phase 3
    pattern);
  - Refuses to run without `TARGET_HOSTS` in harness mode unless the local
    fallback is explicitly requested.
- DoD/report additions: `LoadReport` carries per-scenario assertion blocks,
  the injection records, and an explicit `perf_assertions: none` note.

### Out of Scope (for this feature)

- **Soft degradation** (`dm-flakey` error injection, `dm-delay` latency) and
  Degraded-vs-Dead health-classifier assertions — follow-up feature, with
  rationale recorded in the epic.
- **Pool-role hard-failure matrix** (WAL boot variant, metadata rebuild,
  data-pool yank, hints, dynamic attach/drain/detach) — **f4**.
- Network partitions, clock skew, multi-failure overlap.
- Any change to product code (Option A: SSH black-box only).
- Migrating Phases 2/3; modifying `load_cluster_churn`/`load_sustained`.

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New `tests/load_degraded.rs`; consumes `e2e::load::fleet_degrade` (f2). No library changes beyond what f2 adds. |
| `scripts` | New `scripts/run-phase4.sh` (modeled on `run-phase3.sh`). |
| `.opencode/skills/vm-test-phase` | Phase-4 invocation documentation (flag/env contract, no local load runs). |

## Interface (Public API)

No new library `pub` items. The filesystem surface this feature produces:

- `e2e/tests/load_degraded.rs` — the `#[tokio::test]` entry point; supports
  `TARGET_HOSTS`, `TARGET_HOST_SSH`, `TARGET_SERVICE`,
  `LOAD_TEST_DURATION_SECS`, `LOAD_TEST_SEED`, `LOAD_TEST_VOLUMES_JSON`/record
  path, `LOAD_TEST_LATENCY_IFACE`.
- `scripts/run-phase4.sh` — CLI identical in spirit to `run-phase3.sh`:
  `--quick|--full`, `--harness HOST`, `--nodes IPLIST`, `--ssh LIST`,
  `--service NAME`, `--seed N`, `--report-dir DIR`, `--report PATH`
  (optional record path forwarding), `--no-injections` (fleet smoke without
  failures), and the phase-4 env passthrough set.
- Report artifact: `LoadReport` JSON (`4_load_degraded_*.json`) with
  `injection_records` + per-scenario assertion blocks — consumed by
  `vm-results` and f4's cross-run comparison.

## Data Flow

```
./scripts/run-phase4.sh --harness oceanfs-harness --full \
    --nodes 10.0.0.2,10.0.0.3,10.0.0.4 --ssh root@10.0.0.2,... \
    --report .hetzner/provision-oceanfs-loadtest-4.json
  ├─ observe.sh tunnel + backup-observability.sh (Phase 3 pattern)
  ├─ ssh harness: cd /root/ocean-fs && run-phase4.sh --full --nodes ... --ssh ...
  │    └─ cargo test -p e2e --release --test load_degraded -- --test-threads=1
  │         ├─ RemoteCluster::connect(TARGET_HOSTS)
  │         ├─ FleetInjector::new(cluster, record)          ← f2
  │         ├─ background load (Orchestrator + Manifest)
  │         ├─ S1: kill VM B via SSH → write completes → restart → heal → verify
  │         ├─ S2: tc on B's internal iface → bounded load → remove → membership recovers
  │         ├─ S3: fill B:/mnt/oceanfs-data → writes degrade gracefully → drain fill → recovers
  │         ├─ S4: corrupt segment on B:data → scrub/AE → read-back heals → verify
  │         ├─ manifest.verify → 0 mismatches
  │         └─ LoadReport → /tmp/oceanfs-reports (Harness VM tmpfs)
  ├─ scp report back to laptop $REPORT_DIR
  └─ exit with the harness run's exit code
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e`; the new test
      compiles for both target modes; `scripts/run-phase4.sh` passes
      `shellcheck` and `--help`.
- [ ] **Tests:** `cargo test -p e2e --lib` passes (any helper units). The
      load suite itself runs only on the cloud harness (PIPELINE §6).
- [ ] **Tests:** fleet run in harness mode passes all four scenarios with
      0 manifest mismatches; each scenario's assertions and injection records
      are present in the report; a second run with `--no-injections`
      (control) also passes, confirming the load path itself is clean.
- [ ] **Tests:** local-spawn quick mode runs disk-fill + corruption and
      records the device/network injectors as skipped (no silent success).
- [ ] **Docs:** every `pub` item in any helper has `# Examples` and
      `#![deny(missing_docs)]` holds in `e2e`; the test's module doc states
      the fleet topology, the hard-yank-vs-graceful classification per
      scenario, and the **no-performance-assertion / non-comparability**
      rule.
- [ ] **ADR:** ADR-0026 (fleet topology; no co-located-process assumptions
      remain), ADR-0029/0031 (real volumes, pools mandatory and role-pinned),
      ADR-0030/0035 (heal path is the real target-pull/rebuild machinery),
      ADR-0036 (no drain/detach mutation in this feature), ADR-0019
      (report on tmpfs; guardrails retained) satisfied.
- [ ] **Perf:** `perf: []`; no threshold assertion exists in the test or the
      runner; any latency observation is recorded as data only. The runner
      does not add harness-side load beyond the existing Phase 3 pattern.
- [ ] **Integration:** `run-phase4.sh` full chain — harness SSH execution,
      injectors over SSH, report fetch, observe/backup — exercised on the
      cloud fleet; the old phase4-degraded-mode doc is marked superseded
      with a pointer to this feature (no conflicting spec left).
- [ ] **Deviations:** the metric-name findings (hinted-handoff, heal/AE),
      the SWIM behavior observed in Scenario 2, the corruption targeting
      rule, and any scenario re-scoped by volume reality are recorded here.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **Metric names (grounding-required).** The old phase4 doc's assertions
  cite unsuffixed metric names and an endpoint that is absent:
  `POST /admin/trigger-anti-entropy` does not exist in
  `oceanfs-server/src/admin.rs` today. The code-side metric registrations
  verified 2026-09-11 are `hinted_handoff_hints_stored_total` /
  `..._delivered_total`, `heal_requests_total` / `heal_completed_total` /
  `heal_failed_total`, and `ae_mismatches_found_total` — confirm them on a
  deployed node's `/admin/metrics` and freeze the assertion names in this doc
  before coding. Same class of finding as the f2 OQ.
- **Scenario 2 membership policy.** Decide the assertion when SWIM legitimately
  suspects a node under injected latency (see Scope), and record the observed
  timeline. Do not silently convert it into a pass/fail on timing.
- **`TARGET_HOST_SSH` cardinality.** Phase 3 skips churn when the SSH list is
  unset; Phase 4 must **fail fast** for scenarios that require kill/injection
  rather than silently skipping (record which scenarios are impossible).
- **Record delivery to the harness.** Confirm the runner copies the
  provisioning record (or passes `LOAD_TEST_VOLUMES_JSON`) before the test
  starts; document the env var in `run-phase4.sh --help` and the
  `vm-test-phase` skill.
- **Old-doc disposition mechanics.** The repo has no `superseded` status in
  the template enum; use `status: cancelled` + a banner pointing at this
  feature (and update the test-harness README row). Do not delete the file.

## Deviations (accepted)

_None yet — filled at implementation close._
