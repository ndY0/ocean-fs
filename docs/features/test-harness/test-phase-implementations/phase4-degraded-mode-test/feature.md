---
feature: "Phase 4 — Degraded Mode Under Load (Failure Injection) Test"
epic: "test-phase-implementations"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure/correctness-gaps
    reason: Need EC decode, hinted handoff delivery, heal worker, read repair all functional
  - epic: test-harness-extensions/manifest-tracker
    reason: Need Manifest for data integrity verification through failures
  - epic: test-harness-extensions/load-scenario-orchestrator
    reason: Need Worker framework for sustained load during failures
  - epic: test-harness-extensions/metrics-scraper
    reason: Need MetricsSnapshot for heal/hinted-handoff metrics
  - epic: test-harness-extensions/load-report
    reason: Need LoadReport for structured results output
  - epic: test-harness-extensions/failure-injectors
    reason: Need Cluster::inject_latency, fill_disk, corrupt_shard, corrupt_and_verify_heal
adr:
  - 0001-segment-packing
  - 0019-test-harness-topology-cost-guardrails
perf:
  - "8.1 FuturesUnordered for parallel shard fetches"
created: 2026-08-05
updated: 2026-08-10
---

# Phase 4 — Degraded Mode Under Load (Failure Injection) Test

## Summary

Implement `e2e/tests/load_degraded.rs` — a `#[tokio::test]` that validates system
behavior under scripted failure injections during sustained load. In the two-VM
topology (per ADR-0019), the harness runs on the dedicated Harness VM (CX22) and
connects to a 3-node OceanFS cluster on the SUT VM (CX32) via `TARGET_HOSTS`.
**All failure injectors run on the SUT VM only** — the Harness VM is completely
isolated from failure effects. `tc netem`, disk fill, and SIGKILL operations
target the SUT VM's OceanFS processes and cannot harm the harness. The harness
continues scraping metrics and writing reports through any failure scenario.
The harness **always** writes its JSON report to `/tmp` (tmpfs) so that the
disk-fill test (Scenario 3: fill disk to 95%) cannot prevent report output.

## Scope

### In Scope

- `#[tokio::test]` function in `e2e/tests/load_degraded.rs`
- Two topology modes (per ADR-0019):
  - **Two-VM (default):** Harness on Harness VM connects via `TARGET_HOSTS`. All failure injections target the SUT VM. Harness is isolated and safe.
  - **Single-VM (opt-in, NOT recommended):** Harness co-located with SUT. WARNING printed.
- Duration: 5-15 minutes (`LOAD_TEST_DURATION_SECS` env var, default 300s)
- Spawns or connects to 3-node cluster with standard config + shortened intervals
- Sustained background load throughout: PUT 40%, GET 50%, DELETE 10%, moderate concurrency (4 workers per node)
- **Report path:** Harness always writes LoadReport JSON to `/tmp` (tmpfs) on the Harness VM regardless of topology, per ADR-0019 Decision 4

#### Scenario 1: Mid-Write Kill
- PUT a known blob of 1MB to key `"degraded-test/mid-write-kill"`
- In parallel: kill one replica node **on the SUT VM** before it acks the PUT
- Assert: write completes with HTTP 200 (remaining W nodes satisfy quorum)
- Assert: killed node's hinted handoff buffer has the pending write (`hinted_handoff_hints_stored > 0`)
- Restart killed node; wait for heal/hinted-handoff delivery
- Assert: object readable from restarted node (hint was delivered)
- Assert: `manifest.verify()` passes for this key

#### Scenario 2: Slow-Node Test
- `cluster.inject_latency(node_1, 500)` — 500ms delay on one SUT VM node
- Run concurrent reads from all nodes for 30 seconds
- Assert: `s3_request_latency_seconds` p99 for reads < 600ms (fastest-k picks the faster nodes)
- Assert: `/admin/cluster` on all 3 nodes — node_1 is still ALIVE (not falsely detected as dead)
- Assert: SWIM failure detector does not trigger a SUSPECTED/DEAD event for node_1
- Cleanup: `cluster.remove_latency(node_1)`

#### Scenario 3: Disk-Full Simulation
- `cluster.fill_disk(node_0, 95)` — fill SUT VM disk to 95%
- **Isolation note:** Disk fill targets the SUT VM. Harness on Harness VM writes report to `/tmp` (tmpfs) — unaffected.
- Run writes to that node for 30 seconds
- Assert: writes to the full node fail gracefully — HTTP 503 or 507, NOT panic
- Assert: `process_resident_memory_bytes` on full node does not OOM-spike (>2×)
- Assert: reads from other nodes (node_1, node_2) for data also stored on node_0 still work — they serve from their own replicas
- Assert: GC does not panic on ENOSPC (if GC cycle triggers during fill) — verified by no crash
- Cleanup: delete fill file

#### Scenario 4: Corruption + Heal
- Write a known blob to the cluster, ensuring replication factor ≥ 2
- Identify a segment on node_0 containing the blob
- `cluster.corrupt_shard(node_0, segment_id)` — overwrite 64 random bytes (on SUT VM)
- Trigger anti-entropy: POST `/admin/trigger-anti-entropy` or wait for next scheduled tick
- Wait for heal to complete (poll `heal_requests_completed_total`)
- Assert: `heal_requests_total` incremented
- Assert: `heal_requests_failed_total` == 0
- Assert: `ae_mismatches_found_total` > 0 (the corruption was detected)
- Second anti-entropy pass on node_0: assert `ae_mismatches_found` does NOT increment further (heal fixed the corruption)
- Assert: blob readable from node_0 with correct content (BLAKE3 hash matches)

#### Cross-Cutting Assertions
- **manifest_integrity**: After all scenarios, `manifest.verify(&cluster)` → 0 mismatches
- **no_permanent_hint_loss**: `hinted_handoff_hints_stored - hinted_handoff_hints_delivered` → 0
- **no_cascading_failure**: One node's degradation does not cause surviving nodes to OOM, deadlock, or degrade
- **all_injections_succeeded**: Each `FailureInjectionRecord` has `success=true`
- **harness_unaffected**: In two-VM mode, the harness never crashes, times out, or fails to write its report due to SUT VM failures

### Out of Scope

- Network partition (iptables-based split) — deferred to future
- Clock skew injection — deferred
- Multi-failure overlap (kill + disk-full simultaneously) — deferred
- Automated retry of failed injections — each injection is scripted; if one fails, test records failure and continues to next

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New test file `tests/load_degraded.rs`. |

## Interface (Public API)

No new `pub` items — this is a `#[tokio::test]` function.

## Data Flow

```
Test: load_degraded

Topology detection:
  → If TARGET_HOSTS is set (cloud two-VM mode):
      connect to remote OceanFS processes on SUT VM
      all failure injectors target SUT VM processes/disk
      harness writes report to /tmp (tmpfs) on Harness VM
  → Else (local spawn, single-VM):
      spawn 3 NodeProcess locally
      failure injectors and harness co-located (NOT recommended)

Test flow:
  1. Parse LOAD_TEST_SEED, LOAD_TEST_DURATION_SECS, TARGET_HOSTS
  2. Connect to or spawn 3-node Cluster
  3. Start background load: Orchestrator with moderate concurrency
  4. Run Scenario 1 (mid-write kill):
        spawn write + kill task → assert write succeeds → restart → verify data
  5. Run Scenario 2 (slow-node):
        inject_latency on SUT VM node → run reads → assert latency bounded → assert no false-SUSPECT → remove_latency
  6. Run Scenario 3 (disk-full):
        fill_disk on SUT VM node → run writes → assert graceful failure → assert reads from others OK → remove fill file
        (Harness writes report to /tmp on Harness VM — unaffected by SUT disk fill)
  7. Run Scenario 4 (corruption + heal):
        write blob → corrupt_shard on SUT VM → trigger AE → wait for heal → verify data restored
  8. Stop background load; collect worker stats
  9. manifest.verify(&cluster) → 0 mismatches
  10. Cross-cutting assertions (no hint loss, no cascading failure, harness unaffected)
  11. Build LoadReport; write JSON to /tmp (tmpfs) + textfile
  12. assert!(report.result == Pass)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [ ] **Code:** Test file `e2e/tests/load_degraded.rs` compiles and links
- [ ] **Code:** Test supports remote-target mode (`TARGET_HOSTS` env var) and local-spawn mode
- [ ] **Tests:** `cargo test -p e2e -- load_degraded` passes on Linux (dedicated runner, local spawn)
- [ ] **Tests:** Remote target: `TARGET_HOSTS=10.0.0.5:9000,10.0.0.5:9001,10.0.0.5:9002 cargo test -p e2e -- load_degraded` passes on cloud two-VM
- [ ] **Tests:** Scenario 1: mid-write kill → write completes, hint delivered, data readable after restart
- [ ] **Tests:** Scenario 2: slow-node → reads bounded, SWIM no false-positive
- [ ] **Tests:** Scenario 3: disk-full → writes fail gracefully (HTTP 503/507), reads from other nodes work, harness report NOT affected (written to /tmp)
- [ ] **Tests:** Scenario 4: corruption + heal → corruption detected, heal reconstructs, second AE finds no mismatch
- [ ] **Tests:** Platform gating: on macOS, slow-node test skipped with clear warning, other 3 scenarios still run
- [ ] **Tests:** Cross-cutting: manifest integrity preserved through all failures
- [ ] **Tests:** No hint loss: hints stored ≈ hints delivered at end
- [ ] **Tests:** No panic throughout entire test run (monitored via child process exit status)
- [ ] **Tests:** In two-VM mode, harness never crashes or fails to write report due to SUT VM failures (verified by report completeness)
- [ ] **Tests:** LoadReport JSON written to `/tmp` (tmpfs) in all modes (two-VM and single-VM)
- [ ] **Docs:** Test doc comment explains each scenario, two-VM isolation guarantees, and expected behavior
- [ ] **Integration:** LoadReport contains per-scenario assertion blocks and failure injection records
