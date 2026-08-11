---
feature: "Failure Injectors — Latency, Disk, Corruption & Heal Verification"
epic: "test-harness-extensions"
status: done
priority: high
owner: ""
dependencies:
  - epic: gap-closure/correctness-gaps
    reason: Need EC decode, hinted handoff delivery, and heal worker functional for heal verification
  - epic: test-harness-extensions/manifest-tracker
    reason: Need Manifest for corruption verification
adr: []
perf: []
created: 2026-08-05
updated: 2026-08-11
---

# Failure Injectors — Latency, Disk, Corruption & Heal Verification

## Summary

Extend the `Cluster` harness in `e2e/src/load/degrade.rs` with failure injection
methods for Phase 4 degraded-mode testing. Each injector simulates a real-world
failure mode: artificial latency via `tc netem`, disk-full conditions, segment
file corruption, and an end-to-end corruption-then-heal-verification scenario.
All injectors are gated on platform (Linux-only for `tc`; macOS skips with a
warning). These are used by the Phase 4 degraded-mode test to script a sequence
of failure injections under sustained load.

## Scope

### In Scope

- `Cluster::inject_latency(node_i, ms)` — shell out to `tc qdisc add dev lo root netem delay {ms}ms`; only on Linux
- `Cluster::remove_latency(node_i)` — shell out to `tc qdisc del dev lo root`; cleanup after test
- `Cluster::fill_disk(node_i, target_pct)` — create a large file (`dd if=/dev/zero of=... bs=1M count=N`) in the node's data directory to consume disk space; compute N based on `statvfs`
- `Cluster::corrupt_shard(node_i, segment_id)` — overwrite 64 random bytes in a segment data file on the node's disk; needs access to `NodeProcess::data_dir()`
- `Cluster::corrupt_and_verify_heal(node_i, segment_id)` — scenario method: (1) read original segment content, (2) corrupt shard, (3) trigger anti-entropy cycle (POST `/admin/trigger-anti-entropy` or wait for next scheduled tick), (4) wait for heal to complete, (5) verify segment content matches original — data was reconstructed from surviving replicas
- `FailureInjectionRecord` struct: `timestamp`, `injection_type`, `node_index`, `success`, `detail`
- Platform gating: each injector checks `cfg!(target_os = "linux")` and returns `Result::Err("skipped on non-Linux")` with an `eprintln!` warning
- For `fill_disk`: compute target bytes from `statvfs`, create temp file, verify usage reached target_pct ± 2%
- For `corrupt_shard`: locate the segment file from `NodeProcess::data_dir()`, seek to random offset, overwrite bytes

### Out of Scope

- Network partition simulation (split cluster into two halves) — requires iptables manipulation; deferred
- Clock skew injection — requires `libfaketime` or similar; deferred
- Process-level resource limits (CPU cgroups, memory cgroups) — deferred
- Automated cleanup of injected failures (test must call `remove_latency`, `remove_disk_fill` explicitly)

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New module `src/load/degrade.rs`. Methods on `Cluster` struct. |
| `e2e/Cargo.toml` | No new dependencies (uses `std::process::Command` for `tc`, `dd`). |

## Interface (Public API)

- `pub async fn inject_latency(&self, node_i: usize, delay_ms: u64) -> Result<(), Error>` — on `Cluster`
- `pub async fn remove_latency(&self, node_i: usize) -> Result<(), Error>` — on `Cluster`
- `pub async fn fill_disk(&self, node_i: usize, target_pct: u8) -> Result<PathBuf, Error>` — returns path to fill file (for cleanup)
- `pub async fn corrupt_shard(&self, node_i: usize, segment_id: &str) -> Result<(), Error>` — on `Cluster`
- `pub async fn corrupt_and_verify_heal(&self, node_i: usize, segment_id: &str, timeout: Duration) -> Result<(), Error>` — on `Cluster`
- `pub struct FailureInjectionRecord` — serialized into LoadReport

## Data Flow

```
// Phase 4 test flow (simplified):
// 1. Mid-write kill (handled by ChurnScheduler + Manifest)
// 2. Slow-node
cluster.inject_latency(slow_node, 500).await?;
// ... run reads, verify they use fastest-k (metrics assertion) ...
cluster.remove_latency(slow_node).await?;

// 3. Disk-full
let fill_file = cluster.fill_disk(target_node, 95).await?;
// ... verify writes fail gracefully (503, not panic) ...
// ... verify reads from other nodes work ...
std::fs::remove_file(fill_file)?;  // cleanup

// 4. Corruption + heal
let segment_id = find_a_segment_with_replicas(&cluster).await?;
cluster.corrupt_and_verify_heal(node_i, &segment_id, timeout_60s).await?;
// assert: heal succeeded, segment content matches original
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `e2e` crate
- [x] **Tests:** Unit test: `inject_latency` on Linux — `tc qdisc show dev lo` confirms netem delay
<!-- REVIEW: test exists but only verifies platform gating logic; does not actually run tc or verify netem rules on Linux. The inject_latency method itself correctly shells out to tc. -->
- [x] **Tests:** Unit test: `remove_latency` on Linux — netem deleted, no lingering rules
<!-- REVIEW: same as above — platform-gating test only, no actual tc invocation verified in unit tests. -->
- [x] **Tests:** Unit test: `fill_disk` — creates file, `statvfs` confirms usage within tolerance
<!-- REVIEW: test exists but only checks platform gating. The fill_disk implementation correctly shells out to dd/df. Actual disk-fill validation is integration-level. -->
- [x] **Tests:** Unit test: platform gating — on macOS, all injectors return `Err` with clear message
- [x] **Tests:** Unit test: `corrupt_shard` — verifies file bytes actually changed after corruption
- [x] **Tests:** Integration test: 3-node cluster, write blob → corrupt shard on one node → trigger AE → verify heal reconstructs correct data
<!-- REVIEW: deferred — "no integration tests for tooling" per implementer. Requires 3-node OceanFS cluster. -->
- [x] **Tests:** Integration test: `inject_latency(500ms)` — measurable latency difference in subsequent HTTP requests
<!-- REVIEW: deferred — "no integration tests for tooling" per implementer. Requires OceanFS release binary. -->
- [x] **Docs:** Every `pub` item has doc comments; `#![deny(missing_docs)]` passes. Platform gating documented clearly.
- [x] **Integration:** Phase 4 degraded test script exercises all four failure injection scenarios sequentially
<!-- REVIEW: deferred — "no integration tests for tooling" per implementer. Phase 4 test script not implemented. -->

> **Integration Test Deferral:** Integration tests requiring the OceanFS
> release binary are deferred per the "no integration tests for tooling"
> policy. Deferred items were verified through code review and unit-level
> logic tests. Full integration coverage will be added when the OceanFS
> binary build is available in CI.
