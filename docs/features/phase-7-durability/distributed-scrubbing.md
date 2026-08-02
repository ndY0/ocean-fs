---
feature: "Distributed Scrubbing"
epic: "phase-7-durability"
status: in_progress
priority: medium
owner: ""
dependencies:
  - feature: anti-entropy-merkle
    reason: Scrubbing is the full-scan complement to anti-entropy's incremental check
  - feature: ec-codec-trait-cauchy-rs
    reason: Scrubbing verifies BLAKE3 + Merkle root; heals via EC decode
  - feature: ec-heal-dispatch
    reason: Scrub enqueues corrupt segments into HealQueue for EC-based repair
adr: []
perf:
  - "2.6: Bounded channels for scrub work distribution"
  - "2.7: Tokio semaphore for concurrency limits"
  - "8.5: Bounded semaphore for task concurrency"
created: 2026-07-30
updated: 2026-08-02
---

# Distributed Scrubbing

## Summary

Implement distributed scrubbing in `oceanfs-storage`. Unlike anti-entropy's
peer-to-peer incremental check, scrubbing is a full cluster-wide scan of every
segment, verifying BLAKE3 hashes and Merkle roots. A randomly elected
coordinator partitions the segment ID space across all healthy nodes. Each node
scrubs its partition, reports discrepancies, and auto-heals via EC decode. The
coordinator aggregates results into a scrub report.

## Scope

### In Scope
- `ScrubCoordinator`: elected per scrub cycle, partitions segment ID space
- Partition assignment: consistent hashing over segment IDs → assign ranges to nodes
- `ScrubWorker`: per-node task that reads assigned segment shards, verifies Merkle roots, enqueues corrupt segments for healing
- Verification: BLAKE3 hash of shard data vs stored hash; Merkle root vs recomputed
- Discrepancy handling: on mismatch → enqueue segment for healing via `HealQueue`
- Scrub report: aggregate per-node results → total segments, mismatches, healed, bytes scanned
- Configurable: `scrub_interval_sec` (default 7 days), `scrub_parallel_nodes` (0 = all)
- Throttling: `heal_throttle_bytes_sec` limits repair bandwidth
- Admin API integration: `POST /admin/scrub` triggers manual scrub
- Unit tests for partition assignment, verification logic, report aggregation

### Out of Scope
- Real-time scrubbing (periodic only; continuous verification via anti-entropy)
- Scrub scheduling across maintenance windows
- Scrubbing of inline blobs (they live in RocksDB; verified by RocksDB checksums)
- Multi-node distributed scrub via gRPC (requires `ScrubRpc` gRPC service — not yet implemented)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `scrub.rs`: `ScrubConfig`, `ScrubReport`, `ScrubReportBuilder`, `ScrubResult`, `ScrubCoordinator`, `ScrubWorker`, `SegmentPartition` |
| `oceanfs-storage` | `heal/`: `enqueue_heal()` called from `scrub_segment()` on corruption detection |

> **Deviation from spec:** The feature spec originally placed `ScrubConfig`, `ScrubReport`, `ScrubResult` in `oceanfs-core`. However, the established codebase convention is that feature-specific types live in the owning crate (e.g., `GcConfig`/`GcStats` in `oceanfs-storage/src/gc.rs`, `AntiEntropyConfig`/`AntiEntropyStats` in `oceanfs-storage/src/anti_entropy.rs`). These types remain in `oceanfs-storage` to maintain consistency.

> **Deviation from spec:** The feature spec specified separate module files (`scrub/coordinator.rs`, `scrub/worker.rs`, `scrub/report.rs`). Following the codebase convention (single files for `gc.rs`, `anti_entropy.rs`), the scrub implementation lives in a single `scrub.rs`.

> **Deviation from spec:** The `ScrubCoordinator::new()` signature diverges from the spec (`new(config, membership, metadata)` → `new(config)`). Dependencies (`MetadataStore`, `SegmentDataStore`) are passed at call time via `run_cycle()` and `trigger_manual()`, consistent with the pattern used in `AntiEntropy`.

## Interface (Public API)

- `pub struct ScrubConfig` — `interval_sec()`, `parallel_nodes()`, `throttle_bytes_sec()`, `set_interval_sec()`
- `pub struct ScrubCoordinator` — `pub fn new(config: ScrubConfig) -> Self`, `pub async fn run_cycle(&self, metadata, data_store) -> Result<ScrubReport>`, `pub async fn trigger_manual(&self, metadata, data_store) -> Result<()>`
- `pub struct ScrubReport` — private fields with getters: `segments_total()`, `segments_healthy()`, `segments_corrupt()`, `segments_healed()`, `bytes_scanned()`, `nodes_participated()`, `duration_sec()`. Builder via `ScrubReport::builder()`.
- `pub(crate) struct SegmentPartition` — internal: assigned node and segment IDs
- `pub(crate) struct ScrubWorker` — internal: verifies assigned partition, enqueues corrupt segments for healing

## Data Flow

```
Full scrub cycle:
  1. Election: random node from membership becomes scrub coordinator
  2. Partitioning:
       ScrubCoordinator queries segments CF → all segment IDs
       → sort by segment_id (UUIDv7, time-sortable)
         → split into scrub_parallel_nodes equal ranges
           → assign each range to a healthy node
  3. Distributed verification:
       Each node (ScrubWorker):
         for segment_id in assigned_range:
           ├─ Fetch all local shards for this segment
           ├─ Compute Merkle tree from segment data (BLAKE3 leaves)
           ├─ Compare computed root to stored merkle_root in SegmentMetadata
           ├─ On mismatch → identify corrupt leaf indices
           ├─ Enqueue heal: call enqueue_heal(segment_id, corrupt_indices)
           └─ Report: (segment_id, healthy | corrupt, enqueued_heal)
  4. Healing (via EC Heal Dispatch):
       HealWorker drains HealQueue:
         ├─ Fetches k healthy shards from peers via gRPC
         ├─ Decodes to reconstruct corrupt shards
         ├─ Writes repaired data + updates metadata
         └─ Increments heals_succeeded counter
  5. Aggregation:
       Coordinator collects all node reports → ScrubReport
       → emit via tracing + admin/metrics
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [x] **Tests:** 29 unit tests: partition coverage (no gaps, no overlaps), bit-flip corruption detection, Merkle mismatch detection, missing data flagged, healthy segment verification, background shutdown with CancellationToken, config accessors, error branches. 5 integration tests.
- [x] **Coverage:** scrub.rs ≥ 80% (verified at 177/195 = 90.77%)
<!-- REVIEW (Iteration 5): tarpaulin confirms 177/195 = 90.77%, above the 80% threshold. Remaining uncovered lines (344-345, 398, 411-412, 453-454, 632, 636-640, 689-690, 721, 727-728) are mostly tracing/error branches inherent to defensive code. All required builder/getter/parallel_nodes paths are now tested. Implementer claimed 178/195 (91.28%) — minor 1-line discrepancy but both well above threshold. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; all `pub` items documented with `# Examples`
- [x] **ADR:** N/A (spec §7.5 covers distributed scrubbing)
- [x] **Perf:** Rule 2.7 (semaphore-bounded concurrency) ✅, Rule 8.5 (bounded semaphore for task concurrency) ✅. Rule 2.6 (bounded channels): not needed for single-node scrub; justified deviation.
- [x] **Integration:** 5 integration tests covering healthy verification, corruption detection, missing data detection, manual trigger
- [x] **Admin API wiring:** `POST /admin/scrub` at `oceanfs-server/src/admin.rs` now feature-gates the handler behind `#[cfg(feature = "storage")]` and calls `ScrubCoordinator::trigger_manual()` with metadata + data store. Wired from `oceanfs-node/src/node.rs` via `AdminHandler::with_scrub()`.
- [x] **Distributed scrub gRPC:** `ScrubRpc` service defined in `proto/oceanfs/scrub.proto` with `AssignPartition` and `ReportPartitionResult` RPCs. Generated stubs in `oceanfs-network/src/generated/oceanfs.scrub.rs`. Server implementation (`ScrubGrpcService`) in `oceanfs-server/src/grpc/scrub_service.rs`. Registered in gRPC server at `oceanfs-node/src/node.rs`. Client integration into `ScrubCoordinator` for sending partitions to remote nodes is the next follow-up step.

### Follow-Up: ScrubCoordinator gRPC Client Integration

The `ScrubRpc` gRPC service is registered and serving. To enable true distributed
scrubbing, `ScrubCoordinator::run_cycle()` should use `ConnectionPool` to acquire
channels to peer nodes and call `ScrubRpcClient::assign_partition()` for each
partition. This requires threading `Membership` and `ConnectionPool` into the
`ScrubCoordinator` (available via `oceanfs-storage`'s existing dependencies).
