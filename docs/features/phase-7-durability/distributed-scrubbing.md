---
feature: "Distributed Scrubbing"
epic: "phase-7-durability"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: anti-entropy-merkle
    reason: Scrubbing is the full-scan complement to anti-entropy's incremental check
  - feature: ec-codec-trait-cauchy-rs
    reason: Scrubbing verifies BLAKE3 + Merkle root; heals via EC decode
adr: []
perf:
  - "2.6: Bounded channels for scrub work distribution"
  - "2.7: Tokio semaphore for concurrency limits"
  - "8.5: Bounded semaphore for task concurrency"
created: 2026-07-30
updated: 2026-07-30
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
- `ScrubWorker`: per-node task that reads assigned segment shards
- Verification: BLAKE3 hash of shard data vs stored hash; Merkle root vs recomputed
- Discrepancy handling: on mismatch → enqueue segment for healing (EC decode from healthy shards)
- Scrub report: aggregate per-node results → total segments, mismatches, healed, bytes scanned
- Configurable: `scrub_interval_sec` (default 7 days), `scrub_parallel_nodes` (0 = all)
- Throttling: `heal_throttle_bytes_sec` limits repair bandwidth
- Admin API integration: `POST /admin/scrub` triggers manual scrub
- Unit tests for partition assignment, verification logic, report aggregation

### Out of Scope
- Real-time scrubbing (periodic only; continuous verification via anti-entropy)
- Scrub scheduling across maintenance windows
- Scrubbing of inline blobs (they live in RocksDB; verified by RocksDB checksums)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `ScrubConfig`, `ScrubReport`, `ScrubResult` |
| `oceanfs-storage` | New modules: `scrub/coordinator.rs`, `scrub/worker.rs`, `scrub/report.rs` |

## Interface (Public API)

- `pub struct ScrubConfig` — `interval_sec: u64` (default 604800), `parallel_nodes: usize` (default 0 = all), `throttle_bytes_sec: u64` (default 0 = unlimited)
- `pub struct ScrubCoordinator` — `pub fn new(config: ScrubConfig, membership: Arc<Membership>, metadata: Arc<MetadataStore>) -> Self`, `pub async fn run_cycle(&self) -> Result<ScrubReport>`, `pub async fn trigger_manual(&self) -> Result<()>`
- `pub struct ScrubReport` — `segments_total: u64`, `segments_healthy: u64`, `segments_corrupt: u64`, `segments_healed: u64`, `bytes_scanned: u64`, `nodes_participated: usize`, `duration_sec: f64`
- `pub(crate) struct ScrubWorker` — internal: verifies assigned partition, reports discrepancies

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
           ├─ For each shard:
           │    ├─ Compute BLAKE3 hash
           │    ├─ Compare to stored hash in SegmentMetadata
           │    ├─ On mismatch → flag as corrupt
           │    └─ Recompute Merkle tree for segment → compare root
           └─ Report: (segment_id, healthy | corrupt_shard_indices)
  4. Healing:
       for corrupt_segment in report:
         ├─ Enqueue heal: EC decode from k healthy shards on other nodes
         ├─ Replace corrupt shard with reconstructed data
         └─ Update SegmentMetadata
  5. Aggregation:
       Coordinator collects all node reports → ScrubReport
       → emit via tracing + admin/metrics
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: partition assignment covers all segments (no gaps, no overlaps), verification detects bit-flip in shard data, Merkle mismatch detected, coordinator election (single leader), report aggregation correct, manual trigger via admin API works
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-storage`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `ScrubCoordinator` documented
- [ ] **ADR:** N/A (spec §7.5 covers distributed scrubbing)
- [ ] **Perf:** Rule 2.6 (bounded work queues), 2.7 (semaphore-bounded scan concurrency), 8.5 (throttle for bandwidth)
- [ ] **Integration:** `tests/distributed_scrub.rs`: 3-node cluster, write segments, corrupt one shard on one node, trigger manual scrub, verify corruption detected, verify auto-healed, verify scrub report shows healed count
- [ ] **Manual:** Example in `ScrubCoordinator` docs compiles and runs
