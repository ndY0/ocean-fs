---
feature: "Pipeline Parallelism & Active Segment Pool"
epic: "phase-4-distributed-read-write"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: segment-sealing-index
    reason: Segment sealing is the operation that pool decouples from writes
  - feature: write-coordinator-quorum
    reason: Write coordinator feeds appends into the pool
  - feature: stripe-layout-parallelism
    reason: EC encoding of sealed segments happens while pool accepts new writes
adr:
  - 0001-segment-packing
perf:
  - "2.5: Sharded segment buffer per worker thread"
  - "2.7: Tokio semaphore for concurrency limits"
  - "2.6: Bounded channels for inter-task communication"
created: 2026-07-30
updated: 2026-07-30
---

# Pipeline Parallelism & Active Segment Pool

## Summary

Implement the active segment pool and pipeline parallelism in
`oceanfs-storage`. A pool of N active segments (default 4) decouples append
latency from EC encode time. While one segment is being EC-encoded
(asynchronously), the next segment in the pool accepts writes. Combined with
per-core segment sharding, this eliminates write blocking during seal+encode
cycles.

## Scope

### In Scope
- `SegmentPool`: manages N active segments per tier per shard
- Pool states: segment lifecycle (`Appending` → `Sealing` → `Encoding` → `Idle`)
- Pool rotation: when current segment fills → seal → move to encoding queue → activate next idle segment
- Bounded async channel for EC encoding work queue (backpressure)
- `Semaphore`-bounded concurrency: limits in-flight EC encodes to prevent memory exhaustion
- Per-core sharding: `hash(connection_id) % shard_count` → independent pool per shard
- Configurable: `segment_active_pool_size`, `segment_shard_count`
- Integration: write coordinator → tier router → shard router → pool → active segment → append
- Unit tests for pool rotation, concurrent writes across shards, encoding queue backpressure

### Out of Scope
- EC encoding execution itself (Phase 3) — pool triggers encoding asynchronously
- Multi-node coordination of pool states (each node manages its own pools)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New config types: `PoolConfig` (active_pool_size, shard_count) |
| `oceanfs-storage` | New modules: `segment/pool.rs`, `segment/shard.rs` |

## Interface (Public API)

- `pub struct SegmentPool` — `pub fn new(config: PoolConfig, tier: SizeTier) -> Self`, `pub(crate) async fn append(&self, data: &[u8]) -> Result<(SegmentId, u64, u32)>`, `pub(crate) fn active_count(&self) -> usize`
- `pub(crate) struct PoolSlot` — internal: holds one ActiveSegment + state
- `pub(crate) enum PoolSlotState` — `Idle`, `Appending`, `Sealing`, `Encoding`
- `pub struct PoolConfig` — `active_pool_size: usize`, `shard_count: usize`
- `pub(crate) struct SegmentShard` — `pub(crate) fn new(pools: Vec<SegmentPool>) -> Self`, `pub(crate) fn route(&self, connection_id: u64) -> &SegmentPool`

## Data Flow

```
Active Segment Pool (per shard, per tier):
  +----+  +----+  +----+  +----+
  | S0 |  | S1 |  | S2 |  | S3 |
  |Appending|Sealing|Encoding| Idle |
  +----+  +----+  +----+  +----+

Write arrives:
  1. Shard router: hash(connection_id) % shard_count → pool P
  2. Pool P: find slot in Appending state → ActiveSegment::append(data)
  3. If current segment full (> target_size):
       ├─ Move slot to Sealing state
       ├─ SegmentSealer::try_seal(segment) → sealed segment handle
       ├─ Enqueue EC encoding task (bounded channel)
       ├─ Move slot to Encoding state
       └─ Activate next Idle slot → Appending state
  4. EC encode worker (async task):
       ├─ Acquire Semaphore permit
       ├─ ParallelEncoder::encode(segment_data) → k+m shards
       ├─ Distribute shards to k+m nodes (Phase 4 feature)
       ├─ Update segment metadata in RocksDB
       ├─ Release Semaphore permit
       └─ Move slot to Idle state

With 4 shards × 4 pool slots = 16 concurrent write buffers:
  Contention reduced by factor of 16 vs single segment
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: pool rotation (fill → seal → new segment), concurrent writes across 4 shards (no data corruption), encoding queue backpressure (writes blocked when queue full), semaphore bounds in-flight encodes, pool slot state transitions, shard routing determinism
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-storage`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes
- [ ] **ADR:** ADR-0001 segment packing (pool enables sealing small segments without blocking writes)
- [ ] **Perf:** Rule 2.5 (sharded per worker thread), 2.7 (semaphore-bound encodes), 2.6 (bounded encode queue)
- [ ] **Integration:** `tests/pipeline_parallelism.rs`: continuous writes at high concurrency (32 threads), verify writes never block > seal_timeout_ms, verify pool rotates through slots, verify all written data readable after encoding completes
- [ ] **Manual:** Example in `SegmentPool` docs compiles and runs
