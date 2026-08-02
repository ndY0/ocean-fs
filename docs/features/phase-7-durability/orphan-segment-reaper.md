---
feature: "Orphaned Segment Reaper"
epic: "phase-7-durability"
status: in_progress
priority: medium
owner: ""
dependencies:
  - feature: gc-tombstone-compaction
    reason: Orphan detection scans the same metadata stores as GC
  - feature: rocksdb-metadata-store
    reason: Compares segments CF against objects CF to find unreferenced segments
adr:
  - 0001-segment-packing
perf: []
created: 2026-07-30
updated: 2026-08-02
---

# Orphaned Segment Reaper

## Summary

Implement the orphaned segment reaper in `oceanfs-storage`. Segments can become
orphaned when all referencing objects are deleted but the segment itself was
never compacted (e.g., GC disabled or compaction failed). The reaper
periodically scans the `segments` column family, cross-references against
`objects`, and permanently deletes any segment unreferenced for longer than
`gc_tombstone_ttl_sec`. This prevents unbounded disk usage from leaked segments.

## Scope

### In Scope
- `OrphanReaper`: background task running every `gc_interval_sec`
- Orphan detection: scan `segments` CF → for each segment_id, check if any `ObjectMetadata` references it
- Reverse index: build in-memory set of all referenced segment_ids from `objects` CF
- Orphan criteria: segment not in referenced set AND `sealed_at` older than `gc_tombstone_ttl_sec`
- Reclamation: delete orphan segment shards from disk + remove `SegmentMetadata` from RocksDB
- Idempotent: double-check before deletion (segment may have been re-referenced concurrently)
- Configurable: reuses `gc_interval_sec` and `gc_tombstone_ttl_sec` from GcConfig
- Unit tests for orphan detection, multi-object segments (only orphan when ALL objects deleted), concurrent write during reaper

### Out of Scope
- Recovery of accidentally deleted segments (orphans are permanently deleted)
- Distributed orphan coordination (each node cleans its own orphan shards)
- Orphan detection for inline blobs (they are in metadata, not segments)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New modules: `gc/orphan.rs` |

## Interface (Public API)

- `pub struct OrphanReaper` — `pub fn new(metadata: Arc<MetadataStore>, store: Arc<dyn SegmentShardStore>, config: GcConfig) -> Self`, `pub async fn run_cycle(&self) -> Result<OrphanStats>`, `pub async fn start_background(self: Arc<Self>) -> JoinHandle<()>`
- `pub trait SegmentShardStore` — `fn delete_shards(&self, segment_id: SegmentId) -> Result<u64>`
- `pub struct InMemorySegmentShardStore` — mock implementation for testing
- `pub struct OrphanStats` — `segments_scanned: u64`, `orphans_found: u64`, `orphans_deleted: u64`, `bytes_reclaimed: u64`

## Data Flow

```
Orphan reaper cycle:
  1. Build referenced segment ID set:
       scan objects CF → for each ObjectMetadata:
         for chunk in chunks[]:
           referenced_set.insert(chunk.segment_id)

  2. Detect orphans:
       scan segments CF → for each SegmentMetadata:
         if segment_id NOT IN referenced_set:
           if now - sealed_at > gc_tombstone_ttl_sec:
             → orphan_candidates.push(segment_id)

  3. Reclaim orphans:
       for segment_id in orphan_candidates:
         ├─ Double-check: re-query objects CF for this segment_id
         │    └─ Still unreferenced? → proceed
         ├─ Remove segment shards from disk
         ├─ Delete SegmentMetadata from segments CF
         └─ bytes_reclaimed += segment.size

  4. Emit OrphanStats
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`
<!-- REVIEW: Verified 2026-08-02. Build passes cleanly. -->
- [x] **Tests:** Unit tests: segment with 0 references = orphan, segment with 1 reference = not orphan, segment where all objects deleted = orphan (after TTL), sealed_at within TTL = not orphan (too young), double-check prevents race (object written between scan and delete), empty segments CF = no orphans, deleted orphan shards truly removed from disk. All 14 orphan-specific tests pass (unit + integration).
<!-- REVIEW: Verified 2026-08-02. 14 unit tests + 7 integration tests = 21 total, all passing (0 failures). -->
- [x] **Coverage:** `cargo tarpaulin` on `oceanfs-storage` — 64.28% overall (+0.50%). `gc.rs` orphan-specific paths (run_cycle, build_referenced_set, is_segment_referenced, start_background) all exercised by tests.
<!-- REVIEW: Verified 2026-08-02. Actual coverage: 64.28% (2933/4563). Below generic 80% threshold but explicit DoD acceptance per coding guidelines §4.6. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes. `RUSTDOCFLAGS="-D warnings" cargo doc` passes.
<!-- REVIEW: Verified 2026-08-02. Docs generate with zero warnings. -->
- [x] **Clippy:** `cargo clippy --lib -p oceanfs-storage -- -D warnings` passes.
<!-- REVIEW: Verified 2026-08-02. --lib check passes cleanly. --all-targets has 6 warnings in test code (gc.rs:1884,2025,2034 expect_used; anti_entropy.rs:2191,2289; heal/worker.rs:710) — per coding guidelines §9.2.1 these are test-code only, not feature gates. -->
- [x] **ADR:** ADR-0001 — orphan reaper is the safety net for segment packing's GC complexity.
<!-- REVIEW: Verified 2026-08-02. ADR-0001 §Consequences says "Garbage collection is required" — the orphan reaper fulfills this as the safety net for leaked segments. No rejected alternatives re-implemented. -->
- [x] **Perf:** N/A (off hot path; background task).
<!-- REVIEW: Verified 2026-08-02. No perf rules cited. Orphan reaper runs as a background task, not on the write/read paths. -->
- [x] **Integration:** `tests/orphan_reaper.rs`: 7 tests pass. Covers: live object not reclaimed, unreferenced = orphan, recently sealed not reclaimed, empty store, metadata deletion verified, shard deletion verified, double-check race test.
<!-- REVIEW: Verified 2026-08-02. All 7 integration tests pass. Test coverage matches the listed scenarios. -->
- [ ] **Interface:** Constructor `new(metadata, store, config)` uses `Arc<dyn SegmentShardStore>` — feature doc specifies `Arc<dyn SegmentStore>`.
<!-- REVIEW: Spec deviation. The feature doc Interface section specifies `Arc<dyn SegmentStore>` but the implementation uses `Arc<dyn SegmentShardStore>` (gc.rs:710). The `SegmentStore` trait does not exist in the codebase. `SegmentShardStore` is functionally correct but diverges from the documented interface. Either accept the implementation (recommended — it's a more specific trait) or update the feature doc Interface section. -->
