---
feature: "Orphaned Segment Reaper"
epic: "phase-7-durability"
status: proposed
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
updated: 2026-07-30
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

- `pub struct OrphanReaper` — `pub fn new(metadata: Arc<MetadataStore>, store: Arc<dyn SegmentStore>, config: GcConfig) -> Self`, `pub async fn run_cycle(&self) -> Result<OrphanStats>`, `pub async fn start_background(self: Arc<Self>) -> JoinHandle<()>`
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

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`
- [ ] **Tests:** Unit tests: segment with 0 references = orphan, segment with 1 reference = not orphan, segment where all objects deleted = orphan (after TTL), sealed_at within TTL = not orphan (too young), double-check prevents race (object written between scan and delete), empty segments CF = no orphans, deleted orphan shards truly removed from disk
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-storage`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes
- [ ] **ADR:** ADR-0001 — orphan reaper is the safety net for segment packing's GC complexity
- [ ] **Perf:** N/A (off hot path; background task)
- [ ] **Integration:** `tests/orphan_reaper.rs`: write objects to segments, delete all objects, run GC + reaper, verify segments reclaimed; write object, *don't* delete, run reaper, verify segment NOT reclaimed
- [ ] **Manual:** Example in `OrphanReaper` docs compiles and runs
