---
feature: "Write Path Unification — Wire Segment Pipeline into S3 Handler"
epic: "write-path-unification"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: config-system-fix
    reason: Need configurable seal timeout / intervals for testing short cycles
  - epic: metrics-infrastructure
    reason: Need segment gauge to verify segment metadata creation
adr:
  - 0001-segment-packing
  - 0004-tiered-segment-sizing
perf:
  - "1.2 arena buffer pool"
  - "2.5 sharded segment buffer per worker thread"
  - "3.1 sequential-only WAL writes"
created: 2026-08-05
updated: 2026-08-05
---

# Write Path Unification — Wire Segment Pipeline into S3 Handler

## Summary

OceanFS has two parallel write paths: the spec-intended segment pipeline
(`TierRouter → SegmentPool → ActiveSegment → SegmentSealer → EC encode → RocksDB metadata`)
which is entirely dead code in 10+ files, and the production `BlobStore` flat-file
path used by the S3 handler. The segment pipeline's `put_segment()` is never
called, the `segments` RocksDB column family is empty in production, and GC/scrub/
anti-entropy/heal operate on zero segments. This feature wires the segment
pipeline into the S3 PUT handler, replacing/coexisting with `BlobStore`, so that
every write creates `SegmentMetadata` entries, populates the segments CF, and
enables the durability subsystem to work on real data.

## Scope

### In Scope

- Wire `SegmentPool` into the S3 PUT handler's write path via `WriteCoordinator` (C1-storage, H1-storage)
- Wire `TierRouter` / `route_write` to select segments by blob size tier (H7-storage)
- Wire `BufferPool` — remove `_buffer_pool` underscore, pass it to active segment writers (C2-storage, H3-integration, L7-storage)
- Wire `SegmentSealer` — remove `_sealer` underscore, call `try_seal()` when segments are full (C2-storage, H3-integration)
- Call `put_segment()` in `RocksDbMetadataStore` / `MetadataOps` when a new segment is created/sealed (C3-storage)
- Wire `ActiveSegment` append into the write path: blob data appends to segment buffer instead of `BlobStore` flat files (C1-storage)
- Wire `SegmentShard` for per-core sharding of active segment groups (perf §2.5)
- Wire WAL truncation: after a segment is sealed and EC-encoded, truncate the WAL past the sealed boundary (H8-storage)
- Add segment metadata creation to the write path: on first append or on seal, write `SegmentMetadata` to RocksDB segments CF (C3-storage)
- Resolve `BlobStore` vs `SegmentStore` ambiguity: document the relationship and ensure the durability subsystem reads from the same physical storage (M5-storage)
- Remove `#[allow(dead_code)]` from `PoolSlotState`, `PoolSlot`, `ChunkListBuilder`, and all other segment code that is now wired (L1-storage, L2-storage)
- Fix `route_write` wildcard arm for unknown `SizeTier` to return `Err` instead of `Ok` (L5-storage)
- Wire `SegmentIndex` (B-tree) into the read path for blob lookup within segments

### Out of Scope

- Full `BlobStore` removal (may still serve as an interchangeable backend option — TBD during implementation)
- EC encode/decode integration into read path (belongs in Epic 4 correctness-gaps)
- GC/scrub/AE/heal distributed peer operations (Epic 4)
- Segment compaction EC re-encode (DEV-002, L6-storage, tracked separately)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | Remove `#[allow(dead_code)]` from `segment/` module (10 files). `SegmentPool`, `ActiveSegment`, `TierRouter`, `SegmentShard`, `SegmentIndex`, `route_write` become production-code. Add `put_segment` call path from `SegmentSealer`. Wire WAL truncation into seal flow. |
| `oceanfs-node` | Wire `BufferPool` and `SegmentSealer` into `WriteCoordinator` (remove `_` prefix). Pass `SegmentPool` to `WriteCoordinator` constructor. `node.rs:199-214` changes. |
| `oceanfs-server` | `WriteCoordinator::put()` routes through `TierRouter` → `SegmentPool` → `ActiveSegment`. Writes segments CF via `MetadataOps::put_segment()`. Coordinates segment seal + EC encode post-ack. |
| `oceanfs-durability` | Ensure GC/scrub/AE/heal `SegmentStore` reads from same physical storage as the write path. |

## Interface (Public API)

- No new `pub` types anticipated. The segment pipeline types (`SegmentPool`, `ActiveSegment`, `SegmentSealer`, `TierRouter`) remain `pub(crate)` within `oceanfs-storage` — exposed to `oceanfs-node` and `oceanfs-server` via the crate facade.
- `WriteCoordinator` gains a `segment_pool: Arc<SegmentPool>` field.
- `SegmentSealer::try_seal()` is called from the write path (not a new API, just newly called).

## Data Flow

```
PUT /{bucket}/{key}
  → WriteCoordinator::put(key, data)
    → TierRouter::route_write(size) → select tier (inline/small/standard)
      ├── inline: write to RocksDB metadata inline (existing path)
      └── non-inline:
            → SegmentPool::append(key, data)
              → shard = hash(conn_id) % shard_count
                → ActiveSegment::append(key, data) → BytesMut buffer
                  → WalWriter::append(entry) → WAL fsync
                    → ACK to client (quorum satisfied)
            → [async, post-ack]
              → SegmentSealer::try_seal(segment)
                → if full or timeout:
                  → EC encode all stripes (AccelDispatcher)
                  → distribute parity shards to k+m nodes
                  → put_segment(segment_metadata) → RocksDB segments CF
                  → update ObjectMetadata with (segment_id, offset, length)
                  → WalWriter::truncate(offset) → free WAL space
```

## Detailed Task List

### Wire Segment Pipeline (Critical)

- [ ] **C1-storage:** Remove `#[allow(dead_code)]` from all segment module files: `pool.rs`, `active.rs`, `route_write.rs`, `tier.rs`, `sealer.rs`, `shard.rs`, `index.rs`, `splitter.rs`, `inline.rs`, `mod.rs`.
- [ ] **H1-storage:** Wire `SegmentPool::new(config)` in `node.rs`. Pass `Arc<SegmentPool>` to `WriteCoordinator` constructor.
- [ ] **C2-storage / H3-integration:** Remove `_buffer_pool` underscore, pass `Arc<BufferPool>` to segment constructors. Remove `_sealer` underscore, wire `SegmentSealer` into the write path.
- [ ] **C3-storage:** In `WriteCoordinator::put()`, after a segment is created or sealed, call `metadata.put_segment(segment_id, metadata)`. Ensure the segments CF is populated.
- [ ] **H7-storage:** Wire `route_write` / `TierRouter` into `WriteCoordinator::put()`. Replace the inline-vs-non-inline decision with tiered routing: inline (≤4KB) → RocksDB inline, small (4KB-256KB) → small segment pool, standard (256KB-4MB) → standard segment pool, multi (>4MB) → `SegmentSplitter`.
- [ ] **SegShard wiring:** Wire `SegmentShard` for per-core active segment groups. `shard_index = hash(connection_id) % shard_count`.
- [ ] **H8-storage:** Wire WAL truncation: after segment seal + EC encode + metadata write succeeds, call `WalWriter::truncate(sealed_offset)`.

### Buffer & Storage Integration

- [ ] **L7-storage:** Wire `BufferPool` into `ActiveSegment`. Appends allocate from pool; `BytesMut` returns to pool on seal. Remove dead-code markers.
- [ ] **M5-storage:** Ensure `SegmentDataStore` (used by heal) and the write path (newly using segments) read from the same physical storage. If they differ, unify them or add a `SegmentStore` adapter that reads from the segment pipeline's output.
- [ ] **SegmentIndex:** Wire `SegmentIndex` (B-tree) into read path. On blob read, lookup `(offset, length)` from the index rather than scanning chunk_offsets.

### Cleanup Task

- [ ] **L1-storage:** Remove `#[allow(dead_code)]` from `PoolSlotState` and `PoolSlot` type-level annotations. The internals are used; only the type-level annotation is dead.
- [ ] **L2-storage:** Remove `#[allow(dead_code)]` from `ChunkListBuilder` methods if now wired, or remove the builder if segment index replaces it.
- [ ] **L5-storage:** Change `route_write` wildcard arm `_ => Ok(...)` for unknown `SizeTier` to `_ => Err(Error::InvalidTier)`.
- [ ] **D1 deviation:** After wiring, `GET /admin/segments` returns non-zero counts. `SegmentMetadata` entries appear in the segments CF.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`, `oceanfs-node`, `oceanfs-server`
- [ ] **Tests:** `cargo test` passes; existing segment pipeline unit tests (11 in `pool.rs`, 5 in `active.rs`, etc.) still pass
- [ ] **Tests:** New integration test: `PUT /bucket/key` → `GET /admin/segments` → `segment_count > 0` (verifies C3-storage)
- [ ] **Tests:** New integration test: PUT blob of 1KB (inline), 128KB (small), 1MB (standard) → each creates correct tier segment
- [ ] **Tests:** New integration test: write N blobs to fill a segment → verify segment seals automatically → verify `SegmentMetadata` in RocksDB
- [ ] **Tests:** New integration test: WAL truncation after seal — WAL file size does not grow unboundedly
- [ ] **Tests:** Existing GC integration tests pass when pointed at segments CF with real data
- [ ] **Docs:** Every newly-uncommented `pub` item has doc comments; `#![deny(missing_docs)]` passes
- [ ] **ADR:** ADR-0001 segment packing compliance — tiered sizing applies correctly; segment index consulted for blob lookup
- [ ] **Perf:** Perf §1.2 (BufferPool bytes reuse) — buffers recycled from pool, not allocated per PUT. Perf §2.5 (sharded segment buffer) — concurrent PUTs route to different shards.
- [ ] **Integration:** `cargo test -p e2e -- garbage_collection` exercises GC on real segment data
- [ ] **Deviation closure:** D1 (segment metadata not created) marked resolved
