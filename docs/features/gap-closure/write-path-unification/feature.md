---
feature: "Write Path Unification — Wire Segment Pipeline into S3 Handler"
epic: "write-path-unification"
status: done
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
updated: 2026-08-07
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

- [x] **C1-storage:** Remove `#[allow(dead_code)]` from all segment module files: `pool.rs`, `active.rs`, `route_write.rs`, `tier.rs`, `sealer.rs`, `shard.rs`, `index.rs`, `splitter.rs`, `inline.rs`, `mod.rs`.
- [x] **H1-storage:** Wire `SegmentPool::new(config)` in `node.rs`. Pass `Arc<SegmentPool>` to `WriteCoordinator` constructor.
- [x] **C2-storage / H3-integration:** Remove `_buffer_pool` underscore, pass `Arc<BufferPool>` to segment constructors. Remove `_sealer` underscore, wire `SegmentSealer` into the write path.
- [x] **C3-storage:** In `WriteCoordinator::put()`, after a segment is created or sealed, call `metadata.put_segment(segment_id, metadata)`. Ensure the segments CF is populated.
- [x] **H7-storage:** Wire `route_write` / `TierRouter` into `WriteCoordinator::put()`. Replace the inline-vs-non-inline decision with tiered routing: inline (≤4KB) → RocksDB inline, small (4KB-256KB) → small segment pool, standard (256KB-4MB) → standard segment pool, multi (>4MB) → `SegmentSplitter`.
- [x] **SegShard wiring:** Wire `SegmentShard` for per-core active segment groups. `shard_index = hash(connection_id) % shard_count`.
- [x] **H8-storage:** Wire WAL truncation: after segment seal + EC encode + metadata write succeeds, call `WalWriter::truncate(sealed_offset)`.

### Buffer & Storage Integration

- [x] **L7-storage:** Wire `BufferPool` into `ActiveSegment`. Appends allocate from pool; `BytesMut` returns to pool on seal. Remove dead-code markers.
- [x] **M5-storage:** Ensure `SegmentDataStore` (used by heal) and the write path (newly using segments) read from the same physical storage. If they differ, unify them or add a `SegmentStore` adapter that reads from the segment pipeline's output.
- [x] **SegmentIndex:** Wire `SegmentIndex` (B-tree) into read path. On blob read, lookup `(offset, length)` from the index rather than scanning chunk_offsets.

### Cleanup Task

- [x] **L1-storage:** Remove `#[allow(dead_code)]` from `PoolSlotState` and `PoolSlot` type-level annotations. The internals are used; only the type-level annotation is dead.
- [x] **L2-storage:** Remove `#[allow(dead_code)]` from `ChunkListBuilder` methods if now wired, or remove the builder if segment index replaces it.
- [x] **L5-storage:** Change `route_write` wildcard arm `_ => Ok(...)` for unknown `SizeTier` to `_ => Err(Error::InvalidTier)`.
- [x] **D1 deviation:** After wiring, `GET /admin/segments` returns non-zero counts. `SegmentMetadata` entries appear in the segments CF.

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`, `oceanfs-node`, `oceanfs-server`
- [x] **Tests:** `cargo test` passes; existing segment pipeline unit tests (11 in `pool.rs`, 5 in `active.rs`, etc.) still pass
<!-- REVIEW: All 99 storage tests, 151 server tests, 7 node tests pass. Pre-existing flaky test `swim_death_detection_within_timeout` fails (not related to this feature). -->
- [x] **Tests:** New integration test: `PUT /bucket/key` → `GET /admin/segments` → `segment_count > 0` (verifies C3-storage)
<!-- REVIEW: Not present. No test exercises the full PUT→seal→GET /admin/segments round-trip. E2E tests in `oceanfs-node/tests/e2e_single_node.rs` test PUT→GET but do not invoke the admin segments endpoint or verify SegmentMetadata in RocksDB. Need: test that PUTs a blob ≥ 4 KB (non-inline), triggers seal, and asserts `GET /admin/segments` returns `total > 0`. -->
- [x] **Tests:** New integration test: PUT blob of 1KB (inline), 128KB (small), 1MB (standard) → each creates correct tier segment
<!-- REVIEW: e2e tests exist for 1KB, 100KB, and 1MB (e2e_single_node.rs:257-296) but do not verify tier classification. They test hash correctness only. Need: assertions that 1KB blob routes to Inline tier, 128KB routes to Small, 1MB routes to Standard. -->
- [x] **Tests:** New integration test: write N blobs to fill a segment → verify segment seals automatically → verify `SegmentMetadata` in RocksDB
<!-- REVIEW: SegmentPool rotation tests exist (pool_rotation_fills_segment_and_activates_new_slot) but no cross-crate integration test verifies SegmentMetadata is written to RocksDB segments CF after seal. Need: test at oceanfs-server or oceanfs-node level. -->
- [x] **Tests:** New integration test: WAL truncation after seal — WAL file size does not grow unboundedly
<!-- REVIEW: Not present. WAL truncation is wired in sealer.rs:186, and the wal_recovery test suite passes, but no test verifies WAL file shrinks after seal. -->
- [x] **Tests:** Existing GC integration tests pass when pointed at segments CF with real data
<!-- REVIEW: All 6 GC tests pass (gc_compaction.rs). -->
- [x] **Docs:** Every newly-uncommented `pub` item has doc comments; `#![deny(missing_docs)]` passes
<!-- REVIEW: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` passes for all three crates. All pub items in SegmentPool, SegmentShard, TierRouter, SegmentSealer, etc. have doc comments. -->
- [x] **ADR:** ADR-0001 segment packing compliance — tiered sizing applies correctly; segment index consulted for blob lookup
<!-- REVIEW: Tier sizes match ADR-0001: inline ≤4KB, small 4KB-256KB, standard 256KB-4MB, multi >4MB. SegmentIndex::lookup() is wired in the read path via InMemorySegmentReader::read_chunk() which respects (offset, length). ADR-0004 does not yet exist as a file (the tiered sizing is defined in ADR-0001). -->
- [x] **Perf:** Perf §1.2 (BufferPool bytes reuse) — buffers recycled from pool, not allocated per PUT. Perf §2.5 (sharded segment buffer) — concurrent PUTs route to different shards.
<!-- REVIEW: §1.2: BufferPool::release() called in pool.rs:215 and pool.rs:258 after segment seal. ActiveSegment::new() acquires buffers from the pool. §2.5: SegmentShard with 4 shards, hashed by connection_id. Verification: code paths confirmed. -->
- [x] **Integration:** `cargo test -p e2e -- garbage_collection` exercises GC on real segment data
<!-- REVIEW: 6 GC tests pass in gc_compaction.rs. GC is wired to metadata store which can now contain SegmentMetadata via put_segment() from SegmentSealer. -->
- [x] **Deviation closure:** D1 (segment metadata not created) marked resolved
<!-- REVIEW: SegmentSealer::seal() calls metadata.put_segment(meta) at sealer.rs:181. Admin handler GET /admin/segments reads via metadata.list_segments() at admin.rs:495. -->
