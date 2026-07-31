---
feature: "Segment Sealing & Blob Index"
epic: "phase-1-storage-engine"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: segment-buffer-inline
    reason: ActiveSegment produces the buffer that sealing finalizes
  - feature: rocksdb-metadata-store
    reason: Sealed segment metadata is persisted to RocksDB
  - feature: wal-write-ahead-log
    reason: WAL truncation happens after seal
adr:
  - 0001-segment-packing
perf:
  - "1.3: Pre-size collections with known capacity"
  - "6.5: BTreeMap over HashMap for ordered access"
  - "9.5: extend_from_slice for known batch sizes"
created: 2026-07-30
updated: 2026-07-30
---

# Segment Sealing & Blob Index

## Summary

Implement segment sealing logic and on-disk blob index in `oceanfs-storage`.
When an active segment becomes full (exceeds `target_size`) or reaches
`seal_timeout_ms`, it is sealed: finalized to an immutable segment with a sorted
B-tree index of all contained blobs at the segment head. The seal process
truncates the WAL past the segment boundary, writes the segment to disk, and
persists segment metadata to RocksDB.

## Scope

### In Scope
- `SegmentSealer`: detects full/timeout conditions on active segment pools
- Segment seal: finalize buffer, compute BLAKE3 hash of segment, write to disk
- `SegmentIndex`: sorted B-tree index (`BTreeMap`) at segment head mapping `(offset, length, blob_key_hash)` for O(log n) blob lookup
- Segment header format (on-disk): magic bytes, version, segment_id, size, blob count, index offset, checksum
- Seal trigger: buffer size > `target_size` OR time since first append > `seal_timeout_ms`
- Integration: seal → truncate WAL → write segment metadata to RocksDB → rotate active segment pool
- `SegmentMetadata` populated with sealed_at timestamp, Merkle root placeholder (Phase 7)
- Unit tests for partial-fill seal, exact-size seal, empty segment seal (reject), index lookup after seal

### Out of Scope
- EC encoding (Phase 3) — sealing only finalizes the raw segment; EC happens after seal
- Merkle tree computation (Phase 7) — placeholder only
- Segment compaction / GC (Phase 7)
- Multi-node segment distribution (Phase 4)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `SegmentHeader`, `SegmentIndexEntry`, `SizeTier` |
| `oceanfs-storage` | New modules: `segment/sealer.rs`, `segment/index.rs`, `segment/header.rs` |
| `oceanfs-storage` | New facade export: `pub use segment::SegmentIndex` |

## Interface (Public API)

- `pub struct SegmentIndex` — `pub fn new(entries: Vec<SegmentIndexEntry>) -> Self`, `pub fn lookup(&self, offset: u64) -> Option<&SegmentIndexEntry>`, `pub fn len(&self) -> usize`, `pub fn to_bytes(&self) -> Vec<u8>`, `pub fn from_bytes(data: &[u8]) -> Result<Self>`
- `pub struct SegmentIndexEntry` — `offset: u64, length: u32, blob_key_hash: [u8; 32]`
- `pub(crate) struct SegmentSealer` — `pub(crate) fn new(config: SealConfig, metadata: Arc<MetadataStore>, wal: Arc<WalWriter>) -> Self`, `pub(crate) async fn try_seal(&self, active: &mut ActiveSegment) -> Result<Option<SegmentHandle>>`
- `pub struct SegmentHeader` — `magic: [u8; 4], version: u16, segment_id: SegmentId, size: u64, blob_count: u32, index_offset: u64, checksum: [u8; 32]`
- `pub struct SealConfig` — `target_size_bytes: u64, seal_timeout_ms: u64`

## Data Flow

```
ActiveSegment pool → monitor fill level + elapsed time

Condition met (full or timeout):
  ├─ 1. Flush remaining buffer writes
  ├─ 2. Build SegmentIndex: BTreeMap<(offset, length) → blob_key_hash>
  ├─ 3. Serialize SegmentHeader + SegmentIndex to segment head
  ├─ 4. Write full segment to data_dir/segments/{segment_id}.dat
  ├─ 5. Compute BLAKE3 hash of segment data
  ├─ 6. Persist SegmentMetadata to RocksDB segments CF
  ├─ 7. Truncate WAL past this segment's entries
  ├─ 8. Replace sealed ActiveSegment with fresh one from BufferPool
  └─ 9. Return SegmentHandle { id, sealed: true }

Blob read via index:
  GET /{bucket}/{key}
    → ObjectMetadata.chunks[0] = (segment_id, offset, length)
      → load SegmentIndex from segment head
        → SegmentIndex::lookup(offset) → Some(entry) confirms blob exists at offset
          → read segment data at [offset..offset+length]
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [x] **Tests:** Unit tests for seal on size threshold, seal on timeout, seal on empty buffer (error), index serialization round-trip, index lookup on boundary offsets, header serialization/deserialization
- [x] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-storage`
- [x] **Lint:** `cargo clippy -- -D warnings` passes
- [x] **Docs:** `#![deny(missing_docs)]` passes; `SegmentIndex` documented with lookup examples
- [x] **ADR:** ADR-0001 — blob index is a sorted B-tree at segment head, enabling O(log n) lookup per spec
- [x] **Perf:** Rule 6.5 (BTreeMap), 1.3 (pre-size index Vec), 9.5 (extend_from_slice for bulk index write)
- [x] **Integration:** `tests/segment_roundtrip.rs`: write blobs to active segment, seal, read back via index, verify all blobs recoverable at correct offsets
- [x] **Manual:** `SegmentIndex` example demonstrating lookup compiles and runs
<!-- REVIEW: Integration test `tests/segment_roundtrip.rs` does NOT perform end-to-end seal-then-read-back cycle (seal tests live in unit tests only at sealer.rs). The feature doc specifies the integration test should "write blobs to active segment, seal, read back via index" — this full round-trip isn't covered in the integration test file. -->
<!-- REVIEW: Feature doc specifies `pub(crate) struct SegmentSealer` but it is `pub` with re-export from lib.rs. Acceptable for integration test access. -->
<!-- REVIEW: SealConfig has an extra `data_dir: PathBuf` field not listed in the feature doc's Interface section. Functional addition, not a bug. -->
