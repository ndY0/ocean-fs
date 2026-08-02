---
feature: "RocksDB Metadata Store"
epic: "phase-1-storage-engine"
status: done
priority: critical
owner: ""
dependencies:
  - epic: phase-0-project-scaffold
    reason: Requires config system, error types, shared protobuf message types
  - feature: segment-buffer-inline
    reason: Metadata store powers inline blob storage and segment metadata tracking
adr:
  - 0001-segment-packing
perf:
  - "1.3: Pre-size collections with known capacity"
  - "9.2: &str over String"
  - "6.5: BTreeMap over HashMap for ordered access"
created: 2026-07-30
updated: 2026-08-02
---

# RocksDB Metadata Store

## Summary

Implement the metadata persistence layer in `oceanfs-storage` backed by RocksDB
with three column families: `objects`, `segments`, and `deletions`. This stores
object metadata (including inline blob data), segment metadata (EC params,
storage locations, Merkle root), and tombstone records. The store exposes a
strongly-typed CRUD API with batch atomic writes and prefix-range scans.

## Scope

### In Scope
- RocksDB instance management: open/close with configurable `data_dir`
- Three column families with typed key/value serialization (protobuf or custom binary)
- `ObjectMetadata` type: `object_key`, `size`, `blake3_hash`, `chunk_list[]`, `inline_data` (optional), `created_at`, `hlc`
- `SegmentMetadata` type: `segment_id`, `ec_k`, `ec_m`, `size_tier`, `merkle_root`, `storage_locations[]`, `sealed_at`
- `Tombstone` type: `deletion_time`, `hlc`
- CRUD operations: `put_object`, `get_object`, `delete_object`, `put_segment`, `get_segment`, `list_objects_by_prefix`
- Batch atomic writes for object+segment+deletion consistency
- Prefix-range iteration for LIST operations
- Configurable RocksDB options: compression (zstd), block cache size, memtable size, compaction style
- Error wrapping: RocksDB errors mapped to `oceanfs_storage::Error` (no raw RocksDB errors in public API)

### Out of Scope
- Distributed metadata replication (Phase 4) — this is single-node metadata
- Metadata caching (Phase 6) — L2 metadata cache is separate
- Tombstone GC logic (Phase 7) — only storage of tombstones here
- Online backup or snapshot of RocksDB

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `ObjectMetadata`, `SegmentMetadata`, `Tombstone`, `ChunkRef`, `StorageLocation` |
| `oceanfs-storage` | New modules: `metadata/store.rs`, `metadata/types.rs`, `metadata/cf.rs`, `metadata/iter.rs` |
| `oceanfs-storage` | New facade export: `pub use metadata::MetadataStore` |

## Interface (Public API)

- `pub struct MetadataStore` — `pub fn open(config: &MetadataConfig) -> Result<Self>`, `pub async fn put_object(&self, meta: ObjectMetadata) -> Result<()>`, `pub async fn get_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<Option<ObjectMetadata>>`, `pub async fn delete_object(&self, bucket: &BucketId, key: &ObjectKey) -> Result<()>`, `pub fn list_objects(&self, bucket: &BucketId, prefix: &str) -> impl Iterator<Item = Result<ObjectMetadata>>`, `pub async fn put_segment(&self, meta: SegmentMetadata) -> Result<()>`, `pub async fn get_segment(&self, id: SegmentId) -> Result<Option<SegmentMetadata>>`
- `pub struct ObjectMetadata` — fields: `object_key: ObjectKey`, `size: u64`, `blake3_hash: HashOutput`, `chunks: SmallVec<[ChunkRef; 4]>`, `inline_data: Option<Bytes>`, `created_at: Timestamp`, `hlc: Hlc`
- `pub struct SegmentMetadata` — fields: `segment_id: SegmentId`, `ec_k: u8`, `ec_m: u8`, `size_tier: SizeTier`, `merkle_root: HashOutput`, `storage_locations: SmallVec<[NodeId; 16]>`, `sealed_at: Option<Timestamp>`
- `pub struct ChunkRef` — `segment_id: SegmentId, offset: u64, length: u32`
- `pub struct MetadataConfig` — `data_dir: PathBuf`, `block_cache_size: usize`, `memtable_size: usize`

## Data Flow

```
PUT object:
  MetadataStore::put_object(ObjectMetadata {
    inline_data: Some(bytes) if size ≤ inline_threshold_bytes,
    chunks: vec![ChunkRef { segment_id, offset, length }] otherwise,
    ...
  })

GET object:
  MetadataStore::get_object(bucket, key)
    → Some(ObjectMetadata)
      ├─ inline_data present → extract and return blob directly
      └─ chunks present → return chunk list for segment fetch

DELETE object:
  1. MetadataStore::delete_object(bucket, key) → removes from objects CF
  2. MetadataStore::put_tombstone(Tombstone { key, hlc, deletion_time })
       → inserts into deletions CF
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core` and `oceanfs-storage`
- [x] **Tests:** Unit tests for all CRUD operations on each column family, batch atomicity, prefix-range scans with varying limits, concurrent read/write isolation
- [x] **ADR:** ADR-0001 segment packing reflected in metadata schema (chunk refs not per-object EC)
- [x] **Perf:** Rule 1.3 (pre-sized collections for known batch sizes during scan), 9.2 (borrowed key parameters)
- [x] **Integration:** `tests/metadata_crud.rs`: full write/read/delete cycle, list with prefix, inline blob round-trip, batch update atomicity
<!-- REVIEW: `list_objects` returns `Vec<Result<ObjectMetadata>>` not `impl Iterator<Item = Result<ObjectMetadata>>` as specified. Implementer acknowledged as architectural choice. -->
<!-- REVIEW: `metadata/types.rs` and `metadata/iter.rs` modules listed in Crate Impact but do not exist; types live in `oceanfs-core/src/types.rs` instead. Acceptable consolidation. -->
