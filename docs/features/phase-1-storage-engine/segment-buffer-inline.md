---
feature: "Segment Buffer & Inline Storage"
epic: "phase-1-storage-engine"
status: done
priority: critical
owner: ""
dependencies:
  - epic: phase-0-project-scaffold
    reason: Requires crate layout, config system, protobuf definitions
adr:
  - 0001-segment-packing
perf:
  - "1.1: Use bytes::Bytes/BytesMut for blob data"
  - "1.2: Arena/buffer pool for segment append buffers"
  - "1.3: Pre-size collections with known capacity"
created: 2026-07-30
updated: 2026-08-02
---

# Segment Buffer & Inline Storage

## Summary

Implement the core segment buffer in `oceanfs-storage`: an append-only
in-memory buffer that accumulates blob writes. Small blobs at or below
`inline_threshold_bytes` are stored directly in the RocksDB metadata
column family — no segment I/O.

## Scope

### In Scope
- `ActiveSegment` struct with append-only `BytesMut` buffer
- Tiered segment sizing logic (inline → small → standard thresholds)
- Inline blob storage in `objects` RocksDB column family value
- `SegmentHandle` public type for referencing active/sealed segments
- `BufferPool` for recycling `BytesMut` between segment lifecycles
- `SegmentShard` with per-request-ID hashing for write concurrency
- Unit tests for append, overflow, inline-vs-segment routing
- Integration test: `PUT` object, verify inline or segment placement

### Out of Scope
- WAL persistence (separate feature: "Write-Ahead Log")
- EC encoding (Phase 3)
- Multi-node distribution (Phase 4)
- Garbage collection (Phase 6)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `SegmentId` (UUIDv7), `InlineThreshold` |
| `oceanfs-storage` | New modules: `segment/buffer.rs`, `segment/shard.rs`, `segment/handle.rs`, `buffer_pool.rs` |
| `oceanfs-storage` | New entry in `lib.rs` facade: `pub use segment::SegmentHandle` |

## Interface (Public API)

- `pub struct SegmentHandle` — opaque handle with `fn id() -> SegmentId` and `fn node_ids() -> &[NodeId]`
- `pub(crate) struct ActiveSegment` — internal: `fn append(&mut self, data: &[u8]) -> (u64, usize)`, `fn is_full(&self) -> bool`
- `pub(crate) struct SegmentShard` — internal: routes writes to one of N active segments by `hash(connection_id) % shard_count`
- `pub struct BufferPool` — `fn acquire() -> BytesMut`, `fn release(buf: BytesMut)`

## Data Flow

```
PUT /{bucket}/{key} with N bytes
  │
  ├─ N ≤ inline_threshold_bytes (4 KB)
  │    └→ RocksDB objects CF: store blob inline in ObjectMetadata value
  │       └→ 200 OK (no segment I/O)
  │
  └─ N > inline_threshold_bytes
       ├─ N ≤ small_threshold (256 KB) → small segment (64 KB target)
       └─ N > small_threshold           → standard segment (4 MB target)
            │
            └→ SegmentShard::hash(connection_id) → ActiveSegment[N]
                 └→ ActiveSegment::append(data)
                      └→ BufferPool buffer ← BytesMut::from(data)
                           └→ return (offset, length)
                               └→ 200 OK (WAL not yet — separate feature)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core` and `oceanfs-storage`
- [x] **Tests:** Unit tests for append boundaries, inline threshold routing, buffer pool acquire/release, shard hashing distribution
- [x] **ADR:** ADR-0001 constraints satisfied (segment packing, tiered sizes)
- [x] **Perf:** Rules 1.1, 1.2, 1.3 verified — no `Vec<u8>` on hot path, buffer pool exists, collections pre-sized
- [x] **Integration:** `tests/segment_roundtrip.rs`: append blobs of 1 B, 4 KB, 64 KB, 256 KB, 1 MB; verify offset accounting and threshold routing
<!-- REVIEW: Integration test `tests/segment_roundtrip.rs` tests ActiveSegment append/buffer-pool/index/header but does NOT perform end-to-end seal-then-read-back cycle as specified in the feature doc ("write blobs to active segment, seal, read back via index"). Sealer end-to-end testing lives in unit tests (sealer.rs) only. -->
