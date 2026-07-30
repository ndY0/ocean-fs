# ADR-0001: Segment Packing vs Per-Object Erasure Coding

**Status:** Accepted
**Date:** 2026-07-30
**Deciders:** OceanFS design team

---

## Context

Erasure coding is the chosen redundancy model for OceanFS (§6 of the
spec). The naive approach — applying EC per-object — is what MinIO
does. This creates a well-known problem: small objects produce EC
stripes that are mostly padding, amplifying metadata, I/O, and
healing cost per stored byte.

We need to choose an EC strategy that eliminates this sensitivity to
object size while maintaining throughput, simplicity, and storage
efficiency.

### Constraints

- The system must handle objects from 1 byte to terabytes without
  pathological behavior at any size.
- EC encode/decode is the most CPU-intensive operation in the system.
  We must not waste cycles on padding.
- Healing granularity must be coarse enough that node failure
  recovery is not dominated by per-object transactional overhead.
- The approach must remain compatible with hardware acceleration
  (batch EC on GPU).
- The architecture must not introduce unbounded GC debt that
  requires stop-the-world compaction.

### Prior Art

| System | Approach | Problem |
|---|---|---|
| MinIO | Per-object EC (Reed-Solomon) | Metadata bloat for small objects; tiny files stored inline in metadata; healing per-object is slow |
| Ceph | RADOS objects (4 MB default), EC applied to object sets | Good for uniform object sizes; small objects still have overhead |
| Facebook's f4 | EC applied to large "cells" of many blobs packed together | Excellent for cold storage; read amplification for single-blob access |
| ScyllaDB | Log-structured storage, compaction | Segments with GC; good efficiency but GC overhead |

## Decision

**Use log-structured segment packing with tiered segment sizes.**

Blobs are written to append-only segments. A segment is the unit of
EC encoding, placement, healing, and scrubbing — not the blob.

### Segment Tiers

| Blob Size | Segment Strategy | Rationale |
|---|---|---|
| ≤ 4 KB | Stored inline in metadata (RocksDB value) | Zero EC overhead; served from memory via metadata cache |
| 4 KB – 256 KB | Packed into small segments (64 KB target) | Reduces read amplification; EC stripes are full |
| 256 KB – 4 MB | One segment per blob (or adaptive k) | EC stripes remain full for blobs near 4 MB |
| > 4 MB | Split into multiple 4 MB segments | Uniform segment size → predictable EC behavior |

### Key Properties

1. **EC always operates on well-sized stripes.** Every segment is
   either 64 KB or 4 MB of actual data before encoding. Padding is
   present only for the final partial stripe (≤ 1 stripe per segment).

2. **Healing is per-segment.** One node failure → one heal operation
   per affected segment, not per affected blob. A 4 MB segment
   holding 10,000 × 400-byte blobs heals in ~10 ms of EC decode
   (one segment), not 10,000 × per-object operations.

3. **Metadata is a separate B-tree.** Object metadata points to
   `(segment_id, offset, length)`. No per-object storage overhead.
   Listing is an O(log n) B-tree scan, not an O(n) disk walk.

4. **Segment blob index.** Each segment stores a sorted B-tree index
   at its head for O(log n) blob lookup within the segment.

### Configuration

```toml
[bucket.my-bucket]
inline_threshold_bytes          = 4096
segment_small_threshold_bytes   = 262144
segment_small_target_size       = 65536
segment_default_target_size     = 4194304
segment_seal_timeout_ms         = 500
```

All thresholds are configurable per bucket.

## Consequences

### Positive
- Blob size has zero impact on EC efficiency. The system performs
  predictably across orders of magnitude (1 byte to 1 TB).
- Healing is fast: one segment operation replaces thousands of
  per-object operations. A node failure that affects 100,000 blobs
  might require healing only 50 segments.
- Read amplification for inline blobs is zero — they are served from
  memory or a single RocksDB GET.
- Hardware acceleration (GPU batch EC) works naturally: a segment's
  stripes are batched into one kernel call.

### Negative
- **Garbage collection is required.** Deleted blobs leave dead space
  in segments. Compaction is needed to reclaim it. This adds
  operational complexity.
- **Read amplification for packed segments.** Reading a 1 KB blob
  from a 64 KB segment reads 64 KB of EC shards. Reading from a 4 MB
  segment reads 4 MB. This is mitigated by the inline tier and the
  L1 object cache.
- **Seal latency.** Writes may wait up to `segment_seal_timeout_ms`
  (500 ms default) before a segment is sealed and the blob is
  durable in EC form. Until then, the blob is WAL-only. Mitigated
  by `write_ack_after_wal=true` which acks the client after WAL
  quorum, not after EC.
- **GC complexity.** Segment compaction is non-trivial and must not
  block the write path.

### Neutral
- The system now has three storage tiers (inline, small segment,
  standard segment) which adds configuration complexity.
- Operators must tune `segment_seal_timeout_ms` and segment sizes
  for their workload.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Per-object EC (MinIO model)** | Simple; no GC; immediate durability per object | Small objects amplify metadata and healing; partial-stripe padding wastes I/O | Rejected: small-object behavior is the primary complaint against MinIO |
| **Content-defined chunking (restic/bup model)** | Deduplication; variable-size boundaries | Chunk boundaries are unrelated to blob boundaries; high read amplification; complex index | Rejected: unnecessary for a blob store (blobs are opaque, not content-addressable) |
| **Fixed 4 MB segments, all blobs packed** | Simpler code (no tiered sizes); uniform EC behavior | Reading any blob reads 4 MB of shards → unacceptable read amplification for small blobs | Rejected: violates throughput requirement for small-object workloads |
| **Separate small-blob key-value store** | Small blobs handled by an optimized KV; EC only for large blobs | Two storage engines to maintain; split-brain for blob routing; operational hell | Rejected: increases operational surface for marginal benefit over inline+small-segment tiers |

## References

- [OceanFS Specification §3.2: Tiered Segment Sizing](../spec.md#32-tiered-segment-sizing-minio-workaround)
- [MinIO Erasure Coding Design](https://min.io/docs/minio/linux/operations/concepts/erasure-coding.html)
- [Facebook f4: Warm BLOB Storage System (OSDI 2014)](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/muralidhar)
- [OceanFS Performance Guidelines §1: Memory & Allocation](../../guidelines/performance.md#1-memory--allocation)
