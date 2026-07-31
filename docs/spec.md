# OceanFS — Specification

**Version:** 0.2.0 — Draft
**Language:** Rust
**Protocol:** S3-compatible HTTP
**Date:** 2026-07-30

---

## 1. Project Overview

**OceanFS** is a distributed, orchestrator-free blob storage system optimized for
throughput, tunable consistency, and configurable redundancy via erasure coding
with hardware acceleration. Written in Rust.

### 1.1 Design Goals

| Goal                   | Approach                                                     |
| ---------------------- | ------------------------------------------------------------ |
| No orchestrator        | DHT overlay + gossip membership, consistent-hashing routing  |
| Error resilience       | BLAKE3 per-segment checksums, Merkle trees, background scrubbing |
| Storage efficiency     | Erasure coding with segment-packing to eliminate small-blob amplification |
| Durability             | Configurable k+m EC, tunable read/write quorum               |
| Maximize throughput    | Log-structured writes, pipeline-parallel EC, multi-layer caching, parallel shard fetch |
| Small-object perf      | Inline storage, tiered segment sizes, L1 object cache, metadata LRU |
| Hardware acceleration  | Tiered: CPU SIMD → ISA-L/libec → GPU/CUDA                    |
| Operational flexibility| Every performance property configurable per bucket           |

### 1.2 Architecture Decision Records

Key design decisions are documented as ADRs in [`docs/adr/`](adr/):

| ADR | Topic |
|---|---|
| [ADR-0001](adr/0001-segment-packing.md) | Segment packing vs per-object erasure coding |
| ADR-0002 | SWIM + consistent hashing vs Raft per shard (forthcoming) |
| ADR-0003 | Cauchy RS vs standard RS vs Clay codes (forthcoming) |
| [ADR-0006](adr/0006-hardware-acceleration-tier-model.md) | Hardware acceleration tier model |

---

## 2. Core Architecture

### 2.1 Node Architecture

```
+------------------------------------------------------------------+
|                          OceanFS Node                             |
|                                                                   |
|  +-----------+  +--------------+  +----------------+  +--------+  |
|  | HTTP/gRPC |  | DHT + Gossip |  | Repair /        |  | Scrub  |  |
|  | Frontend  |  | Membership   |  | Heal Scheduler  |  | Worker |  |
|  +-----+-----+  +------+-------+  +-------+--------+  +---+----+  |
|        |               |                   |               |       |
|  +-----+-----+  +------+-------+  +--------+-------+  +---+----+  |
|  | Object    |  | Routing      |  | Prefetch       |  | Buffer  |  |
|  | L1 Cache  |  | (ring cache) |  | Engine         |  | Pool    |  |
|  +-----+-----+  +------+-------+  +----------------+  +--------+  |
|        |               |                                           |
|  +-----+-------+-------+-----------------------+-------------+    |
|  |                    Routing Layer                            |    |
|  |      (consistent hashing -> local vs forward)               |    |
|  +-----------------------------+-----------------------------+    |
|                                |                                  |
|  +-----------------------------+-----------------------------+    |
|  |                    Storage Engine                          |    |
|  |  +----------+  +----------+  +-------------+  +---------+ |    |
|  |  | Metadata |  | Metadata |  | Commit Log  |  | Segment | |    |
|  |  | RocksDB  |  | LRU Cache|  | (WAL, ring) |  | Store   | |    |
|  |  | (CF:     |  | (hot obj |  |              |  | (EC'd   | |    |
|  |  | objects, |  | metadata)|  |              |  | shards) | |    |
|  |  | segments,|  +----------+  +-------------+  +---------+ |    |
|  |  | deletions|                                               |    |
|  |  +----------+                                               |    |
|  +------------------------------------------------------------+    |
|                                                                   |
|  +------------------------------------------------------------+    |
|  |  Acceleration Subsystem (tiered)                            |    |
|  |  Tier 0: CPU SIMD (AVX-512, NEON)                          |    |
|  |  Tier 1: ISA-L / libec (CPU-optimized libraries)           |    |
|  |  Tier 2: GPU / CUDA (optional, for batch EC)               |    |
|  +------------------------------------------------------------+    |
|                                                                   |
|  +------------------------------------------------------------+    |
|  |  Connection Pool (gRPC keepalive, per-peer reuse)           |    |
|  +------------------------------------------------------------+    |
+------------------------------------------------------------------+
```

### 2.2 Distributed Model

#### Membership (SWIM + Gossip)

- SWIM-based failure detection. Each node maintains a partial view of the
  cluster.
- Direct ping -> indirect ping (through k random peers) -> SUSPECT after
  `suspicion_timeout_ms` -> DEAD after `failure_timeout_ms`.
- State (membership, ring layout) disseminated via push-pull gossip every
  `gossip_interval_ms`.

#### Key Routing (Consistent Hashing)

- Each node owns `vnodes_per_node` virtual nodes distributed around a 256-bit
  ring (SHA-256 hash space).
- A blob key hashes to a position on the ring. The `replication_factor` (N)
  successors form the replica set for that key.
- Per-shard coordination: the N successors form an implicit group. Reads/writes
  use a tunable quorum (R, W per bucket). No explicit leader election for reads;
  writes coordinate through the first successor.
- **Ring cache:** The ring topology is cached locally. Updated on gossip events
  only. Routing becomes a binary search + modulo lookup — microseconds.

```
Key -> SHA-256 -> Ring position -> {node_1, node_2, node_3} (successors)
                                    |
                                    +- W nodes must ack write
                                    +- R nodes must ack read
```

#### Why This Over Raft Per Shard

> ADR-0002 (forthcoming) covers the full distribution model tradeoff.

- Raft imposes a latency floor (leader election, log append to majority).
- For a blob store where writes are large and reads dominate, the quorum model
  allows R=1 reads for speed and W=N writes when consistency matters, without
  leader overhead.
- Consistent hashing means node additions/removals affect only O(N/M) keys on
  average, not a full rebalance.

---

## 3. Data Model

### 3.1 Hierarchy

```
Bucket
 +- name, policy (EC params, quorum, tuning knobs, cache config)
 +- Object_1 ... Object_n
 |    +- object_key (UTF-8)
 |    +- size, mime, created_at
 |    +- blake3_hash
 |    +- inline_data (if size <= inline_threshold_bytes)
 |    +- chunks[] -> (segment_id, offset, length)
 +- Segment_1 ... Segment_n
      +- segment_id (UUIDv7, time-sortable)
      +- blake3_hash (of entire segment)
      +- EC params: (k, m) used for this segment
      +- size_tier: "small" | "standard"
      +- blob_index: sorted B-tree index of blobs within this segment
      +- chunk_offsets[] -> positions of each blob within
      +- merkle_root (Merkle tree over 64 KB leaf hashes)
```

A **Segment** is the unit of EC encoding, placement, healing, and scrubbing. A
segment is an append-only container holding one or more blobs. Once sealed (full
or timeout), it is EC-encoded and distributed across k+m nodes.

### 3.2 Tiered Segment Sizing (MinIO Workaround)

MinIO applies EC per-object, regardless of size, leading to:
- Metadata bloat for small objects (inline in `xl.meta`).
- Partial-stripe amplification for objects < stripe size.
- Slow healing (one operation per object).

**OceanFS uses a four-tier approach based on blob size:**
(Rationale: [ADR-0001](adr/0001-segment-packing.md))

| Blob Size      | Tier              | Strategy                                                   |
| -------------- | ----------------- | ---------------------------------------------------------- |
| ≤ T_inline     | **Inline**        | Blob stored directly in RocksDB metadata value. Served in a single RocksDB GET. Zero segment I/O. |
| T_inline – T_sm | **Small segment** | Batched into 64 KB target segments with other small blobs. EC delayed until segment full or `seal_timeout`. |
| T_sm – T_std   | **Standard segment** | Own 4 MB segment, EC'd immediately. k chosen adaptively so stripes are full. |
| > T_std        | **Multi-segment** | Split into multiple 4 MB segments, EC'd independently. |

Default thresholds:

```toml
inline_threshold_bytes         = 4096    # 4 KB
segment_small_threshold_bytes  = 262144  # 256 KB
segment_small_target_size      = 65536   # 64 KB
segment_default_target_size    = 4194304 # 4 MB
```

**Read amplification for a single blob read:**

| Blob size    | Tier       | Lookup path                            | Approx I/O     |
| ------------ | ---------- | -------------------------------------- | -------------- |
| ≤ 4 KB       | Inline     | RocksDB GET (or metadata cache hit)    | 0-1            |
| 4-256 KB     | Small seg  | Metadata → segment → EC decode (≤64 KB)| 0-1 + k        |
| 256 KB-4 MB  | Standard   | Metadata → segment → EC decode (≤4 MB) | 0-1 + k        |
| > 4 MB       | Multi-seg  | Metadata → N segs → EC decode (N×4 MB) | 0-1 + N×k      |

### 3.3 Inline Storage

For blobs at or below `inline_threshold_bytes`, the blob payload is stored
directly in the `objects` column family value. This is what CockroachDB,
FoundationDB, and TiKV do.

**Read path for inline blobs:**
1. Check L1 object cache → hit → serve from memory (0 I/O).
2. Check metadata LRU cache → hit → extract inline data from cached metadata (0 I/O).
3. RocksDB GET → extract inline data from value (1 I/O).

No segment lookup. No shard fetch. No EC decode. Hot small blobs served purely
from memory.

**Tradeoff:** RocksDB value size grows. Mitigated by:
- The metadata LRU cache (hot metadata stays in RAM).
- `inline_threshold_bytes` is configurable per bucket — can be 0 to disable.
- Block-based compression in RocksDB (zstd).

### 3.4 Segment-Level Blob Index

Segments containing multiple blobs (small and standard tiers) store a sorted
B-tree index at the segment head, mapping `(offset, length, blob_key_hash)` for
each blob. When a blob is read from a segment, the index is consulted (O(log n)
vs O(n) scan of `chunk_offsets[]`). The index is loaded on first access and
cached in segment metadata.

### 3.5 Metadata Store

RocksDB per node, with three column families:

| Column Family | Key                      | Value                                                                                     |
| ------------- | ------------------------ | ----------------------------------------------------------------------------------------- |
| `objects`     | `(bucket_id, object_key)`| ObjectMetadata: size, blake3_hash, chunk_list[], inline_data (if applicable), created_at, hlc |
| `segments`    | `segment_id`             | SegmentMetadata: ec_k, ec_m, size_tier, merkle_root, storage_locations[], sealed_at, ...   |
| `deletions`   | `(bucket_id, object_key)`| Tombstone: deletion_time, hlc                                                              |

---

## 4. Write Path

### 4.1 Write Flow

```
PUT /{bucket}/{key}
       |
       v
+----------------------------+
| 1. Hash key -> ring         |  Route to responsible node set.
|    Find N successors        |
+-----------+----------------+
            |
            v
+----------------------------+
| 2. Coordinator routes blob  |  Based on blob size, choose tier:
|    to active segment        |    ≤ T_inline  -> write to RocksDB metadata inline
|    (pool of active segments)|    T_inline-T_sm -> small segment pool
|                             |    T_sm-T_std    -> standard segment pool
|                             |    > T_std       -> multi-segment writer
+-----------+----------------+
            |
            v
+----------------------------+
| 3. Append to segment buffer |  In-memory ring buffer + WAL.
|    (per-core sharded for    |  Segments are sharded by request thread ID
|    concurrency)             |  to reduce lock contention.
+-----------+----------------+
            |
            v
+----------------------------+
| 4. WAL-ack to W successors  |  fsynced commit-log on each successor.
|    Wait for quorum W.       |  Quorum satisfied -> ack 200 to client.
+-----------+----------------+        <-- CLIENT RECEIVES 200
            |
            v  (async, post-ack if write_ec_async=true)
+----------------------------+
| 5. Segment sealed?          |  On full (> target_size) or timeout:
|    If yes: EC-encode all    |  encode all stripes in parallel (rayon,
|    stripes in parallel.     |    or batched CUDA kernel).
|    Distribute m parity      |  k data + m parity shards -> k+m nodes.
|    shards.                  |  Update segment metadata in RocksDB.
+-----------+----------------+
            |
            v
+----------------------------+
| 6. Update object metadata   |  Write (segment_id, offset, length) or
|    on W metadata nodes.     |  inline_data to metadata.
|    Truncate WAL.            |  Truncate WAL past sealed segment boundary.
+----------------------------+
```

### 4.2 Pipeline Parallelism

Segments are append-only; a sealed segment cannot accept new writes. A **pool of
active segments** (default 4) per node per tier prevents EC encoding of segment
N from blocking writes to segment N+1. While one segment is being EC-encoded
(asynchronously), the next segment in the pool accepts writes. This decouples
append latency from EC encode time.

```
 Active Segment Pool
+----+  +----+  +----+  +----+
| S0 |  | S1 |  | S2 |  | S3 |
|appending| |sealing| | EC'ing| | idle  |
+----+  +----+  +----+  +----+
```

The pool size is configurable:

```toml
segment_active_pool_size = 4
```

### 4.3 Per-Core Segment Sharding

Multiple concurrent PUTs contend on the active segment's append lock. To reduce
contention, the coordinator hashes the connection or request ID to one of N
active segment groups (sharded by thread/core). Each group has its own active
segment pool. Segments from different shards are sealed and EC-encoded
independently.

```toml
segment_shard_count = 4   # number of independent active segment groups
```

### 4.4 Failure Handling During Write

| Failure Point                       | Behavior                                                           |
| ----------------------------------- | -----------------------------------------------------------------  |
| Step 4: cannot reach quorum W       | 503 to client, data discarded.                                     |
| Step 5: EC-encode node crash        | WAL replay on restart picks up unsealed segments.                  |
| Step 6: metadata write fails        | Segment exists but unreferenced; background GC reclaims.           |
| Successor unreachable during write  | Hinted handoff: next successor temporarily accepts with `{intended_for}` hint. |

---

## 5. Read Path & Caching

### 5.1 Read Flow

```
GET /{bucket}/{key}
       |
       v
+----------------------------+
| 0. L1 Object Cache          |  Bucket-scoped LRU of hot blob payloads.
|    (if enabled)             |  HIT -> verify BLAKE3, serve from memory.
+-----------+----------------+        <-- 0 I/O path for hot objects
            | MISS
            v
+----------------------------+
| 1. Metadata LRU Cache       |  LRU of ObjectMetadata entries.
|    (if enabled)             |  HIT -> get chunk_list or inline_data.
+-----------+----------------+
            | MISS
            v
+----------------------------+
| 2. Negative Cache           |  Per-bucket Bloom filter.
|    (if enabled)             |  MISS -> definitely not present -> 404.
+-----------+----------------+        <-- avoids RocksDB lookup
            | MAYBE present
            v
+----------------------------+
| 3. RocksDB metadata lookup. |  Query -> ObjectMetadata.
|    (R nodes consulted)      |  If R > 1, compare HLC across replicas.
|    If inline_data present:  |  Extract & return. Populate caches.
+-----------+----------------+
            | (non-inline blob)
            v
+----------------------------+
| 4. For each chunk:          |
|    Fetch k of k+m shards    |  All required stripes fetched in parallel
|    in parallel.             |  across all chunks (inter-chunk + intra-
|    Use fastest k responses. |  stripe parallelism).
|    EC-decode if needed.     |  Decode stripes in parallel (rayon).
+-----------+----------------+
            |
            v
+----------------------------+
| 5. Extract blob from        |  Segment blob index -> offset + length.
|    segment.                 |
+-----------+----------------+
            |
            v
+----------------------------+
| 6. Verify BLAKE3 checksum.  |  Compare computed hash vs stored.
|    Mismatch -> initiate      |  Populate L1 cache on success.
|    healing for segment.     |
+-----------+----------------+
            |
            v
      200 + blob bytes
```

### 5.2 Caching Layers

OceanFS adds three caching layers between the HTTP frontend and the storage
engine, progressively trading memory for reduced I/O:

#### L1: Object Data Cache

An in-memory LRU cache of hot blob payloads. Serves frequently accessed blobs
with zero disk I/O. Scoped by bucket.

```toml
object_cache_enabled         = true
object_cache_size_bytes      = 536870912    # 512 MB
object_cache_ttl_ms          = 60000        # 1 min (0 = no expiry)
object_cache_max_blob_size   = 1048576      # only cache blobs ≤ 1 MB
```

- **Eviction:** LRU with TTL. Entries evicted on expiry or memory pressure.
- **Invalidation:** On PUT or DELETE of the same blob key (best-effort; cache is
  allowed to serve stale data within TTL, since BLAKE3 verification happens on
  each read anyway).
- **Population:** On successful GET, the blob is inserted into the L1 cache if
  `blob_size <= object_cache_max_blob_size`.

#### L2: Metadata Cache

An in-memory LRU cache of `ObjectMetadata` entries. Avoids RocksDB lookup for
hot objects.

```toml
metadata_cache_enabled       = true
metadata_cache_size_bytes    = 1073741824   # 1 GB
metadata_cache_ttl_ms        = 300000       # 5 min (0 = no expiry)
```

- **Benefit:** Serves inline blobs (≤ `inline_threshold_bytes`) directly from
  cached metadata. For larger blobs, provides the chunk list without the RocksDB
  query.
- **Invalidation:** On write/delete, invalidate the cache entry for that key.
  Metadata nodes gossip invalidations lazily via the anti-entropy channel.

#### L3: Negative Cache

A per-bucket Bloom filter (or Cuckoo filter) that answers "does this key exist?"
without touching RocksDB.

```toml
negative_cache_enabled       = true
negative_cache_size_bytes    = 67108864     # 64 MB bloom filter
negative_cache_rebuild_sec   = 3600         # rebuild from metadata every hour
```

- **Benefit:** `HEAD` requests for non-existent objects return 404 in constant
  time. Critical for workloads with many ephemeral keys (e.g., compute
  intermediates).
- **False positives:** Bloom filters can produce false positives
  (configurable fp rate, default 0.01%). A false positive means an unnecessary
  RocksDB lookup — not incorrect behavior.
- **False negatives:** Impossible by construction.
- **Rebuild:** Periodically rebuilt from the `objects` column family by scanning
  for deleted tombstones.

#### Cache Coherence

Caches are node-local and eventually consistent. The L1 cache serves stale data
at worst within the TTL window. BLAKE3 verification on every read catches
corruption regardless. Metadata cache invalidations propagate through gossip
(hard invalidation on write) or expire via TTL (soft invalidation).

### 5.3 Read Optimizations

| Configuration                  | Effect                                                   |
| ------------------------------ | -------------------------------------------------------- |
| `read_quorum = 1`              | Fastest reads (one node). May serve stale data.          |
| `read_quorum = W`              | Strong consistency (read overlaps write quorum).         |
| `read_parallel_fetch = true`   | Fetch all k+m shards in parallel.                        |
| `read_use_fastest_k = true`    | Return as soon as k shards arrive.                       |
| `read_cache_segments = true`    | mmap hot segments in page cache.                         |
| `read_stripe_parallelism = 16` | Number of stripes to fetch + decode concurrently.        |

---

## 6. Erasure Coding

### 6.1 Supported Codecs

| Codec            | Description                                      | Best For                          |
| ---------------- | ------------------------------------------------ | --------------------------------- |
| **Cauchy RS**    | Reed-Solomon over Cauchy matrices, XOR-only ops  | General-purpose, fast encode/decode |
| **Standard RS**  | Classical Reed-Solomon RS(k,m) over GF(2^8)      | Maximum flexibility               |
| **LRC**          | Local Reconstruction Codes, local parity groups  | Reduced repair bandwidth          |
| **ISA-L RS**     | Intel ISA-L optimized Reed-Solomon               | x86 with AVX-512 (line-rate)      |
| **Clay**         | MSR-optimal (Minimum Storage Regeneration) codes | Repair-bandwidth bottleneck       |

Default: **Cauchy Reed-Solomon** via ISA-L on x86, GF-complete on ARM.
> ADR-0003 (forthcoming) covers the EC codec selection tradeoffs.

### 6.2 Striping Layout

```
     +-------- Segment (e.g. 4 MB target) --------+
     |                                               |
     |  +-----+-----+-----+-----+-----+-----+-----+ |
     |  | D_0 | D_1 | D_2 | D_3 | P_0 | P_1 | P_2 | |
     |  | 64KB| 64KB| 64KB| 64KB| 64KB| 64KB| 64KB| |
     |  +-----+-----+-----+-----+-----+-----+-----+ |
     |  |  D  |  D  |  D  |  D  |  P  |  P  |  P  | |  ... rows (stripes)
     |  +-----+-----+-----+-----+-----+-----+-----+ |
     |         ^                   ^                  |
     |    k data shards        m parity shards         |
     +-----------------------------------------------+

     Strip size: ec_strip_size_bytes (default 64 KB, configurable).
     Total stripes per segment = segment_size / (k * strip_size).
```

### 6.3 Parity Placement

- k data shards + m parity shards distributed across k+m distinct nodes.
- No single node holds >1 shard for a given segment.
- The coordinator picks k+m distinct nodes from the ring (successors +
  lookahead), rotating per segment to balance load.

### 6.4 EC Parallelism

#### Intra-Segment Stripe Parallelism

A segment's stripes are **independent** — each stripe is encoded/decoded from
its row alone. All stripes within a segment are encoded in parallel:

- **CPU (rayon):** A parallel iterator dispatches each row's GF(2^8) matrix
  multiply across all available cores. Work-stealing handles uneven stripe
  counts.
- **GPU (CUDA):** The entire segment's stripes are batched into a single CUDA
  kernel call. The GPU performs the matrix multiply on all stripes
  simultaneously.

```toml
ec_parallel_stripes = 0   # 0 = auto (num_cpus), N = fixed thread count
```

#### Inter-Segment Parallelism (Write)

When multiple segments are being sealed concurrently (from different shard
groups or pool slots), their EC encoding runs in parallel using the global tokio
thread pool.

### 6.5 Healing

When a node fails (DEAD detected via SWIM), for every segment that had a shard
on the dead node:
1. Read k surviving shards from remaining nodes.
2. EC-decode in parallel across all affected segments (inter-segment) and all
   stripes within each segment (intra-stripe), using the acceleration tier.
3. Place reconstructed shards on new successor nodes.
4. Update segment metadata with new `storage_locations[]`.

Healing is batched (one operation per segment, not per blob) and maximally
parallel.

```toml
heal_parallel_segments  = 16    # max concurrent segments to heal
heal_parallel_stripes   = 0     # 0 = auto (num_cpus) per segment
heal_throttle_bytes_sec = 0     # 0 = unlimited, N = rate limit
```

---

## 7. Consistency & Anti-Entropy

### 7.1 Tunable Quorum

Per bucket:

```toml
[bucket.my-bucket.consistency]
write_quorum    = 2    # W: nodes that must ack writes
read_quorum     = 2    # R: nodes that must ack reads
total_replicas  = 3    # N: total nodes in replica set
```

| Invariant  | Meaning                                       |
| ---------- | --------------------------------------------- |
| `W + R > N`| Strong consistency (read sees every write)    |
| `W + R ≤ N`| Eventual consistency (read may miss writes)   |

### 7.2 Hinted Handoff

If a node in a write set is unreachable, a fallback node (next successor on the
ring) accepts the write with a hint `{intended_for: node_X}`. When `node_X`
returns, the fallback pushes the buffered data and clears the hint.

### 7.3 Read Repair

On a read with `R > 1`, if responses disagree (checksum mismatch or stale
version vector), the coordinator:
1. Serves the latest version to the client.
2. Asynchronously pushes corrected data to stale nodes.

### 7.4 Anti-Entropy (Background)

- **Merkle tree exchange:** Every `anti_entropy_interval_sec` (default 300s),
  neighbor nodes exchange Merkle roots for shared segments. On mismatch they
  descend the tree to identify diverged shards and repair.
- **Active scrubbing:** Every `scrub_interval_sec` (default 7 days), walk every
  segment, read all shards, verify against stored BLAKE3 hash and Merkle root.
  Auto-heal on mismatch.

### 7.5 Distributed Scrubbing

Scrubbing is a partitioned, fan-out operation — not a single node's background
task:
1. A randomly elected scrub coordinator partitions the segment ID space across
   all healthy nodes.
2. Each node scrubs its assigned partition: reads shards, verifies BLAKE3 + Merkle
   root, reports discrepancies.
3. Discrepant segments are placed on a repair queue and healed (see §6.5).
4. The coordinator aggregates results and emits a scrub report to
   `/admin/metrics`.

```toml
scrub_parallel_nodes = 0    # 0 = all nodes participate, N = max nodes
```

### 7.6 Versioning & Conflict Resolution

- Each object write gets a **Hybrid Logical Clock (HLC)** timestamp.
- Default: **Last-Write-Wins (LWW)** by HLC.
- Pluggable conflict resolver for multi-writer scenarios (configurable per
  bucket).

---

## 8. Throughput Tuning

### 8.1 Configuration Surface

Almost every performance-sensitive parameter is configurable per bucket:

```toml
[bucket.my-bucket.tuning]
# Segment sizing (tiered)
inline_threshold_bytes          = 4096       # 4 KB
segment_small_threshold_bytes   = 262144     # 256 KB
segment_small_target_size       = 65536      # 64 KB
segment_default_target_size     = 4194304    # 4 MB
segment_seal_timeout_ms         = 500
segment_active_pool_size        = 4          # active segment pool
segment_shard_count             = 4          # per-core segment groups

# Quorum
write_quorum    = 2
read_quorum     = 1
total_replicas  = 3

# Erasure coding
ec_data_shards      = 4        # k
ec_parity_shards    = 2        # m
ec_strip_size_bytes = 65536    # 64 KB per shard
ec_codec            = "cauchy_rs"
ec_parallel_stripes = 0        # 0 = auto (num_cpus)

# Read path
read_parallel_fetch  = true
read_use_fastest_k   = true
read_cache_segments  = true
read_stripe_parallelism = 16

# Write path
write_ack_after_wal  = true    # ack after step 4 vs after step 5
write_ec_async       = true    # EC encode post-ack

# Caching
object_cache_enabled        = true
object_cache_size_bytes     = 536870912    # 512 MB
object_cache_ttl_ms         = 60000
object_cache_max_blob_size  = 1048576

metadata_cache_enabled      = true
metadata_cache_size_bytes   = 1073741824   # 1 GB
metadata_cache_ttl_ms       = 300000

negative_cache_enabled      = true
negative_cache_size_bytes   = 67108864     # 64 MB

# Hardware acceleration
accel_ec_tier   = "auto"        # auto | cpu_simd | isa_l | gpu_cuda
accel_hash_tier = "auto"        # auto | cpu | avx512

# Healing
heal_parallel_segments  = 16
heal_throttle_bytes_sec = 0     # 0 = unlimited

# Compaction / GC
gc_interval_sec       = 3600
gc_tombstone_ttl_sec  = 259200  # 3 days
gc_compact_threshold  = 0.5
```

### 8.2 Read-Optimized Profile

```toml
read_quorum             = 1
write_quorum            = 3
ec_data_shards          = 8      # wide stripe -> more parallelism
ec_parity_shards        = 2
read_parallel_fetch     = true
read_use_fastest_k      = true
write_ec_async          = true
write_ack_after_wal     = true
object_cache_enabled    = true
object_cache_size_bytes = 8589934592   # 8 GB
metadata_cache_size_bytes = 2147483648 # 2 GB
negative_cache_enabled  = true
segment_active_pool_size = 8
```

### 8.3 Write-Optimized Profile

```toml
read_quorum             = 3
write_quorum            = 1
ec_data_shards          = 2      # narrow stripe -> fewer writes
ec_parity_shards        = 2
write_ec_async          = false
write_ack_after_wal     = false
object_cache_enabled    = false   # avoid invalidation overhead
segment_active_pool_size = 8
segment_shard_count     = 8       # maximize write concurrency
```

### 8.4 Tuning Tradeoff Summary

| Knob                    | Read-Optimized       | Write-Optimized       |
| ----------------------- | -------------------- | --------------------- |
| Stripe width (k)        | Wide (8-12)          | Narrow (2-4)          |
| Write quorum (W)        | High (N)             | Low (1)               |
| Read quorum (R)         | Low (1)              | High (N)              |
| WAL-only ack            | true                 | false                 |
| Async EC                | true                 | false                 |
| L1 object cache         | Large (8 GB)         | Disabled              |
| Metadata cache          | Large (2 GB)         | Moderate (256 MB)      |
| Active segment pool     | 4-8                  | 8-16                  |
| Segment shard count     | 1-4                  | 4-8                   |
| Parallel fetch          | true, all shards     | false                 |

---

## 9. Hardware Acceleration

OceanFS accelerates computationally intensive operations through a three-tier
model that probes available hardware at startup and delegates work to the most
capable backend. The acceleration subsystem implements the `Encoder`/`Decoder`
traits from the EC layer.

```
Operation              Tier 0 (baseline)        Tier 1 (optimized)         Tier 2 (offload)
------------------------------------------------------------------------------------------
BLAKE3 hashing         CPU (blake3 auto-detect) AVX-512 intrinsics         n/a (line-rate)
EC encode/decode       GF-complete (portable)   ISA-L (Intel, AVX-512)     CUDA kernel
                                               ARM NEON/SVE (aarch64)     (batch EC ops)
Compression (zstd)     CPU (zstd crate)         ISA-L igzip                nvCOMP (GPU batch)
Encryption (AES-GCM)   CPU (aes-gcm crate)      AES-NI intrinsics          GPU (future)
```

The tier is selected per bucket via `accel_ec_tier`. The `auto` tier probes:
CUDA → ISA-L → CPU SIMD, selecting the first available.

### 9.1 Acceleration Subsystem Architecture

The acceleration subsystem is composed of three layers:

```
oceanfs-ec                          ← trait definitions (Encoder, Decoder)
      ↑
oceanfs-accel                       ← backend implementations + dispatcher
      ↑
oceanfs-storage, oceanfs-server     ← consumers (via AccelDispatcher)
```

#### 9.1.1 Component Diagram

```
+-------------------------------------------------------------+
|                     AccelDispatcher                          |
|                                                              |
|  +------------------+  +------------------+  +------------+  |
|  | Tier 0           |  | Tier 1           |  | Tier 2     |  |
|  | CPU SIMD         |  | ISA-L            |  | GPU/CUDA   |  |
|  |                  |  |                  |  |            |  |
|  | GF-complete RS   |  | Intel ISA-L RS   |  | CUDA       |  |
|  | (portable)       |  | (AVX-512)        |  | kernel     |  |
|  |                  |  |                  |  |            |  |
|  | Runtime SIMD     |  | Feature: isa-l   |  | Feature:   |  |
|  | dispatch         |  |                  |  | cuda       |  |
|  | (SSE4.1/AVX2/    |  | libec (ARM SVE)  |  |            |  |
|  |  AVX-512)        |  | (future)         |  | nvCOMP     |  |
|  +------------------+  +------------------+  +------------+  |
|                                                              |
|  +------------------+  +-----------------------------------+ |
|  | Hash             |  | Compression / Encryption          | |
|  | BLAKE3 (auto)    |  | zstd crate / nvCOMP / AES-GCM    | |
|  +------------------+  +-----------------------------------+ |
+-------------------------------------------------------------+
        |
        | (implements Encoder, Decoder from oceanfs-ec)
        v
+------------------+
| Consumer         |
| ParallelEncoder  |
| WriteCoordinator |
| Heal Scheduler   |
+------------------+
```

#### 9.1.2 Backend Lifecycle

Every backend follows the same lifecycle:

```
Construction → Probe → Initialize → Available
                                    │
                                    ├── encode/decode calls (hot path)
                                    │
                                    └── Drop (release GPU memory, close FFI handles)
```

Backends that fail to probe (e.g., `CudaBackend` when no GPU present, `IsalEncoder`
when AVX-512 absent) are never constructed. The dispatcher skips them during tier
resolution.

#### 9.1.3 Concurrency Model

Each backend declares its own concurrency characteristics:

| Backend | Concurrency | Mechanism |
|---|---|---|
| CPU SIMD | Unlimited (CPU-bound, rayon work-stealing) | None needed |
| ISA-L | Unlimited (CPU-bound, single-threaded per stripe) | None needed |
| CUDA | Semaphore-bounded (default 1) | `tokio::sync::Semaphore` |

The CUDA semaphore is acquired before every GPU operation and released on
completion. This serializes GPU access because GF(2^8) matrix multiplication
saturates GPU compute with a single kernel launch. Multiple concurrent launches
contend for SMs and memory bandwidth, reducing total throughput through context
switching overhead.

```toml
ec_gpu_max_concurrent_ops = 1   # permits for the GPU semaphore
```

### 9.2 Backend Discovery & Selection

#### 9.2.1 Startup Probing

When `AccelDispatcher::new(config)` is called at node startup, it performs:

1. **Tier 0 (CPU SIMD):** Always available. Constructs `CauchyEncoder` from
   the EC layer. GF arithmetic uses runtime CPU feature detection (SSE4.1, AVX2,
   AVX-512) via `std::is_x86_feature_detected!` or equivalent on ARM (NEON).

2. **Tier 1 (ISA-L):** Available if:
   - The `isa-l` Cargo feature is enabled at compile time.
   - `CPUID` reports AVX-512F + AVX-512BW at runtime.
   - The ISA-L shared library (`libisal.so`) can be loaded (via FFI binding).

   If any check fails, ISA-L is marked unavailable with a `DEBUG` log (not a
   warning, since ISA-L absence is expected on most hardware).

3. **Tier 2 (CUDA):** Available if:
   - The `cuda` Cargo feature is enabled at compile time.
   - `cudarc::init()` succeeds.
   - At least one CUDA device is present (`device_count > 0`).
   - The device has sufficient VRAM for the configured `ec_gpu_batch_size`
     (minimum 256 MB).

   If any check fails, CUDA is marked unavailable with a `DEBUG` log.

4. **Tier 2 (nvCOMP):** Available if CUDA is available AND the nvCOMP library
   (`libnvcomp.so`) can be loaded. nvCOMP is probed independently from the
   CUDA EC kernel — a system may have CUDA for EC but not nvCOMP for compression.

**Probing latency:** <200ms in the common case. CPUID is a single instruction.
CUDA device enumeration is ~50ms. Library loading is ~10ms.

#### 9.2.2 Tier Resolution

After probing, the dispatcher resolves the effective tier:

```
Requested Tier    Available Backends         Resolved Tier
────────────────  ──────────────────────     ─────────────
Auto              CUDA, ISA-L, CPU           CUDA
Auto              ISA-L, CPU                 ISA-L
Auto              CPU                        CPU SIMD
GpuCuda           CUDA, ISA-L, CPU           CUDA
GpuCuda           ISA-L, CPU                 ISA-L (+ WARN)
GpuCuda           CPU                        CPU SIMD (+ WARN)
IsaL              ISA-L, CPU                 ISA-L
IsaL              CPU                        CPU SIMD (+ WARN)
CpuSimd           CPU                        CPU SIMD
```

When a fallback occurs, the dispatcher:
1. Logs at `WARN` level: `"GPU acceleration requested but CUDA unavailable; falling back to ISA-L"`
2. If ISA-L is also unavailable: `"ISA-L not available; falling back to CPU SIMD"`
3. Increments the `accel_fallback_total` counter (labeled by `from_tier` and `to_tier`)

Falling back from `Auto` (where no explicit tier was requested) produces a
`DEBUG` log, not a `WARN` — because `Auto` means "use whatever is best."

#### 9.2.3 Caching

The resolved backend is cached for the lifetime of the `AccelDispatcher`:

```rust
struct AccelDispatcher {
    encoder: Arc<dyn Encoder>,    // cached, no branches on hot path
    decoder: Arc<dyn Decoder>,
    active_tier: AccelTier,
    // Per-tier caches for per-bucket overrides
    tier_encoders: HashMap<AccelTier, Arc<dyn Encoder>>,
    tier_decoders: HashMap<AccelTier, Arc<dyn Decoder>>,
}
```

There is no re-probing at runtime. Hardware does not change while a process
runs. If the GPU is hot-unplugged (an extremely rare event), the CUDA kernel
launch will fail with an error — the caller receives an `Err` and the healing
or write path retries with CPU SIMD.

#### 9.2.4 Per-Bucket Override

A bucket may specify `accel_ec_tier` in its policy. When `WriteCoordinator`
or `ReadCoordinator` calls the dispatcher for a bucket-scoped operation:

1. If the bucket's tier matches the node's tier: use the cached backend (no
   allocation).
2. If the bucket's tier differs: resolve against available hardware and return
   a temporary `Arc<dyn Encoder>` from the per-tier cache. If the bucket requests
   a tier that is unavailable, the fallback chain applies.

```
WriteCoordinator::put(bucket, key, data):
  encoder = dispatcher.resolve_for_bucket(bucket.accel_ec_tier)
  → if bucket tier == GpuCuda but GPU absent → fallback to ISA-L → WARN
  → encode proceeds with ISA-L
```

### 9.3 Tier 0: CPU SIMD

#### 9.3.1 GF-Complete Portable Path

The baseline EC codec is the Cauchy Reed-Solomon implementation specified in
§6.1. It uses GF(2^8) arithmetic with log/exp lookup tables for multiplication
and division. This path requires no SIMD instructions and runs on any CPU.

#### 9.3.2 Runtime SIMD Dispatch

The GF arithmetic layer uses runtime CPU feature detection to select the fastest
available multiplication path:

```
GF(2^8) multiply:
  ├── AVX-512 (VPCLMULQDQ): 512-bit carry-less multiply → ~4× faster than lookup
  ├── AVX2 (PCLMULQDQ):     128-bit carry-less multiply → ~2× faster than lookup
  ├── SSE4.1:               vectorized table lookup        → ~1.5× faster
  └── Portable:             log/exp table lookup            → baseline
```

Detection uses `std::is_x86_feature_detected!` on x86 and
`std::arch::is_aarch64_feature_detected!` on ARM. The selected implementation
is cached in a `static AtomicU8` set once at first GF operation.

#### 9.3.3 BLAKE3 Hashing

BLAKE3 hashing uses the upstream `blake3` crate, which performs its own runtime
CPU feature detection at program initialization. OceanFS does not implement
custom BLAKE3 acceleration. The `accel_hash_tier` configuration is a
pass-through:

- `"auto"`: use the `blake3` crate's default (auto-detect AVX-512, AVX2, SSE4.1, NEON)
- `"avx512"`: force AVX-512 implementation (useful for benchmarking; falls back to portable if unavailable)

No GPU path for BLAKE3 is planned — the crate achieves ~4 GB/s/core on AVX-512,
which is line-rate for any realistic network throughput.

### 9.4 Tier 1: ISA-L / libec

#### 9.4.1 Intel ISA-L (x86)

Intel's Intelligent Storage Acceleration Library (ISA-L) provides hand-tuned
AVX-512 assembly for Reed-Solomon encode and decode. It achieves line-rate
encoding for EC parameters up to k=24, m=8 on a single core.

**Integration:**

```rust
// ISA-L encoder (feature-gated)
pub struct IsalEncoder {
    // FFI handles to ISA-L encode/decode tables
}

impl Encoder for IsalEncoder {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> Result<Vec<Vec<u8>>> {
        // Calls ISA-L C functions via FFI:
        //   ec_encode_data(strip_size, k, m, encode_table,
        //                  data_ptrs, parity_ptrs)
    }
}
```

**FFI surface:** The ISA-L binding exposes exactly two functions:

| Function | Signature | Purpose |
|---|---|---|
| `ec_init_tables` | `(k, m, &mut [u8; 32*k*m])` | Precompute encoding matrix tables |
| `ec_encode_data` | `(len, k, m, &tables, &[&[u8]; k], &mut [&mut [u8]; m])` | Encode k data shards → m parity shards |

The decode path uses the same functions with a reconstructed matrix.

**Safety:** The ISA-L FFI is `unsafe`. All calls are wrapped in `// SAFETY:`
blocks that verify:
- Input pointers are non-null and aligned to 64 bytes
- Lengths match k × strip_size_bytes
- The encode table was initialized with matching k,m parameters
- The FFI function is guaranteed to be thread-safe by the ISA-L documentation

#### 9.4.2 ARM NEON + SVE / libec

ARM deployments use architecture-specific SIMD paths. The Tier 1 backend on ARM
is a Rust-native implementation using NEON and SVE intrinsics — not an FFI
binding to a C library. This avoids the build-time complexity of cross-compiling
ISA-L (which is Intel x86-only) and keeps the `unsafe` surface auditable in pure
Rust.

**Feature detection (at startup, cached):**

```
Probe ARM capabilities:
  ├── SVE2 available?  → use SVE2 256-bit GF(2^8) multiply  (Graviton4, Neoverse V2)
  ├── SVE available?   → use SVE 128-bit GF(2^8) multiply   (Graviton3, Neoverse V1)
  ├── NEON available?  → use NEON 128-bit GF(2^8) multiply   (Graviton2, Apple M1/M2)
  └── none             → portable GF-complete (log/exp tables)
```

**SVE GF(2^8) multiply kernel (conceptual):**

SVE's key advantage for EC is predicated vector operations — the same kernel
handles any vector width (128–2048 bits) without recompilation:

```rust
#[cfg(all(target_arch = "aarch64", feature = "arm-sve"))]
pub struct ArmEncoder {
    sve_level: ArmSveLevel,  // SVE2, SVE, NEON, or Portable
}

impl Encoder for ArmEncoder {
    fn encode(&self, data_shards: &[&[u8]], parity_count: u8) -> Result<Vec<Vec<u8>>> {
        match self.sve_level {
            ArmSveLevel::Sve2  => encode_sve2(data_shards, parity_count),
            ArmSveLevel::Sve   => encode_sve(data_shards, parity_count),
            ArmSveLevel::Neon  => encode_neon(data_shards, parity_count),
            ArmSveLevel::Portable => cauchy_portable_encode(data_shards, parity_count),
        }
    }
}
```

**SVE vs ISA-L performance parity:**

SVE2 on Graviton4 achieves comparable throughput to AVX-512 on x86 for EC
operations because GF(2^8) multiplication is compute-bound, not memory-bound.
Both ISAs perform the same number of XOR + table-lookup operations per byte.

| Architecture | SIMD Width | GF(2^8) Bytes/Cycle | Relative Throughput |
|---|---|---|---|
| x86 AVX-512 (ISA-L) | 512-bit | 64 bytes/cycle | 1.0× (baseline) |
| ARM SVE2 (256-bit) | 256-bit | 32 bytes/cycle | ~0.5× per core |
| ARM NEON | 128-bit | 16 bytes/cycle | ~0.25× per core |
| Portable | — | ~1 byte/cycle | ~0.01× |

ARM servers typically have higher core counts (64–128 cores on Graviton),
so aggregate throughput with SVE across all cores exceeds x86 throughput with
fewer cores.

### 9.5 Tier 2: GPU / CUDA

#### 9.5.1 GPU Usage Model

GPUs accelerate **batch EC operations** — not per blob. When a segment is
sealed, or a node is being rebuilt, the CPU coordinator sends a batch of
stripe rows to the GPU:

```
+----------+     batch of stripes        +---------------+
|   CPU    | --------------------------->|  GPU kernel   |
| (coord)  | <---------------------------|  GF(2^8) mat  |
+----------+     parity/decode shards    |  multiply     |
                                         +---------------+
```

Useful when:
- Large EC k+m values (e.g., 20+4, matrix ops dominate).
- Concurrent rebuild (many segments repairing simultaneously, inter-segment
  batching).
- High write throughput with `write_ec_async=false` (EC on hot path).

#### 9.5.2 CUDA Kernel Design

The GPU kernel performs GF(2^8) matrix multiplication for all stripes in a
segment simultaneously:

```
Kernel: gf256_encode_stripes
  Input:  data_shards[k][strip_size]    (k data shards, 64 KB each)
  Output: parity_shards[m][strip_size]  (m parity shards)
  Matrix: encode_matrix[m][k]           (precomputed on CPU, copied to GPU constant memory)

  Grid:  (num_stripes, 1, 1)            // one block per stripe
  Block: (strip_size, 1, 1)            // one thread per byte

  Each thread (stripe s, byte position b):
    for j in 0..m:
      acc = 0
      for i in 0..k:
        acc ^= gf_mul(encode_matrix[j][i], data_shards[i][s][b])
      parity_shards[j][s][b] = acc
```

**Thread count:** For a 4 MB segment with k=4, m=2, strip_size=64 KB:
- Num stripes = 4 MB / (4 × 64 KB) = 16
- Threads per block = 64 KB = 65,536
- Total threads = 16 × 65,536 = 1,048,576

This saturates a modern GPU (e.g., NVIDIA A100 has 6,912 CUDA cores × 128
threads/SM = ~880K threads in flight).

**GF arithmetic on GPU:** The GF(2^8) multiplication table is stored in GPU
constant memory (64 KB cache, very fast for uniform access). Each thread
performs a single table lookup per multiplication.

#### 9.5.3 Device Memory Management

GPU buffers are allocated per operation and freed immediately after:

```
encode(data_shards, m):
  1. acquire semaphore permit
  2. allocate device memory:
       d_data   = cuda_malloc(k * strip_size * num_stripes)     // input
       d_parity = cuda_malloc(m * strip_size * num_stripes)     // output
       d_matrix = cuda_malloc(m * k)                            // constant
  3. copy data: host → device (cudaMemcpyAsync on stream)
  4. copy matrix: host → device (cudaMemcpyAsync on stream)
  5. launch kernel (non-blocking on stream)
  6. copy output: device → host (cudaMemcpyAsync on stream)
  7. stream_synchronize()
  8. free device memory
  9. release semaphore permit
  10. return parity shards
```

**Pinned memory:** Input data is copied into pinned (page-locked) host memory
before transfer. Pinned memory enables DMA from the GPU without CPU
intervention, doubling PCIe throughput. The pinned buffer is recycled from a
pool (`GpuBufferPool`) to avoid per-operation `cudaMallocHost` overhead.

```
Transfer without pinned memory:  CPU buffer → driver copy → pinned → DMA → GPU  (2 copies)
Transfer with pinned memory:     pinned buffer → DMA → GPU                       (1 copy)
```

#### 9.5.4 CUDA Streams

All GPU operations for a single encode/decode call are submitted to a
dedicated CUDA stream. The stream enables asynchronous overlap of:

- Memory copy H→D (DMA engine)
- Kernel execution (compute)
- Memory copy D→H (DMA engine)

Without streams, each operation blocks until the previous completes. With
streams, the GPU scheduler overlaps DMA and compute automatically.

#### 9.5.5 GPU Batch Threshold

GPU offload has a fixed overhead: device memory allocation (~100 µs), H→D
transfer (~50 µs for 4 MB on PCIe 3.0 x16), kernel launch (~10 µs), D→H
transfer (~50 µs). For small segments, this overhead exceeds the CPU encode
time.

The `ec_gpu_min_segment_size` threshold (default 100 MB) prevents GPU offload
for segments where the CPU is faster. This applies per-segment: a 4 MB
standard segment uses CPU SIMD; a 100 MB multi-segment write uses GPU.

```toml
ec_gpu_min_segment_size = 104857600   # 100 MB — only offload large segments
```

**Break-even analysis (approximate, x86 with AVX-512):**

| Segment Size | CPU (ISA-L) | GPU (RTX 4090) | Winner |
|---|---|---|---|
| 4 MB (1 stripe) | ~50 µs | ~200 µs (overhead dominates) | CPU |
| 64 MB (16 stripes) | ~800 µs | ~300 µs | GPU |
| 256 MB (64 stripes) | ~3.2 ms | ~0.8 ms | GPU (4×) |
| 1 GB (256 stripes) | ~12.8 ms | ~3 ms | GPU (4×) |

#### 9.5.6 GPU Error Handling

GPU operations can fail for reasons outside OceanFS control:

| Failure | Cause | Behavior |
|---|---|---|
| `cudaMalloc` fails | VRAM exhausted | Release semaphore, return `Err(AccelError::GpuOutOfMemory)`, caller falls back to CPU |
| Kernel launch fails | Device lost, driver crash | Release semaphore, log ERROR, return `Err(AccelError::GpuDeviceLost)`, caller falls back to CPU |
| `cudaMemcpy` fails | PCIe error | Release semaphore, log ERROR, return `Err(AccelError::GpuTransferError)` |
| Kernel timeout | Kernel runs >5s (TDR) | Release semaphore, log ERROR, mark GPU unavailable for 60s, fall back to CPU |

After a device-lost error, the `CudaBackend` marks itself as unavailable for a
cooldown period (default 60 seconds). During cooldown, all GPU requests fall
back to ISA-L (or CPU SIMD) without attempting GPU access. After cooldown,
a single probe operation (encode a tiny dummy stripe) tests if the device has
recovered. If successful, GPU operations resume. If not, cooldown restarts.

This prevents the system from hammering a failed GPU with operations that will
all fail.

### 9.6 Non-EC Acceleration

#### 9.6.1 BLAKE3 Hashing

BLAKE3 is accelerated via the upstream `blake3` crate, which performs runtime
CPU feature detection at program initialization. The crate benchmarks itself at
initialization and selects:

- AVX-512: ~4 GB/s/core
- AVX2: ~3 GB/s/core
- SSE4.1: ~1.5 GB/s/core
- Portable: ~400 MB/s/core

OceanFS does not implement custom BLAKE3 acceleration. The `accel_hash_tier`
configuration is a pass-through; `"auto"` delegates entirely to the crate.

No GPU path for BLAKE3 is planned — even the portable implementation is faster
than any realistic network throughput, and the overhead of GPU offload (PCIe
transfer + kernel launch) would make it slower than CPU for all practical blob
sizes.

#### 9.6.2 zstd Compression

Segment data may be compressed before EC encoding (future feature, designed
here but implemented as a **separate epic** from GPU EC acceleration). The
compression acceleration model mirrors the EC model:

| Tier | Backend | Availability |
|---|---|---|
| Tier 0 | `zstd` crate (CPU) | Always |
| Tier 1 | ISA-L `igzip` (CPU, AVX-512) | `isa-l` feature + AVX-512 |
| Tier 2 | nvCOMP (GPU batch) | `cuda` feature + nvCOMP library |

Compression tier selection is **per-bucket only** (`compress_tier` in bucket
policy). There is no node-level `compress_tier` default — unlike EC
acceleration, compression is workload-dependent and only meaningful to enable
for specific buckets with compressible data.

**nvCOMP integration:** When the `cuda` feature is enabled and nvCOMP is
available, the dispatcher provides a `Compressor` trait that delegates to
GPU-accelerated compression. The GPU performs batched compression of segment
data using the same semaphore-controlled model as EC encoding.

```rust
pub trait Compressor: Send + Sync {
    fn compress(&self, data: &[u8], level: u32) -> Result<Vec<u8>>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
}
```

**nvCOMP batch behavior:** Compression is batched across multiple segments
when sealing or healing. The CPU accumulates segments into a batch, sends the
batch to nvCOMP, and receives compressed buffers. The batch threshold mirrors
`ec_gpu_batch_size`.

**Epic placement:** The `Compressor` trait + nvCOMP/igzip backends are a
**separate epic** from the GPU EC acceleration epic and the CPU acceleration
backends epic (ISA-L + ARM SVE). This allows compression acceleration to be
prioritized independently and shipped when segment compression is ready.

#### 9.6.3 AES-GCM Encryption

Encryption uses the `aes-gcm` crate, which leverages AES-NI instructions via
the `aes` crate's runtime detection. AES-NI provides hardware-accelerated
AES rounds (~1 CPU cycle per byte on modern x86).

GPU batch encryption is deferred to future work. The current bottleneck for a
blob store is EC encoding, not encryption. A GPU path for AES-GCM would
require a dedicated kernel and adds complexity without a clear throughput
benefit for the target workload (S3-compatible blob storage where TLS
terminates at the load balancer, and most deployments use network-level
encryption, not per-blob encryption).

### 9.7 Fallback & Error Handling

#### 9.7.1 Fallback Chain

The fallback chain is fixed and always terminates at CPU SIMD:

```
GpuCuda → IsaL → CpuSimd   (always available)
```

A fallback occurs in two scenarios:

1. **Startup fallback:** The configured tier is unavailable at node startup.
   The dispatcher resolves to the highest available tier and caches it. A
   one-time `WARN` is logged.

2. **Runtime fallback:** The active tier fails during an operation (GPU device
   lost, ISA-L FFI error). The dispatcher:
   - Logs an `ERROR` with the failure reason
   - Marks the failed backend as unavailable
   - Re-resolves to the next available tier
   - Increments `accel_runtime_fallback_total`
   - Retries the operation with the new backend

Runtime fallback is transparent to the caller — the dispatcher handles it
internally. The caller sees only the result of the retried operation (or an
error if all backends fail, which can only happen if CPU SIMD fails — an
extremely unlikely scenario).

#### 9.7.2 GPU Cooldown

When the CUDA backend fails at runtime (device lost, repeated OOM):

1. The backend is marked `Unavailable` with a cooldown timestamp
2. All subsequent GPU requests fall back without attempting GPU access
3. After `ec_gpu_cooldown_sec` (default 60), a probe operation tests recovery
4. If probe succeeds: backend marked `Available`, normal operation resumes
5. If probe fails: cooldown reset, another `ERROR` logged

```toml
ec_gpu_cooldown_sec = 60   # seconds before retrying a failed GPU
```

This prevents thundering-herd GPU failures from flooding the log and ensures
the CPU path is used reliably during GPU outages.

#### 9.7.3 Error Types

```rust
pub enum AccelError {
    #[error("GPU out of memory: requested {requested}, available {available}")]
    GpuOutOfMemory { requested: u64, available: u64 },

    #[error("GPU device lost")]
    GpuDeviceLost,

    #[error("GPU data transfer error")]
    GpuTransferError(#[source] std::io::Error),

    #[error("ISA-L FFI error: {0}")]
    IsalFfi(String),

    #[error("Backend temporarily unavailable: {backend}")]
    BackendUnavailable { backend: String },
}
```

### 9.8 Observability

#### 9.8.1 Metrics

All metrics are exposed at `/admin/metrics` in Prometheus format.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `accel_tier_active` | Gauge | `tier`, `operation` | Currently active tier (0=CPU, 1=ISA-L, 2=GPU) |
| `accel_encode_duration_seconds` | Histogram | `tier`, `k`, `m` | EC encode latency |
| `accel_decode_duration_seconds` | Histogram | `tier`, `k`, `m` | EC decode latency |
| `accel_bytes_processed_total` | Counter | `tier`, `operation` | Bytes processed by each tier |
| `accel_fallback_total` | Counter | `from_tier`, `to_tier` | Startup fallback events |
| `accel_runtime_fallback_total` | Counter | `from_tier`, `to_tier`, `reason` | Runtime fallback events |
| `accel_gpu_utilization` | Gauge | `device` | GPU utilization (0.0–1.0) |
| `accel_gpu_memory_bytes` | Gauge | `device`, `kind` | GPU memory used/free |
| `accel_gpu_semaphore_wait_seconds` | Histogram | `device` | Time spent waiting for GPU semaphore |
| `accel_compress_duration_seconds` | Histogram | `tier`, `algorithm` | Compression latency |
| `accel_hash_duration_seconds` | Histogram | `tier` | Hash computation latency |

#### 9.8.2 Tracing

The dispatcher emits `tracing` spans at key points:

```
INFO  oceanfs_accel: acceleration subsystem initialized, active_tier=isa_l
DEBUG oceanfs_accel: probing hardware, cuda=unavailable, isa_l=available, cpu=available
WARN  oceanfs_accel: GPU acceleration requested but CUDA unavailable; falling back to ISA-L
ERROR oceanfs_accel: GPU device lost during encode; falling back to ISA-L
DEBUG oceanfs_accel: per-bucket tier override, bucket=my-bucket, requested=gpu_cuda, resolved=isa_l
```

#### 9.8.3 Admin API

The `/admin/acceleration` endpoint returns the current acceleration status:

```json
{
  "active_tier": "isa_l",
  "available_backends": ["cpu_simd", "isa_l"],
  "unavailable_backends": ["gpu_cuda"],
  "gpu_status": {
    "available": false,
    "reason": "no_cuda_device",
    "cooldown_remaining_sec": 0
  },
  "fallback_count": 0,
  "runtime_fallback_count": 0
}
```

### 9.9 Configuration Reference

#### 9.9.1 Node Configuration (`oceanfs.toml`)

```toml
[acceleration]
# EC acceleration tier
#   "auto"     — probe: CUDA > ISA-L > CPU SIMD (default)
#   "cpu_simd" — GF-complete portable + runtime SIMD dispatch
#   "isa_l"    — Intel ISA-L (requires AVX-512 + isa-l feature)
#   "gpu_cuda" — NVIDIA CUDA (requires GPU + cuda feature)
ec_tier = "auto"

# Hash acceleration tier
#   "auto"  — BLAKE3 crate auto-detection (AVX-512, AVX2, SSE4.1, NEON)
#   "avx512" — force AVX-512 (falls back to auto if unavailable)
hash_tier = "auto"

# GPU-specific configuration
ec_gpu_device_id          = 0             # CUDA device index
ec_gpu_batch_size         = 64            # stripes per GPU kernel launch
ec_gpu_min_segment_size   = 104857600     # 100 MB — minimum segment size for GPU offload
ec_gpu_max_concurrent_ops = 1             # permits for GPU semaphore (1 = serialize)
ec_gpu_cooldown_sec       = 60            # seconds to wait before retrying after GPU failure

# ISA-L configuration
isal_prefer_avx512        = true          # prefer AVX-512 code path if available
```

#### 9.9.2 Bucket Configuration (per-bucket override)

```toml
[bucket.my-bucket.acceleration]
ec_tier         = "gpu_cuda"   # override node-level ec_tier
hash_tier       = "auto"       # override node-level hash_tier
compress_tier   = "auto"       # per-bucket only — no node-level default
                               #   "auto"  — probe: nvCOMP > ISA-L igzip > CPU
                               #   "cpu"   — zstd crate (CPU)
                               #   "gpu"   — nvCOMP GPU batch (requires cuda feature)
```

Any bucket field left unset inherits the node-level configuration (for `ec_tier`
and `hash_tier`). `compress_tier` has no node-level default — it defaults to
`"cpu"` if unset (no compression acceleration unless explicitly requested per
bucket).

---

## 10. Garbage Collection & Compaction

### 10.1 Deletion

Deletions are tombstone-based:
1. Mark object as deleted in `deletions` RocksDB column family.
2. Invalidate object from L1 cache and metadata cache.
3. Do **not** immediately free segment space.
4. After `gc_tombstone_ttl_sec`, GC marks the associated chunk as free.

### 10.2 Segment Compaction

When a segment's **liveness ratio** (live bytes / total bytes) drops below
`gc_compact_threshold` (default 0.5):
1. Read all live blobs from the segment.
2. Re-pack them into a new segment (using the tiered sizing rules from §3.2).
3. Update object metadata to point to the new segment.
4. Remove old segment shards from storage nodes.

### 10.3 Orphaned Segment Reaper

Periodically scan the `segments` RocksDB column family. Any segment
unreferenced from `objects` for longer than `gc_tombstone_ttl_sec` has its
shards permanently deleted.

---

## 11. Connection Pooling & I/O

### 11.1 gRPC Connection Pool

Internal node-to-node communication uses persistent gRPC connections with a
pool per peer:

```toml
[grpc]
pool_size_per_peer        = 4
keepalive_sec             = 30
max_idle_connections      = 128
connect_timeout_ms        = 5000
request_timeout_ms        = 30000
```

### 11.2 Buffer Pool

Segment append operations allocate buffers. A global `BytesMut` pool recycles
buffers after segment seal, avoiding repeated allocation pressure:

```toml
buffer_pool_chunk_bytes   = 65536    # 64 KB buffer chunks
buffer_pool_max_chunks    = 1024     # ~64 MB pool
```

### 11.3 Prefetch Engine

After a `LIST` operation returns object keys, an optional prefetch engine can
pre-warm the metadata cache and segment shards for those keys. Useful when
clients iterate over list results.

```toml
prefetch_enabled         = false
prefetch_after_list      = 16        # prefetch N objects ahead
prefetch_after_get       = 4         # prefetch N subsequent objects
```

---

## 12. API

### 12.1 S3-Compatible HTTP API (External)

| Method   | Path                          | Description                  |
| -------- | ----------------------------- | ---------------------------- |
| `PUT`    | `/{bucket}/{key}`             | Create or overwrite object   |
| `GET`    | `/{bucket}/{key}`             | Retrieve object              |
| `HEAD`   | `/{bucket}/{key}`             | Object metadata              |
| `DELETE` | `/{bucket}/{key}`             | Delete object                |
| `PUT`    | `/{bucket}`                   | Create bucket                |
| `GET`    | `/{bucket}`                   | List objects (prefix, delimiter, marker) |
| `DELETE` | `/{bucket}`                   | Delete bucket (if empty)     |
| `POST`   | `/{bucket}?policy`            | Set/update bucket policy    |

Standard S3 authentication (AWS Signature V4, configurable) plus optional mTLS
for internal network isolation.

### 12.2 Admin API (External / Internal)

| Method | Path                     | Description                       |
| ------ | ------------------------ | --------------------------------- |
| `GET`  | `/admin/cluster`         | Cluster membership + ring view    |
| `GET`  | `/admin/segments`        | Segment health report              |
| `GET`  | `/admin/caches`          | Cache hit/miss rates per tier      |
| `POST` | `/admin/scrub`           | Trigger full scrub                 |
| `GET`  | `/admin/metrics`         | Prometheus metrics endpoint        |

### 12.3 Internal gRPC (Node-to-Node)

```protobuf
service NodeRPC {
  rpc AppendSegment(stream SegmentAppendRequest) returns (SegmentAppendResponse);
  rpc FetchShard(ShardRequest) returns (stream ShardResponse);
  rpc GossipPush(stream GossipMessage) returns (GossipAck);
  rpc GossipPull(GossipPullRequest) returns (stream GossipMessage);
  rpc MerkleExchange(MerkleRequest) returns (MerkleResponse);
  rpc HintedHandoff(HintRequest) returns (HintResponse);
  rpc Probe(ProbeRequest) returns (ProbeResponse);  // SWIM direct/indirect ping
  rpc CacheInvalidate(CacheInvalidateRequest) returns (CacheInvalidateResponse);
}
```

---

## 13. Node Lifecycle

### 13.1 Join

1. New node contacts any node in `seed_nodes`.
2. Receives current membership list + ring state via gossip.
3. Announces itself with `Incarnation=1` via gossip.
4. Vnodes are assigned; data migrates gradually through background rebalancing
   (streamed segment shards).

### 13.2 Graceful Leave

1. Node announces `LEAVING` status via gossip.
2. Hands off active WAL segments to successors.
3. Enters drain: refuses new writes, completes in-flight requests.
4. Streams owned segment shards to successors.
5. Announces `LEFT` -> ring recomputed, node removed.

### 13.3 Failure Detection

```
ALIVE --> direct ping --> indirect ping (k peers) --> SUSPECT --> DEAD
                |              |                          |
            ack received   ack received              suspicion_timeout_ms
```

- **SUSPECT:** Node is suspected down but not confirmed. Reads/writes still
  include it but with shorter timeouts.
- **DEAD:** Confirmed dead. Triggers segment healing, ring recomputation, and
  hinted-handoff delivery.

| Parameter                  | Default | Description                               |
| -------------------------- | ------- | ----------------------------------------- |
| `gossip_interval_ms`       | 1000    | Interval between gossip rounds            |
| `suspicion_timeout_ms`     | 5000    | Time in SUSPECT before declaring DEAD     |
| `failure_timeout_ms`       | 15000   | Total time before declaring DEAD          |
| `indirect_ping_count`      | 3       | Number of peers to route indirect pings   |

---

## 14. Configuration Reference

### 14.1 Node Configuration (`oceanfs.toml`)

```toml
[node]
id              = "node-1"
data_dir        = "/var/lib/oceanfs"
listen_addr     = "0.0.0.0:9000"
gprc_listen_addr = "0.0.0.0:9001"
seed_nodes      = ["10.0.1.2:9000", "10.0.1.3:9000"]

[ring]
vnodes_per_node     = 256
replication_factor  = 3

[segment]
inline_threshold_bytes          = 4096       # 4 KB
segment_small_threshold_bytes   = 262144     # 256 KB
segment_small_target_size       = 65536      # 64 KB
segment_default_target_size     = 4194304    # 4 MB
seal_timeout_ms                 = 500
max_blobs_per_segment           = 10000
active_pool_size                = 4
shard_count                     = 4

[acceleration]
ec_tier                 = "auto"  # auto | cpu_simd | isa_l | gpu_cuda
ec_gpu_device_id        = 0
ec_gpu_batch_size       = 64
ec_gpu_min_segment_size = 104857600   # 100 MB
hash_tier               = "auto"      # auto | cpu | avx512
ec_parallel_stripes     = 0           # 0 = auto

[cache]
# L1: Object data
object_cache_enabled        = true
object_cache_size_bytes     = 536870912    # 512 MB
object_cache_ttl_ms         = 60000
object_cache_max_blob_size  = 1048576

# L2: Metadata
metadata_cache_enabled      = true
metadata_cache_size_bytes   = 1073741824   # 1 GB
metadata_cache_ttl_ms       = 300000

# L3: Negative (Bloom)
negative_cache_enabled      = true
negative_cache_size_bytes   = 67108864     # 64 MB
negative_cache_rebuild_sec  = 3600

[grpc]
pool_size_per_peer    = 4
keepalive_sec         = 30
max_idle_connections  = 128
connect_timeout_ms    = 5000
request_timeout_ms    = 30000

[buffer_pool]
chunk_bytes       = 65536      # 64 KB
max_chunks        = 1024       # ~64 MB total pool

[prefetch]
enabled            = false
after_list         = 16
after_get          = 4

[anti_entropy]
interval_sec        = 300
scrub_interval_sec  = 604800      # 7 days
scrub_parallel_nodes = 0          # 0 = all nodes

[heal]
parallel_segments   = 16
parallel_stripes    = 0           # 0 = auto
throttle_bytes_sec  = 0           # 0 = unlimited

[gc]
interval_sec        = 3600
tombstone_ttl_sec   = 259200      # 3 days
compact_threshold   = 0.5

[gossip]
interval_ms             = 1000
suspicion_timeout_ms    = 5000
failure_timeout_ms      = 15000
indirect_ping_count     = 3

[logging]
level = "info"                      # trace | debug | info | warn | error

[metrics]
enabled         = true
listen_addr     = "0.0.0.0:9090"
```

### 14.2 Bucket Configuration (per-bucket policy override)

```toml
[bucket.my-bucket]
# Consistency
write_quorum    = 2
read_quorum     = 2
total_replicas  = 3

# Segment sizing
inline_threshold_bytes        = 4096
segment_small_threshold_bytes = 262144
segment_small_target_size     = 65536
segment_default_target_size   = 4194304
segment_seal_timeout_ms       = 500
segment_active_pool_size      = 4
segment_shard_count           = 4

# Erasure coding
ec_data_shards      = 4
ec_parity_shards    = 2
ec_strip_size_bytes = 65536
ec_codec            = "cauchy_rs"

# Read
read_parallel_fetch     = true
read_use_fastest_k      = true
read_cache_segments      = true
read_stripe_parallelism = 16

# Write
write_ack_after_wal = true
write_ec_async      = true

# Caching
object_cache_enabled        = true
object_cache_size_bytes     = 536870912
object_cache_ttl_ms         = 60000
object_cache_max_blob_size  = 1048576
metadata_cache_enabled      = true
metadata_cache_size_bytes   = 1073741824
metadata_cache_ttl_ms       = 300000
negative_cache_enabled      = true
negative_cache_size_bytes   = 67108864

# Acceleration
accel_ec_tier   = "auto"
accel_hash_tier = "auto"

# Healing
heal_parallel_segments  = 16

# GC
gc_interval_sec       = 3600
gc_tombstone_ttl_sec  = 259200
gc_compact_threshold  = 0.5
```

---

## 15. Implementation Phases

| Phase   | Scope                                                                   | Deliverable                     |
| ------- | ----------------------------------------------------------------------- | ------------------------------- |
| **0**   | Project scaffold, crate layout, protobufs, config system, CI            | Compiling skeleton              |
| **1**   | Storage engine: segment buffer, WAL, RocksDB metadata, inline storage, tiered segment sizing | Single-node blob store          |
| **2**   | DHT ring, consistent hashing, gossip membership, connection pooling, basic routing | Multi-node connectivity         |
| **3**   | Erasure coding (CPU tier: Cauchy RS via GF-complete), intra-segment stripe parallelism | EC encode/decode/heal working   |
| **4**   | Distributed write path (coordinator, quorum, hinted handoff, pipeline parallelism) + read path (parallel fetch+decode) | Multi-node blob store           |
| **5**   | S3-compatible HTTP API, bucket configuration, tuning endpoints          | Usable API                      |
| **6**   | Caching layers (L1 object, L2 metadata, L3 negative), prefetch engine   | Read acceleration               |
| **7**   | GC, compaction, anti-entropy, Merkle tree exchange, distributed scrubbing | Production-grade durability     |
| **8**   | GPU acceleration tier (CUDA kernels, batch EC), ARM NEON/SVE acceleration, benchmark suite | Hardware offload |

---

## 16. Open Questions / Future Work

- **Auth model:** Mutual TLS for node-to-node; S3 auth for clients. Multi-tenancy
  (IAM-style policies)?
- **Multi-region:** How to extend the DHT ring across regions? Latency-aware
  routing?
- **Tiered storage:** Cold segments to S3/NFS/tape via pluggable storage
  backends.
- **WAN replication:** Async segment streaming between distinct DHT rings.
- **Observability:** Distributed tracing (OpenTelemetry) for end-to-end request
  flows.
- **Object locking & retention:** S3 Object Lock for compliance.
- **Versioning:** S3-style versioned buckets (keeps all versions of an object).
- **Range requests:** HTTP Range header support for partial blob reads — requires
  segment-aware offset calculation.
