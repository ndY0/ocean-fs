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

### 9.1 Tiered Acceleration Model

```
Operation              Tier 0 (baseline)        Tier 1 (optimized)         Tier 2 (offload)
------------------------------------------------------------------------------------------
BLAKE3 hashing         CPU (portable)           AVX-512 intrinsics         n/a (line-rate)
EC encode/decode       GF-complete (portable)   ISA-L (Intel),             CUDA kernel
                                                libec (ARM SVE)            (batch EC ops)
Compression (zstd)     CPU (portable)           AVX-512                    nvCOMP
Encryption (AES-GCM)   CPU (portable)           AES-NI                     GPU (batch)
```

### 9.2 GPU Usage Model

GPUs accelerate **batch EC operations** — not per blob. When a segment is
sealed, or a node is being rebuilt, the CPU coordinator sends a batch of stripe
rows (all stripes within a segment, or across multiple segments during heal) to
the GPU:

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

### 9.3 Acceleration Configuration

```toml
[acceleration]
ec_tier                   = "auto"       # auto | cpu_simd | isa_l | gpu_cuda
ec_gpu_device_id          = 0
ec_gpu_batch_size         = 64           # stripes per GPU batch
ec_gpu_min_segment_size   = 104857600    # 100 MB — only offload large segments

hash_tier                 = "auto"       # auto | cpu | avx512
```

The `ec_tier` is also configurable per bucket for ultimate flexibility.

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
| **3**   | Erasure coding (CPU tier: Cauchy RS via GF-complete / ISA-L), intra-segment stripe parallelism | EC encode/decode/heal working   |
| **4**   | Distributed write path (coordinator, quorum, hinted handoff, pipeline parallelism) + read path (parallel fetch+decode) | Multi-node blob store           |
| **5**   | S3-compatible HTTP API, bucket configuration, tuning endpoints          | Usable API                      |
| **6**   | Caching layers (L1 object, L2 metadata, L3 negative), prefetch engine   | Read acceleration               |
| **7**   | GC, compaction, anti-entropy, Merkle tree exchange, distributed scrubbing | Production-grade durability     |
| **8**   | GPU acceleration tier (CUDA kernels, batch EC), benchmark suite         | Hardware offload                |

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
