---
audit_date: 2026-08-05
scope: targeted
target_crates: oceanfs-network, oceanfs-server, oceanfs-membership, oceanfs-storage, oceanfs-core
severity_counts:
  critical: 2
  high: 6
  medium: 8
  low: 5
---

# Audit Report: Network Communication Performance

## Summary

The network layer has solid architectural foundations — a proper connection pool with semaphore-bounded concurrency, `FuturesUnordered` for parallel fan-out, explicit `OperationTimeouts`, and correct protobuf streaming declarations. However, the implementations under-deliver on the streaming promise: the write replication path copies full payloads into intermediate `Vec<u8>` buffers, the server-side `append_segment` handler accumulates the entire stream before writing, and the gossip protocol scales quadratically (O(N²) bandwidth per round). These findings put a ceiling on throughput as data sizes and cluster sizes grow. The two critical issues — O(N²) gossip and full-buffer allocation on the write path — should be addressed before production deployment.

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `oceanfs-membership/src/gossip.rs:168-174` | Gossip pushes full membership delta to **all** alive peers on every tick (O(N²) bandwidth). For N=100 nodes with ~100 bytes per entry, this is ~1 MB/s per node for gossip alone. | Select a random subset of peers (e.g., `sqrt(N)` or configurable fanout) per tick rather than pushing to all. This is the standard SWIM optimization: push to k peers, pull from k peers, reducing total bandwidth to O(k·N). |
| C2 | `oceanfs-server/src/write/replication.rs:126` | `data.to_vec()` allocates a full copy of the blob payload for every replica RPC. For a 4 MB blob and W=3 replicas, that's 12 MB of allocation per PUT (3 copies of the original data). | Pass `Bytes` through the protobuf message without `.to_vec()`. The protobuf `SegmentAppendRequest.data` field should accept `Bytes` (via `prost` with `bytes::Bytes` backing) so the reference-counted payload flows zero-copy from the S3 handler buffer through to each gRPC channel. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `oceanfs-server/src/grpc/segment_service.rs:83-84` | `append_segment()` accumulates the **entire stream** into a `Vec<u8>` before writing to the store. This defeats the streaming RPC: a 4 MB blob is fully buffered in memory on the receiving node before any disk I/O begins. | Stream chunks directly to the segment store as they arrive: call `data_store.write_chunk(&segment_id, &chunk.data)?;` in the `while let Some(chunk)` loop rather than `segment_data.extend_from_slice()`. Only send the ack after the last chunk is written. |
| H2 | `oceanfs-server/src/write/replication.rs:125-135` | `SegmentAppendRequest` is constructed with new `vec![]` allocations for every field (`blake3_hash`, `chunk_segment_ids`, `chunk_offsets`, `chunk_lengths`) even when not carrying metadata. | Default to empty slices or use `SmallVec`/static empties. Prefer `Default::default()` for fields that are not needed in the replication path. |
| H3 | `oceanfs-server/src/s3_handler/handlers.rs:208,219,228,250` | L1 cache hit path calls `.to_vec()` on cached `Bytes`, allocating a full copy on every cache hit. This creates an allocation proportional to object size on the read hot path. | Return `Bytes` directly via axum `Body::from(cached_data)` which accepts `Bytes` (zero-copy via `hyper::body::Bytes`). The S3 handler should not convert to `Vec<u8>`. |
| H4 | `oceanfs-server/src/grpc/segment_service.rs:279,291-292` | `fetch_shard` copies segment data with `.to_vec()` then copies each chunk with `.to_vec()` again. A 4 MB shard gets at least two full copies before reaching the client. | Use `Bytes` slicing: `let shard_data = Bytes::copy_from_slice(&segment_data[shard_start..shard_end]);` then `shard_data.slice(chunk_start..chunk_end)` for each chunk to avoid per-chunk allocation. |
| H5 | `oceanfs-server/src/read/fetch.rs:229-234` | gRPC shard fetch accumulates the entire streamed response into an intermediate `Vec<u8>` (`let mut data = Vec::new()`), then converts to `Bytes::from(data)`. | Build a `BytesMut` directly from the stream, or use a pre-allocated buffer pool. Call `writer_bytes_mut().extend_from_slice(&chunk.data)` during the while loop. |
| H6 | `oceanfs-network/src/pool.rs:192-199` | All `pool_size_per_peer` channels are pre-connected **eagerly** in a sequential loop. For `pool_size_per_peer=4` and a 5-second connect timeout, pool creation can block for up to 20 seconds if the peer is unreachable. | Connect channels lazily (on first `get_channel` call) or in parallel via `FuturesUnordered`. The first few calls to `get_channel` can establish connections while initial channels are used immediately via `Endpoint::connect_lazy()`. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `oceanfs-server/src/write/coordinator.rs:274-284` | `forward_write` constructs `SegmentAppendRequest` with empty `vec![]` for metadata fields (`blake3_hash`, `chunk_segment_ids`, etc.), same issue as H2. Also calls `req.data.to_vec()` at line 275. | Same fix as H2. Use `Bytes` for data field. |
| M2 | `oceanfs-network/src/pool.rs:149-151` | `health_check()` is a no-op placeholder. Dead channels never get evicted. If a peer crashes, callers will wait for connection timeouts on every failed RPC until the semaphore drains. | Implement periodic health probing: send a gRPC health check RPC (or use HTTP/2 PING frames) on each channel every `keepalive_sec`. Remove failed channels from the pool and reconnect. |
| M3 | `oceanfs-network/src/pool.rs:183` | gRPC connections always use `format!("http://{peer}")` — no TLS. All inter-node traffic is plaintext HTTP/2. The `tls.rs` module is a placeholder returning `false`. | Implement mTLS per the architecture roadmap (Phase 5). Until then, document that inter-node traffic is unencrypted and add a config flag to warn/error when TLS is not configured. |
| M4 | `oceanfs-core/src/config/node.rs:71` | Default `max_body_size` is 2 MB. This is a hard HTTP body limit. Objects larger than 2 MB will be rejected at the HTTP layer. This is far below the 4 MB segment target. | Raise to at least 4 MB (segment size) or 256 MB with chunked transfer encoding. Consider configuring separate limits for the S3 API vs admin API. |
| M5 | `oceanfs-server/src/write/replication.rs:46-48` | Replication timeout uses `tokio::time::sleep` inside `tokio::select!`, but `replicate_to_single` does **not** apply any timeout to the gRPC call itself. The `_timeout_ms` parameter in `fetch_single_chunk` is unused (prefixed with `_`). | Apply `tonic::Request::set_timeout` on each gRPC call, or wrap the gRPC future in `tokio::time::timeout`. The `select!` only races against the outer sleep, not individual RPC timeouts. |
| M6 | `oceanfs-server/src/grpc/segment_service.rs:259-261` | `fetch_shard` uses a hardcoded `total_shards = 6` for computing shard size. This is a magic number unrelated to actual segment EC parameters. For a non-EC segment with k=1,m=0, the shard size calculation is wrong, causing out-of-range errors. | Read `ec_k` and `ec_m` from segment metadata (stored alongside the segment data) and compute `total_shards = ec_k + ec_m`. |
| M7 | `oceanfs-membership/src/gossip.rs:199-217` | Gossip proto conversion allocates a new `Vec<MembershipEntry>` from scratch on every push (line 199: `let entries: Vec<_> = delta.changed.iter().map(...).collect()`). For N=100 nodes, this is a 100-element allocation per push × N pushes per tick. | Pre-allocate with `Vec::with_capacity(delta.changed.len())`. Cache the serialized form when the delta hasn't changed since the last tick. |
| M8 | `oceanfs-server/src/read/repair.rs:77-91` | `schedule_repair` fires a background task but the repair path (`perform_read_repair`) is a no-op that only logs — it never actually repairs anything. Comment at line 44-47 acknowledges this. | Implement the actual repair: compare HLCs across all replica responses, push corrected data to stale nodes via gRPC `AppendSegment`. Or remove the dead code to avoid confusion. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-server/src/grpc/segment_service.rs:173-177` | `append_segment` stores replicated metadata with `Hlc::zero()` (line 165), losing the actual timestamp from the coordinator. | Extract `hlc` from the request's `SegmentAppendRequest.hlc` field and pass it through. |
| L2 | `oceanfs-server/src/s3_handler/handlers.rs:418` | `invalidate_cache_on_replicas` is called **twice** consecutively on line 417-418 for the same bucket/key/hk. | Remove the duplicate call. |
| L3 | `oceanfs-server/src/write/coordinator.rs:264` | `SegmentRpcClient::new(channel)` is constructed per RPC call. `SegmentRpcClient` is `Clone` (since `RpcClient` trait requires it). The channel could be cloned into the client once and reused. | Store `SegmentRpcClient` alongside the channel in the pool, or construct it once in `PeerPool`. Tonic clients are designed to be cloned and reused. |
| L4 | `oceanfs-network/src/pool.rs:157` | `max_idle_connections` field exists in `RpcConfig` (line 157) but is never enforced or used in `ConnectionPool`. No idle eviction logic exists. | Either implement idle connection eviction or remove the config field. |
| L5 | `oceanfs-core/src/config/node.rs:71` | `max_body_size` default is `2 * 1024 * 1024` with comment "2 MB" — but the value is only enforced via axum's `DefaultBodyLimit::max()`, which limits the HTTP body. gRPC streaming payloads are not bounded by this. | Document that this limit applies to the S3 HTTP API only. gRPC streams have their own limits (should be configured via tonic's `max_decoding_message_size`). |

## Findings by Guideline Section

### Networking (§4)

| Rule | Status | Evidence |
|---|---|---|
| §4.1 Persistent gRPC connection pool per peer | **COMPLIANT** | `ConnectionPool` (`pool.rs`) maintains a `DashMap<SocketAddr, Arc<PeerPool>>` with per-peer channel pools, round-robin selection, and semaphore-bounded concurrency. |
| §4.2 HTTP/2 multiplexing for client API | **PARTIAL** | gRPC inter-node uses HTTP/2 via tonic (compliant). Client-facing S3 API uses `axum::serve` without explicit HTTP/2 enablement — defaults to HTTP/1.1. No `hyper` h2 feature or axum HTTP/2 configuration found. |
| §4.3 TCP_NODELAY on all sockets | **PARTIAL** | gRPC outbound channels set `.tcp_nodelay(true)` at `pool.rs:186`. Server-side listeners (HTTP listener in `node.rs:423`, gRPC in `node.rs:469`) do NOT set nodelay. The axum `TcpListener` does not configure socket options. |
| §4.4 Streaming gRPC for large data transfers | **PARTIAL** (protos) / **VIOLATION** (impl) | Proto definitions correctly declare `stream` on `AppendSegment` and `FetchShard`. But `append_segment` impl buffers full stream to `Vec<u8>` before writing (C2/H1). Replication sends one chunk via `tokio_stream::once`. |
| §4.5 Adaptive per-operation timeouts | **COMPLIANT** | `OperationTimeouts` (`timeouts.rs`) defines eight per-operation timeout durations. Write replication uses `wal_write_ms`. Read uses `read_default_ms`. |

### Memory & Allocation (§1)

| Rule | Status | Evidence |
|---|---|---|
| §1.1 Bytes/BytesMut for blob data | **PARTIAL** | S3 handler receives body as `Bytes`. `WriteRequest.data` is `Bytes`. But replication calls `.to_vec()` (H2), creating full copies. Cache hit path calls `.to_vec()` (H3). |
| §1.5 Zero-copy protobuf deserialization | **VIOLATION** | All protobuf message construction copies data into new `Vec<u8>` (`.to_vec()` at replication.rs:126, coordinator.rs:275). Server-side handler buffers into `Vec<u8>`. No evidence of `Bytes`-backed prost configuration. |
| §1.6 Object pool for RPC request/response structs | **VIOLATION** | No object pooling found. Every RPC constructs a new `SegmentAppendRequest`, `WriteRequest`, `WriteAck`, etc. per call. |

### Concurrency (§2)

| Rule | Status | Evidence |
|---|---|---|
| §2.3 parking_lot locks in connection pool | **COMPLIANT** | `pool.rs:75` uses `parking_lot::Mutex` for channels. Membership uses `parking_lot::RwLock`. |
| §2.6 Bounded channels | **COMPLIANT** | All `mpsc::channel` calls have explicit capacities (8, 16, 64). No `unbounded_channel` found. |
| §2.7 Semaphore for connection pool limits | **COMPLIANT** | `pool.rs:203` creates `Semaphore::new(pool_size)` per peer pool. |

### Async (§8)

| Rule | Status | Evidence |
|---|---|---|
| §8.1 FuturesUnordered for parallel shard fetches | **COMPLIANT** | `fetch.rs:103` and `replication.rs:50` both use `FuturesUnordered` for parallel operations. |
| §8.2 tokio::select! with timeout branches | **COMPLIANT** | `replication.rs:58-76` uses `tokio::select!` with a timeout branch. |
| §8.3 spawn_blocking usage | **PARTIAL** | Used correctly for RocksDB operations (`metadata/store.rs:408,433,460`) but traces show it's also used for segment pool operations which should be async. |
| §8.4 Avoid Box::pin in network hot paths | **COMPLIANT** | `Box::pin` only appears in generated tonic code (unavoidable) and auth middleware (off hot path). |
| §8.5 Bounded semaphore for concurrent RPCs | **COMPLIANT** | Connection pool semaphore bounds concurrent RPCs per peer. |

## Network Trace: PUT

```
Client                  S3Handler             WriteCoordinator          gRPC                Replica Node
  |                         |                        |                      |                    |
  |-- PUT /bucket/key ------|                        |                      |                    |
  |   (Bytes body)          |                        |                      |                    |
  |                         |-- WriteRequest --------->                      |                    |
  |                         |   {data: Bytes, ...}    |                      |                    |
  |                         |                        |-- Ring::lookup() -----|                    |
  |                         |                        |<-- replica_set [n1..nN]                  |
  |                         |                        |                      |                    |
  |                         |                        |-- blake3::hash(data) -|                    |
  |                         |                        |    (in-memory)        |                    |
  |                         |                        |                      |                    |
  |                         |                        |-- FuturesUnordered ---|                    |
  |                         |                        |   for ea. remote:     |                    |
  |                         |                        |   data.to_vec()  ⚠️   |                    |
  |                         |                        |   → Vec<u8> (FULL COPY)|                   |
  |                         |                        |   → SegmentAppendReq  |                    |
  |                         |                        |   tokio_stream::once() |                    |
  |                         |                        |                      |-- AppendSegment --> |
  |                         |                        |                      |   (client stream)   |
  |                         |                        |                      |                    |-- vec![]
  |                         |                        |                      |                    |   .extend() ⚠️
  |                         |                        |                      |                    |   (FULL BUFFER)
  |                         |                        |                      |                    |-- write to store
  |                         |                        |                      |<--- Ack ----------|
  |                         |                        |<-- WriteAck --------|                    |
  |                         |                        |                      |                    |
  |                         |                        |-- quorum check (>W)   |                    |
  |                         |<-- WriteResult --------|                      |                    |
  |                         |   {chunks, hash, hlc}  |                      |                    |
  |                         |                        |                      |                    |
  |                         |-- seg.store.put()      |                      |                    |
  |                         |   (body.clone())  ⚠️   |                      |                    |
  |                         |-- persist to disk      |                      |                    |
  |                         |-- metadata.put_object()|                      |                    |
  |<-- 200 OK, ETag -------|                        |                      |                    |
```

**Key allocations per hop:**
| Hop | Allocation | Size |
|---|---|---|
| axum body extract | `Bytes` (reference-counted) | ~blob size |
| `replicate_to_single` | `data.to_vec()` | blob size × (W-1) replicas |
| `append_segment` (server) | `Vec::new() + extend` | blob size per replica |
| `put_object`: segment store | `body.clone()` | blob size |
| `put_object`: disk persist | `std::fs::write` | blob size |

**Total copies per PUT:** 1 original + (W-1) to_vec + W extends + 1 clone + 1 fs write = 2W+3 copies for W replicas. For W=3: 9 full copies.

## Network Trace: GET

```
Client                  S3Handler                   ReadCoordinator          gRPC/Store
  |                         |                            |                      |
  |-- GET /bucket/key ------|                            |                      |
  |                         |-- L1 cache check           |                      |
  |                         |   hit→ to_vec() ⚠️         |                      |
  |                         |   (full alloc on cache hit)|                      |
  |                         |                            |                      |
  |                         |-- L2 cache (metadata)      |                      |
  |                         |-- L3 negative cache        |                      |
  |                         |                            |                      |
  |                         |-- ReadRequest ------------->|                      |
  |                         |                            |-- metadata lookup --->|
  |                         |                            |<-- ObjectMetadata ----|
  |                         |                            |                      |
  |                         |                            |-- inline? → done      |
  |                         |                            |                      |
  |                         |                            |-- assemble_chunks()   |
  |                         |                            |   FuturesUnordered    |
  |                         |                            |   for ea. chunk:      |
  |                         |                            |   try local reader    |
  |                         |                            |   fallback: gRPC      |
  |                         |                            |                      |-- FetchShard →|
  |                         |                            |                      |   to_vec() ⚠️ |
  |                         |                            |                      |   chunk loop  |
  |                         |                            |                      |   to_vec() ⚠️ |
  |                         |                            |                      |<-- stream ----|
  |                         |                            |   Vec<u8>::new() ⚠️   |
  |                         |                            |   extend_from_slice() |
  |                         |                            |   Bytes::from(vec)    |
  |                         |                            |                      |
  |                         |                            |-- MultiChunkAssembler  |
  |                         |                            |   .hasher.update()    |
  |                         |                            |   .buffer.extend()    |
  |                         |                            |   .finalize()         |
  |                         |                            |   → Bytes (verified)  |
  |                         |                            |                      |
  |                         |<-- GetResult {data,meta}---|                      |
  |                         |                            |                      |
  |                         |-- L1 cache populate        |                      |
  |                         |   (data.clone())           |                      |
  |                         |                            |                      |
  |<-- 200 OK, body --------|                            |                      |
```

## Gossip Bandwidth Analysis

**Protocol:** Push-all-to-all every `gossip_interval_ms` (1s default)

**Per-tick message flow:** For N alive nodes:
1. Build full membership delta (all known nodes): O(N·E) bytes where E is entry size
2. Push to all N-1 alive peers: (N-1) × O(N·E) bytes transmitted

**Entry size estimate:**
- NodeId (string, ~20 bytes)
- state (enum, 1 byte)
- incarnation (u64, 8 bytes)
- address (string "host:port", ~20 bytes)
- Proto overhead (tags + lengths, ~20 bytes)
- Total per entry: ~70 bytes

**Bandwidth table:**

| Cluster Size (N) | Entries per push | Bytes per push | Pushes per tick | Total bytes/tick | Bytes/sec |
|---|---|---|---|---|---|
| 3 | 3 | 210 | 2 | 420 | 420 |
| 10 | 10 | 700 | 9 | 6,300 | 6,300 |
| 50 | 50 | 3,500 | 49 | 171,500 | ~172 KB/s |
| 100 | 100 | 7,000 | 99 | 693,000 | ~693 KB/s |
| 500 | 500 | 35,000 | 499 | 17,465,000 | ~17.5 MB/s |
| 1000 | 1000 | 70,000 | 999 | 69,930,000 | ~70 MB/s |

**Key concern:** Bandwidth grows O(N²). At 500 nodes, gossip consumes ~17.5 MB/s per node — roughly 140 Mbps, which saturates a 1 Gbps link for gossip alone.

**Recommendation:** Implement standard SWIM optimization — push to `k` random peers (k=3 is typical) and use gossip pull for the rest. This reduces bandwidth to O(k·N·E) = linear scaling.

## Connection Pool Audit

### Creation
- **Algorithm:** `get_or_create_pool` checks `DashMap` for existing pool; on miss, `create_peer_pool` is called
- **Creation:** All `pool_size_per_peer` (default 4) channels are connected eagerly in a sequential loop (`pool.rs:193-198`)
- **URI:** Always plain `http://{peer}` — no TLS support active (`tls.rs` is a placeholder)
- **Endpoint config:** `tcp_nodelay(true)`, keepalive enabled, connect_timeout applied, request_timeout applied

### Reuse
- **Method:** `get_channel` acquires a semaphore permit, then selects a channel via round-robin (`AtomicUsize`)
- **Return:** On `drop(PooledChannel)`, the semaphore permit is released. The `Channel` itself (tonic channel) is reference-counted and stays in the pool
- **Cloning:** `pooled.channel().clone()` is called by every user — tonic channels are cheap to clone (Arc-based)

### Eviction
- **Status:** **None implemented.** `health_check()` is a placeholder no-op. Dead channels are never removed.
- **Idle timeout:** Not enforced despite `max_idle_connections` existing in config.
- **Consequence:** If a peer crashes, callers retry against dead channels, each waiting `connect_timeout_ms` (5s default) before failing. With `pool_size_per_peer=4`, up to 20s of cumulative timeouts before the semaphore drains.

### Concurrency
- **Bound:** `Semaphore::new(pool_size_per_peer)` limits concurrent RPCs to the pool size
- **Overflow:** Callers block on `semaphore.acquire_owned().await` when all channels are in use

## Top 5 Bottlenecks

### 1. O(N²) Gossip Bandwidth (Critical)
- **Location:** `gossip.rs:148-174` (`on_gossip_tick`)
- **Impact:** At 100 nodes, ~700 KB/s per node for gossip. At 500 nodes, ~17.5 MB/s (saturates NIC).
- **Fix:** Push to random subset of k peers (k=3). Add pull-based gossip for remaining peers.
- **Priority:** Must fix before scaling past 50 nodes.

### 2. Full Payload Copy on Write Replication (Critical)
- **Location:** `replication.rs:126` (`data.to_vec()`), `coordinator.rs:275`
- **Impact:** For W=3 replicas, an additional 3× blob size in allocations per PUT. For 4 MB blobs: 12 MB alloc per request.
- **Fix:** Use `Bytes` throughout the protobuf path. Configure prost to use `bytes::Bytes` as the wire type for `bytes` fields.
- **Priority:** Fix before handling blobs > 1 MB.

### 3. Server-Side Stream Buffering Defeats Streaming (High)
- **Location:** `segment_service.rs:83-120` (`append_segment`), `segment_service.rs:279` (`fetch_shard`)
- **Impact:** 4 MB blobs fully buffered on receiver before disk I/O. Streaming RPC overhead is wasted.
- **Fix:** Stream chunks to store incrementally in append_segment. Use `Bytes` slicing in fetch_shard.
- **Priority:** Fix to realize network throughput gains.

### 4. No gRPC Message Size Limits (High)
- **Location:** `node.rs:469` (`tonic::transport::Server::builder()`), `pool.rs:184-190` (`Endpoint::from_shared`)
- **Impact:** No protection against oversized protobuf messages. A malformed or malicious client could send multi-gigabyte messages causing OOM.
- **Fix:** Set `max_decoding_message_size` and `max_encoding_message_size` on both server builder (4 MB default) and channel endpoint. Document limits in config.
- **Priority:** Fix before accepting untrusted traffic.

### 5. Cache Hit Path Copies on Every Read (High)
- **Location:** `handlers.rs:208,219,228,250`
- **Impact:** L1 cache hit copies full blob via `.to_vec()`. For hot objects served 1000× per second, this is 1000 extra allocations per second.
- **Fix:** Return `Body::from(cached_data)` directly — axum/hyper accepts `Bytes` natively.
- **Priority:** Fix for read-heavy workloads.

## Recommendations (Prioritized)

1. **Implement SWIM k-peer gossip** (C1) — reduces gossip bandwidth from O(N²) to O(N)
2. **Eliminate `.to_vec()` on write path** (C2, H2) — use `Bytes` throughout protobuf
3. **Stream append_segment incrementally** (H1) — write chunks as they arrive
4. **Fix cache-hit copies** (H3) — return `Bytes` directly from L1 cache
5. **Add gRPC message size limits** (M4/H6) — configure `max_decoding_message_size`
6. **Implement channel health checking** (M2) — evict dead channels from pool
7. **Set TCP_NODELAY on server sockets** (L1) — add to HTTP and gRPC listeners
8. **Raise default max_body_size** (M4) — from 2 MB to 256 MB or segment-size-aware
9. **Fix gossip duplicate cache invalidation** (L2) — remove double call
10. **Reuse `SegmentRpcClient` in pool** (L3) — construct once per peer
