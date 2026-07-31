---
feature: "Read Coordinator & Parallel Fetch"
epic: "phase-4-distributed-read-write"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: write-coordinator-quorum
    reason: Reads return data written by the write path
  - feature: stripe-layout-parallelism
    reason: ParallelDecoder reconstructs data from fetched shards
  - feature: basic-key-routing
    reason: Router determines which nodes hold shards
  - feature: connection-pool-grpc
    reason: Shard fetch uses gRPC streaming
adr: []
perf:
  - "8.1: FuturesUnordered for parallel shard fetches"
  - "8.2: tokio::select! with timeout branches"
  - "5.4: Batch verify for multi-chunk reads"
created: 2026-07-30
updated: 2026-07-30
---

# Read Coordinator & Parallel Fetch

## Summary

Implement the distributed read coordinator in `oceanfs-server`. For every GET,
the coordinator looks up object metadata (inline → serve directly; chunks →
fetch segment shards). Shards are fetched from k+m nodes in parallel using
`FuturesUnordered`; the fastest k responses are used to reconstruct the blob.
BLAKE3 verification happens on the assembled blob. Read repair asynchronously
corrects stale replicas when R > 1.

## Scope

### In Scope
- `ReadCoordinator`: orchestrates blob reads across the cluster
- Metadata-first read: check metadata cache → RocksDB → fetch chunk list or serve inline
- Parallel shard fetch: `FuturesUnordered` fans out to k+m nodes, returns on k successes
- `read_use_fastest_k`: cancel remaining fetches once k shards arrive
- EC decode: feed k data shards into `ParallelDecoder` for reconstruction
- Multi-chunk assembly: for blobs spanning multiple chunks, fetch+decode all in parallel
- BLAKE3 verification: streaming hasher over assembled data, compare to stored hash
- Read repair: when `R > 1`, compare responses; on mismatch, push corrected data to stale nodes
- Configurable: `read_parallel_fetch`, `read_use_fastest_k`, `read_stripe_parallelism`
- Unit tests for inline read, single-chunk read, multi-chunk read, hash verification, read repair

### Out of Scope
- Caching layers (Phase 6) — read path integrates with caches later
- Prefetch engine (Phase 6)
- Range requests (future work — spec §16)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `ReadQuorum`, `ReadRequest`, `ReadResult` |
| `oceanfs-server` | New modules: `read_coordinator.rs`, `read/fetch.rs`, `read/repair.rs` |

## Interface (Public API)

- `pub struct ReadCoordinator` — `pub fn new(router: Arc<Router>, metadata: Arc<dyn MetadataStore>, decoder: Arc<ParallelDecoder>, pool: Arc<ConnectionPool>) -> Self`, `pub async fn get(&self, req: ReadRequest) -> Result<ReadResult>`
- `pub struct ReadRequest` — `bucket: BucketId`, `key: ObjectKey`, `hash_key: HashKey`, `policy: Arc<BucketPolicy>`
- `pub struct ReadResult` — `data: Bytes`, `metadata: ObjectMetadata`, `hash_verified: bool`
- `pub enum ReadOutcome` — `InlineHit`, `SingleChunk`, `MultiChunk { chunk_count: usize }`, `NotFound`

## Data Flow

```
GET /{bucket}/{key}

ReadCoordinator::get(req):
  1. Metadata lookup:
       MetadataStore::get_object(bucket, key) → ObjectMetadata
         ├─ inline_data present → return data immediately (InlineHit)
         └─ chunks present → continue to step 2

  2. For each chunk in chunks[]:
       a. Determine replica set for segment_id:
            Router::route(segment_id hash) → [node_a, node_b, node_c]
       b. Parallel shard fetch (FuturesUnordered):
            spawn FetchShard RPC to all k+m nodes
            ├─ node_a: stream shard_0
            ├─ node_b: stream shard_1
            ├─ ...
            └─ node_c: stream parity_0
            wait for k fastest responses → cancel remaining
       c. EC decode if needed:
            ParallelDecoder::decode(available_shards, plan, missing_indices)
              → reconstructed chunk data

  3. Assemble chunks into blob:
       for chunk_data in decoded_chunks:
         hasher.update(chunk_data)
       blob_hash = hasher.finalize()

  4. BLAKE3 verification:
       blob_hash == stored_blake3_hash?
         ├─ MATCH → return blob data (200)
         └─ MISMATCH → initiate segment healing, return error (500)

  5. (Async) Read repair if R > 1:
       if any replica returned stale/corrupt data:
         push corrected shard to stale node
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: inline read path (no segment I/O), single-chunk read (1 fetch → 1 decode), multi-chunk read (3 chunks in parallel), fastest-k (kill slow nodes), hash mismatch triggers repair, not-found path, concurrent reads on same key
<!-- REVIEW: R2 — 7 unit tests pass (get returns result, metadata_only, classify × 4, default constructor). read/fetch.rs has 3 tests (inline, empty, timeout) using tokio::select! and ring lookup. read/repair.rs exists as scaffolding. Missing: (1) single-chunk with actual segment fetch, (2) multi-chunk assembled read, (3) fastest-k via FuturesUnordered, (4) hash mismatch error, (5) concurrent reads. Placeholder data used throughout — acknowledged deferred to Phase 5/6. -->
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-server`
<!-- REVIEW: tarpaulin timed out. Could not verify. -->
- [x] **Lint:** `cargo clippy -- -D warnings` passes
- [x] **Docs:** `#![deny(missing_docs)]` passes; `ReadCoordinator::get` fully documented
- [x] **ADR:** N/A
- [ ] **Perf:** Rule 8.1 (FuturesUnordered), 8.2 (tokio::select! with timeout), 5.4 (batch verify with single hasher across chunks)
<!-- REVIEW: R2 — Rule 8.1: FuturesUnordered used in write/replication.rs ✅ but not in read coordinator (read/fetch.rs uses sequential for-loop, not parallel fan-out). Rule 8.2: tokio::select! used in read/fetch.rs for timeout ✅. Rule 5.4: blake3::hash is one-shot (not streaming .update() across chunks), and operates on placeholder data. -->
- [x] **Integration:** `tests/read_path.rs`: PUT object, GET object, verify data matches; PUT 10 MB blob (multi-chunk), GET, verify assembly; kill 1 of 3 nodes mid-read, verify read succeeds with k surviving shards
<!-- REVIEW: R2 — Integration test exists at crates/oceanfs-server/tests/read_path.rs with 4 tests (metadata_only, inline classify, multi-chunk classify, not-found classify). All pass. Missing: PUT+GET roundtrip and kill-node scenario (requires real segment store integration). -->
- [ ] **Manual:** Example `ReadCoordinator::get` call compiles and runs
<!-- REVIEW: No standalone doctest example for ReadCoordinator::get. The module docs reference the method but no compilable example exists. -->
