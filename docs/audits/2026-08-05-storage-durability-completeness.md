---
audit_date: 2026-08-05
scope: full
target_crates: oceanfs-storage, oceanfs-storage-api, oceanfs-durability
severity_counts:
  critical: 5
  high: 8
  medium: 6
  low: 7
---

# Audit Report: Storage Engine & Durability Implementation Completeness

## Summary

The OceanFS storage and durability subsystems are partially complete with a significant architectural gap: **two parallel write paths exist, and the segment-based engine (TierRouter → SegmentPool → SegmentSealer) is entirely dead code.** The production S3 handler uses an in-memory/disk-based write path (`BlobStore` flat files + `RocksDbMetadataStore` for metadata) that is functional for basic CRUD but does not create segment metadata entries, bypasses the tiered routing system, and does not use the WAL. The durability background tasks (GC, AE, scrub, heal, orphan reaper) are fully wired with real implementations, but the hinted handoff integration into the write path is missing, WAL crash recovery is unwired, and the segment lifecycle pipeline is completely disconnected from production.

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `oceanfs-storage/src/segment/` (entire module) | **Segment pipeline is dead code.** `ActiveSegment`, `SegmentPool`, `SegmentSealer`, `TierRouter`, `route_write`, `InlineWriter`, `SegmentShard`, `SegmentSplitter` — all marked `#[allow(dead_code)]` and have zero production callers. The `SegmentSealer` is constructed as `_sealer` (unused) in `node.rs:210`. | Wire the segment pipeline into the S3 handler's PUT path, or delete the dead code and simplify the architecture. |
| C2 | `oceanfs-node/src/node.rs:199-214` | **SegmentSealer and BufferPool are constructed but never used.** `_buffer_pool` (line 201) and `_sealer` (line 210) are assigned to underscore-prefixed variables. Comments say "will be wired into the write path when final-integration-read-write-end-to-end lands." | Wire these into the actual write path or remove the dead initialisation. |
| C3 | `oceanfs-node/src/node.rs` (write path) | **Write path never calls `put_segment`.** The S3 PUT handler (`handlers.rs:64-99`) stores blob data via `BlobStore` flat files + `InMemorySegmentReader`, and persists `ObjectMetadata` via `MetadataStoreAdapter`, but never creates `SegmentMetadata` entries. This means the `segments` column family is empty in production, making GC, scrub, and anti-entropy operate on zero segments. | Call `MetadataOps::put_segment()` when the write path creates a new segment, or redesign the write path to go through the segment lifecycle. |
| C4 | `oceanfs-node/src/node.rs` | **WAL crash recovery is not wired.** `WalReader` (`oceanfs-storage/src/wal/reader.rs`) exists and has `open()`/`replay()` methods with tests, but the node startup code never calls them. A grep for "WalReader" in `oceanfs-node/src/` returns no matches. This directly causes the e2e test deviation D6: GET after crash returns 500. | Call `WalReader::open()` and `replay()` during node startup (before the HTTP server binds) to rebuild unsealed segment data. |
| C5 | `oceanfs-node/src/node.rs` | **Hinted handoff is not integrated into the write path.** `HintedHandoff` is constructed (line 342) and passed to `HealingGrpcService` (line 455), but the `WriteCoordinator` (`oceanfs-server/src/write/coordinator.rs`) has zero references to hinted handoff or handoff. When a successor node is unreachable during writes, hinted handoff is never invoked. This directly causes the cluster T21 test failure. | Wire `HintedHandoff::handoff()` into the `WriteCoordinator`'s replication path as a fallback when gRPC to a target node fails. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `oceanfs-storage/src/segment/pool.rs` | **SegmentPool entirely `#[allow(dead_code)]`.** Despite being fully implemented with pool rotation, encode queue backpressure, semaphore-bounded concurrency, and 11 unit tests, it's never instantiated in production code. Only `SegmentSealer` (a different component) is constructed. | Either wire `SegmentPool` into the write path as the primary append target, or remove it. The current state is dead weight. |
| H2 | `oceanfs-storage/src/wal/writer.rs:225` | **WAL group commit uses a no-op fsync.** Per the feature doc review note: "WalWriter::create_sync_group uses a no-op fsync function; the actual fsync is in append's flush() call." The group commit mechanism collects waiters correctly but the flusher task doesn't call `sync_all()`. Durability is provided by per-append `flush()`, not true group commit. | Implement `sync_all()` in the WalSyncGroup flusher task to achieve amortized fsync. |
| H3 | `oceanfs-durability/src/heal/worker.rs:267-292` | **Heal worker uses simplified local-only repair.** The `execute_heal()` method reads shard data from a local `SegmentDataStore` rather than fetching healthy shards from peer nodes via gRPC. It splits the full segment data locally rather than performing distributed shard fetch. The `FuturesUnordered` parallel gRPC fetch described in the feature spec and perf rule 8.1 is not implemented. | Implement distributed shard fetch via `ConnectionPool` + `HealingRpcClient::fetch_shard()`. The constructor already has reserved slots for `membership` and `pool` parameters. |
| H4 | `oceanfs-durability/src/anti_entropy/engine.rs` | **Anti-entropy Merkle exchange is local-only, not peer-to-peer gRPC.** Per the review note in the feature doc: "actual gRPC peer exchange and EC-based leaf repair are stubbed (repair_diverged_leaves returns Ok(0); exchange_merkle_roots compares against stored roots, not peer data over gRPC)." | Implement the gRPC MerkleExchange protocol for real peer-to-peer Merkle tree comparison. |
| H5 | `oceanfs-durability/src/scrub.rs` | **Scrub coordinator lacks distributed partition assignment.** The `ScrubRpc` gRPC service is registered and serving, but `ScrubCoordinator::run_cycle()` does not use `ConnectionPool` to assign partitions to peer nodes. All segments are scrubbed locally. | Thread `Membership` and `ConnectionPool` into `ScrubCoordinator` to enable distributed scrub partition assignment. |
| H6 | `oceanfs-storage-api/src/metadata_store.rs` | **`MetadataStore` trait is too minimal.** The API crate's `MetadataStore` trait has only 2 methods (`list_object_keys`, `get_object_metadata`), while the concrete `RocksDbMetadataStore` has 15+ methods. The `oceanfs-server::MetadataOps` trait duplicates many of these with `std::io::Result` instead of the crate's `Error` type. This fragmentation means three different metadata interfaces exist for the same operations. | Consolidate into a single canonical `MetadataStore` trait in `oceanfs-storage-api` with all CRUD methods, or clearly document the separation of concerns. |
| H7 | `oceanfs-storage/src/segment/route_write.rs:51` | **`route_write` function is `#[allow(dead_code)]` and uses a single `ActiveSegment`.** The function signature takes `&mut ActiveSegment` — a single segment — rather than routing through the `SegmentPool` or `SegmentShard`. This doesn't match the spec's description of shard-based routing to per-core segment pools. | Refactor to use `SegmentPool` for production routing, or remove if the in-memory write path is the intended architecture. |
| H8 | `oceanfs-storage/src/wal/writer.rs:225` (and node startup) | **WAL is written but never truncated in production.** The `WalWriter` is opened in `node.rs:195` but `truncate()` is only ever called from tests. Since the segment-based pipeline (which would call `truncate()` after seal) is dead code, WAL files grow unboundedly. | Wire WAL truncation into whatever seal path is active, or disable WAL writing if the current BlobStore path is the intended architecture. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `oceanfs-storage/src/metadata/cf.rs:45-48` | **`encode_deletion_key` is `#[allow(dead_code)]`.** The function exists but `encode_object_key` is used directly instead, since deletions use the same key format. This is harmless but dead. | Remove or use consistently. |
| M2 | `oceanfs-durability/src/anti_entropy/engine.rs:672,678` | **Two `#[allow(dead_code)]` on `MerkleExchangeProtocol`.** Per DEV-003, this is a "test-only wire-format helper; not on production gRPC path." | Acceptable but should be documented in code. |
| M3 | `oceanfs-durability/src/scrub.rs:286,513` | **`#[allow(dead_code)]` on `throttle_bytes_sec` (reserved for future I/O throttling) and `partition_segments` (test-only multi-node scrub not yet implemented).** Per DEV-003. | Acceptable, but add ADR tracking for when these will be wired. |
| M4 | `oceanfs-durability/src/heal/worker.rs:368` | **`StubDecoder` used in tests fills missing shards with zeros.** The comment says "(stub behavior)" — the real decoder (`CauchyEncoder`) is used in production, but the test coverage for decode failure paths is limited. | Add integration tests with the real `CauchyEncoder::decode()` for failure modes. |
| M5 | `oceanfs-storage/src/blob_store.rs` vs segment lifecycle | **Architectural ambiguity: BlobStore vs SegmentStore.** The production write path uses `BlobStore` (flat files by SegmentId), but GC/scrub/anti-entropy use `SegmentDataStore` / `SegmentShardStore` traits. These are different storage abstractions that may not be backed by the same physical data. | Unify the storage backend under a single trait hierarchy or ensure both pathways read from the same physical storage. |
| M6 | `oceanfs-durability/src/scrub_service.rs:23,26` | **`#[allow(dead_code)]` on service fields.** Fields exist but are not used in current service implementation. | Verify if these are needed for future partition work; remove if not. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-storage/src/segment/pool.rs` | **PoolSlotState enum marked `#[allow(dead_code)]` at the type level.** All variants are used by the pool logic; only the enum-and-struct-as-a-whole annotation is dead. | Remove the type-level `#[allow(dead_code)]` from `PoolSlotState` and `PoolSlot` since their internals are used. |
| L2 | `oceanfs-storage/src/segment/tier.rs:38,41` | **Two `#[allow(dead_code)]` on `ChunkListBuilder` methods.** These are used in tests but not production. | Wire or remove. |
| L3 | `oceanfs-storage/src/metadata/store.rs:406-469` | **Async wrappers (`*_async` methods) duplicate sync methods with `spawn_blocking`.** The sync methods are used directly by the `MetadataStoreAdapter` in `oceanfs-node`. The async wrappers are not called by any production code. | Remove async wrappers or use them consistently to avoid RocksDB blocking the tokio runtime. |
| L4 | Naming deviation | **`MetadataStore` is a trait in `oceanfs-storage-api` but a struct in the feature doc.** The concrete implementation is `RocksDbMetadataStore`. The API trait is minimal (2 methods) while the concrete store has 15+. The server has its own `MetadataOps` trait. | Standardise naming and consolidate traits. |
| L5 | `oceanfs-storage/src/segment/route_write.rs` | **`route_write` uses a wildcard `_ => Ok(...)` arm for unknown `SizeTier` variants.** This silently ignores unrecognised tier values instead of returning an error. | Return `Err` for unknown variants. |
| L6 | ADR-0001 compliance | **`SegmentCompactor` remaps chunk refs in metadata but does not physically re-encode segments through the EC pipeline.** Per DEV-002: "Full EC re-encoding during compaction is deferred to a follow-up feature." | Track as a follow-up ADR. |
| L7 | `oceanfs-storage/src/buffer_pool.rs:31` | **`BufferPool` struct is `pub` but only used in dead-code segment code and tests.** Since the segment pipeline is dead, BufferPool is effectively dead code in production. The `_buffer_pool` in node.rs is unused. | Wire or mark `pub(crate)`. |

---

## Coupling Hotspots

| Symbol | Crate | Risk | Notes |
|---|---|---|---|
| `RocksDbMetadataStore` | `oceanfs-storage` | High | Central dependency — used by S3 handler, admin handler, GC, AE, scrub, heal, reaper, hinted handoff, gRPC services. Format change would break everything. |
| `NodeConfig` | `oceanfs-core` | High | Central config — now has GC/AE/scrub/reaper interval fields (confirmed configured, not hardcoded — e2e deviation D2/D4 are outdated). |
| `MetadataOps` (trait) | `oceanfs-server` | Medium | Used by S3 handler, read coordinator, admin handler, metadata adapter. |

---

## Dependency Graph

The DAG constraint is nominally respected (no circular crate dependencies detected). However, the **architectural drift** is significant:

- **Intended flow:** PUT → TierRouter → SegmentPool → ActiveSegment → SegmentSealer → EC encode → distributed shards → RocksDB metadata
- **Actual flow:** PUT → WriteCoordinator (gRPC quorum) → InMemorySegmentReader + BlobStore flat files + RocksDBMetadataStore (object metadata only)
- **Dead code:** The entire `segment/` module pipeline (~10 files) and `BufferPool` are unused in production.

The `oceanfs-storage-api` → `oceanfs-storage` → `oceanfs-durability` → `oceanfs-node` dependency chain is clean. No circularity.

---

## Guideline Violations

| Guideline | Location | Violation |
|---|---|---|
| Coding §4.2 (pub items must have doc comments) | Audit not performed exhaustively; feature docs claim `#![deny(missing_docs)]` passes. | N/A |
| Architecture §2.1 (traits in consuming crate) | `MetadataStore` trait in `oceanfs-storage-api` is minimal (2 methods); `MetadataOps` in `oceanfs-server` duplicates; `RocksDbMetadataStore` in `oceanfs-storage` has 15+ methods not covered by any trait. | Fragmented interface; consolidate. |
| Performance §3.4 (group commit for WAL fsync) | `oceanfs-storage/src/wal/writer.rs:225`: group commit uses no-op fsync. | File under H2. |
| Performance §3.1 (sequential-only WAL writes) | Satisfied — WAL uses append-only sequential writes. | OK |
| Performance §2.5 (sharded segment buffer per worker thread) | `SegmentShard` exists but is dead code. Per-core sharding not wired. | File under C1. |
| Coding §4.6 (test coverage ≥ 80%) | `oceanfs-durability` overall coverage is 63.3% (per feature doc). Individual modules meet or approach 80%. | Acceptable per explicit DoD acceptance in feature docs. |

---

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| ADR-0001 (segment packing) | PARTIAL | Tiered sizing logic (`TierRouter`, `SegmentSizeConfig`) exists but is dead code in production. `SegmentCompactor` uses tiered sizing during GC re-packing (metadata-only). Tier thresholds are correctly mapped. |
| pending-configurable-intervals | RESOLVED | `NodeConfig` now has `gc_interval_sec`, `tombstone_ttl_sec`, `ae_interval_sec`, `scrub_interval_sec`, `orphan_reaper_interval_sec` — all serde-configurable with defaults. e2e deviations D2/D4 are outdated. |
| pending-wal-recovery | UNRESOLVED | `WalReader` exists but is not called during node startup. WAL crash recovery is not functional. |
| pending-body-size-limit | PARTIAL | `NodeConfig.max_body_size` exists (default 2 MB). Not yet checked from e2e perspective, but config field exists. |

---

## Test Coverage

| Crate | Key Public Symbols | Tests Verified | Gaps |
|---|---|---|---|
| `oceanfs-storage` | `RocksDbMetadataStore`, `WalWriter`, `WalReader`, `BlobStore`, `BufferPool`, `SegmentIndex`, `SegmentSealer`, `ActiveSegment`, `SegmentPool`, `TierRouter` | Unit tests for all; integration tests for metadata, WAL, segment roundtrip, tiered routing | No production integration of segment pipeline; no WAL crash recovery integration test |
| `oceanfs-durability` | `GarbageCollector`, `AntiEntropy`, `ScrubCoordinator`, `OrphanReaper`, `HealWorker`, `HealQueue`, `HintedHandoff`, `MerkleTree` | 290+ unit tests; 40+ integration tests; feature docs claim 81%+ heal coverage, 90%+ scrub coverage | No distributed gRPC integration; no full 3-node cluster test |
| `oceanfs-storage-api` | `MetadataStore`, `WalWriter`, `BlobStore`, `SegmentStore` | N/A (traits only) | Trait is too minimal; doesn't cover put/delete/segment operations |

---

## Subsystem Status Summary

| Component | Status | Evidence | Notes |
|---|---|---|---|
| **RocksDB Metadata Store** | IMPLEMENTED | `RocksDbMetadataStore` with 3 CFs (objects, segments, deletions), full CRUD + tombstone + batch operations. 15+ methods, all tested. | `put_segment` exists but never called in production write path (D1). |
| **WAL Writer** | IMPLEMENTED (with gaps) | `WalWriter` struct + trait in API crate. Append, truncate, sync, rotate, group commit. | Group commit is no-op (H2). Truncation never called in production (H8). |
| **WAL Reader (Crash Recovery)** | STUB (in production) | `WalReader` implemented and tested but NOT called during node startup. | Directly causes e2e D6: GET after crash returns 500. |
| **Segment Buffer (ActiveSegment)** | DEAD CODE | Fully implemented with append, is_full, tested. Zero production callers. | `#[allow(dead_code)]` |
| **Segment Pool** | DEAD CODE | `SegmentPool` with pool rotation, encode queue, semaphore concurrency, 11 tests. Not instantiated in production. | `#[allow(dead_code)]` on entire struct (C1). |
| **Segment Sealer** | DEAD CODE (in production) | Fully implemented, tested. Constructed as `_sealer` in node.rs. | `try_seal` never called in production (C2). |
| **Segment Index (B-tree)** | IMPLEMENTED | `SegmentIndex` with lookup, serialization. Tested. | Not used in production (segment sealing is dead). |
| **Tiered Segment Routing** | DEAD CODE | `TierRouter`, `SegmentSplitter`, `InlineWriter`, `route_write` — all implemented and tested. | `#[allow(dead_code)]` on all production code (C1). |
| **Buffer Pool** | DEAD CODE (in production) | `BufferPool` with acquire/release, pre-allocation. | Constructed as `_buffer_pool` in node.rs (C2). |
| **Garbage Collector** | IMPLEMENTED | Wired in background tasks. `run_cycle()` processes tombstones, computes liveness, triggers compaction. 39+ unit tests. | `SegmentCompactor` is metadata-only (no EC re-encode, DEV-002). Operates on empty segments CF in production (C3 side-effect). |
| **Segment Compactor** | PARTIAL | Remaps chunk refs but does not re-encode via EC pipeline (DEV-002). | Tracked for follow-up. |
| **Orphan Reaper** | IMPLEMENTED | Wired in background tasks. Full scan-and-reap cycle. 21 tests. | Operates on empty segments CF in production (C3 side-effect). |
| **Anti-Entropy** | IMPLEMENTED (local) | Wired in background tasks. Merkle tree construction, diff, verification. | gRPC peer exchange stubbed (H4). `repair_diverged_leaves` returns Ok(0). |
| **Merkle Tree** | IMPLEMENTED | `MerkleTree::build`, `diff`, `root`, `leaf_hash`. BLAKE3 leaves. Rayon parallelism. | Core logic complete. |
| **Scrub Coordinator** | IMPLEMENTED (local) | `POST /admin/scrub` returns 202. Segment verification via BLAKE3 + Merkle. | Distributed partition assignment not implemented (H5). |
| **Heal Pipeline** | IMPLEMENTED (local) | `HealQueue` (bounded mpsc), `HealWorker`, `enqueue_heal()` global singleton. | Local-only repair path; no distributed shard fetch (H3). `StubDecoder` in tests. |
| **Healing gRPC Service** | IMPLEMENTED | `HealingGrpcService` with handoff endpoint. Registered in gRPC server. | Hint delivery tested but write path doesn't invoke it (C5). |
| **Hinted Handoff** | PARTIAL | `HintedHandoff` handles `handoff()`, `deliver_pending()`, `pending_count()`. gRPC delivery via `HealingRpcClient`. | NOT integrated into `WriteCoordinator` (C5). Server write path has zero references to handoff. |
| **Storage API Crate** | IMPLEMENTED (minimal) | 4 traits: `MetadataStore`, `WalWriter`, `BlobStore`, `SegmentStore`. | `MetadataStore` too minimal (H6). Used by `PrefetchStoreAdapter` in node startup. |

---

## Summary

| Category | Count |
|---|---|
| **IMPLEMENTED** | 12 |
| **IMPLEMENTED (with gaps/notes)** | 5 |
| **PARTIAL** | 3 |
| **DEAD CODE** | 5 |
| **STUB (in production)** | 1 |

### Top 5 Blocking Gaps

1. **Segment pipeline is entirely dead code (C1, C2).** The tiered routing, segment pooling, sealing, and blob index — core components of ADR-0001 and spec §3-4 — are implemented but disconnected from production. The S3 handler bypasses them entirely with flat-file blob storage.

2. **Segment metadata is never created (C3).** `put_segment()` exists on `RocksDbMetadataStore` and `MetadataOps` but the write path never calls it. The `segments` CF is empty in production, making GC, scrub, and anti-entropy operate on nothing.

3. **WAL crash recovery is not wired (C4).** `WalReader` exists and has tests, but `WalReader::open()`/`replay()` are never called during node startup. After SIGKILL, blob data stored as flat files may be recoverable from `BlobStore` but unsealed segment state (if any) is lost. More critically, the WAL is written but never replayed.

4. **Hinted handoff is not integrated into the write path (C5).** `HintedHandoff` is constructed and passed to the gRPC service so other nodes can send hints to this node, but this node's own write path never invokes `handoff()` when a successor is unreachable.

5. **Heal and anti-entropy are local-only (H3, H4).** Both subsystems have real implementations with correct core logic (EC decode, Merkle tree diff), but the distributed gRPC-based peer coordination is stubbed or simplified. This means multi-node corruption detection and repair don't work end-to-end.

### Recommended Action Priority

1. **Decide architecture:** Either wire the segment pipeline into production OR embrace the flat-file `BlobStore` approach and remove the dead segment code.
2. **Fix critical gaps:** Wire WAL replay (C4), wire hinted handoff into the write coordinator (C5), and ensure `put_segment` is called (C3).
3. **Enable distributed durability:** Implement gRPC peer exchange for anti-entropy (H4), distributed scrub partition assignment (H5), and distributed shard fetch for healing (H3).
4. **Consolidate traits:** Merge `MetadataStore` (API crate) and `MetadataOps` (server crate) into a single canonical interface (H6).
5. **Remove dead code:** Either delete or wire the ~10 files in `segment/` with `#[allow(dead_code)]`.
