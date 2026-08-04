---
audit_date: 2026-08-03
scope: full
target_crates: all
severity_counts:
  critical: 3
  high: 8
  medium: 12
  low: 9
---

# Audit Report: Two-Stage Structural Audit (Crate-Level + Intra-Crate)

## Summary

OceanFS has 162 Rust source files across 12 crates (~48,000 lines). The codebase
has evolved rapidly and the structural debt is concentrated in three areas: a
**god-file in `oceanfs-core`** (2,198-line `types.rs` that holds the entire
shared type system), **two mega-crates** (`oceanfs-storage` at 12,684 lines and
`oceanfs-server` at 11,130 lines), and an **empty skeleton crate**
(`oceanfs-hash` at 19 lines). The crate-level DAG from `architecture.md` is
broadly honored, but `oceanfs-server` has optional dependencies that partially
undermine the trait-in-consuming-crate pattern. At the file level, multiple
files exceed 1,000 lines with mixed responsibilities (anti-entropy, GC,
S3 handler, read coordinator, node startup), violating the architecture
guideline that "each public type gets its own file." The codebase is functional
but the structural debt will increasingly burden maintainability if left
unaddressed.

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `crates/oceanfs-core/src/types.rs` (2,198 lines) | God file containing 45+ public types (`SegmentId`, `NodeId`, `BucketId`, `ObjectKey`, `HashOutput`, `SizeTier`, `SegmentSizeConfig`, `SegmentMetadata`, `ObjectMetadata`, `ChunkRef`, `HashKey`, `CodecType`, `CodecConfig`, `NodeState`, `VnodeRange`, `StorageLocation`, `Tombstone`, `WriteQuorum`, `WriteResult`, `EncodingPlan`, `HealRequest`, `HealStats`, `RpcConfig`, `PoolConfig`, `GpuConfig`, `GossipConfig`, `NvcompConfig`, `CompressConfig`, `CompressionTier`, `Incarnation`, `IntendedFor`, `PeerAddress`, `CacheInvalidateRequest`, `OperationType`, `SegmentIndexEntry`, `ShardIndex`, `NvcompCodec`, `WriteAck`, `MetadataStore` trait, and more). The coupling hotspot analysis confirms this: 6 of the top-20 most-depended-upon symbols are constructor functions in this file. The architecture guideline §3.3 requires "each public type gets its own file." | Split `types.rs` into type-specific files (see §Recommendations). Minimum split: `id_types.rs` (SegmentId, NodeId, BucketId, ObjectKey), `metadata_types.rs` (ObjectMetadata, SegmentMetadata, ChunkRef, SegmentIndexEntry), `config_types.rs` (RpcConfig, PoolConfig, GpuConfig, GossipConfig, etc.), `codec_types.rs` (CodecType, CodecConfig, EncodingPlan), and `heal_types.rs` (HealRequest, HealStats). This also fixes code-graph index duplication (the same symbols appear at 7+ different line positions due to repeated re-indexing of the monolithic file). |
| C2 | `crates/oceanfs-hash/src/lib.rs` (19 lines) | Empty skeleton crate. No `Blake3Hasher`, no `BatchHasher`, no `HashOutput` (the latter lives in `oceanfs-core::types`). The architecture §1.2 table promises `Blake3Hasher` (streaming), `BatchHasher` (multi-chunk), and `HashOutput` from this crate. The lints are configured but there is no implementation. This crate currently costs compilation time with zero value. | Either **implement** the hashing subsystem here (move `HashOutput` from `oceanfs-core`, add `Blake3Hasher` and `BatchHasher`), or **merge into `oceanfs-core`** and remove the crate from the workspace. The former is preferred per the spec's subsystem partitioning. |
| C3 | `crates/oceanfs-storage/src/anti_entropy.rs` (2,580 lines) + `gc.rs` (2,126 lines) | Two massive files each containing 5+ distinct public types. `anti_entropy.rs` contains: `MerkleTree`, `MerkleRoot`, `MerkleProof`, `LeafRange`, `AntiEntropyConfig`, `AntiEntropy`, `AntiEntropyStats`, plus test-only types. `gc.rs` contains: `GcConfig`, `GcStats`, `LivenessTracker`, `SegmentCompactor`, `GarbageCollector`, `OrphanStats`, `OrphanReaper`. Violates §3.3 (one-type-per-file). | Split each into separate files per type: `merkle_tree.rs`, `merkle_proof.rs`, `anti_entropy.rs` (the coordinator only), `gc_config.rs`, `liveness_tracker.rs`, `segment_compactor.rs`, `garbage_collector.rs`, `orphan_reaper.rs`. This aligns with the pattern already used for `segment/` submodule (which has `buffer.rs`, `handle.rs`, `header.rs`, `index.rs`, etc.) |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `crates/oceanfs-server/Cargo.toml:13-16` | `oceanfs-server` has optional dependencies on `oceanfs-storage`, `oceanfs-ec`, `oceanfs-cache`, `oceanfs-accel`. Architecture §2.1 requires traits to be defined in the consuming crate and §4.1 states "oceanfs-server never imports oceanfs-storage, oceanfs-membership, or any concrete crate." While these are optional (feature-gated), the `default` feature enables them all, meaning in practice `server` links against `storage` and `ec`. This partially undermines the trait-in-consuming-crate inversion pattern. | Audit whether these optional deps are for type re-exports (acceptable per §2.4) or for concrete implementation access (violation). If the latter, move those code paths into `oceanfs-node` (the composition root). At minimum, document the justification for each optional dep in `Cargo.toml` comments or a dedicated ADR. |
| H2 | `crates/oceanfs-storage` (12,684 lines, 9 top-level modules + 4 subdirectory module groups) | The crate mixes low-level primitives (buffer pool, WAL, segment buffer) with high-level orchestration (anti-entropy, scrubbing, GC, healing). The architecture §1.2 table shows only `SegmentStore`, `MetadataStore`, `WalWriter`, `SegmentHandle`, `SegmentIndex`, `BufferPool` as `storage`'s API, but the actual `lib.rs` re-exports 45+ types across 9 modules including heal, anti-entropy, scrub, and GC. | Consider splitting `oceanfs-storage` into: (a) `oceanfs-storage-core` (buffer_pool, segment, wal, metadata, blob_store) — the actual storage engine, and (b) `oceanfs-durability` (anti_entropy, scrub, gc, heal) — background maintenance. This would reduce the crate from 12.6K to ~7K + ~5.6K lines. Alternatively, re-evaluate whether anti-entropy, scrub, and GC belong in `oceanfs-node` per the architecture's "background tasks" model. |
| H3 | `crates/oceanfs-server` (11,130 lines, 26 source files, 6 submodules) | The crate mixes HTTP handlers (s3_handler, s3_xml), coordination logic (read_coordinator, write_coordinator), auth, bucket config, admin, hinted handoff, gRPC service implementations, and sub-modules for read/write/grpc/auth. The architecture §1.2 expects `S3Handler`, `WriteCoordinator`, `ReadCoordinator`, `AdminHandler` — but the actual crate has grown far beyond this. | Consider a split: `oceanfs-server` (S3 API surface: s3_handler, s3_xml, router, auth) and `oceanfs-coordination` (read_coordinator, write_coordinator, hinted_handoff, metadata_ops, bucket_config, admin). The read/write submodules are a good start but need completion. |
| H4 | `crates/oceanfs-server/src/s3_handler.rs` (1,252 lines) | Mixed responsibilities: S3 HTTP handler functions (`put_object`, `get_object`, `head_object`, `delete_object`, `create_bucket`, `delete_bucket`, `list_objects`), `AppState` struct, response types (`PutObjectResponse`, `GetObjectResponse`, `ListObjectsResponse`, `ListObjectEntry`), `MimeMap` type, error helpers, and `MockMetadata` for tests. Violates §3.3. | Split the response types into `s3_responses.rs`. Move `MimeMap` to its own file. Keep `s3_handler.rs` for the axum handler functions only. Move `MockMetadata` into a `#[cfg(test)]` module in `tests/` or a dedicated test-support module. |
| H5 | `crates/oceanfs-node/src/node.rs` (1,012 lines) | Monolithic file containing: `PrefetchStoreAdapter`, `BackgroundTasks` (8 task handles + 8 cancel tokens = 16 fields), `Node` struct (6 fields), `Node::start` (the entire startup/wiring), `validate_config`, `spawn_background_tasks`, plus comprehensive tests. The start function alone wires 12+ subsystems. | Split `BackgroundTasks` into `background_tasks.rs`. Split `Node` wiring/startup from tests. Consider extracting `validate_config` into `config.rs`. The `Node::start` function should delegate to smaller `build_*` helper functions. |
| H6 | `crates/oceanfs-server/src/read_coordinator.rs` (1,192 lines) | Contains `ReadRequest`, `ReadResult`, `ReadOutcome` enum, `ReadCoordinator`, and all read-path logic. The `read/` subdirectory exists (with `assembly.rs`, `fetch.rs`, `repair.rs`) but `read_coordinator.rs` at the top level duplicates the coordinator role. | Move `ReadCoordinator` into `read/coordinator.rs`. The top-level `read_coordinator.rs` should become a thin re-export facade or be removed in favor of `read/mod.rs` re-exports. |
| H7 | `crates/oceanfs-accel` (20 files, 6,878 lines) | Contains 6 distinct backends (tier0 CPU SIMD, ISA-L, ARM SVE, CUDA, nvCOMP, igzip) plus the dispatcher, compressor, and metrics. Each backend is feature-gated as required but they all live in one crate. The architecture §2.3 requires feature-gated code in dedicated modules — this is satisfied structurally, but 6 backends in one crate creates compilation coupling: changing any backend's FFI requires rebuilding the entire accel crate. | Consider: keep `oceanfs-accel` as the dispatcher + tier0 only, and move each optional backend into its own sub-crate (`oceanfs-accel-isal`, `oceanfs-accel-cuda`, `oceanfs-accel-arm-sve`, `oceanfs-accel-nvcomp`, `oceanfs-accel-igzip`). This would parallelize compilation and isolate FFI risk. Low priority if compilation time isn't a current issue. |
| H8 | `crates/oceanfs-network` (12 files) | Contains generated protobuf service stubs for **all** services (cache, gossip, healing, scrub, storage) plus common types. Architecture §2.4 says "service definitions belong to the crate that implements them." The `oceanfs-network` crate should not own service stubs for services it doesn't implement — those belong in `oceanfs-server` or the respective service crates. | Move generated service stubs to their owning crates: `oceanfs.cache.rs` → `oceanfs-cache`, `oceanfs.healing.rs` → `oceanfs-storage` (heal module), `oceanfs.scrub.rs` → `oceanfs-storage` (scrub module), `oceanfs.storage.rs` → `oceanfs-storage`. Keep only `oceanfs.common.rs` and `oceanfs.gossip.rs`/`oceanfs.membership.rs` in `oceanfs-network` if it genuinely owns those services. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `crates/oceanfs-core/src/types.rs` — symbol duplication | The code-graph index shows multiple copies of the same symbol at different line positions (e.g., `SegmentId` at lines 23 and 3665, `BucketId` at lines 114, 119, and multiple `impl` blocks). This likely results from the file being re-indexed across multiple passes or containing duplicated code sections. | Verify that `types.rs` does not contain literal duplicate type definitions. If duplicates exist, deduplicate. If this is a code-graph artifact, file a tooling issue. The file size (2,198 lines) makes manual review error-prone — splitting the file (C1) will eliminate this risk. |
| M2 | `crates/oceanfs-server` — `write_coordinator.rs` + `write/` submodule | Same structural issue as H6: `write_coordinator.rs` (687 lines) exists alongside `write/mod.rs` and `write/replication.rs` (217 lines). The coordinator logic should live under `write/coordinator.rs` for consistency. | Move `write_coordinator.rs` into `write/coordinator.rs`. Keep `write/mod.rs` as the re-export facade. |
| M3 | `crates/oceanfs-server/src/bucket_config.rs` (646 lines) | Contains bucket configuration store logic. This is a cross-cutting configuration concern. The architecture §1.2 lists `BucketPolicy` in `oceanfs-core` but the actual bucket configuration logic is in `oceanfs-server`. | Evaluate whether `bucket_config.rs` belongs in `oceanfs-server` (it's server-scoped configuration) or should move to `oceanfs-core` (if it's a shared type). The current placement seems reasonable but needs explicit documentation justifying it. |
| M4 | `crates/oceanfs-core/src/types.rs` — `MetadataStore` trait | The `MetadataStore` trait is defined in `oceanfs-core::types` (line ~1900 range based on re-exports in `lib.rs`). This violates the trait-in-consuming-crate pattern (§2.1): `MetadataStore` is consumed by `oceanfs-server` and should be defined there. Having it in `core` creates an implicit contract that every crate depending on `core` must know about metadata operations. | Move `MetadataStore` trait to `oceanfs-server` (or a new `oceanfs-metadata` crate if shared between server and storage). See ADR-0005 for the trait-in-consuming-crate pattern rationale. Keep only the associated data types (`ObjectMetadata`, `SegmentMetadata`) in `core`. |
| M5 | `crates/oceanfs-storage/src/segment/pool.rs` (649 lines) | Segment pool management is fairly large for a submodule file. Contains pool lifecycle, active segment management, sharding logic. | Consider splitting into `pool/manager.rs` and `pool/shard.rs` if the pool logic continues to grow. |
| M6 | `crates/oceanfs-server/src/admin.rs` (797 lines) | Admin handler is approaching the size where splitting would help. Contains admin API handlers, metrics endpoints, health checks. | Split into `admin/handlers.rs` and `admin/metrics.rs`. |
| M7 | `crates/oceanfs-membership/src/gossip.rs` (527 lines) + `membership.rs` (822 lines) | Two large files in a relatively small crate. `membership.rs` is particularly large for the membership state management. | Split `membership.rs` into `membership/state.rs` (already exists but small at 80 lines — consider expanding) and `membership/manager.rs` for the lifecycle logic. |
| M8 | `crates/oceanfs-core/src/config.rs` (504 lines) | Config module is growing. Contains `NodeConfig`, `MetadataConfig`, `RingConfig`, `WalConfig`, `AccelConfig`, `AuthConfig`, `CompressionConfig` — all in one file. | Split into per-subsystem config files: `config/node.rs`, `config/metadata.rs`, `config/ring.rs`, etc., with `config/mod.rs` as the re-export facade. |
| M9 | `crates/oceanfs` — `main.rs` at 224 lines | The binary crate's `main.rs` is modest but could delegate CLI parsing and signal handling to separate modules for clarity. | Extract `cli.rs` for argument parsing and `signals.rs` for OS signal handling. |
| M10 | `crates/oceanfs-membership/src/failure_detector.rs` (519 lines) | Single-file failure detector. The SWIM protocol is complex and the implementation could benefit from internal structure. | Split into `failure_detector/ping.rs`, `failure_detector/suspicion.rs`, `failure_detector/mod.rs` as the coordinator. |
| M11 | Code-graph index shows 23,722 symbols but only 0 files counted | The `code-graph_get_stats` reports `files: 0` despite 23,722 symbols and `indexing: true`. This may indicate the index is still in progress or there's a file-counting bug. | Verify the code-graph indexer is tracking file counts correctly. The 0 file count makes it harder to audit file-level metrics. |
| M12 | No `.proto` files under `crates/` | Architecture §2.4 expects `oceanfs-core/proto/` and `oceanfs-storage/proto/` directories with `.proto` source files. None exist — all protobuf is pre-generated into `src/generated/`. | Either add the `.proto` source files to the expected locations for documentation/reproducibility, or update `architecture.md` §2.4 to reflect that protobuf sources live under a workspace-level `proto/` directory with generated code in `src/generated/`. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-routing/src/ring.rs` (316 lines) | The ring implementation is well-sized but contains both the ring data structure and ring operations. Architecture §3.3 suggests one-type-per-file but the ring is a single coherent concept; splitting would be over-engineering at current size. | Monitor. If `ring.rs` exceeds 500 lines, split into `ring.rs` (data) and `ring_ops.rs` (lookup, successor computation). |
| L2 | All crates — `#![deny(missing_docs)]` present | Good: all inspected `lib.rs` files include `missing_docs` in their deny attributes. However, the large files like `types.rs` and `gc.rs` have many `pub` items with empty `symbol_doc: ""` per code-graph, suggesting doc comments may be missing on individual items. | Run `cargo doc --no-deps` and check for warnings. Add doc comments to all `pub` items that currently lack them. |
| L3 | `crates/oceanfs-server/src/s3_xml.rs` (156 lines) | XML generation for S3 responses. Good separation, well-sized. No action needed — included for completeness. | None — this is exemplary structure. |
| L4 | `crates/oceanfs-ec/src/stripe/` submodule | Well-structured sub-module with `mod.rs`, `batch.rs`, `layout.rs`, `parallel.rs`. The SoA layout from performance rule 6.2 is captured in `layout.rs`. | None — this is exemplary intra-crate organization. |
| L5 | `crates/oceanfs-cache` | Well-structured with one file per cache layer (`l1_object.rs`, `l2_metadata.rs`, `l3_negative.rs`, `prefetch.rs`). | None — this is exemplary crate organization matching the three-tier cache model. |
| L6 | Test co-location | All inspected source files have `#[cfg(test)] mod tests` at the bottom. Good compliance with coding guideline §4.1. | None — this pattern is consistently followed. |
| L7 | `crates/oceanfs-server` — `auth/` submodule | `auth/mod.rs` (26 lines), `auth/key_store.rs` (144 lines), `auth/middleware.rs` (183 lines), `auth/sigv4.rs` (410 lines). Well-organized auth subsystem. | Consider extracting `sigv4.rs` into `auth/sigv4/` with separate files for signing and verification if the module continues growing. |
| L8 | `crates/oceanfs-storage/src/segment/` submodule | Already split into `buffer.rs`, `handle.rs`, `header.rs`, `index.rs`, `pool.rs`, `route_write.rs`, `sealer.rs`, `shard.rs`, `splitter.rs`, `tier.rs` — this is good structural organization. | None — this is the pattern that `anti_entropy.rs` and `gc.rs` (C3) should follow. |
| L9 | `clippy.toml` disallows `std::sync::Mutex` and `std::sync::RwLock` | Good compliance with performance rule 2.3. The codebase uses `parking_lot` throughout (confirmed by code-graph symbol analysis showing `parking_lot::RwLock` in mock types in `s3_handler.rs`). | None — continue enforcing. |

---

## Coupling Hotspots

| Symbol | Crate | In-Degree | Risk |
|---|---|---|---|
| `BucketId::new` | oceanfs-core::types | 591 | **HIGH** — change breaks every crate |
| `ObjectKey::new` | oceanfs-core::types | 532 | **HIGH** — change breaks every crate |
| `ObjectKey::as_str` | oceanfs-core::types | 498 | **HIGH** |
| `BucketId::as_str` | oceanfs-core::types | 449 | **HIGH** |
| `NodeId::new` | oceanfs-core::types | 400 | **HIGH** |
| `MetadataStore::open` | oceanfs-storage | 319 | **HIGH** — central initialization |
| `GcConfig::default` | oceanfs-storage::gc | 272 | **MEDIUM** |
| `HashOutput::from_bytes` | oceanfs-core::types | 248 | **HIGH** |
| `BucketId::as_str` (duplicate) | oceanfs-core::types | 228 | See M1 |
| `MerkleTree::build_from_hashes` | oceanfs-storage | 210 | **MEDIUM** |
| `SegmentSizeConfig::default` | oceanfs-core::types | 206 | **MEDIUM** |
| `SegmentSizeConfig::classify` | oceanfs-core::types | 196 | **MEDIUM** |
| `MerkleTree::root` (×2) | oceanfs-storage | 190+188 | **MEDIUM** |
| `Hlc::zero` | oceanfs-core::hlc | 187 | **MEDIUM** |
| `SegmentId::new` | oceanfs-core::types | 187 | **HIGH** |
| `MerkleTree::hash` | oceanfs-storage | 176 | **MEDIUM** |
| `SegmentSizeConfig::default` (duplicate) | oceanfs-core::types | 180 | See M1 |
| `Ring::new` | oceanfs-routing | 156 | **MEDIUM** |

**Observation:** 12 of the top 20 hotspots are in `oceanfs-core::types`. This confirms C1
(the god-file problem). Every change to a shared type's constructor or signature
ripples through the entire workspace. Splitting `types.rs` won't reduce in-degree
but will make the file navigable and changes auditable.

---

## Dependency Graph

### DAG Compliance

The architecture §1.1 specifies 12 crates in a DAG. The actual dependency
graph from Cargo.toml analysis:

```
oceanfs-core  (leaf — zero internal deps ✓)
    ↓
oceanfs-hash → (only 1 file, no deps — effectively unused)
oceanfs-ec → core ✓
oceanfs-routing → core ✓
oceanfs-membership → core ✓
oceanfs-network → core ✓
oceanfs-cache → core ✓
oceanfs-accel → core ✓
oceanfs-storage → core ✓
    ↓
oceanfs-server → core, routing, membership, network, (+ optional: storage, ec, cache, accel)
    ↓
oceanfs-node → server, storage, cache, accel, membership, routing
    ↓
oceanfs (binary) → node
```

### Violations / Concerns

1. **`oceanfs-server` → `oceanfs-storage` (optional):** Architecture §4.1 says
   "oceanfs-server never imports oceanfs-storage." The optional feature (`default = ["storage"]`)
   means this dep is active by default. The architecture DAG itself (§1.1) *does*
   show `server` depending on `storage`, so the guideline and DAG diagram
   contradict each other. **Resolution needed:** Either update §4.1 to acknowledge
   the optional dependency with justification, or remove it.

2. **`oceanfs-hash` is unused:** The DAG promises `oceanfs-hash` → `core` but
   the crate has zero implementation. No other crate depends on it. This is dead
   weight in the workspace.

3. **Architecture DAG shows `hash → accel` and `accel → cache` dependencies**
   that don't exist in Cargo.toml. The DAG diagram in §1.1 also shows
   `network` as a shared dependency of `server`, but the actual `oceanfs-server`
   Cargo.toml has a non-optional dep on `oceanfs-network` — consistent.

4. **No circular dependencies detected.** The DAG constraint is technically
   satisfied (no bidirectional edges).

---

## Guideline Violations

### Architecture Guidelines

| Guideline | Location | Violation |
|---|---|---|
| §3.3 — Each public type gets its own file | `oceanfs-core/src/types.rs` (45+ types) | All shared types in single file (see C1) |
| §3.3 — Each public type gets its own file | `oceanfs-storage/src/anti_entropy.rs` (6 types) | Multiple types in single file (see C3) |
| §3.3 — Each public type gets its own file | `oceanfs-storage/src/gc.rs` (7 types) | Multiple types in single file (see C3) |
| §4.1 — `oceanfs-server` never imports `oceanfs-storage` | `oceanfs-server/Cargo.toml:13` | Optional dependency breaks the invariant (see H1) |
| §2.1 — Traits in consuming crate | `oceanfs-core/src/types.rs` — `MetadataStore` trait | Trait in `core` should be in `server` (see M4) |
| §2.4 — Protobuf services in owner crates | `oceanfs-network/src/generated/` | Service stubs for cache, healing, scrub, storage live in network crate (see H8) |

### Coding Guidelines

| Guideline | Location | Violation |
|---|---|---|
| §5.1 — All `pub` items have doc comments | Multiple files | Code-graph shows many `pub` symbols with empty `symbol_doc: ""`. Run `cargo doc` to enumerate. |
| §5.2 — Module-level documentation | `oceanfs-hash/src/lib.rs` | Has a `//!` doc but no implementation. |
| §1.5 — `#[non_exhaustive]` on public enums | To be audited | Spot-check: `ReadOutcome`, `SizeTier` need verification. |

### Performance Guidelines

| Guideline | Location | Concern |
|---|---|---|
| §1.1 — Use `bytes::Bytes` for blob data | `oceanfs-server/src/s3_handler.rs` — `GetObjectResponse::data: Vec<u8>` | Response type uses `Vec<u8>` instead of `Bytes`. Not a hot path (S3 handler is an entry point) but the `ReadResult::data` field already uses `Bytes` — inconsistent. |
| §2.3 — `parking_lot` everywhere | Confirmed via code-graph | `parking_lot::RwLock` used in mock types. Good compliance. |
| §10.1-10.3 — LTO, single codegen unit, panic abort | `Cargo.toml` | All present in `[profile.release]`. ✓ |

---

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| 0001 — Segment packing | **Implemented** | `SizeTier` enum, `SegmentSizeConfig` with `classify()`, tiered routing in `oceanfs-storage/src/segment/tier.rs`. Code matches the ADR's decision for tiered segment sizing. |
| 0006 — Hardware acceleration tier model | **Implemented** | `AccelDispatcher` with tiered backend selection, feature-gated `CudaBackend`, `IsalEncoder`, `ArmEncoder`. Code matches ADR's three-tier model and probing flow. |
| 0007 — Compression tier governance | **Implemented** | `CompressionTier` config, `igzip.rs`, `nvcomp.rs` backends, `compressor.rs`. |
| 0005 — Trait-in-consuming-crate | **Partial** | `MetadataStore` trait is in `core` (violation M4). The pattern is followed elsewhere (gRPC services in `oceanfs-server`). |

---

## Test Coverage

| Crate | Public Symbols (approx) | Tests (approx) | Assessment |
|---|---|---|---|
| `oceanfs-core` | ~50 | 15+ test functions | Adequate for current size. Type-level tests only. |
| `oceanfs-hash` | 0 | 0 | **No tests** — crate is empty |
| `oceanfs-ec` | ~15 | 15+ tests + integration test | Good coverage with roundtrip tests. |
| `oceanfs-accel` | ~20 | 8 integration test files | Good coverage across backends. |
| `oceanfs-storage` | ~45 | 50+ unit tests + 9 integration test files | Strong coverage. The large files have comprehensive tests. |
| `oceanfs-routing` | ~10 | 10+ tests + 2 integration tests | Good coverage. |
| `oceanfs-membership` | ~12 | 10+ tests + 1 integration test | Adequate. |
| `oceanfs-network` | ~5 | 3+ tests + 1 integration test | Light but network layer is thin. |
| `oceanfs-cache` | ~10 | 4× test modules + 1 integration test | Good coverage matching layer model. |
| `oceanfs-server` | ~30 | 30+ tests + 5 integration test files | Strong coverage across handlers and coordinators. |
| `oceanfs-node` | ~10 | 10+ tests + 11 integration test files | Excellent cross-crate integration testing. |
| `oceanfs` (binary) | 2 | 0 | Binary — unit tests not applicable. E2E tests in dedicated `e2e/` crate. |

**Assessment:** The test culture is strong. Integration tests at `oceanfs-node/tests/`
are particularly valuable (e2e single node, cache behavior, GC compaction, scrub
cycle, read repair, orphan reaper, anti-entropy, write roundtrip). The main gap
is the empty `oceanfs-hash` crate (no code → no tests).

---

## Recommendations

### Immediate (this sprint)

1. **Split `oceanfs-core/src/types.rs`** (C1). Minimum viable split:
   - `types/id.rs` — SegmentId, NodeId, BucketId, ObjectKey
   - `types/hash.rs` — HashOutput, HashKey
   - `types/metadata.rs` — ObjectMetadata, SegmentMetadata, ChunkRef, SegmentIndexEntry, Tombstone, StorageLocation
   - `types/config.rs` — RpcConfig, PoolConfig, GpuConfig, GossipConfig, NvcompConfig, CompressConfig, CompressionTier, HealConfig
   - `types/codec.rs` — CodecType, CodecConfig, EncodingPlan
   - `types/heal.rs` — HealRequest, HealStats, ShardIndex
   - `types/node.rs` — NodeState, VnodeRange, Incarnation, IntendedFor, PeerAddress, WriteQuorum, WriteResult, WriteAck
   - `types/cache.rs` — CacheInvalidateRequest
   - `types/mod.rs` — re-exports all of the above
   
   Keep `types.rs` as `types/mod.rs` (the re-export facade per §3.1).

2. **Implement or remove `oceanfs-hash`** (C2). Decision needed:
   - **Option A:** Implement `Blake3Hasher`, `BatchHasher`, move `HashOutput` from `core`
   - **Option B:** Delete the crate and keep hashing in `core`. Update architecture docs.

3. **Split `oceanfs-storage/src/anti_entropy.rs`** (C3). Move each public type to its own file:
   - `anti_entropy/merkle_tree.rs`
   - `anti_entropy/merkle_root.rs`
   - `anti_entropy/merkle_proof.rs`
   - `anti_entropy/config.rs`
   - `anti_entropy/engine.rs` (the `AntiEntropy` struct + `run_cycle`, `start_background`)
   - `anti_entropy/mod.rs` — re-exports

4. **Split `oceanfs-storage/src/gc.rs`** (C3). Same pattern:
   - `gc/config.rs`
   - `gc/stats.rs`
   - `gc/liveness_tracker.rs`
   - `gc/segment_compactor.rs`
   - `gc/garbage_collector.rs`
   - `gc/orphan_reaper.rs`
   - `gc/mod.rs` — re-exports

### Short-term (next 2 sprints)

5. **Resolve `oceanfs-server` → `oceanfs-storage` optional dependency** (H1).
   Update either `architecture.md` §4.1 or `Cargo.toml` to make them consistent.

6. **Split `oceanfs-server/src/s3_handler.rs`** (H4) into handler + response types + MimeMap.

7. **Move `read_coordinator.rs` and `write_coordinator.rs` into their `read/` and `write/` submodules** (H6, M2).

8. **Move `MetadataStore` trait from `core` to `server`** (M4).

9. **Move generated protobuf service stubs to their owning crates** (H8).

### Medium-term (next month)

10. **Evaluate splitting `oceanfs-storage`** into core storage + durability (H2).

11. **Evaluate splitting `oceanfs-server`** into S3 API + coordination (H3).

12. **Split `oceanfs-node/src/node.rs`** (H5).

13. **Split `oceanfs-core/src/config.rs`** (M8) into per-subsystem config files.

### Long-term (backlog)

14. **Evaluate sub-crates for `oceanfs-accel` backends** (H7).

15. **Audit all `pub` items for missing doc comments** (L2).

16. **Resolve code-graph symbol duplication** (M1, M11).

---

*Report generated by Auditor Agent. Evidence sourced from code-graph queries, source file analysis, and guideline cross-referencing. 162 source files, 12 crates, ~48,000 lines analyzed.*
