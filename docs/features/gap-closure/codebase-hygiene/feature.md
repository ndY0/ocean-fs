---
feature: "Codebase Hygiene — Dead Code, Consolidation, Cleanup"
epic: "codebase-hygiene"
status: proposed
priority: medium
owner: ""
dependencies: []
adr:
  - 0001-segment-packing
  - 0005-trait-in-consuming-crate
  - 0006-hardware-acceleration-tier-model
  - 0007-compression-tier-governance
  - 0008-hash-crate-implementation
  - 0009-storage-crate-split
  - 0010-server-crate-split-rejected
perf:
  - "1.1 bytes BytesMut for blob data"
  - "2.2 dashmap for concurrent caches"
created: 2026-08-05
updated: 2026-08-05
---

# Codebase Hygiene — Dead Code, Consolidation, Cleanup

## Summary

Six audits identified 28 low-severity and 33 medium-severity findings spanning
dead code, crate-level `#[allow(dead_code)]` annotations, unused dependencies,
duplicate config types, missing trait annotations, incomplete acceleration
features, stale comments, and structural cleanup opportunities. None of these
are correctness-blocking, but together they create maintenance burden, mislead
developers, and increase binary size. This feature removes all dead code,
consolidates duplicates, adds missing annotations, and completes the structural
cleanup items deferred from earlier phases. Fixes span all 14 crates.

## Scope

### In Scope

**Dead Code Removal:**
- Remove `oceanfs-ec/src/isal.rs` — the bare `IsalEncoder` struct stub with no trait impls (C1-accel). The real ISA-L backend is in `oceanfs-accel`.
- Remove crate-level `#![allow(dead_code)]` from `oceanfs-membership/src/lib.rs:18` (L1-distributed)
- Remove crate-level `#![allow(dead_code)]` from `oceanfs-network/src/lib.rs:26` (L2-integration)
- Replace with targeted `#[allow(dead_code)]` on specific items with justification comments
- Remove unused `bytes` dependency from `oceanfs-hash/Cargo.toml` (L4-accel)
- Remove `RpcClient` marker trait from `oceanfs-network/src/client.rs:9` — zero implementors, no consumers (L3-distributed)
- Remove `DEFAULT_READ_TIMEOUT_MS` dead code from `oceanfs-server/src/read_coordinator.rs:36-39` (L4-server)
- Remove `encode_deletion_key` `#[allow(dead_code)]` from `oceanfs-storage/src/metadata/cf.rs:45-48` (M1-storage)
- Remove async wrapper methods (`*_async`) from `oceanfs-storage/src/metadata/store.rs:406-469` (L3-storage)

**Consolidation:**
- Consolidate `MetadataStore` trait (API crate, 2 methods) and `MetadataOps` trait (server crate, 15+ methods) into a single canonical trait in `oceanfs-storage-api` (H6-storage, L4-storage)
- Consolidate duplicate `GossipConfig` — already in Epic 1, verify completion here
- Consolidate GF(2^8) log/exp tables into a single shared module (M3-accel). Move tables to `oceanfs-core` and reference from `oceanfs-ec`, `oceanfs-accel/src/arm_sve.rs`, `oceanfs-accel/src/cuda/mod.rs`.
- Remove duplicate `invalidate_cache_on_replicas()` call at `handlers.rs:417-418` (M3-server)

**Missing Annotations / Features:**
- Add `#[non_exhaustive]` to `ArmSveLevel` enum in `oceanfs-accel/src/arm_sve.rs:56` (L3-accel)
- Gate `mod tls` behind a `tls` feature flag in `oceanfs-network` (L2-distributed)
- Add prominent doc comment explaining inverted Bloom filter semantics at `NegativeCache::contains()` (L5-server)
- Document `MerkleExchangeProtocol` `#[allow(dead_code)]` reason in code (M2-storage)
- Document `throttle_bytes_sec` and `partition_segments` `#[allow(dead_code)]` with ADR tracking (M3-storage)

**Acceleration Completeness:**
- Implement GPU cooldown/recovery: 60-second timer + automatic re-probe after `mark_unavailable()` (H1-accel)
- Implement `NvcompBufferPool` for pinned/zero-copy host memory for DMA transfers (M1-accel)
- Add nvCOMP Snappy and zstd FFI bindings, or document LZ4-only as initial release scope (M2-accel)
- Fix nvCOMP `num_chunks` hardcoded to 1 — either remove misleading `batch_size` field or implement true batch compression (L1-accel)
- Fix CUDA probing: call `CudaDevice::new(0).is_ok()` in `probe_cuda` instead of returning `true` unconditionally (L2-accel)

**Structural & Cleanup:**
- Execute `split-node-rs` refactoring: extract `node.rs` (1015 lines) into `node.rs` (struct + start), `background_tasks.rs`, `config.rs` (validate_config) (M6-integration)
- Replace manual CLI parsing in `oceanfs/src/main.rs:96-153` with `clap` derive-based parser (M4-integration)
- Add CORS middleware: `tower_http::cors::CorsLayer` to S3 handler router (L1-server)
- Log warning when prefetch runtime handle is unavailable (L2-server)
- Un-hardcode `/admin/segments` encoding state — track from active segment pool state (M1-server)
- Add `StubDecoder` test improvement: add integration tests with real `CauchyEncoder::decode()` for failure modes (M4-storage)
- Verify `#![deny(missing_docs)]` passes in all crates; fix any violations
- Create `docs/metrics.md` cataloging every exposed metric (M4-metrics-audit)
- Remove stale e2e comment at `cluster_write_path.rs:193` (M6-distributed) — already in Epic 5, verify

**Test Improvements:**
- Add `AccelDispatcher` warning log path test: verify `WARN` is logged when GPU tier requested but unavailable
- Add `NoCacheServerError` warning log test: verify `WARN` emitted when cache server error occurs
- Add `oceanfs/src/config.rs` tests: `load_config`, `merge_config`, `parse_args`, `init_tracing`, `wait_for_shutdown` (integration audit coverage gap)

### Out of Scope

- EC re-encode during segment compaction (DEV-002, tracked separately)
- Full nvCOMP batched compression implementation (future epic)
- GPU stress tests for concurrent ops
- `RegistryHandle` trait or push-based observer pattern for metrics

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-ec` | Remove `src/isal.rs`. Remove `pub use isal::IsalEncoder` from `lib.rs`. |
| `oceanfs-accel` | GPU cooldown timer + re-probe. `NvcompBufferPool` implementation. nvCOMP Snappy/zstd FFI (or scope docs). `ArmSveLevel` non_exhaustive. `batch_size` fix. CUDA probe fix. |
| `oceanfs-hash` | Remove unused `bytes` dependency from `Cargo.toml`. |
| `oceanfs-core` | Add shared GF(2^8) table module. Consolidate `GossipConfig`. |
| `oceanfs-storage` | Remove `encode_deletion_key`. Remove async wrappers. |
| `oceanfs-storage-api` | Consolidate `MetadataStore` trait with all CRUD methods from `MetadataOps`. |
| `oceanfs-server` | Remove duplicate cache invalidation. Remove `DEFAULT_READ_TIMEOUT_MS`. Track encoding state. Add CORS. |
| `oceanfs-membership` | Remove crate-level `#![allow(dead_code)]`. Targeted allows. |
| `oceanfs-network` | Remove crate-level `#![allow(dead_code)]`. Remove `RpcClient`. Gate `tls` module. |
| `oceanfs-node` | `split-node-rs` refactoring. |
| `oceanfs` (binary) | `clap` migration. |
| `oceanfs-durability` | Documentation annotations on `#[allow(dead_code)]` items. `StubDecoder` test improvement. |
| `oceanfs-cache` | Inverted Bloom filter semantics doc comment. |
| `docs/` | New `docs/metrics.md` catalog. |

## Interface (Public API)

- `oceanfs_ec::IsalEncoder` — **REMOVED**. Consumers should use `oceanfs_accel::IsalEncoder`.
- `oceanfs_network::RpcClient` — **REMOVED**. Marker trait with zero implementors.
- `oceanfs_storage_api::MetadataStore` — expanded trait with all CRUD methods: `put_object`, `get_object`, `delete_object`, `list_objects`, `put_segment`, `get_segment`, `delete_segment`, `list_segments`, `put_tombstone`, `get_tombstone`
- `oceanfs_server::MetadataOps` — **REMOVED** (consolidated into `MetadataStore`)
- `oceanfs_accel::ArmSveLevel` — now `#[non_exhaustive]`
- `oceanfs_accel::NvcompBufferPool` — new public type for pinned DMA memory pool

## Detailed Task List

### Dead Code Removal
- [ ] **C1-accel:** Delete `oceanfs-ec/src/isal.rs`. Remove `pub use isal::IsalEncoder` from `oceanfs-ec/src/lib.rs:39`. Update any imports (there should be none in production — the real backend is in `oceanfs-accel`).
- [ ] **L1-distributed:** Remove `#![allow(dead_code)]` from `oceanfs-membership/src/lib.rs:18`. Audit all dead-code warnings that surface. Add targeted `#[allow(dead_code)]` with justification comments on items intentionally not yet used.
- [ ] **L2-integration:** Remove `#![allow(dead_code)]` from `oceanfs-network/src/lib.rs:26`. Same treatment.
- [ ] **L4-accel:** Remove `bytes` from `oceanfs-hash/Cargo.toml` dependencies.
- [ ] **L3-distributed:** Remove `RpcClient` marker trait from `oceanfs-network/src/client.rs`.
- [ ] **L4-server:** Remove `DEFAULT_READ_TIMEOUT_MS` from `read_coordinator.rs:36-39`.
- [ ] **M1-storage:** Remove `encode_deletion_key` function from `cf.rs:45-48`.
- [ ] **L3-storage:** Remove `*_async` wrapper methods from `store.rs:406-469`. (Sync methods used directly by `MetadataStoreAdapter`.)

### Consolidation
- [ ] **H6-storage / L4-storage:** Consolidate `MetadataStore` trait. Move the expanded trait definition into `oceanfs-storage-api/src/lib.rs`. Implement on `RocksDbMetadataStore` in `oceanfs-storage`. Remove `MetadataOps` trait from `oceanfs-server`. Update all references.
- [ ] **M3-accel:** Move GF(2^8) log/exp tables into `oceanfs-core` (e.g., `oceanfs-core/src/gf_tables.rs`). `oceanfs-ec/src/gf.rs` references via `use oceanfs_core::gf_tables`. Same for `arm_sve.rs` and `cuda/mod.rs`. Tables: `GF_LOG: [u8; 256]`, `GF_EXP: [u8; 512]`.
- [ ] **M3-server:** Remove duplicate `invalidate_cache_on_replicas()` call at `handlers.rs:417-418`. Keep one call.

### Missing Annotations
- [ ] **L3-accel:** Add `#[non_exhaustive]` to `pub enum ArmSveLevel` in `arm_sve.rs:56`.
- [ ] **L2-distributed:** Add `#[cfg(feature = "tls")]` gate on `mod tls` in `oceanfs-network/src/lib.rs`. Add `tls` feature to `Cargo.toml`. Default disabled.
- [ ] **L5-server:** Add doc comment to `NegativeCache::contains()`: "Returns `true` when the key is *definitely absent* (inverted from standard Bloom filter semantics). This is intentional: `true` = skip RocksDB and return 404."
- [ ] **M2-storage:** Add code comment on `MerkleExchangeProtocol`: "// Test-only wire-format helper; not on production gRPC path (DEV-003)."
- [ ] **M3-storage:** Add code comment on `throttle_bytes_sec`: "// Reserved for future I/O throttling (tracked: ADR-future-throttle)." On `partition_segments`: "// Test-only multi-node scrub not yet implemented (tracked: H5-storage)."

### Acceleration Completeness
- [ ] **H1-accel:** Implement GPU cooldown. In `mark_unavailable()`: set `AtomicBool` + store `Instant::now()`. In `try_recover_ec_backend()`: if cooldown elapsed, probe with a tiny dummy encode. If probe succeeds, clear unavailable flag. If probe fails, reset cooldown timer.
- [ ] **M1-accel:** Implement `NvcompBufferPool` with `cudaHostAlloc` pinned memory. Pre-allocate N buffers. Provide `acquire()`/`release()`. Use in nvCOMP compress/decompress paths instead of per-call allocation.
- [ ] **M2-accel:** Either add FFI bindings for `nvcompBatchedSnappy*` and `nvcompBatchedZstd*`, or document LZ4-only as initial release scope. If adding: follow same pattern as LZ4 bindings. If not: mark Snappy/Zstd `NvcompCodec` variants with `#[doc(hidden)]` or `#[deprecated]`.
- [ ] **L1-accel:** Fix `num_chunks` hardcoded to 1. Either remove the `batch_size` config field (if single-buffer is intentional), or implement batch accumulation in `NvcompCompressor::compress()`.
- [ ] **L2-accel:** In `probe_cuda()`, call `CudaDevice::new(0).is_ok()` instead of returning `true` unconditionally.

### Structural Cleanup
- [ ] **M6-integration:** Execute `split-node-rs` refactoring. Split `node.rs` (1015 lines) into:
  - `node.rs` — `Node` struct, `new()`, `start()`, `shutdown()`, public getters
  - `background_tasks.rs` — `BackgroundTasks` struct, task spawning, cancellation
  - `config.rs` — `validate_config()`
  - Update `lib.rs` facade accordingly.
- [ ] **M4-integration:** Replace manual CLI parsing in `oceanfs/src/main.rs:96-153` with `clap` derive. Add `--config`, `--node-id`, `--listen-addr`, `--grpc-listen-addr`, `--data-dir`, `--seed-nodes`, `--log-level`. Generate `--help` and `--version` automatically.
- [ ] **L1-server:** Add `tower_http::cors::CorsLayer::permissive()` (or configured) to the axum router in S3 handler.
- [ ] **L2-server:** In `prefetch.rs`, log a `WARN` when runtime handle is unavailable: "prefetch disabled: no tokio runtime handle available."
- [ ] **M1-server:** Track encoding state in segment metadata or compute from active segment pool. Update `/admin/segments` response to report real encoding counts, not always 0.
- [ ] **M4-storage:** Add integration tests for heal decode failure paths using real `CauchyEncoder::decode()` (not `StubDecoder` which fills with zeros).
- [ ] **M4-metrics-audit:** Create `docs/metrics.md` cataloging every exposed metric: name, type, labels, description.

### Test Improvements
- [ ] **Test: IsalEncoder removal** — Verify no compilation errors in any crate that imported `oceanfs_ec::IsalEncoder`. Update any test imports to use `oceanfs_accel::IsalEncoder`.
- [ ] **Test: dead_code removal** — After removing crate-level `#![allow(dead_code)]`, verify `cargo build` produces zero dead-code warnings (or that all remaining warnings have targeted `#[allow]` with justification).
- [ ] **Test: GF table consolidation** — Verify `cargo test -p oceanfs-ec -- gf` and `cargo test -p oceanfs-accel` still pass after table refactoring.
- [ ] **Test: MetadataStore consolidation** — All existing `MetadataOps` tests pass with new `MetadataStore` trait.
- [ ] **Test: clap migration** — `oceanfs --help` produces formatted help. `oceanfs --version` prints version.
- [ ] **Test: CORS** — Preflight `OPTIONS` request returns CORS headers.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in all 14 crates; zero dead-code warnings from `oceanfs-membership` and `oceanfs-network`
- [ ] **Tests:** All existing tests pass. New tests for GPU cooldown, `MetadataStore` consolidation, GF table refactoring, nvCOMP batch_size fix.
- [ ] **Docs:** Every new/expanded `pub` item has doc comments. `#![deny(missing_docs)]` passes in all crates. `docs/metrics.md` created.
- [ ] **ADR:** All ADR constraints satisfied. ADR-0005 (trait-in-consuming-crate) applied to `MetadataStore` in `oceanfs-storage-api`.
- [ ] **Perf:** GF table sharing does not introduce runtime overhead (compile-time constants). Perf §1.1, §2.2 satisfied.
- [ ] **Integration:** `cargo test --workspace` passes. `oceanfs --help` works. `cargo clippy --lib -- -D warnings` passes on production code.
- [ ] **Verification:** Grep for `#![allow(dead_code)]` in `oceanfs-membership/src/lib.rs` and `oceanfs-network/src/lib.rs` returns zero results.
- [ ] **Verification:** Grep for `pub use isal::IsalEncoder` in `oceanfs-ec/src/lib.rs` returns zero results.
- [ ] **Verification:** `oceanfs-hash/Cargo.toml` no longer lists `bytes` dependency.
