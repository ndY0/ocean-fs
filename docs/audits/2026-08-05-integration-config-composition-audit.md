---
audit_date: 2026-08-05
scope: targeted
target_crates: oceanfs-node, oceanfs (binary), oceanfs-core, oceanfs-storage-api, protobuf stubs, config system, workspace-level
severity_counts:
  critical: 1
  high: 5
  medium: 7
  low: 6
---

# Audit Report: Integration Layer, Config System & Composition Root

## Summary

The integration layer is substantially complete: `Node::start()` wires 20+ subsystem components, `oceanfs/src/main.rs` parses CLI, loads config, handles signals, and orchestrates graceful shutdown. The crate dependency DAG is clean — no circular dependencies, and `oceanfs-core` passes the purity check (only depends on `oceanfs-hash`). All 8 protobuf files compile successfully, and gRPC service stubs are generated in their owning crates per architecture §2.4.

However, **one critical bug** makes the configuration system partially non-functional: `merge_config()` in `oceanfs/src/config.rs` only copies a subset of fields from TOML files, silently discarding all maintenance interval values, cluster bootstrap settings, auth/prefetch toggles, and the body size limit. Five high-severity wiring gaps remain — the gossip and failure-detector background tasks are dormant placeholders, `BufferPool` and `SegmentSealer` are constructed but never wired to consumers, and `MetricsRegistry` is created empty with no subsystem feeding stats into it.

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `crates/oceanfs/src/config.rs:100-119` (`merge_config`) | **Config TOML fields silently dropped.** `merge_config()` only copies 6 fields (node_id, data_dir, listen_addr, grpc_listen_addr, seed_nodes, log_level) from the TOML file into the target config. All maintenance intervals (`gc_interval_sec`, `tombstone_ttl_sec`, `ae_interval_sec`, `scrub_interval_sec`, `orphan_reaper_interval_sec`), cluster bootstrap timings (`gossip_interval_ms`, `suspicion_timeout_ms`, `failure_timeout_ms`), feature toggles (`s3_auth_enabled`, `prefetch_enabled`, `metrics_enabled`), and `max_body_size` are parsed by serde from the TOML but **never applied** to the final config. This means the e2e smoke test shortened-interval configs never work, and the deviations D2/D4 in `docs/features/broad-smoke-tests/feature.md` are caused by this bug — not by missing config fields. | Replace `merge_config` with `*target = source.clone()` and re-apply CLI overrides. Or, add missing field merges for all NodeConfig fields. Also add env var support for key intervals. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `crates/oceanfs-node/src/node.rs:599-600` | **Gossip background task is a dormant `std::future::pending`.** The `gossip` join handle spawns `std::future::pending::<()>().await` — it will never complete and never runs the gossip protocol loop. The comment says "Gossip: placeholder (driven by Membership internally)" but this conflicts with the fact that `Membership::start()` was fixed in the cluster-bootstrap PR to spawn `GossipProtocol`. The node's background gossip task should either be removed (since Membership owns its own gossip) or wired to Membership's gossip handle. | Remove the `gossip` background task entirely or store the `GossipProtocol` handle from `Membership` after `start()`. The `gossip_cancel` token should cancel Membership's internal gossip. |
| H2 | `crates/oceanfs-node/src/node.rs:721-736` | **Failure detector background task is a 1-second sleep heartbeat.** The `failure_detector` spawn runs an endless loop of `tokio::time::sleep(Duration::from_secs(1))` labeled "Heartbeat placeholder." Since `Membership::start()` spawns the real `FailureDetector` internally (cluster-bootstrap PR fix), this background task is dead weight that cancels the token on shutdown but does nothing else. | Remove the `failure_detector` background task or wire it to expose the actual `FailureDetector` join handle from `Membership`. |
| H3 | `crates/oceanfs-node/src/node.rs:201,210` | **BufferPool and SegmentSealer constructed but never wired.** `_buffer_pool` and `_sealer` are created with underscore prefixes because they are unused. They are not passed to the write coordinator, active segment writers, or any consumer. The comment on line 199-200 acknowledges this: "BufferPool constructed here; will be wired to active segment writers when final-integration-read-write-end-to-end lands." This means the segment write path cannot use pooled buffers or the sealer — every write goes through ad-hoc allocation. | Wire BufferPool into the write coordinator or segment handle constructor. Wire SegmentSealer into the active segment lifecycle. This is blocking `final-integration-read-write-end-to-end`. |
| H4 | `crates/oceanfs-node/src/node.rs:370` | **MetricsRegistry constructed empty — no subsystem feeds metrics.** The `MetricsRegistry` is created at line 370 and passed to `AdminHandler::new_with_cluster`, but no subsystem calls `registry.register_counter()`, `registry.register_gauge()`, or pushes any stats. Subsystems with stats to report (GC stats, cache hit/miss counts, HealStats, AccelMetrics, segment counts) are not wired to the registry. The `/metrics` endpoint returns empty data. | Wire GC stats, cache stats, heal stats, and accel stats into the `MetricsRegistry`. Pass `Arc<MetricsRegistry>` (or a metrics handle) to `GarbageCollector`, `HealWorker`, `AccelDispatcher`, and cache instances. |
| H5 | `crates/oceanfs-node/src/node.rs:703-719` | **Prefetch background task is a 60-second sleep keep-alive.** The `prefetch` join handle spawns a loop that sleeps for 60 seconds on each iteration — it does not trigger any prefetch warming cycles. The `PrefetchEngine` is moved into the closure to keep it alive, but its internal warming worker is never triggered. | Either wire periodic prefetch warming cycles (e.g., drain the engine's work queue every N seconds) or ensure `PrefetchEngine` runs its own background worker internally so this task is unnecessary. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `crates/oceanfs-core/src/types/config.rs:60-70` | **`SegmentSizeConfig` not serde-deserializable.** `SegmentSizeConfig` derives only `Debug, Clone` — not `serde::Deserialize`. It always uses hardcoded defaults (inline=4KB, small=256KB, standard=4MB). The thresholds cannot be tuned from `oceanfs.toml`. | Add `serde::Deserialize` derive to `SegmentSizeConfig` and reference it from `NodeConfig` so it can be configured in TOML. |
| M2 | `crates/oceanfs-core/src/config/node.rs` | **No `read_quorum`/`write_quorum`/`total_replicas` in `NodeConfig`.** Per spec §14.1, these replication parameters should be configurable per node and per bucket. They are not present in any config struct, and searches for `BucketPolicy` in `oceanfs-core` return zero results. | Add replication policy fields to `NodeConfig` (as defaults) and create a per-bucket `BucketPolicy` override struct in `oceanfs-server/src/bucket_config.rs` or `oceanfs-core`. |
| M3 | `crates/oceanfs/src/config.rs:100-119` (`merge_config`) | **No env var support for intervals or feature toggles.** Only `OCEANFS_LISTEN_ADDR`, `OCEANFS_GRPC_LISTEN_ADDR`, `OCEANFS_DATA_DIR`, `OCEANFS_SEED_NODES`, and `OCEANFS_LOG_LEVEL` are read from environment. No way to override intervals or toggles via environment variables. | Add env var support: `OCEANFS_GC_INTERVAL`, `OCEANFS_AE_INTERVAL`, `OCEANFS_GOSSIP_INTERVAL_MS`, `OCEANFS_MAX_BODY_SIZE`, etc. |
| M4 | `crates/oceanfs/src/main.rs:96-153` (`parse_args`) | **Manual CLI parsing — no `clap`.** CLI arguments are parsed with a hand-written while-loop over `std::env::args()`. No `--help` flag, no validation of flag values, no `--version` flag. Unknown flags are silently skipped (line 146: `// Unknown flag; skip`). | Replace with `clap` derive-based parser for robustness, `--help`, and argument validation. Low priority but high UX impact. |
| M5 | `crates/oceanfs/src/config.rs:100-119` (`merge_config`) | **`merge_config` uses hardcoded default sentinel values.** The function checks `if source.listen_addr != "0.0.0.0:9000"` to detect "user changed this." This breaks if a user genuinely wants to use the default address — the field won't be merged. It also couples the merge logic to specific default string values that may change. | Replace with `Option<T>` fields in a `CliArgs` overlay or use a `ConfigBuilder` pattern that explicitly tracks which fields were set. |
| M6 | `crates/oceanfs-node/src/node.rs` (entire file) | **`node.rs` is 1,015 lines — approaching mega-file territory.** Per the structural roadmap (Epic 5, feature `split-node-rs`), this should be split into `node.rs` (struct + `start`), `background_tasks.rs`, and `config.rs` (`validate_config`). | Execute the planned `split-node-rs` refactoring from the structural roadmap. |
| M7 | `crates/oceanfs-core/src/types/config.rs:108-131` | **`GossipConfig` not serde-deserializable.** Like `SegmentSizeConfig`, `GossipConfig` lacks `serde::Deserialize`. The gossip interval values are manually copied from `NodeConfig` at node.rs:168-174, which works, but means `GossipConfig` cannot be independently configured in sub-config sections of `oceanfs.toml`. | Add `serde::Deserialize` to `GossipConfig` and all other config structs in `types/config.rs` (`RpcConfig`, `PoolConfig`, `GpuConfig`, `HealConfig`, `CompressConfig`, etc.) for nested TOML support. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `crates/oceanfs-node/src/node.rs:262-268` | **`ec_decoder` cloned needlessly.** The `heal_decoder` is cloned before `HealWorker::new` and the clone (`ec_decoder`) is passed to `ReadCoordinator::with_decoder`. Both `HealWorker` and `ReadCoordinator` take `Arc<dyn Decoder>`, so the clone already wraps in Arc. The intermediate `ec_decoder` binding is unnecessary. | Simplify: pass `heal_decoder.clone()` directly to `with_decoder`. |
| L2 | Multiple crates | **Crate-level `#![allow(dead_code)]` in `oceanfs-membership/src/lib.rs:18` and `oceanfs-network/src/lib.rs:26`.** These suppress dead-code warnings across the entire crate, hiding genuinely unused symbols. | Remove crate-level `allow(dead_code)` and use targeted `#[allow(dead_code)]` only on individual symbols with justification comments. |
| L3 | `crates/oceanfs-node/src/node.rs:476-480` | **gRPC server spawn has no graceful shutdown wired.** The `grpc_router.serve(grpc_addr)` is spawned but the resulting `JoinHandle` is not stored for graceful shutdown. The tonic server will be force-killed when the tokio runtime drops but has no drain sequence. | Store the gRPC `JoinHandle` (or a `CancellationToken`-aware shutdown future) and cancel it during `Node::shutdown()`. |
| L4 | `crates/oceanfs-node/src/node.rs:530-556` (`shutdown`) | **Shutdown does not cancel gRPC server or close MetadataStore.** `shutdown()` cancels all background task tokens and drains HTTP, but does not send a shutdown signal to the tonic gRPC server, close the `RocksDbMetadataStore` (flush RocksDB), or flush the `WalWriter`. | Add gRPC shutdown signal, `metadata_store.close()` call, and `wal_writer.flush()` call to the shutdown sequence. |
| L5 | `crates/oceanfs-core/src/types/config.rs` + `src/config/node.rs` | **Two `GossipConfig` structs exist.** `oceanfs_core::types::config::GossipConfig` (in `types/config.rs`) and `oceanfs_core::config::node::NodeConfig` (in `config/node.rs`) both carry gossip-related fields. `NodeConfig` has the intervals; `GossipConfig` has the indirect_ping_count and seed_nodes. `NodeConfig` also has seed_nodes. This is duplicated state. | Consolidate: either embed `GossipConfig` as a field on `NodeConfig`, or remove the duplicate fields from one location. |
| L6 | `crates/oceanfs-node/src/node.rs:392-416` | **Auth middleware key loading logs warnings for default (disabled) state.** When `s3_auth_enabled = false`, `AuthMiddleware::passthrough()` is used, which is correct. But the warning paths at lines 401-411 only execute when auth IS enabled and keys fail to load — this is fine but the auth config should be validated at startup (not at first request). | Validate auth config during `validate_config()` rather than deferring to request time. |

## Coupling Hotspots

| Symbol | Crate | In-Degree | Risk |
|---|---|---|---|
| `NodeId::new` | oceanfs-core | 436 | **High.** Used everywhere — acceptable for a core identifier. |
| `SegmentSizeConfig::default` | oceanfs-core | 132 | **Medium.** Default used in many places; should support deserialization to reduce hardcoded use. |
| `MetadataStore::open` | oceanfs-storage | 111 | **Medium.** Central storage open — expected. |
| `HashOutput::from_bytes` | oceanfs-hash | 68 | **Low.** Core hash type, expected. |
| `Hlc::zero` | oceanfs-core | 63 | **Low.** Clock initialization, expected. |

## Dependency Graph

The DAG constraint from architecture.md §1.1 holds. Verified via manual Cargo.toml inspection:

```
oceanfs-hash
    ↓
oceanfs-core (only depends on oceanfs-hash) ✅ PURE
    ↓
oceanfs-storage-api → core ✅
oceanfs-ec → core ✅
oceanfs-accel → core, ec ✅
oceanfs-storage → core, hash, ec, accel ✅
oceanfs-routing → core ✅
oceanfs-membership → core ✅
oceanfs-network → core ✅
oceanfs-cache → core ✅
oceanfs-durability → core, storage-api, storage, ec, hash, membership, network ✅
    ↓
oceanfs-server → core, storage, routing, membership, network, cache ✅
    ↓
oceanfs-node → core, server, storage-api, storage, durability, routing, membership, network, cache, accel, ec ✅
    ↓
oceanfs (binary) → core, node ✅
```

**No circular dependencies detected.** The graph is a proper DAG. `oceanfs-node` is the only crate importing across all subsystem boundaries, per architecture.md §4.1.

**Core purity check passes:** `oceanfs-core/Cargo.toml` depends only on `oceanfs-hash` among internal crates.

## Guideline Violations

| Guideline | Location | Violation |
|---|---|---|
| Coding §1.4 (struct fields always private) | `node.rs:62-101` (`BackgroundTasks`) | Fields changed to `pub(crate)` per feature doc reviewer note — accepted deviation. |
| Architecture §3.3 (one-type-per-file) | `node.rs` (1,015 lines) | Contains `BackgroundTasks`, `Node`, `PrefetchStoreAdapter`, tests all in one file. `split-node-rs` refactoring deferred. |
| Architecture §2.1 (traits in consuming crate) | `oceanfs-storage-api/src/lib.rs` | `MetadataStore` trait was moved from `oceanfs-core` to `oceanfs-storage-api` per ADR-0009. This is an explicit decision, not a violation. |
| Performance 8.5 (bounded semaphore for task concurrency) | `node.rs` | No `tokio::sync::Semaphore` used. Deferred per feature doc: "bounded semaphore will be added when workloads are finalized." |

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| ADR-0006 (acceleration probing) | ✅ Compliant | `AccelDispatcher::new()` called eagerly in `Node::start()` (line 156), stored on `Node` struct with public getter (line 509). |
| ADR-0009 (storage crate split) | ⚠️ Partial | `oceanfs-storage-api` and `oceanfs-durability` crates exist. But `SegmentStore` and `MetadataStore` traits have not been fully migrated — `MetadataStore` trait in `oceanfs-storage-api` exists but `SegmentStore` may still be in server. The `execute-storage-split` feature is still `Pending` per the structural roadmap. |
| ADR-0010 (server crate split rejected) | ✅ Compliant | Server remains a single crate. No server split was performed. The gRPC service stubs have been moved as planned in Epic 4. |
| ADR-0001 (segment packing) | ✅ Compliant | `SegmentSizeConfig::default_target_size` used for `SealConfig` (node.rs:204). Tiered sizing is config-driven. |

## Test Coverage

| Crate | Key Public Symbols | Tests | Coverage Notes |
|---|---|---|---|
| `oceanfs-node` | `Node`, `BackgroundTasks`, `MetadataStoreAdapter` | 9 unit tests in node.rs, adapter tests in metadata_adapter.rs, 2 integration test files | ~93% line coverage on node.rs, 100% on metadata_adapter.rs |
| `oceanfs` (binary) | `main()`, `parse_args()`, `load_config()` | No unit tests in `crates/oceanfs/src/` | main.rs does not have `#[cfg(test)]`; config.rs has no tests |
| `oceanfs-core` | `NodeConfig`, `SegmentSizeConfig`, `Hlc`, `Error` | Tests in config/node.rs, types/*, hlc.rs, error.rs | Good coverage on types and config defaults |

**Coverage gaps:**
- No tests for `oceanfs/src/config.rs` (`load_config`, `merge_config`, `parse_args`, `init_tracing`, `wait_for_shutdown`)
- No tests for the TOML deserialization of all `NodeConfig` fields
- The `merge_config` bug (C1) would have been caught by a simple test: load TOML with `gc_interval_sec = 10` → assert config has `gc_interval_sec = 10`

## Subsystem Wiring Status Table

| Subsystem | Constructed in `Node::start`? | Wired to consumers? | Background task spawned? | Graceful shutdown? |
|---|---|---|---|---|
| RocksDB MetadataStore | ✅ Yes (line 150) | ✅ Yes (multiple) | N/A | ❌ No `.close()` in shutdown |
| AccelDispatcher | ✅ Yes (line 156) | ✅ Via `Node::accel()` getter + AdminHandler | N/A | ❌ Not shut down |
| Ring / RingCache | ✅ Yes (lines 160-161) | ✅ Yes (Membership, coordinators) | N/A | N/A |
| Membership | ✅ Yes (line 175) | ✅ Yes (coordinators, router) | ✅ Internal (GossipProtocol + FailureDetector) | ⚠️ Token cancels but gossip bg task is dormant |
| ConnectionPool | ✅ Yes (line 184) | ✅ Yes (coordinators, membership, router) | N/A | ❌ Not drained |
| BufferPool | ✅ Yes (line 201) | ❌ `_buffer_pool` — not wired | N/A | N/A |
| SegmentSealer | ✅ Yes (line 210) | ❌ `_sealer` — not wired | N/A | N/A |
| WalWriter | ✅ Yes (line 195) | ⚠️ Only passed to SegmentSealer | N/A | ❌ Not flushed on shutdown |
| GarbageCollector | ✅ Yes (line 223) | ✅ Yes (background task) | ✅ Yes (line 607) | ✅ `gc_cancel` canceled |
| AntiEntropy | ✅ Yes (line 224) | ✅ Yes (background task) | ✅ Yes (line 628) | ✅ `ae_cancel` canceled |
| ScrubCoordinator | ✅ Yes (line 232) | ✅ Yes (background task + admin) | ✅ Yes (line 651) | ✅ `scrub_cancel` canceled |
| OrphanReaper | ✅ Yes (line 237) | ✅ Yes (background task) | ✅ Yes (line 683) | ✅ `reaper_cancel` canceled |
| HealWorker | ✅ Yes (line 262) | ✅ Yes (background task) | ✅ Yes (line 741) | ✅ `heal_cancel` canceled |
| ObjectCache | ✅ Yes (line 272) | ✅ Yes (S3Handler, AdminHandler) | N/A | ❌ Not flushed on shutdown |
| MetadataCache | ✅ Yes (line 273) | ✅ Yes (S3Handler, AdminHandler, Prefetch) | N/A | ❌ Not flushed on shutdown |
| NegativeCache | ✅ Yes (line 276) | ✅ Yes (S3Handler, AdminHandler) | N/A | ❌ Not flushed on shutdown |
| PrefetchEngine | ✅ Yes (line 287) | ✅ Yes (S3Handler) | ⚠️ Keep-alive loop only (line 706) | ✅ `prefetch_cancel` canceled |
| MetadataStoreAdapter | ✅ Yes (line 296) | ✅ Yes (coordinators, S3Handler) | N/A | N/A |
| WriteCoordinator | ✅ Yes (line 321) | ✅ Yes (S3Handler) | N/A | N/A |
| ReadCoordinator | ✅ Yes (line 329) | ✅ Yes (S3Handler) | N/A | N/A |
| HintedHandoff | ✅ Yes (line 342) | ✅ Yes (healing service) | N/A | N/A |
| Router | ✅ Yes (line 347) | ✅ Yes (S3Handler) | N/A | N/A |
| S3Handler | ✅ Yes (line 356) | ✅ Yes (axum router) | N/A | N/A |
| AdminHandler | ✅ Yes (line 371) | ✅ Yes (axum router) | N/A | N/A |
| MetricsRegistry | ✅ Yes (line 370) | ⚠️ Passed to AdminHandler but empty | N/A | N/A |
| Axum HTTP server | ✅ Yes (line 429) | ✅ Bound + spawned | N/A | ✅ `http_shutdown` token |
| gRPC server | ✅ Yes (line 478) | ✅ 5 services registered | N/A | ❌ No shutdown signal stored |
| Gossip (background) | ⚠️ Spawned (line 600) | ❌ `std::future::pending` — never runs | ⚠️ Dormant placeholder | ✅ Token canceled |
| Failure Detector (bg) | ⚠️ Spawned (line 724) | ❌ 1s sleep heartbeat — not real SWIM | ⚠️ Dormant placeholder | ✅ Token canceled |

## Recommendations

### Immediate (Blocking Correctness)

1. **Fix C1** — Replace or complete `merge_config()` so all `NodeConfig` fields from `oceanfs.toml` are applied. This is the root cause of the smoke test deviations D2/D4/D5/D8 (the config fields exist in `NodeConfig` but `merge_config` drops them). Remove the sentinel-value approach; use a typed overlay instead.

2. **Fix H1 + H2** — Remove the dormant gossip and failure-detector placeholder background tasks. `Membership::start()` already spawns the real loops. Store Membership's join handles and wire their cancellation.

3. **Fix H3** — Wire `BufferPool` and `SegmentSealer` into the write path. Deferring them to `final-integration-read-write-end-to-end` is acceptable only if a tracking issue exists.

4. **Fix H4** — Wire subsystem metrics into `MetricsRegistry`. At minimum: GC stats (segments compacted, bytes reclaimed), cache stats (L1/L2/L3 hits/misses), heal stats (shards repaired), and accel tier usage.

### Short-term (Sprint 1–2)

5. **Fix M1** — Add `serde::Deserialize` to `SegmentSizeConfig` so segment thresholds can be tuned from TOML.

6. **Fix L3 + L4** — Add graceful shutdown for gRPC server and close/fsync for `MetadataStore` and `WalWriter`.

7. **Fix M6** — Execute the `split-node-rs` refactoring to keep `node.rs` manageable.

8. **Fix L2** — Remove crate-level `#![allow(dead_code)]` from `oceanfs-membership` and `oceanfs-network`.

### Medium-term

9. **Fix M2** — Add replication policy fields to config and `BucketPolicy` per spec §14.1.

10. **Fix H5** — Wire actual prefetch warming cycles or ensure `PrefetchEngine` is self-driving.

11. **Fix M4** — Replace manual CLI parsing with `clap`.

12. **Fix M7** — Add `serde::Deserialize` to all config structs in `types/config.rs`.
