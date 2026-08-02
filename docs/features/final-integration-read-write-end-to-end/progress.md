# Progress Report: Read-Write Path End-to-End Integration

**Feature:** `docs/features/final-integration-read-write-end-to-end/feature.md`
**Epic:** `final-integration`
**Status:** In progress — 60% complete
**Last updated:** 2026-08-02

---

## In-Scope Items Status

| # | Item | Status | Notes |
|---|---|---|---|
| 1 | ReadCoordinator — real read path | ⚠️ 70% | Metadata lookup, inline data, multi-chunk assembly, streaming BLAKE3 all work. `fetch_chunks` integrated with `SegmentReader` fallback. gRPC shard fetch + EC decode blocked. |
| 2 | WriteCoordinator — real forwarding | ⚠️ 40% | Local writes work. Non-local returns `ForwardFailed` with target info. Real gRPC forwarding blocked. |
| 3 | write/replication.rs | ✅ 80% | Replication fan-out works. Uses Membership + simulated acks. Real gRPC blocked. |
| 4 | hinted_handoff.rs | ✅ 60% | Storage/retrieval of hints works. `deliver_single()` is no-op; real delivery blocked by gRPC. |
| 5 | router.rs — real forwarding | ⚠️ 70% | Ring lookup, replica routing, dead-node skipping work. `try_forward()` validates membership + aliveness. Real gRPC forwarding blocked. |
| 6 | Wire L1/L2/L3 caches | ✅ 95% | L1 check/serve/populate, L2 check/serve-inline, L3 negative→404, cache invalidation on PUT/DELETE, L3 insert on DELETE. Remaining: BLAKE3-verify L1 hits, chunk_list from L2 cache. |
| 7 | Wire prefetch engine | ✅ 90% | `PrefetchEngine` passed to `S3Handler` via `with_prefetch_engine()`. Enqueues `after_list` hints from `list_objects`, `after_get` hints from `get_object`. Wired in `node.rs`. Remaining: adjacent key discovery for GET prefetch. |
| 8 | Apply auth middleware | ✅ 80% | `AuthMiddleware::passthrough()` applied to S3 routes via `into_router_with_auth()`. Remaining: config-driven enable/disable, actual SigV4 verification call in middleware. |
| 9 | End-to-end tests | ✅ 85% | Single-node roundtrip (7 tests), e2e (4 tests), cache behavior (9 tests), auth middleware (5 tests), read repair (3 tests). Remaining: multi-node tests (need gRPC), HTTP handler cache cascade tests (partially done). |

---

## Files Modified / Created

### New Files
| File | Description |
|---|---|
| `crates/oceanfs-server/src/read/assembly.rs` | `MultiChunkAssembler` — streaming BLAKE3 multi-chunk assembly |
| `crates/oceanfs-node/tests/read_write_roundtrip.rs` | 7 roundtrip tests (1KB, 100KB, 1MB, small, empty, multi-blob, overwrite) |
| `crates/oceanfs-node/tests/cache_behavior.rs` | 9 cache behavior tests (L1/L2/L3 hit/miss/invalidate) |
| `crates/oceanfs-node/tests/e2e_single_node.rs` | 4 E2E tests (1KB, 100KB, 1MB, hash verification) |
| `crates/oceanfs-node/tests/auth_middleware.rs` | 5 auth middleware tests |
| `crates/oceanfs-node/tests/read_repair.rs` | 3 conflict resolution tests |

### Modified Files
| File | Key Changes |
|---|---|
| `crates/oceanfs-server/src/read_coordinator.rs` | Added `SegmentReader` trait, `InMemorySegmentReader`, `GetResult`, `CacheHitLevel`. Replaced `assemble_chunks` error stub with real implementation using `fetch_chunks` + `MultiChunkAssembler`. |
| `crates/oceanfs-server/src/read/fetch.rs` | Made `fetch_chunks` accept optional `SegmentReader` for local fallback. Removed `#[allow(dead_code)]` — now actively called. |
| `crates/oceanfs-server/src/read/mod.rs` | Added `assembly` module, made public-accessible. |
| `crates/oceanfs-server/src/read/repair.rs` | No changes (still stubbed; requires gRPC). |
| `crates/oceanfs-server/src/s3_handler.rs` | Full L2 cache short-circuit (serve inline), L3 negative check in GET/HEAD, L1+L2 population on GET, cache invalidation on PUT/DELETE, L3 insert on DELETE, prefetch enqueuing after LIST/GET, segment store for roundtrip. |
| `crates/oceanfs-server/src/write_coordinator.rs` | Non-local writes return `ForwardFailed` with target + reason (was generic `Routing` error). Test updated. |
| `crates/oceanfs-server/src/router.rs` | `try_forward` validates node aliveness (was membership-only check). |
| `crates/oceanfs-server/src/lib.rs` | Exported new public types: `CacheHitLevel`, `GetResult`, `SegmentReader`, `InMemorySegmentReader`, `MultiChunkAssembler`. |
| `crates/oceanfs-node/src/node.rs` | Wired `AuthMiddleware::passthrough()` via `into_router_with_auth()`. Wired `PrefetchEngine` into `S3Handler` via `with_prefetch_engine()`. |
| `crates/oceanfs-node/Cargo.toml` | Added `bytes`, `blake3`, `smallvec` dev-dependencies for integration tests. |

---

## Blockers

### gRPC-dependent (requires `final-integration-grpc-services` or Membership API extensions)

| Blocker | Detail | Affected Items |
|---|---|---|
| `Membership` lacks `addr_of()` method | Cannot resolve `NodeId` → `SocketAddr` for gRPC channel acquisition | #1 (shard fetch), #2 (forwarding), #3 (replication), #5 (try_forward) |
| `ConnectionPool::get_channel()` needs `SocketAddr` | Same root cause as above | #1, #2, #3, #5 |
| `SegmentRpcClient` gRPC client exists but not wired | Generated code in `oceanfs-network/src/generated/` exists; needs address + channel to call | #1 (parallel shard fetch) |
| `oceanfs-ec::Decoder` trait unused | EC decode logic exists in `oceanfs-ec` crate but never called from read path | #1 (EC decode) |
| Read repair push requires gRPC | `read/repair.rs` functions are no-op stubs | #1 (read repair) |

### Independent (no gRPC required)

| Gap | Detail | Priority |
|---|---|---|
| L1 cache BLAKE3 verification | `get_object` handler serves L1 hits without verifying BLAKE3 | Low |
| L2 chunk_list from cache | L2 hit returns inline or falls through; chunk_list path from cache not used | Low |
| Adjacent keys for GET prefetch | `after_get` needs key ordering context; currently passes empty slice | Low |
| Interface deviation from spec | `ReadCoordinator::new` takes 3 params vs spec's 9; uses `ReadRequest` struct | Low |
| `NodeConfig.auth_enabled` not wired | Auth middleware always passthrough; cannot enable via config | Low |
| `PrefetchConfig.enabled` defaults to `false` | Prefetch engine constructed with `enabled: false` in `node.rs:232` | Low |
| Multi-node integration tests | Need running gRPC servers on multiple nodes | Low |

---

## Verification

| Check | Status |
|---|---|
| `cargo build --all-targets` | ✅ Pass |
| `cargo test --all-targets` | ✅ ~175 tests pass across all crates |
| `cargo clippy --all-targets -- -D warnings` | ✅ Clean |
| `cargo doc --no-deps -p oceanfs-server` | ✅ Clean |

---

## Next Steps

1. **Extend `Membership` API** with `addr_of(&self, node_id: &NodeId) -> Option<SocketAddr>` — unblocks gRPC forwarding, shard fetch, replication, and try_forward.
2. **Wire `SegmentRpcClient`** into `fetch_chunks` — once address resolution works, spawn parallel gRPC shard fetches via `FuturesUnordered`.
3. **Wire `oceanfs-ec::Decoder`** into read path — feed fetched shard data through EC decode before chunk assembly.
4. **Implement read repair push** — use `SegmentRpcClient` to push corrected data to stale replicas in `read/repair.rs`.
6. **Add adjacent-key discovery** for GET prefetch — either maintain per-bucket key iterators or query metadata store.
7. **Wire `NodeConfig.s3_auth_enabled`** into auth middleware — replace hardcoded `passthrough()` with config-driven enable/disable.
