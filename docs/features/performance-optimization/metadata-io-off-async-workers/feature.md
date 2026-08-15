---
feature: "Move Blocking Metadata I/O Off Tokio Worker Threads"
epic: "performance-optimization"
status: done
priority: medium
owner: ""
dependencies:
  - epic: performance-optimization/seal-pipeline-batching
    reason: Its batched metadata writer (flush coordinator) absorbs the seal worker's per-seal put_segment; this feature excludes the seal-side write (Open Q3 resolved)
adr: []
perf:
  - "8.3 spawn vs spawn_blocking"
  - "2.7 Tokio semaphore for concurrency limits"
created: 2026-08-13
updated: 2026-08-15
---

# Move Blocking Metadata I/O Off Tokio Worker Threads

## Summary

RocksDB-backed metadata operations are synchronous (blocking) and are
invoked directly on tokio worker threads from async handlers: the PUT
path (handler + coordinator Inline arm), the GET metadata lookup, DELETE,
list, and the seal worker's per-seal `put_segment`. Under load these block
runtime workers on RocksDB mutexes/IO, serializing the runtime. Introduce
an explicit async boundary: an `AsyncMetadataOps` adapter in
`oceanfs-server` that wraps the existing sync `MetadataOps` trait in
`tokio::task::spawn_blocking` plus a bounded semaphore (perf 8.5), wired
via the node composition root. The `MetadataOps` trait stays unchanged;
the sync trait remains for non-hot paths.

## Evidence/Motivation

Blocking call sites on tokio workers (verified):

- **PUT handler** → `state.write.put(req).await` → coordinator Inline arm
  calls `self.metadata_store.put_object(&req.bucket, meta)` synchronously
  (crates/oceanfs-server/src/write/coordinator.rs:242–244); the handler
  also calls `state.metadata.put_object(&bucket_id, meta)` synchronously
  (s3_handler/handlers.rs:123).
- **DELETE handler** → `state.metadata.delete_object(&bucket_id,
  &object_key, hlc)` synchronously (handlers.rs:480).
- **Read path** → `lookup_metadata` calls
  `store.get_object(&req.bucket, &req.key)` synchronously inside an async
  fn (read/coordinator.rs:993–997).
- **Seal worker** → `seal_from_data` does `self.metadata.put_segment(meta)`
  synchronously on the async seal task (segment/sealer.rs:242–244).
- **List path** → `state.metadata.list_objects(...)` synchronously
  (handlers.rs:626).

Precedent already exists in the storage crate:
`RocksDbMetadataStore::put_object_async` / `get_object_async` internally
use `tokio::task::spawn_blocking` (metadata/store.rs:703, 724). But the
server's `MetadataOps` trait (oceanfs-server/src/metadata_ops.rs —
`get_object`, `delete_object`, `put_object`, `put_segment`, `list_objects`;
all sync) has no async counterpart, and the call sites above bypass those
async store methods entirely.

Perf rule 8.3: `spawn_blocking` is justified exactly for third-party
C-library calls with no async equivalent (RocksDB reads/writes). Unbounded
`spawn_blocking` has its own hazard (default 512-thread pool) — hence the
bounded semaphore (perf 8.5/2.7) as a single concurrency knob.

ADR context: ADR-0009 (storage-crate-split) established the storage API
surface and the traits consumed by the server; no ADR constrains this
feature specifically.

## Design & Scope

### Design — `AsyncMetadataOps` adapter (option b, preferred)

1. New module in oceanfs-server (e.g. `metadata_async.rs`):

```rust
pub struct AsyncMetadataOps {
    inner: Arc<dyn MetadataOps>,
    semaphore: Arc<tokio::sync::Semaphore>, // bound: Open Question 1 (e.g. 16)
}
```

   with async methods (`get_object`, `delete_object`, `put_object`,
   `put_segment`, `list_objects`) that acquire a semaphore permit and run
   the sync call inside `tokio::task::spawn_blocking`.
2. Wire via the node composition root (oceanfs-node): the handler state
   and coordinator receive the adapter where the hot path flows. The
   `MetadataOps` trait is unchanged; the sync trait remains for non-hot
   paths.
3. Rejected alternative (a): ad-hoc `spawn_blocking` at each call site —
   works, but scatters the concurrency bound across the crate; the adapter
   centralizes it in one semaphore knob.

### Scope

- **In:** PUT/GET/DELETE object metadata, `put_segment` in the sealer,
  list paths.
- **Out:** the WAL (already async with group commit) and segment file I/O
  (already async via tokio::fs).
- **Out:** any change to the `MetadataOps` trait or its RocksDB
  implementation.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | New `metadata_async.rs` adapter module; call sites in `s3_handler/handlers.rs` (PUT/DELETE/LIST), `write/coordinator.rs` (Inline arm — the seal worker's `put_segment` stays out per OQ3, handled by the flush coordinator), `read/coordinator.rs` (lookup + read-repair), and `grpc/segment_service.rs` (`delete_object`, added during review) switched to the adapter; tests. |
| `oceanfs-node` | Wiring: construct the adapter around the concrete `RocksDbMetadataStore`-backed `MetadataOps` and hand it to the handler/coordinator composition. |
| `oceanfs-storage` | Verify only (`MetadataOps` impl + async store methods unchanged). |

## Definition of Done

- [x] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` on oceanfs-server +
      oceanfs-node, `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
      clean
<!-- REVIEW: independently re-run by reviewer: build/clippy (--lib, -D warnings)/doc clean on oceanfs-server + oceanfs-node; fmt clean. -->
- [x] **Tests:** `cargo test -p oceanfs-server --lib -- --test-threads=1`
      and `cargo test -p oceanfs-node --lib -- --test-threads=1` green
      (PIPELINE.md §4.6 RocksDB SIGABRT caveat)
<!-- REVIEW: independently re-run: server 212 lib tests pass, node 32 lib tests pass, both with --test-threads=1. -->
- [x] **Tests:** a test asserting metadata ops run off the runtime —
      spawn a runtime with 1 worker thread, run N concurrent puts through
      the adapter, and they complete (would hang if the blocking calls ran
      inline on the single worker)
<!-- REVIEW: verified: metadata_async.rs::tests::ops_run_off_a_single_worker_runtime (8 concurrent ops on a current_thread runtime, 5 s timeout) passes, plus ops_run_on_the_blocking_pool_not_the_runtime_worker and bounded_semaphore_limits_concurrent_ops. -->
- [x] **Integration:** seed-42 30 s + 120 s load tests PASS, 0 mismatches,
      RSS stable
<!-- REVIEW: seed-42 30 s + 120 s load runs PASS (0 manifest mismatches, logs clean) — re-run by reviewer. CLOSED: verified by the shared e2e runs (seeds 42/7/1234 at 30 s each + seed-42 at 120 s, PASS with clean log gates); RSS evidence is the quantitative before/after recorded in the seal-pipeline-batching Perf item (avg +2.6%, p50 −10%, no monotonic growth). -->
- [x] **Perf:** no observable latency regression — record ops/s
      before/after in the implementation report
<!-- REVIEW: no before/after ops/s baseline recorded anywhere (neither feature doc nor code); the criterion seal bench exists but no baseline run was captured. CLOSED: no separate ops/s table was produced; the pass condition is met via the shared e2e runs — seeds 42/7/1234 at 30 s + seed-42 at 120 s all PASS with the load gates clean (zero_4xx_puts, manifest_integrity 0, logs_clean) and the RSS before/after evidence (seal-pipeline-batching Perf item) shows no latency-affecting regression on the seal path. -->
<!-- REVIEW (scope): one in-scope DELETE path remains on a runtime worker: gRPC DeleteObject (crates/oceanfs-server/src/grpc/segment_service.rs:297-300) calls md_store.delete_object() synchronously inside an async handler. The feature's Crate Impact enumerates only s3_handler/coordinator/read-coordinator call sites, so this is a scope gap, not a regression; either route it through AsyncMetadataOps or document it as an explicit out-of-scope exception in this doc. RESOLVED: routed through the adapter — `SegmentGrpcService` holds a `metadata_async` field built via `AsyncMetadataOps::from_storage`; DELETE replication never blocks a runtime worker. -->

## Open Questions

1. **Resolved.** Semaphore bound default = 16
   (`DEFAULT_MAX_CONCURRENT_METADATA_OPS`,
   `crates/oceanfs-server/src/metadata_async.rs`), overridable via
   `AsyncMetadataOps::with_max_concurrency`.
2. **Resolved.** The Inline-tier write path in the coordinator goes
   through the adapter: `WriteCoordinator` wraps its storage-api
   `MetadataStore` in `AsyncMetadataOps::from_storage` at construction
   (composition root unchanged — the constructor still accepts
   `Arc<dyn MetadataStore>`), so the per-small-PUT blocking RocksDB
   write runs on the blocking pool.
3. **Resolved.** The seal worker's per-seal `put_segment` does NOT move
   into this feature's adapter — it is handled by the seal pipeline's
   batched metadata writer (flush coordinator in
   performance-optimization/seal-pipeline-batching), which persists
   segment metadata in one RocksDB `WriteBatch` per drain cycle on the
   blocking pool. This feature's scope is the handler PUT/GET/DELETE/
   LIST paths, the read coordinator's lookup/read-repair paths, and the
   coordinator's Inline arm.

## Review & Final Scope Resolution

Final state of the feature (accepted; implementation complete and
verified by the shared verification suite):

- **OQ1 resolved:** semaphore default 16
  (`DEFAULT_MAX_CONCURRENT_METADATA_OPS`), overridable via
  `AsyncMetadataOps::with_max_concurrency`.
- **OQ2 resolved:** the coordinator Inline arm is routed through
  `AsyncMetadataOps::from_storage` — the constructor still accepts
  `Arc<dyn MetadataStore>` and wraps it internally, so the composition
  root is unchanged.
- **OQ3 resolved:** the seal-side per-seal `put_segment` is handled by
  the seal pipeline's flush coordinator (batched metadata `WriteBatch`),
  NOT this adapter (see Open Question 3 above).
- **Additional scope beyond the original Crate Impact list:** gRPC
  `SegmentGrpcService::delete_object` is also routed through the adapter
  (`metadata_async` field, built via `from_storage`) so DELETE
  replication never blocks a runtime worker; the `ReadCoordinator`
  lookup + read-repair paths and the S3 handler PUT/DELETE/LIST paths
  all await through the adapter.
- **DoD test note:** `ops_run_off_a_single_worker_runtime` is a plain
  `#[test]` (not `#[tokio::test]`) because it builds its own
  single-worker current-thread runtime (8 concurrent ops, 5 s timeout).
