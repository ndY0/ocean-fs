---
feature: "Move Blocking Metadata I/O Off Tokio Worker Threads"
epic: "performance-optimization"
status: proposed
priority: medium
owner: ""
dependencies: []
adr: []
perf:
  - "8.3 spawn vs spawn_blocking"
  - "2.7 Tokio semaphore for concurrency limits"
created: 2026-08-13
updated: 2026-08-13
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
| `oceanfs-server` | New `metadata_async.rs` adapter module; call sites in `s3_handler/handlers.rs`, `write/coordinator.rs` (Inline arm + seal worker's `put_segment`), `read/coordinator.rs` (`lookup_metadata`) switched to the adapter where the hot path flows; tests. |
| `oceanfs-node` | Wiring: construct the adapter around the concrete `RocksDbMetadataStore`-backed `MetadataOps` and hand it to the handler/coordinator composition. |
| `oceanfs-storage` | Verify only (`MetadataOps` impl + async store methods unchanged). |

## Definition of Done

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` on oceanfs-server +
      oceanfs-node, `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
      clean
- [ ] **Tests:** `cargo test -p oceanfs-server --lib -- --test-threads=1`
      and `cargo test -p oceanfs-node --lib -- --test-threads=1` green
      (PIPELINE.md §4.6 RocksDB SIGABRT caveat)
- [ ] **Tests:** a test asserting metadata ops run off the runtime —
      spawn a runtime with 1 worker thread, run N concurrent puts through
      the adapter, and they complete (would hang if the blocking calls ran
      inline on the single worker)
- [ ] **Integration:** seed-42 30 s + 120 s load tests PASS, 0 mismatches,
      RSS stable
- [ ] **Perf:** no observable latency regression — record ops/s
      before/after in the implementation report

## Open Questions

1. Semaphore bound default (e.g. 16)?
2. Does the Inline-tier write path in the coordinator go through the
   adapter (one blocking write per small PUT — high frequency)?
3. Does the seal worker's per-seal `put_segment` move into Feature 3's
   batched metadata writer instead (coordinate with
   performance-optimization/seal-pipeline-batching)?
