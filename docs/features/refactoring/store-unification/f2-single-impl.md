---
feature: "f2: Single DiskSegmentStore in oceanfs-storage with Coordinated, Optimized-I/O Writes"
epic: "refactoring/store-unification"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: c1 consolidates the 8 node.rs constructions into StorageModule first; deleting the durability impls otherwise breaks 8 construction sites at once
  - feature: f2-store-path-delegacy
    epic: refactoring/legacy-mode-removal
    reason: "MUST land first: this feature deletes segment_store_impl.rs + DiskSegmentShardStore, which legacy f2 edits (removes legacy_dir). Parallel execution conflicts on the same files. Roadmap §4."
  - feature: f1-unify-trait
    reason: The unified oceanfs_storage_api::SegmentDataStore trait must exist and all consumers migrated before the impls are merged/deleted (ADR-0032 migration ordering)
adr:
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf:
  - "3.2 O_DIRECT for segment data files"
  - "3.3 mmap for hot segment reads"
  - "1.1 bytes::Bytes for blob data (SegmentFile.data)"
  - "7.1 minimize lock hold duration (per-segment write lock)"
created: 2026-09-04
updated: 2026-09-04
---

# f2: Single DiskSegmentStore in oceanfs-storage with Coordinated, Optimized-I/O Writes

## Summary

Implement the ONE production `oceanfs_storage_api::SegmentDataStore` impl
in `oceanfs-storage` — `crates/oceanfs-storage/src/segment/data_store.rs`
— beside the `SegmentLifecycleCoordinator` (ADR-0025) and the
`io::SegmentReader`/`IoBackend` layer it must share (ADR-0032 D2). Delete
the two durability impls: `crates/oceanfs-durability/src/segment_store_impl.rs`
(`DiskSegmentStore`, line 27) and `garbage_collector.rs`'s
`DiskSegmentShardStore` (line 626) — the field-for-field duplicate pair
(`data_pools`, `legacy_dir`, `pool_id_for`). The merged impl resolves a
segment's pool root from the **lifecycle registry's** `pool_id`
(ADR-0031: pools mandatory; no `legacy_dir`, no empty-`data_pools`
branch). Reads/writes stop using raw `std::fs` whole-file calls and route
through the storage `io` layer; a per-segment exclusive write lock makes
concurrent writers to the same `.dat` unrepresentable (ADR-0032 D3),
restoring at the data-file level the single-writer invariant ADR-0025
established for lifecycle state. Lifecycle state transitions themselves
(reserve/seal/delete/stamp) keep flowing through the coordinator exactly
as today — f2 serializes the `.dat` mutation underneath them.

## Scope

### In Scope
- New `crates/oceanfs-storage/src/segment/data_store.rs` defining
  `pub struct DiskSegmentStore` implementing the unified async
  `oceanfs_storage_api::SegmentDataStore`; export it from
  `crates/oceanfs-storage/src/segment/mod.rs` and the crate facade
  (`lib.rs` `pub use segment::{..., DiskSegmentStore}`).
- **Read path via the optimized layer:** `read_segment_data` resolves the
  `.dat` through the same reader surface the server uses
  (`io::SegmentReader` / `io::DiskSegmentReader`, mmap / O_DIRECT /
  buffered per `IoReadMode`) rather than `std::fs::read`. Header parse
  reuses `oceanfs_storage::SegmentHeader::from_bytes` +
  `SegmentHeader::header_size(version)`; returns `SegmentFile`.
- **Write path via the optimized layer:** whole-file `.dat` writes use
  the same atomic temp-file discipline as the seal pipeline
  (`io::atomic_write`: `SegmentWriteMode::probe` at construction,
  `create_temp` → write → `sync_data` → `finalize_temp`), recorded on the
  pool's `IoObserver` through `ObservedIo` (g1 per-pool signals). No
  `std::fs::write` of a whole `.dat`.
- **Per-segment write serialization:** the store holds a sharded
  keyed-lock map (`SegmentId → Arc<tokio::sync::Mutex<()>>`, or reuse the
  registry's writer-join discipline) so two writers can never mutate the
  same `.dat` concurrently. `write_segment_data` acquires the lock
  internally; multi-step read-modify-write flows (heal decode+splice)
  take an explicit guard across the whole sequence. Concurrent writers to
  one `.dat` are **unrepresentable**, not just discouraged.
- **Delete/list carried over from the shard store:** `delete_shards`
  (registry-resolved pool), `delete_shards_with_pool` (explicit pool —
  the GC-compaction fast path), and root-scoped `list_segment_files`
  (per-pool-root read_dir sweep) with the ADR-0031 pools-only resolution.
- Delete `crates/oceanfs-durability/src/segment_store_impl.rs`;
  delete `DiskSegmentShardStore` from `garbage_collector.rs`;
  drop their re-exports (`lib.rs:52,68`); delete the legacy-mode tests in
  `segment_store_impl.rs` (`legacy_store`, the `Vec::new()` no-pools
  helpers) — their pool-aware assertions move to the storage impl's tests.
- Re-point every construction site of the deleted types to the storage
  impl. Post-c1 these are the two `StorageModule` fields
  (`data_store` + `shard_store` in `modules/storage.rs`); the
  durability/test harness constructions in
  `gc/compaction_crash.rs:135-137` and `gc/orphan_reaper.rs:736` move to
  the storage type as well.
- `InMemorySegmentStore` / `InMemorySegmentShardStore` (now impls of the
  unified trait after f1) stay test-local in `oceanfs-durability`.

### Out of Scope
- Folding the trait (f1 — done before this starts).
- Collapsing the two StorageModule instances to one (f3) — during f2
  both `StorageModule` fields may hold `Arc<dyn SegmentDataStore>`
  pointing at two `DiskSegmentStore` instances or two clones of one.
- Changing the seal pipeline, the data WAL, the objects/metadata CFs, or
  the event log (ADR-0025/ADR-0024 machinery is untouched).
- ADR-0023's native-store direction (this unifies access to the existing
  file layout).
- Streaming read path / compression flags (ADR-0032 Out of scope).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New `segment/data_store.rs` (`DiskSegmentStore`, keyed-lock map, pool resolution via registry + `PoolRegistry`); `lib.rs` exports `DiskSegmentStore`; unit tests for io-layer round-trip, header v1/v2, pool resolution, lock exclusion |
| `oceanfs-durability` | Delete `segment_store_impl.rs`; delete `DiskSegmentShardStore` + its impl block in `garbage_collector.rs`; delete re-exports in `lib.rs`; `gc/compaction_crash.rs` and tests construct the storage impl or an in-memory store |
| `oceanfs-node` | `modules/storage.rs` / `node.rs` construct `oceanfs_storage::DiskSegmentStore` (not the deleted durability types) |

The impl needs from `oceanfs-storage` internals (all already present):
`SegmentHeader` (`segment/header.rs`), `SegmentLifecycleRegistry` +
`SegmentLifecycleCoordinator` (`segment/lifecycle.rs`), `PoolRegistry` /
`StoragePool` (`pool/`), and the `io` module. Construction inputs come
from `StorageModule` (c1): the pools, the registry, the coordinator, the
probed `SegmentWriteMode`, `Arc<IoBackend>`, `Arc<IoObserver>`, and the
segment reader.

## Interface (Public API)

New in `oceanfs-storage`:

- `pub struct DiskSegmentStore` — implements
  `oceanfs_storage_api::SegmentDataStore`. Constructed by
  `StorageModule` (f3) with:
  ```rust
  pub struct DiskSegmentStore {
      pools: Arc<oceanfs_storage::PoolRegistry>,      // live pool roots (ADR-0031: pools only)
      lifecycle_registry: Arc<SegmentLifecycleRegistry>, // pool_id per segment (ADR-0025)
      // coordinator: Arc<SegmentLifecycleCoordinator> may be held for future
      // coordinated write primitives; today's flows already call it directly
      reader: Arc<dyn oceanfs_storage::io::SegmentReader>, // shared with server path
      write_mode: oceanfs_storage::io::SegmentWriteMode,   // probed at construction
      io: Arc<oceanfs_storage::io::IoBackend>,             // O_DIRECT/io_uring/buffered
      observer: Arc<oceanfs_storage::io::IoObserver>,      // per-pool signals (g1)
      write_locks: /* sharded SegmentId → Mutex<()> */,
  }
  ```
  Exact field set is the implementer's choice; the invariants are: pools
  come from the registry snapshot, pool resolution reads the lifecycle
  registry, reads/writes go through `io`, writes are per-segment
  exclusive.
- `pub async fn lock_segment(&self, id: &SegmentId) -> SegmentWriteGuard` —
  the explicit per-segment exclusive guard for multi-step
  read-modify-write flows (heal). `write_segment_data` takes the same
  lock internally; holding the guard marks "I own this `.dat` right now"
  so two writers cannot interleave.

## Data Flow

```
(coordinated, serialized write — heal example, heal/worker.rs:411 after f2)
HealWorker::execute_heal
  → data_store.read_segment_data(id)            // io-layer read; Ok(None) → fetch from replica
  → decode + splice corrupt shards
  → guard = data_store.lock_segment(id).await   // exclusive per-.dat (D3)
  → data_store.write_segment_data(id, &updated) // atomic temp+fsync+finalize via io,
  │                                             //   ObservedIo signals on the pool
  → lifecycle.request_refresh_metadata(id, ...) // coordinator stamp (unchanged, ADR-0025)
  → drop(guard)

(new-segment write — re-rep pull, repair.rs:437 after f2)
ReRepWorker::execute_repair
  → fetch data from holder, verify merkle root
  → lifecycle.request_reserve(id, tier, ec_k, ec_m)   // coordinator (unchanged)
  → data_store.write_segment_data(id, &data)          // io-layer, per-segment exclusive
  → lifecycle.request_seal(id, meta) / request_refresh_metadata // coordinator (unchanged)
```

Every `.dat` mutation (heal/worker.rs:411, repair.rs:437,
anti_entropy/engine.rs:790, gc/segment_compactor.rs:328,
healing_service.rs:1333, server segment_service.rs:837) lands on the
single store under the per-segment lock; no subsystem holds its own store
or calls `std::fs` directly. Deletes (`delete_shards[_with_pool]`) and
the reaper's per-root `list_segment_files(root)` call keep their current
callers after f1's migration.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds;
      `grep -rn "segment_store_impl\|DiskSegmentShardStore" crates
      --include=*.rs` is empty; `oceanfs-durability` has no disk impl of
      the data trait.
- [ ] **Tests:** `cargo test -p oceanfs-storage --lib --
      --test-threads=1` green, including NEW tests:
      io-layer write→read round-trip produces a header-valid v1/v2 file
      readable by `SegmentHeader::from_bytes`; missing `.dat` reads as
      `Ok(None)`; pool resolution reads the lifecycle registry (a segment
      whose entry names pool N lands in pool N's root — no legacy
      fallback); **two concurrent writers to the same id serialize**
      (spawn N tasks writing distinct payloads to one `.dat`, assert the
      final file equals exactly one payload and no interleaved bytes —
      the multi-writer regression test);
      delete/list on a multi-pool layout returns per-root results.
      `cargo test -p oceanfs-durability --lib -- --test-threads=1`,
      `-p oceanfs-node --lib -- --test-threads=1`,
      `-p oceanfs-server --lib -- --test-threads=1` green
      (PIPELINE.md §4.6).
- [ ] **Docs:** `#![deny(missing_docs)]` passes; the new
      `DiskSegmentStore`/`SegmentWriteGuard` items have `# Examples`
      (in-memory or tempdir-based).
- [ ] **ADR:** ADR-0032 D2/D3 satisfied: one impl in `oceanfs-storage`
      beside the coordinator; no legacy branches (ADR-0031 — no
      `legacy_dir` field, no empty-`data_pools` resolution, pools
      mandatory at boot already enforced); no `std::fs` whole-file
      `.dat` writes in durability paths.
- [ ] **Perf:** perf rules in frontmatter followed — reads/writes go
      through `io` (`O_DIRECT`/mmap per `IoReadMode`); data carried as
      `Bytes` (no extra copies); the per-segment lock is held only across
      the file mutation (perf §7.1) — never across network I/O or EC
      decode.
- [ ] **Integration:** the ADR-0025 compaction crash-window matrix
      harness (`crates/oceanfs-durability/src/gc/compaction_crash.rs`)
      runs against the storage impl (or in-memory substitute) and stays
      green; `cargo test -p oceanfs-durability --test gc_compaction --
      --test-threads=1` and `--test orphan_reaper` pass; e2e write/read
      green.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Migration path

Merge, don't rewrite: the new impl is the union of the two deleted
structs' logic (read/write from `segment_store_impl.rs`, delete/list
from `DiskSegmentShardStore`) with resolution switched to
registry+pools and I/O switched to the `io` layer. Until f3 lands,
`StorageModule` may construct two instances (data + shard roles) or one
cloned instance; either compiles because both fields are the unified
trait type. The durability crate's unit tests that previously built
`DiskSegmentStore::new(Vec::new(), ...)` (no-pools legacy shape) are
rewritten to the pools-mandatory construction.
