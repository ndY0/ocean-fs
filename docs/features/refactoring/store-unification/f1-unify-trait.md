---
feature: "f1: Unify SegmentDataStore Trait in oceanfs-storage-api"
epic: "refactoring/store-unification"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: c1 must exist first so trait-consumer migration (incl. modules/storage.rs) touches one wiring point; f1 updates the StorageModule field types that c1 introduces
adr:
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f1: Unify SegmentDataStore Trait in oceanfs-storage-api

## Summary

Fold `SegmentShardStore`'s delete/list responsibilities into a single
`SegmentDataStore` trait and move that trait into `oceanfs-storage-api`,
the shared trait crate (`oceanfs-storage-api` depends only on
`oceanfs-core`; both `oceanfs-durability` and `oceanfs-server` consume it
without a new crate edge — the ADR-0009 precedent). Today the data
abstraction is split across two traits over the same `.dat` files:
`SegmentDataStore` (read/write whole segment;
`crates/oceanfs-durability/src/anti_entropy/merkle_tree.rs:40`) and
`SegmentShardStore` (delete/list shards;
`crates/oceanfs-durability/src/gc/garbage_collector.rs:561`). The unified
trait is async and its `read_segment_data` returns a parsed
`SegmentFile` (version + payload + offsets) so consumers stop
hand-rolling the 76/92-byte header slicing that produced review #35.
During the transition the two durability impl structs stay in place
(dual-impl is acceptable per ADR-0032) and both implement the unified
trait; the impl merge is f2, the single-instance wiring is f3.

## Scope

### In Scope
- Add `crates/oceanfs-storage-api/src/segment_data_store.rs`: the unified
  `SegmentDataStore` trait + the `SegmentFile` value type; extend
  `crates/oceanfs-storage-api/src/lib.rs` exports. No new crate
  dependencies (types come from `oceanfs-core` / `bytes`).
- Delete the `SegmentShardStore` trait from
  `crates/oceanfs-durability/src/gc/garbage_collector.rs` (and its
  re-exports in `gc/mod.rs:22-23` and `lib.rs:52-53`).
- Migrate every production consumer of either trait to the unified
  `oceanfs_storage_api::SegmentDataStore`:
  - `oceanfs-durability`: `heal/worker.rs`, `repair.rs` (re-rep worker),
    `scrub.rs`, `scrub_service.rs`, `healing_service.rs`,
    `anti_entropy/engine.rs`, `gc/segment_compactor.rs`,
    `gc/orphan_reaper.rs`, `gc/compaction_crash.rs` (fault-injection
    harness).
  - `oceanfs-server`: `grpc/segment_service.rs` (imports today from
    `oceanfs_durability::SegmentDataStore`), `admin.rs` (the
    `Option<Arc<dyn SegmentDataStore>>` admin field), test impls in
    `read/fetch.rs` and `write/coordinator.rs`.
  - `oceanfs-node`: `segment_replicator.rs`,
    `modules/storage.rs` (post-c1 — its `data_store`/`shard_store`
    fields become the unified trait type).
- Update the shared test double `InMemorySegmentStore`
  (`merkle_tree.rs:65`) and `InMemorySegmentShardStore`
  (`garbage_collector.rs:608`) to implement the unified trait.
  Keep them **test-local in `oceanfs-durability`** (review #17/#26
  precedent) — they do NOT ship in `oceanfs-storage-api`; crates that
  need a disk-free store for their own tests keep a local test impl (as
  `healing_service.rs:1732 TestHealStore`,
  `segment_service.rs:957 TestSegmentStore` already do).
- Re-shape the trait methods to the ADR-0032 D1 shape (rendering deltas
  documented in `## Interface`).

### Out of Scope
- Merging/deleting the two disk impl structs (`DiskSegmentStore`,
  `DiskSegmentShardStore`) — that is f2.
- Introducing the optimized-I/O implementation or per-segment write locks
  — f2.
- Reducing the 8 composition-root instances to one — c1 starts it, f3
  finishes it.
- Moving `MetadataStore`, `SegmentStore`, or the data WAL (ADR-0032 Out of
  scope).
- ADR-0031's event-WAL/checkpoint legacy decode removal (separate
  feature).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage-api` | New module `segment_data_store.rs` (`SegmentDataStore` trait + `SegmentFile`); `lib.rs` exports `segment_data_store::{SegmentDataStore, SegmentFile}`; re-export `SegmentId` etc. unchanged |
| `oceanfs-durability` | Delete `SegmentShardStore` trait; delete the `SegmentDataStore` trait from `anti_entropy/merkle_tree.rs` (the in-memory store stays); all consumers import from `oceanfs_storage_api`; `lib.rs` re-exports `SegmentDataStore`/`SegmentShardStore` removed (kept only for the two in-memory test impls, which stay `pub` for cross-crate tests) |
| `oceanfs-server` | `grpc/segment_service.rs` + `admin.rs` import the trait from `oceanfs_storage_api`; test impls re-target the unified trait |
| `oceanfs-node` | `segment_replicator.rs` and `modules/storage.rs` (post-c1) use the unified trait type |

## Interface (Public API)

New in `oceanfs-storage-api` — the implementable rendering of ADR-0032
D1:

```rust
/// A `.dat` file's parsed header + payload — the reason `read` returns a
/// value instead of raw bytes: callers must stop hand-rolling the
/// 76/92-byte v1/v2 header logic (review #35).
#[derive(Debug, Clone)]
pub struct SegmentFile {
    /// The segment this file belongs to.
    pub segment_id: SegmentId,
    /// On-disk format version (v1 = 76-byte header, v2 = 92-byte header).
    pub version: u16,
    /// Byte length of the parsed header.
    pub header_len: usize,
    /// End offset (exclusive) of the data section within the file.
    pub data_end: u64,
    /// The data section payload (`file[header_len..data_end]`).
    pub data: Bytes,
}

/// Data access to a segment's `.dat` file(s).
///
/// ADR-0032 D1: this is the ONLY segment data-access abstraction. The
/// old read/write-only `SegmentDataStore` (merkle_tree.rs:40) and
/// delete/list-only `SegmentShardStore` (garbage_collector.rs:561) fold
/// into this one trait. Implementations live in `oceanfs-storage`
/// (production) or test crates (in-memory).
#[async_trait::async_trait]
pub trait SegmentDataStore: Send + Sync {
    /// Full-file read. Returns the parsed header + data section, or
    /// `None` when no `.dat` exists for the segment (NotFound is a
    /// value, not an error — scrub/heal currently sniff `ErrorKind`).
    async fn read_segment_data(&self, id: &SegmentId) -> Result<Option<SegmentFile>>;

    /// Full-file write of the data section (a valid header is
    /// synthesized by the implementation). Authoritative persistence —
    /// see ADR-0032 D3 (single-writer per `.dat`, coordinated).
    async fn write_segment_data(&self, id: &SegmentId, data: &[u8]) -> Result<()>;

    /// Delete a segment's `.dat`, resolving the pool through the
    /// lifecycle registry. Returns the reclaimed byte count.
    async fn delete_shards(&self, id: &SegmentId) -> Result<u64>;

    /// Delete a `.dat` under an explicit pool root (GC-compaction /
    /// recovery fast path — the caller already holds the pool id).
    async fn delete_shards_with_pool(&self, id: &SegmentId, pool_id: u32) -> Result<u64>;

    /// List `.dat` files under one root (multi-root orphan sweep: the
    /// caller invokes this once per candidate pool root).
    fn list_segment_files(&self, root: &Path) -> Result<Vec<PathBuf>>;
}
```

Where `Result`/`Error` is `oceanfs_storage_api::error::Error`
(existing variants `SegmentNotFound(SegmentId)`, `Io(#[from]
io::Error)`, `InvalidArgument`, `Internal` cover every consumer need).

### Rendering deltas from the ADR-0032 §D1 sketch (flagged for the
implementer)

The ADR's Rust block is a sketch; three reconciling decisions are baked
into the signatures above so the migration is mechanical:

1. **`delete_shards` / `delete_shards_with_pool` return `Result<u64>`**
   (reclaimed bytes), not `Result<()>`. The orphan reaper adds the return
   to `stats.bytes_reclaimed` (`orphan_reaper.rs:230-237`) and the
   disk-fill monitoring depends on that accounting. Computing bytes in
   the caller would double the stat.
2. **`delete_shards_with_pool(id, pool_id)` argument order** follows the
   sketch (`id` first). The old `SegmentShardStore` order was
   `(pool_id, id)` — all three production call sites flip.
3. **`pool_id` stays `u32`** (today's code and ADR-0031); the sketch's
   `PoolId` is not a distinct type in the codebase.

## Data Flow

```
Today (two traits, sync, raw bytes):
  AE/scrub/heal/repair ──► SegmentDataStore::read/write ──► std::fs
  reaper/compactor      ──► SegmentShardStore::delete/list ──► std::fs

After f1 (one trait, async, parsed header):
  AE/scrub/heal/repair ─┐
  reaper/compactor     ─┼──► oceanfs_storage_api::SegmentDataStore
                        │        (read → Option<SegmentFile>; delete → u64)
                        ▼
  disk impls (still in oceanfs-durability during the transition window;
  merged + moved to oceanfs-storage in f2)
```

Lifecycle coordination is unchanged by f1: writers keep calling
`SegmentLifecycleCoordinator::request_reserve`/`request_seal`/
`request_delete`/`request_refresh_metadata` around their data writes (f2
adds the per-`.dat` serialization underneath).

### Migration notes — delete/list callers

- **`gc/orphan_reaper.rs`** — Phase 2b calls
  `store.list_segment_files()` (`orphan_reaper.rs:164-176`) expecting
  `Vec<(SegmentId, i64, u32)>` (id, mtime, pool) from a store-wide scan.
  New shape: the reaper lists **per pool root** — it gains the candidate
  roots (node's data-pool roots, injected at construction — the node
  builder already holds `data_pools`) and calls
  `list_segment_files(root)` once per root. The pool id for an orphan is
  the root's `StoragePool::id()` (no more `(id, mtime, pool)` tuple).
  File mtime for the TTL grace gate comes from `std::fs::metadata` on
  each listed path (the reaper already stats paths to unlink); segment id
  is parsed from the `{uuid}.dat` file name as today.
  Reclaim (`orphan_reaper.rs:230`) becomes
  `store.delete_shards_with_pool(&segment_id, pool_id)`.
- **`gc/segment_compactor.rs`** — `delete_shards_with_pool(
  segment_meta.pool_id, segment_id)` at lines 212 and 468 flips to
  `delete_shards_with_pool(&segment_id, segment_meta.pool_id)`; the
  read at line 225 (`read_segment_data(&segment_id)` returning bytes)
  now maps `None` → "not present" and consumes `file.data`. The compactor
  field `shard_store: Arc<dyn SegmentShardStore>` (`segment_compactor.rs:67`)
  becomes a second `Arc<dyn SegmentDataStore>` only if the transition
  keeps two instances; f3 shares one.
- **`healing_service.rs`** — the `Option<Arc<dyn SegmentShardStore>>`
  field (`healing_service.rs:285`) + `with_shard_store` builder
  (`healing_service.rs:405`) migrate to the unified trait; the remap
  handler's `shard_store.delete_shards_with_pool(0, old_sid)`
  (`healing_service.rs:1561`) flips its arguments.
- **`gc/compaction_crash.rs`** (fault-injection matrix harness) imports
  both traits + impls (`compaction_crash.rs:61-64`); it migrates to the
  unified trait and keeps constructing the durability impl structs until
  f2.
- **Read callers** (`scrub.rs:331`, `heal/worker.rs:327`, `engine.rs:209,590`,
  `healing_service.rs:1113,1183`, `segment_service.rs:457`, `repair.rs`
  verification reads) replace "missing file == `Err` with
  `io::ErrorKind::NotFound`" with `Ok(None)`, and take `.data` (now that
  headers are parsed once by the store). `heal/worker.rs:327`
  (`unwrap_or_default()` on error) splits into explicit `None` → empty
  handling vs `Err` → propagate.
- **Write callers** (`heal/worker.rs:411`, `repair.rs:437`,
  `engine.rs:790`, `segment_compactor.rs:328`, `healing_service.rs:1333`,
  `segment_service.rs:837`) switch to the async method (`.await`) with
  identical byte semantics; coordination behavior lands in f2.

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds; the only
      `SegmentDataStore`/data-access trait in the workspace is
      `oceanfs_storage_api::SegmentDataStore`
      (`grep -rn "trait SegmentShardStore" crates --include=*.rs` is
      empty; `merkle_tree.rs` no longer defines a trait).
- [ ] **Tests:** `cargo test -p oceanfs-storage-api` (new trait doc
      tests) passes; `cargo test -p oceanfs-durability --lib --
      --test-threads=1`, `cargo test -p oceanfs-server --lib --
      --test-threads=1`, `cargo test -p oceanfs-node --lib --
      --test-threads=1` all pass (PIPELINE.md §4.6 — RocksDB crates must
      run single-threaded). New tests: `SegmentFile` header parsing
      covers v1 (76-byte) and v2 (92-byte) headers; a read of a missing
      `.dat` returns `Ok(None)` (regression for the scrub NotFound
      contract).
- [ ] **Docs:** `#![deny(missing_docs)]` passes in `oceanfs-storage-api`
      (new public items have docs + `# Examples`).
- [ ] **ADR:** ADR-0032 D1 satisfied (single trait, storage-api home);
      ADR-0031 respected (no legacy branches introduced);
      ADR-0025 state transitions untouched (reserve/seal/delete remain
      coordinator-routed).
- [ ] **Perf:** no perf rules newly violated — the trait move itself adds
      no I/O; `Option<SegmentFile>` avoids a heap copy only where the impl
      already slices (f2's impl owns that).
- [ ] **Integration:** a `oceanfs-durability` integration test
      (`crates/oceanfs-durability/tests/`) drives one complete
      repair→write→read round-trip through the migrated trait;
      `cargo test -p oceanfs-node --test durability_wiring -- --test-threads=1`
      is green.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Migration path

Pure trait relocation + fold, no behavior change, no format change:
`oceanfs-durability` re-exports nothing new at the crate root after f1
except the test-only in-memory stores; `oceanfs-server`'s segment gRPC
service imports the trait from `oceanfs_storage_api` (it already depends
on that crate). The two durability impl structs keep compiling through
f1 as dual impls of the unified trait, so `node.rs` / `modules/storage.rs`
need only a type-import update — this is what lets f1 and f2 land
sequentially green.
