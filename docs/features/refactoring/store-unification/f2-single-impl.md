---
feature: "f2: Single DiskSegmentStore in oceanfs-storage with Coordinated, Optimized-I/O Writes"
epic: "refactoring/store-unification"
status: done
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
updated: 2026-09-05
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

- [x] **Code:** `cargo build --all-targets` succeeds;
      `grep -rn "segment_store_impl\|DiskSegmentShardStore" crates
      --include=*.rs` is empty; `oceanfs-durability` has no disk impl of
      the data trait.
<!-- REVIEW: verified 2026-09-05 (iteration 2 re-run) — `cargo build --all-targets` EXIT 0; the only warning is the pre-existing hint_wal.rs:848 cfg(test) dead-code warning (untouched by this diff — the iteration-1 unused-import warning at gc/compaction_crash.rs was removed in iteration 2). Grep gates: `segment_store_impl|DiskSegmentShardStore|SegmentShardStore` across crates/*.rs = 0 hits; durability lib.rs re-exports only the in-memory doubles (InMemorySegmentStore, InMemoryShardStore); the only disk impl of the trait is crates/oceanfs-storage/src/segment/data_store.rs:340 (`impl SegmentDataStore for DiskSegmentStore`). segment_store_impl.rs deleted (421 lines, staged). One `DiskSegmentStore::new` in crates/oceanfs-node (modules/storage.rs:442) serves both data_store + shard_store fields. -->
- [x] **Tests:** `cargo test -p oceanfs-storage --lib --
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
<!-- REVIEW: verified 2026-09-05 — storage lib 435/435 (iteration 2: 433 + the two new guard tests below), durability lib 263/263 (incl. all 9 compaction_crash matrix rows: kill_before_dat_write / kill_between_dat_write_and_seal / row7 / row8 / row9 / fully_dead / repacked-digest), node lib 66/66, server lib 244/244, all under --test-threads=1; storage-api 6/6 doc tests. Integration (iteration 2 re-run): durability --test gc_compaction 5/5, orphan_reaper 7/7, segment_data_roundtrip 2/2, anti_entropy 14/14, distributed_scrub 5/5, merkle_recovery 3/3; node --test durability_wiring 4/4, orphan_reaper 8/8, re_replication 2/2 (announcement + reconciliation — the reconciliation run exercises the registered-pool-gone → Ok(None) replica-fallback contract at data_store.rs:343-355). NEW tests at segment/data_store.rs:600-714: io_layer_write_read_roundtrip_is_header_valid_v1 (asserts SegmentHeader::from_bytes parse + data_end), missing_dat_reads_ok_none (registered + unregistered), registry_pool_id_selects_the_pool_root, unregistered_write_is_rejected (D3), concurrent_writers_serialize_exactly_one_payload_survives (8×4096B distinct payloads), delete_and_list_work_per_pool_root. Multi-pool per-root list/delete also re-covered at durability gc/garbage_collector.rs:1190 shard_store_lists_and_unlinks_across_pool_roots (now against the storage impl). -->
<!-- REVIEW: RESOLVED iteration 2 (2026-09-05) — the iteration-1 HIGH deadlock gap is fixed and verified: `write_segment_data` (data_store.rs:389-394) acquires the per-segment lock and delegates to the lock-free private `write_unlocked` (data_store.rs:230-286); the new public `write_segment_data_guarded` (data_store.rs:209-222) rewrites through `write_unlocked` WITHOUT re-acquiring the mutex (rejects a guard for a different segment with Error::InvalidArgument at data_store.rs:215-220; `SegmentWriteGuard` carries its segment_id + accessor at data_store.rs:78-85). Grep-verified: `lock_segment`/`write_segment_data_guarded`/`write_unlocked` appear ONLY inside data_store.rs — no production path calls the plain trait write while holding a guard (all production writers hold `Arc<dyn SegmentDataStore>` and call only the trait write). Guard doc (data_store.rs:58-72) documents the non-reentrant mutex and shows the guarded entry. Tests: guarded_rewrite_serializes_against_concurrent_plain_writer (data_store.rs:770-806) + guarded_write_rejects_wrong_segment (data_store.rs:812-828) — both pass (2/2 filtered run). -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; the new
      `DiskSegmentStore`/`SegmentWriteGuard` items have `# Examples`
      (in-memory or tempdir-based).
<!-- REVIEW: verified 2026-09-05 (iteration 2 re-run) — RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p oceanfs-storage-api -p oceanfs-storage -p oceanfs-durability -p oceanfs-node -p oceanfs-server EXIT 0; #![deny(missing_docs)] present in all five crates; DiskSegmentStore + SegmentWriteGuard carry `# Examples` (ignore-tagged — non-gating per the doc note); the guard's example now shows the guarded rewrite entry (data_store.rs:63-73) and its doc states the per-segment mutex is not reentrant (data_store.rs:58-61). `io::segment_file::SegmentFileReader` is pub(crate) throughout (io/mod.rs:51 `pub(crate) mod segment_file`) — no public leak; the only public additions are the sanctioned `SegmentReader::purge_cache` default method (io/segment_reader.rs:90, overridden by DiskSegmentReader:407 and forwarded by PoolFallbackReader:540) and the f2-doc-sanctioned DiskSegmentStore/SegmentWriteGuard/lock_segment/write_segment_data_guarded exports. -->
- [x] **ADR:** ADR-0032 D2/D3 satisfied: one impl in `oceanfs-storage`
      beside the coordinator; no legacy branches (ADR-0031 — no
      `legacy_dir` field, no empty-`data_pools` resolution, pools
      mandatory at boot already enforced); no `std::fs` whole-file
      `.dat` writes in durability paths.
<!-- REVIEW: verified 2026-09-05 (iteration 2 re-run) — D2: single disk impl at crates/oceanfs-storage/src/segment/data_store.rs beside the lifecycle registry/coordinator (segment/lifecycle.rs); durability twin impls + DiskSegmentShardStore deleted; node constructs exactly one `oceanfs_storage::DiskSegmentStore::new` (modules/storage.rs:442) serving both data_store + shard_store fields (f3 collapses). D3: write path = reserve-before-write in every flow (server push_sealed_segment segment_service.rs:930-1045 with Fresh/Existing arms + write-failure reservation cleanup via request_delete; re-rep repair.rs:450-470 with cleanup; compactor segment_compactor.rs:327-345 with cleanup_reserved_new; heal/AE target already-registered segments); every `.dat` write routes through write_dat_atomic (data_store.rs:466-525) — atomic temp+fsync+finalize through pool-aware ObservedIo, no std::fs whole-file .dat write anywhere in durability production paths (grep re-verified iteration 2; remaining fs::write hits on `.dat` paths are #[cfg(test)] seeders only); purge-on-write after rewrite (data_store.rs:284 → reader.purge_cache). No legacy branches: resolve_pool (data_store.rs:292-307) is registry-only, no legacy_dir field, unknown-pool = Internal error; unregistered_write_is_rejected covers the f1 write-before-register bridge removal. Keyed per-segment locks: write_locks map data_store.rs:134 + lock_segment:185; concurrent writers to one .dat serialize (byte-interleaving unrepresentable). ADR-0031 D1 (pools mandatory at boot) is exercised by the storage tests' pools-only fixture (data_store.rs:551-626). -->
- [x] **Perf:** perf rules in frontmatter followed — reads/writes go
      through `io` (`O_DIRECT`/mmap per `IoReadMode`); data carried as
      `Bytes` (no extra copies); the per-segment lock is held only across
      the file mutation (perf §7.1) — never across network I/O or EC
      decode.
<!-- REVIEW: verified 2026-09-05 — rule 3.2/3.3: the store reads through the shared path-agnostic core io/segment_file.rs (SegmentFileReader: Mmap→mmap-cache get_or_map, Direct→DirectIoBuf with the >2MiB short-read loop at segment_file.rs:177-194, Buffered→tokio::fs read_exact); node wiring passes the configured io_mode. Iteration 2 (reader-core extraction): `verify_header` (segment_file.rs:86-102) now returns the parsed `SegmentHeader` and `read_range` (segment_file.rs:115-198) carries the verbatim mode dispatch — the DiskSegmentReader semantics preserved 1:1 (diff-verified: u32::MAX whole-segment sentinel resolved at segment_reader.rs:386; sources MmapBacked/DirectIo per serves_from_mmap_cache; evict_after_read retained via SegmentFileReader.evict_after_read). The store read path is now ONE sync header-only open (verify_header, data_store.rs:362) + the ranged read — the iteration-1 triple-open gap reduced as claimed. Rule 1.1: read payload is Bytes (read_range), write payload copy is the single whole-file copy on spawn_blocking (data_store.rs:258); DiskSegmentStore fields hold Bytes-free slices. Rule 7.1: the per-segment lock spans only the file mutation + purge — never network I/O or EC decode (decode precedes the store call in heal/AE flows). Residual note (non-blocking): the sync 128-byte header open+read executes on the runtime worker (std::fs, data_store.rs:362→segment_file.rs:92-96) — inherent to the shared core and identical to the pre-f2 DiskSegmentReader first-touch behavior. -->
- [x] **Integration:** the ADR-0025 compaction crash-window matrix
      harness (`crates/oceanfs-durability/src/gc/compaction_crash.rs`)
      runs against the storage impl (or in-memory substitute) and stays
      green; `cargo test -p oceanfs-durability --test gc_compaction --
      --test-threads=1` and `--test orphan_reaper` pass; e2e write/read
      green.
<!-- REVIEW: verified 2026-09-05 — the matrix now constructs the unified storage impl (compaction_crash.rs:199-211, shard_store = clone of data_store) and adds the recovery-first ordering (`let (_, _) = self.recover().await;` before each drive_to_milestone at compaction_crash.rs:326-327 — the registry fold precedes compactor reads, matching production boot order); all 9 matrix rows pass under --test-threads=1 (iteration 2 re-run); gc_compaction + orphan_reaper integration green (see Tests evidence). Node startup recovery sweep captures the registry pool_id BEFORE the durable delete and sweeps explicit-pool (modules/storage.rs:626-661); pure-residue SweepOldDat is left to the reaper's per-root listing — matches the delete_shards registry-only + explicit-pool sweeps + reaper-backstop semantics decision. Iteration 2: row7/row8/row9 dispatch their post-delete `.dat` sweeps via `delete_shards_with_pool(&id, 0)` (compaction_crash.rs:545, 604, 659) — the entry is evicted with the durable delete, so the explicit-pool fast path + reaper backstop is the production-shape sweep. E2e write/read (iteration 2 independent run, release binary at target/release/oceanfs 19:03 — staleness-checked newer than every source file): crash_restart 4/4, wal_recovery 5/5, segment_lifecycle 6/6, cluster_write_path 1/1, cluster_read_path 1/1, garbage_collection 1/1, rewrite_leak_test 1/1, cluster_lifecycle 1/1 — 20/20 green; no load suites run locally (PIPELINE.md §6). -->

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

## Implementation notes (accepted)

Status: independent review **PASS**, iteration 2 (2026-09-05). Iteration 1
**FAIL** was a single HIGH finding — a guard self-deadlock in the write
path — fixed and re-verified in iteration 2; all DoD items are verified
and ticked with evidence comments above. The notes below record the
accepted decisions and behavior notes so the document reflects what was
built.

- **RESOLVED (iteration 1, HIGH) — guard self-deadlock.** Calling the
  plain trait `write_segment_data` while holding the explicit per-segment
  guard would re-acquire the same mutex and deadlock. Fix: the public
  `write_segment_data` now splits into lock acquisition + the lock-free
  private `write_unlocked` (data_store.rs:230-286), and the new public
  `write_segment_data_guarded` (data_store.rs:209-222) rewrites through
  `write_unlocked` **without** re-acquiring the mutex. A guard for a
  different segment is rejected with `Error::InvalidArgument`
  (data_store.rs:215-220), and `SegmentWriteGuard` carries its
  `segment_id` (data_store.rs:78-85). Two new tests lock the behavior in:
  `guarded_rewrite_serializes_against_concurrent_plain_writer`
  (data_store.rs:770-806) and `guarded_write_rejects_wrong_segment`
  (data_store.rs:812-828) — both pass (see Tests evidence).
- **DEC-1 — read semantics for a registered segment whose pool is gone.**
  Kept from the original design: a registered segment whose pool is gone
  (f8 detach / dead pool) reads as `Ok(None)` — the read coordinator
  treats a missing local copy as replica-fallback, and the `re_replication`
  dead-pool integration test depends on this contract (exercised at
  data_store.rs:343-355). `delete_shards` is registry-resolved only
  (unregistered → `Ok(0)`); residue sweeps are explicit-pool
  (`delete_shards_with_pool`), and the orphan reaper's per-root sweep is
  the backstop — documented at the call sites (see Integration evidence).
- **DEC-2 — write-mode degradation for existing `.dat` rewrites.**
  `O_TMPFILE` + `linkat` cannot overwrite an existing file, so rewrites of
  an existing `.dat` degrade to the rename-based temp path (atomic
  `rename(2)`); fresh files keep the `O_TMPFILE` path. Both arms funnel
  through `write_dat_atomic` (data_store.rs:466-525) — temp + fsync +
  finalize, recorded on the pool's `IoObserver`.
- **Crash-harness ordering — startup recovery first.** The compaction
  crash harness's `drive_to_milestone` now runs startup recovery before
  each milestone (compaction_crash.rs:326-327): production ordering,
  because the unified store resolves segments through the lifecycle
  registry, which is folded at startup. This is what lets the row7/row8/
  row9 post-delete sweeps take their registry-evicted, explicit-pool
  shape (compaction_crash.rs:545, 604, 659).
- **Renamed in-memory double — `InMemoryShardStore`.** The durability
  in-memory delete/list double is now `InMemoryShardStore`: the epic grep
  gate bans the `SegmentShardStore` identifier (the f1-deleted trait's
  name), so durability `lib.rs` re-exports only the in-memory doubles
  (`InMemorySegmentStore`, `InMemoryShardStore`). The double's distinct
  delete-tracking semantics are preserved unchanged.
- **Guard reachability — production flows use the internally-locked
  path.** Production RMW flows (heal / AE / repair) hold
  `Arc<dyn SegmentDataStore>` and can only reach the trait write, which
  acquires the per-segment lock internally; whole-file writes are
  additionally atomic (temp + fsync + finalize), so torn or interleaved
  writes are unrepresentable on that path. The explicit guard
  (`lock_segment` / `write_segment_data_guarded`) is the concrete-store
  API for flows needing read → decode → write serialization across
  multiple store calls.

