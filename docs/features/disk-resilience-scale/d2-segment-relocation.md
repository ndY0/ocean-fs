---
feature: "Segment Relocation (Durable pool_id Mutation)"
epic: "disk-resilience-scale"
status: done
priority: high
owner: ""
dependencies: ["d1-pool-drain-state"]
adr: [0036, 0025, 0030, 0032, 0034]
perf: []
created: 2026-09-07
updated: 2026-09-09
---

# Segment Relocation (Durable pool_id Mutation)

## Summary

The core primitive of Phase C: **durably move a sealed segment from one
data pool to another on the same node** by mutating the segment's
`pool_id` through the existing metadata-refresh event family — the
ADR-0036 D2 decision is to **extend `MetadataRefreshEvent` with an optional
`pool_id` section**, not to invent a new event kind. The relocation is
**copy → commit → unlink** under the unified store's per-segment write
lock (ADR-0036 D3 / ADR-0032 D3): copy the `.dat` to the target pool root
via the existing atomic write path, commit the `pool_id` refresh event
(event-WAL + checkpoint fold — the only durable writer, ADR-0025), then
unlink the source copy. Both crash windows are safe. Reader per-segment
pool-root caches are purged at the commit. GC/reaper interplay is
specified. Lives in `oceanfs-storage` (event encoding + fold, data-store
relocate operation, reader-cache purge); node has no direct change (the
workers d3/d4 call the primitive). This is the primitive d3's intra-node
mover and d4's source-release story build on.

## Scope

### In Scope

- **Wire/event format: optional `pool_id` section on the refresh event.**
  - `MetadataRefreshEvent` today carries `segment_id`, an optional
    `merkle_root`, and an optional `storage_locations`
    (`crates/oceanfs-storage/src/segment/event_wal.rs:270-291`, field
    declarations at 272/274/277). The `KIND_METADATA_REFRESH` encoding is
    length-discriminated (refresh constants at `event_wal.rs:125-139`;
    `SegmentEvent::to_record_bytes` at `event_wal.rs:322`, decode at
    `from_record_bytes:412` / `decode_payload:473`).
  - Add an optional **`pool_id: Option<u32>`** section following the same
    length-discriminated, backward-compatible discipline the
    `storage_locations` extension used (ADR-0030's refresh payload was the
    precedent; ADR-0036 D2 mandates the same family/discipline). Old
    records without the section decode to `None`; a record with the
    section decodes to `Some(u32)`. New `REFRESH_POOL_ID_*` framing
    constants join the existing ones; `MAX_PAYLOAD_SIZE` grows if needed.
  - **No proto change.** The event-WAL record is node-local; `pool_id` is
    not a wire type (ADR-0036 Neutral: "no wire/proto change for the
    intra-node half").
- **Fold updates `SegmentMetadata.pool_id`.**
  - `SegmentLifecycleRegistry::fold_refresh`
    (`crates/oceanfs-storage/src/segment/lifecycle.rs:1101`) applies the
    new section to the registry entry's `SegmentMetadata.pool_id`
    (`crates/oceanfs-core/src/types/metadata.rs:143`); the coordinator's
    `request_refresh_metadata` (`lifecycle.rs:2100`, three impl-site
    variants) gains the optional `pool_id` parameter and appends the
    event through the existing event-WAL path (ADR-0025 — durable before
    the file unlink). `pool_id` was stamped-immutable at seal (f5); this
    event is the single mutation path, so the fold is the only writer.
  - Accounting (`total_bytes`/`contained_objects`) is **unchanged** — the
    segment does not change, only its pool — so no accounting delta is
    expected in the ADR-0034 bookkeeping; only the file's physical
    location moves (source root free bytes rise, target falls — via the
    existing `refresh_capacity`/statvfs path, not a manual ledger).
- **`relocate` operation: copy → commit → unlink.**
  - New orchestration type in `oceanfs-storage` (same-crate so it can use
    the store's crate-private write-lock seam), e.g.
    `oceanfs-storage/src/segment/relocate.rs` with
    `pub struct SegmentRelocator` built from
    `Arc<SegmentLifecycleCoordinator>` + `Arc<DiskSegmentStore>`
    (`crates/oceanfs-storage/src/segment/data_store.rs:108`).
  - Operation, under the store's per-segment write lock
    (`DiskSegmentStore::write_locks`, `data_store.rs:134`; `lock_segment`
    at `data_store.rs:185`):
    1. **copy**: write the `.dat` to the **target pool root** through the
       existing atomic write path (`write_dat_atomic`, `data_store.rs:466`,
       and the guarded writer `write_segment_data_guarded:209`) — the
       target pool id is explicit, **skipping `resolve_pool`**
       (`data_store.rs:292`), per the sketch's "dedicated method that
       takes the target pool id and skips `resolve_pool`" decision;
    2. **commit**: `request_refresh_metadata(id, None, None,
       Some(target_pool_id))` — durable event-WAL append + fold;
    3. **unlink**: remove the source-root `.dat` (`DiskSegmentStore::unlink`,
       `data_store.rs:329`, or `delete_shards_with_pool` for the source
       pool at `data_store.rs:408`).
  - Error states to define: target pool missing / not data-role / not
    present in the registry; target == current `pool_id`; segment not
    sealed / not held locally / not found; source `.dat` missing (the
    registry says we hold it but the file is gone — surface as a distinct
    error so the worker can block with a reason rather than silently
    "relocate" a ghost).
- **Reader-cache purge at commit.**
  - `DiskSegmentReader` memoizes the resolved root per segment in
    `pool_root_cache` (`crates/oceanfs-storage/src/io/segment_reader.rs:170`).
    On commit the cache entry for the segment is purged; the next read
    re-resolves through the registry's new `pool_id`. (The epic/sketch
    call this "pool_id cache"; the field is `pool_root_cache` — the doc
    cites the actual name.) New `pub fn purge_pool_root(&self,
    segment_id: SegmentId)` (or a notifier hook in the
    `StorageLocationsNotifier` style — `lifecycle.rs:1351` — wired by the
    node if the reader lives outside the relocator's crate module).
  - Reads resolve by registry `pool_id`, so the source→target switch is
    **atomic at the commit** even while two `.dat` copies exist.
- **Crash-window safety (ADR-0036 D3), made into tests:**
  - pre-commit crash (copy done, event not durable): source is
    authoritative; the target copy is an unregistered file on the target
    root → boot-reapable residue. The drain re-runs after restart and
    re-copies.
  - post-commit crash (event durable, unlink not done): target is
    authoritative (registry `pool_id` = target); the source `.dat` is now
    an unregistered file on the old root → boot-reapable residue.
  - Either window leaves exactly one authoritative copy; neither loses
    data nor orphans-then-loses a copy. See the reaper/GC note below for
    the residue-sweep rule that makes the "reapable" claim safe.
- **GC/reaper/compaction interplay (bounded-metadata discipline,
  ADR-0034).**
  - Drain enumerates the lifecycle registry (`SegmentLifecycleRegistry::for_each`,
    `lifecycle.rs:793`), never a disk scan — the d3/d4 workers' guarantee,
    but the primitive must not create files that a *periodic* reaper would
    misread mid-flight. In-process there is no window: copy+commit+unlink
    run inside one lock hold, and GC/compaction/read resolve the segment
    by registry `pool_id`, so a target copy is invisible to them until
    commit flips the registry.
  - The only cross-restart exposure is a crash leaving an unregistered
    `.dat` (either side, per the windows above). Rule: unregistered
    `.dat` files on a data-pool root are reapable **only** by the
    once-per-boot residue sweep / orphan reaper operating on
    registry-unknown files (`OrphanTask`, durability
    `scheduler/adaptors.rs:110`; the boot sweep path in
    `crates/oceanfs-node/src/modules/storage.rs` ~`run_startup_recovery`).
    Both windows' residue is exactly that — an unregistered file — so
    reaping is safe and the drain re-runs from the registry. The target
    file must NOT be made visible to listing before commit (the
    `write_dat_atomic` temp+rename path already keeps partial writes out
    of the namespace until the rename, `data_store.rs:466`).
  - Post-relocation, GC/compaction/AE/scrub all see the new `pool_id`
    (registry-driven); no stale root references survive the reader purge.
- Tests:
  - unit (event encoding): refresh event with `pool_id` round-trips
    byte-exact; a record WITHOUT the section decodes to `None`
    (backward-compat regression, in the `event_wal.rs` test family e.g.
    `metadata_refresh_with_locations_roundtrips` at `event_wal.rs:1668`);
    unknown/length-mismatched section rejected not panicked;
  - unit (fold): `fold_refresh` with `Some(pool_id)` updates
    `SegmentMetadata.pool_id`; with `None` leaves it unchanged;
  - unit (relocate): a sealed segment on pool 0 relocates to pool 1 —
    `.dat` present on pool 1's root, absent on pool 0's, registry
    `pool_id == 1`, reader resolves the new root (cache purged);
  - unit: relocate rejects missing target / data-role violation /
    target==source / not-sealed / source-file-gone (each error surfaced,
    no partial state);
  - unit: relocate serializes against a concurrent plain writer/delete on
    the same segment (per-segment lock; model on
    `guarded_rewrite_serializes_against_concurrent_plain_writer`,
    `data_store.rs:771`);
  - unit/crash-matrix: the `segment/crash_matrix.rs` family gains
    relocate windows — kill after copy (source authoritative), after
    commit (target authoritative); restart, fold the event WAL, run the
    boot residue sweep, and assert exactly the authoritative `.dat`
    survives and the other is reaped without data loss;
  - integration (crate boundary): seed a 2-data-pool store + registry,
    relocate under a concurrent reader, assert continuous read
    correctness across the switch and byte-identical content after.

### Out of Scope (for this feature)

- The drain workers/controllers (d3 C1a, d4 C1b) and source-release —
  d2 supplies the primitive d3 uses and the commit half d4 reuses.
- Making `pool_id` mutable on the `.dat` header or any proto — the 
  mutation is event-WAL-only and node-local (ADR-0036 D2, Neutral).
- Rebalance (C2b), ring re-weighting (d6/C2a), segment self-description
  (C3), graceful-leave redesign — epic non-goals.
- Detach (d5) — the primitive that empties a pool precedes the detach
  that removes it.
- Any change to `request_refresh_metadata`'s existing `merkle_root` /
  `storage_locations` behavior (the new parameter is additive).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `segment/event_wal.rs` optional `pool_id` refresh section + framing constants + strict section decode; `segment/lifecycle.rs` `fold_refresh` + `request_refresh_metadata` `pool_id` parameter; new `segment/relocate.rs` (`SegmentRelocator`, `RelocateError`, `is_startup_residue`); `segment/data_store.rs` explicit-target guarded writer `write_segment_data_to_pool_guarded` + store-level `purge_reader_cache`; `lib.rs` exports `SegmentRelocator`, `RelocateError`, `is_startup_residue` |
| `oceanfs-node` | additive touch (Deviation *Node/durability additive touch*): the once-per-boot residue sweep in `modules/storage.rs` delegates its per-file decision to the shared `oceanfs_storage::is_startup_residue` (:839); `None` `pool_id` arg added at two `request_refresh_metadata` call sites (`modules/wal_recovery.rs:870`, `repair.rs:581`). Composition-root wiring of the relocator is deferred to d3 |
| `oceanfs-durability` | `None` `pool_id` arg added at two `request_refresh_metadata` call sites (`heal/worker.rs:443`, `repair.rs:511`) |
| `oceanfs-core` | (none — `SegmentMetadata.pool_id` already exists in `types/metadata.rs:143`) |

## Interface (Public API)

- `request_refresh_metadata(id: SegmentId, merkle_root: Option<HashOutput>,
  storage_locations: Option<SmallVec<[NodeId; 16]>>, pool_id: Option<u32>)`
  — additive `pool_id` parameter on the existing coordinator method
  (`crates/oceanfs-storage/src/segment/lifecycle.rs:2200`); existing
  callers pass `None`.
- `pub struct SegmentRelocator` (relocate.rs:123) — constructed by
  `SegmentRelocator::new(coordinator: Arc<SegmentLifecycleCoordinator>,
  store: Arc<DiskSegmentStore>)` (relocate.rs:131) over the lifecycle
  coordinator + the concrete unified store; `pub async fn
  relocate(&self, id: SegmentId, target_pool_id: u32) -> Result<(),
  RelocateError>` (relocate.rs:162). `lock_segment` and
  `write_segment_data_guarded` were already `pub`; the explicit-target
  copy uses the crate-private `write_segment_data_to_pool_guarded`, so
  the relocator stays same-crate. Composition-root wiring is deferred to
  d3.
- Reader-cache purge is a store-level seam, not a new reader method:
  `DiskSegmentStore::purge_reader_cache(id)` (data_store.rs:353,
  `pub(crate)`) forwards to the pre-existing `SegmentReader::purge_cache`
  trait default (io/segment_reader.rs:90); `SegmentRelocator` invokes it
  after the durable commit (relocate.rs:220). No `purge_pool_root`, no
  notifier hook, no direct `DiskSegmentReader` reference (Resolved
  Decision 2 / Deviation *Reader-cache purge seam*).
- `pub fn is_startup_residue(state: Option<SegmentState>,
  authoritative_pool_id: u32, found_pool_id: u32) -> bool` (relocate.rs:266)
  — exported classifier the node boot sweep calls for each `.dat` found
  on a data-pool root.
- `pub enum RelocateError { TargetPoolMissing, TargetNotDataRole,
  SamePool, NotSealed, NotHeldLocally, SourceFileMissing, … }` —
  deterministic, worker-visible error taxonomy so d3/d4 can block with a
  precise reason.

## Data Flow

```
worker (d3) or operator ──▶ SegmentRelocator::relocate(id, target_pool_id)
   ├─ lock per-segment write lock (DiskSegmentStore::write_locks)
   ├─ copy .dat → target pool root (write_dat_atomic; explicit target, resolve_pool skipped)
   ├─ commit MetadataRefreshEvent { pool_id = Some(target) }      ← durable (event-WAL fsync)
   │    └─ fold updates SegmentMetadata.pool_id (lifecycle registry)
   │    └─ purge reader pool_root_cache(segment_id)
   ├─ unlink source-root .dat
   └─ unlock; capacity refresh sees source free↑ / target free↓

crash windows:
  after copy / before commit ──▶ source authoritative; target file = unregistered residue → reaped at boot, drain re-runs
  after commit / before unlink ──▶ target authoritative; source file  = unregistered residue → reaped at boot
reads: resolve by registry pool_id → atomic source→target switch at the commit
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in
      `oceanfs-storage` (and `oceanfs-node` if wiring lands there).
- [x] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      passes; new tests cover every `pub` API path and the scenario list
      in Scope — including the backward-compatible decode of a
      pool_id-less refresh record (byte-exact), the fold update, the
      end-to-end relocate (file moved, registry `pool_id` flipped, reader
      root re-resolved), the error taxonomy, per-segment-lock
      serialization, and the **crash-matrix windows** (pre-commit /
      post-commit restarts leave exactly one authoritative copy; the other
      is reaped as unregistered residue with no data loss).
<!-- REVIEW (iter 2): crash windows are end-to-end restarts — `precommit_crash_restart_fold_keeps_source_and_boot_sweep_reaps_target` (relocate.rs:611) and `postcommit_crash_restart_fold_keeps_target_and_boot_sweep_reaps_source` (:660): build the crash state, drop the env, `cold_restart` (:409 — reopen EventWal on the same dirs + `rebuild_from_events` boot fold), then `run_boot_sweep` (:423 — the modules/storage.rs loop: list root → `is_startup_residue` → `delete_shards_with_pool`) and assert exactly the authoritative `.dat` survives, byte-identical. Verified: 497 lib tests, 106 doctests green; the 7 relocate/crash/fold tests (6 in relocate.rs + fold Some/None lifecycle.rs:3303) and the 2 event-encoding tests pass. The iter-1 count slip (report said 3 vs 2 new encoding #[test] fns) is corrected. -->
- [x] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the refresh-record framing docs at `event_wal.rs` (constants
      `:125-139`) are updated for the optional section.
<!-- REVIEW: verified `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` green on storage/durability/node; framing docs + REFRESH_FLAG_* / REFRESH_POOL_ID_SIZE constants documented (event_wal.rs:180-189); the two runnable new doctests (RelocateError, is_startup_residue) pass; SegmentRelocator examples are `ignore` (non-gating per Lint note). -->
- [x] **ADR:** ADR-0036 D2 (optional `pool_id` on `MetadataRefreshEvent`,
      one durable writer, length-discriminated backward-compatible decode,
      reader cache purge) and D3 (copy→commit→unlink, both crash windows
      safe, registry-driven switch) satisfied; ADR-0025 (event-WAL is the
      segment-state writer), ADR-0030 (the refresh-payload extension
      pattern being reused), ADR-0032 D3 (unified store per-segment write
      lock), ADR-0034 (registry enumeration only; no disk scans; the 
      residue rule is boot-sweep/orphan-reaper-only) satisfied.
- [x] **Perf:** frontmatter `perf: []`; prose constraints: one registry
      snapshot/fold per relocate (rare background op, perf rule 7.1);
      reader cache purge is O(1) per segment (`HashMap` remove); the copy
      uses the existing buffered/atomic write path — no new allocation
      regime; no accounting delta.
- [x] **Integration:** integration test at the storage crate boundary
      exercises a complete relocate under concurrent reads (continuous
      correctness across the commit switch, byte-identical read-back),
      plus the crash-window scenario against a real event-WAL + boot
      residue sweep. **No load suite is run locally** (PIPELINE §6).
<!-- REVIEW (iter 2): DoD integration gap CLOSED. The concurrent-read relocate test passes (crates/oceanfs-storage/tests/segment_relocate.rs:93; storage integration 11/11 binaries green). The crash-window scenario is now a genuine restart + fold + boot sweep: relocate.rs:611/:660 drop the env after a simulated pre-/post-commit crash, `cold_restart` reopens the EventWal and replays the fold via `lifecycle.rebuild_from_events`, then `run_boot_sweep` (:423) replicates the node sweep loop (list each data-pool root → `is_startup_residue(state, entry.pool_id, found_pool)` → `delete_shards_with_pool`) and asserts exactly the authoritative `.dat` survives while the mismatched copy is reaped (byte-identical read-back). The sweep exercises the same shared classifier `is_startup_residue` (relocate.rs:266) that crates/oceanfs-node/src/modules/storage.rs:839 now calls — the node loop's only d2 change is that classifier swap. Residual LOW (non-blocking): the sweep loop in the test is a storage-level replica; no node-level test boots the literal `StorageModule::run_startup_recovery` over a d2 pool-mismatch residue, but the d2-specific decision logic (the classifier) is fully exercised against real roots/files. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Resolved Decisions

Recorded 2026-09-08 at final spec close. The four questions posed under
"Open Questions for the Implementer" are resolved; where the outcome is
also recorded under [Deviations (accepted)](#deviations-accepted), the
cross-reference is given.

1. **Where the relocate operation lives.** A same-crate `SegmentRelocator`
   over `Arc<SegmentLifecycleCoordinator>` + concrete
   `Arc<DiskSegmentStore>` (relocate.rs:123-136). The store seams
   `lock_segment` (data_store.rs:185) and `write_segment_data_guarded`
   (data_store.rs:209) were already `pub` — no visibility lift was needed;
   the new explicit-target guarded writer
   `write_segment_data_to_pool_guarded` (data_store.rs:321, `pub(crate)`)
   lands the copy on the target root before the commit. The node
   retaining the concrete store `Arc` for the composition root is
   deferred to d3.
2. **Reader-cache purge mechanism.** A store-level
   `DiskSegmentStore::purge_reader_cache(id)` (data_store.rs:353)
   forwarding to the pre-existing `SegmentReader::purge_cache` trait
   default seam (io/segment_reader.rs:90), invoked by `SegmentRelocator`
   after the durable commit (relocate.rs:220). No node notifier; no
   direct `DiskSegmentReader` reference is needed. (Deviation:
   *Reader-cache purge seam*.)
3. **Residue-reap ordering on the boot sweep.** The sweep's rule is
   extended so a `Sealed` `.dat` whose authoritative pool differs from the
   root it is found on is residue — `oceanfs_storage::is_startup_residue`
   (relocate.rs:266) — and `Reserved` files are deliberately left to the
   data-WAL row-3 adoption path. The node boot sweep now calls the shared
   storage classifier (modules/storage.rs:839). (Deviations:
   *Node/durability additive touch* and *Residue-sweep loop location*.)
4. **`relocate` re-entry after a crash.** The explicit-target copy uses
   the store's atomic temp+rename write (`write_dat_atomic`,
   data_store.rs:535), which idempotently overwrites a leftover target
   residue — re-running the drain after a pre-commit crash is safe.

> **Cross-note (d4 close, 2026-09-09):** the relocate commit now passes
> the **source merkle root explicitly** (`request_refresh_metadata` with
> `entry.metadata.merkle_root`, relocate.rs:215-223) because the refresh's
> merkle parameter is a **value replacement** (`None` clears the anchor) —
> a pool_id-only commit previously cleared the seal-time anchor, a latent
> anchor-clear fixed during d4's Option-A work (see
> [d4-cluster-drain.md](d4-cluster-drain.md) Resolved Decisions 6 /
> Deviations a).

## Deviations (accepted)

Recorded 2026-09-08 after implementation review (PASS) — decisions
validated by the stakeholder before implementation.

- **Node/durability additive touch.** Contrary to the proposal's Crate
  Impact estimate "`oceanfs-node` (none direct)", d2 necessarily touches
  `oceanfs-node/src/modules/storage.rs` (the once-per-boot residue sweep
  now delegates its per-file decision to the shared
  `oceanfs_storage::is_startup_residue`, storage.rs:839 — required by the
  Scope crash-window residue rule) and adds the `None` pool_id argument at
  four `request_refresh_metadata` call sites
  (`oceanfs-node/src/modules/wal_recovery.rs:870`,
  `oceanfs-node/src/repair.rs:581`,
  `oceanfs-durability/src/heal/worker.rs:443`,
  `oceanfs-durability/src/repair.rs:511`). This is a consequence of the
  additive-parameter Interface (existing callers pass `None`) plus the D2
  boot-sweep residue rule, consistent with the Scope/Interface prose.
  The Crate Impact table above reflects this touch.
- **Reader-cache purge seam.** The Interface's `purge_pool_root` /
  notifier option was implemented as `DiskSegmentStore::purge_reader_cache`
  (`data_store.rs:353`) calling the pre-existing `SegmentReader::purge_cache`
  default seam (`io/segment_reader.rs:90`), invoked by `SegmentRelocator`
  after the durable commit; the injected reader (`TrackingReader` in
  relocate.rs tests) records the purge, proving the cache is invalidated.
- **Residue-sweep loop location.** The crash-window tests run a
  storage-level replica of the node's boot sweep loop (relocate.rs:423)
  using the same shared classifier `is_startup_residue` + store APIs; no
  node-level test boots `StorageModule::run_startup_recovery` over a d2
  pool-mismatch residue. Accepted: the d2 decision logic (the classifier)
  is what the node sweep change introduced and is fully exercised.
- **Decoder strict-tail change.** The extended `MetadataRefresh` section
  parser is now strict at the tail: an unknown flag bit or trailing bytes
  after the last declared section reject the record (event_wal.rs:695-777,
  cursor == payload-end check at :766-770; regression test
  `metadata_refresh_rejects_unknown_flags_and_trailing_bytes`, :2016) —
  closing the previously permissive end of the ADR-0030 extended decode,
  which silently accepted trailing bytes after the declared sections.
  Legacy ADR-0030 records parse identically (their bytes are unchanged),
  so the tightening only rejects records that were malformed-but-tolerated;
  the Scope's backward-compatible "no section decodes to `None`" promise
  is unaffected.
- **Copy semantics: store read path + synthesized v1 header.** The copy
  step is not a raw `.dat` byte copy: `SegmentRelocator` reads the source
  data section through the store trait read path
  (`read_segment_data`, data_store.rs:411 — header-verified, data-only)
  and writes the target through the explicit-pool atomic writer, which
  synthesizes a fresh v1 header over the data — 76 bytes, `blob_count 0`,
  `index_offset` at the data end, checksum over the data — rather than
  carrying the source file's header bytes over (`write_dat_at_root`
  data_store.rs:241 + header synthesis at :252-257; `v1_header_bytes`
  :511; `write_dat_atomic` :535; relocate copy step relocate.rs:194-207).
  The target is the same normalized v1 file the existing whole-file write
  path (heal/AE/re-rep) always produces; read-back is byte-identical at
  the segment-data layer (asserted end-to-end by the unit and crash
  tests).
