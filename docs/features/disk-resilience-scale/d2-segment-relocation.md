---
feature: "Segment Relocation (Durable pool_id Mutation)"
epic: "disk-resilience-scale"
status: proposed
priority: high
owner: ""
dependencies: ["d1-pool-drain-state"]
adr: [0036, 0025, 0030, 0032, 0034]
perf: []
created: 2026-09-07
updated: 2026-09-07
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
| `oceanfs-storage` | `segment/event_wal.rs` optional `pool_id` refresh section + framing constants + decode; `segment/lifecycle.rs` `fold_refresh` + `request_refresh_metadata` `pool_id` parameter; new `segment/relocate.rs` (`SegmentRelocator`); `segment/data_store.rs` explicit-target copy seam + guarded unlink; `io/segment_reader.rs` `purge_pool_root` (or notifier hook) |
| `oceanfs-node` | (none direct — wiring of the relocator into the composition root for d3/d4; a reader-purge notifier closure may be wired here if the reader lives outside the relocator) |
| `oceanfs-core` | (none — `SegmentMetadata.pool_id` already exists in `types/metadata.rs:143`) |

## Interface (Public API)

- `request_refresh_metadata(id: SegmentId, merkle_root: Option<HashOutput>,
  storage_locations: Option<SmallVec<[NodeId; 16]>>, pool_id: Option<u32>)`
  — additive `pool_id` parameter on the existing coordinator method
  (`crates/oceanfs-storage/src/segment/lifecycle.rs:2100`); existing
  callers pass `None`.
- `pub struct SegmentRelocator` — constructed from the lifecycle
  coordinator + the unified store; `pub async fn relocate(&self,
  id: SegmentId, target_pool_id: u32) -> Result<(), RelocateError>`.
  `<!-- TODO(spec): verify anchor -->` The exact constructor/wiring seam
  (whether the relocator lives on the coordinator, the store, or a new
  module; whether it needs `lock_segment` exposed `pub(crate)`) is for the
  implementer to confirm against `data_store.rs:185` and the composition
  root's builder ordering (`crates/oceanfs-node/src/modules/storage.rs`,
  `modules/durability.rs`).
- `DiskSegmentReader::purge_pool_root(&self, segment_id: SegmentId)` — new
  pub method clearing the `pool_root_cache` entry
  (`crates/oceanfs-storage/src/io/segment_reader.rs:170`), OR a
  `PoolIdChangedNotifier` hook in the `StorageLocationsNotifier` pattern
  (`lifecycle.rs:1351`).
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

- [ ] **Code:** `cargo build --all-targets` succeeds in
      `oceanfs-storage` (and `oceanfs-node` if wiring lands there).
- [ ] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      passes; new tests cover every `pub` API path and the scenario list
      in Scope — including the backward-compatible decode of a
      pool_id-less refresh record (byte-exact), the fold update, the
      end-to-end relocate (file moved, registry `pool_id` flipped, reader
      root re-resolved), the error taxonomy, per-segment-lock
      serialization, and the **crash-matrix windows** (pre-commit /
      post-commit restarts leave exactly one authoritative copy; the other
      is reaped as unregistered residue with no data loss).
- [ ] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the refresh-record framing docs at `event_wal.rs` (constants
      `:125-139`) are updated for the optional section.
- [ ] **ADR:** ADR-0036 D2 (optional `pool_id` on `MetadataRefreshEvent`,
      one durable writer, length-discriminated backward-compatible decode,
      reader cache purge) and D3 (copy→commit→unlink, both crash windows
      safe, registry-driven switch) satisfied; ADR-0025 (event-WAL is the
      segment-state writer), ADR-0030 (the refresh-payload extension
      pattern being reused), ADR-0032 D3 (unified store per-segment write
      lock), ADR-0034 (registry enumeration only; no disk scans; the 
      residue rule is boot-sweep/orphan-reaper-only) satisfied.
- [ ] **Perf:** frontmatter `perf: []`; prose constraints: one registry
      snapshot/fold per relocate (rare background op, perf rule 7.1);
      reader cache purge is O(1) per segment (`HashMap` remove); the copy
      uses the existing buffered/atomic write path — no new allocation
      regime; no accounting delta.
- [ ] **Integration:** integration test at the storage crate boundary
      exercises a complete relocate under concurrent reads (continuous
      correctness across the commit switch, byte-identical read-back),
      plus the crash-window scenario against a real event-WAL + boot
      residue sweep. **No load suite is run locally** (PIPELINE §6).

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **Where the relocate operation lives.** The coordinator appends events
  but has no store write-lock access; the store has the lock but no
  coordinator. The sketch's "dedicated store method" cannot commit alone,
  so the primitive must be an orchestration over both. Recommend a new
  same-crate `SegmentRelocator` (this doc's Interface), but confirm the
  seam — specifically whether `DiskSegmentStore::lock_segment` /
  `write_segment_data_guarded` need a visibility lift to `pub(crate)` and
  how the composition root constructs the relocator after the
  coordinator's event-WAL is attached (`lifecycle.rs:1312`
  `with_event_wal`).
- **Reader-cache purge mechanism.** Purge via a direct
  `DiskSegmentReader::purge_pool_root` call (needs the reader Arc inside
  the relocator) vs. a notifier hook in the `StorageLocationsNotifier`
  pattern. If the reader is node-wired, the node must pass the purge
  callback; decide and record.
- **Residue-reap ordering on the boot sweep.** The doc asserts pre-commit
  target residue and post-commit source residue are both registry-unknown
  and safely reapable. Verify the boot residue sweep's exact rule
  (`modules/storage.rs` `run_startup_recovery`) treats an
  event-WAL-registered-but-fileless source correctly on the post-commit
  window (the registry says target; the source `.dat` must be reaped as
  residue, never interpreted as a data loss for the target).
- **`relocate` re-entry after a crash.** After a pre-commit crash the
  drain re-runs and re-copies; confirm re-copy over a leftover target
  residue is idempotent (the atomic write path overwrites via temp+rename).

## Deviations (accepted)

None yet — this document is proposed. Expected-deviation candidates from
the sketch's open questions (each is recorded and resolved during
implementation): the wire encoding of the optional section (chosen
length-discriminated form + framing constants), the no-accounting-delta
confirmation, and the explicit-pool-override vs. dedicated store method
placement (the doc chooses the dedicated same-crate relocator over a
store method because the event commit needs the coordinator).
