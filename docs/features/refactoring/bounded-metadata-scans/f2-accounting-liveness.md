---
feature: "f2: Accounting-Based GC Liveness & Fully-Dead Orphan Reaper"
epic: "refactoring/bounded-metadata-scans"
status: done
priority: critical
owner: ""
dependencies:
  - feature: f1-supersede-capture
    epic: refactoring/bounded-metadata-scans
    reason: GC liveness and orphan detection consume the dead-chunk records (tombstones + supersedes) that f1 captures and enumerates via MetadataStore::list_dead_chunk_records_all; supersede records must exist and be classifiable before accounting can trust them
  - feature: f3-seal-membership-list
    epic: refactoring/bounded-metadata-scans
    reason: ADR-0034 D1's live = logical_total − dead requires the durable SegmentMetadata.total_bytes that f3 records at seal; today the registry stores no total and GC seeds register_segment(id, 0)
  - epic: refactoring/legacy-mode-removal
    reason: The orphan reaper's phase-2b unregistered-.dat sweep is retired because ADR-0031/0032 enforcement makes unregistered .dat writers unreachable (the receiver registers; the last unregistered writer is purged)
adr:
  - 0034-bounded-metadata-accounting
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf:
  - "7.1 minimize lock hold duration"
  - "1.4 SmallVec for small metadata structures"
created: 2026-09-04
updated: 2026-09-06
---

# f2: Accounting-Based GC Liveness & Fully-Dead Orphan Reaper

## Summary

ADR-0034 D3/D4 replaces the two object-*counting* consumers with byte
*accounting*:

- **GC liveness (D3).** `process_tombstones`'s Phase 2
  (`gc/garbage_collector.rs:518-543`) currently calls
  `metadata.list_objects_all_with_bucket()` to accumulate live bytes per
  segment from every surviving object row — O(all objects) per GC cycle.
  Under D3 the tracker registers segments from the ADR-0025 registry (each
  entry now carries `total_bytes`, recorded at seal by f3) and marks dead
  from the **aged** dead-chunk captures (plain tombstones + supersedes, both
  enumerated by f1's `list_dead_chunk_records_all`). live = total − dead.
  The `LivenessTracker` counting model is reworked; no `list_objects_all`
  call remains on the GC path.
- **Orphan reaper (D4).** `orphan(S) := dead_bytes(S) ≥ logical_total(S)` is
  now derivable from accounting. The reaper iterates the registry, applies
  the same TTL gate, and deletes fully-dead segments — **without** building a
  referenced set from all objects. `build_referenced_set`
  (`gc/orphan_reaper.rs:294-313`) is deleted. The historical phase-2b
  "unregistered `.dat`" sweep (`orphan_reaper.rs:149-176`) is obsolete once
  lifecycle registration is enforced everywhere (ADR-0031/0032) and is
  removed.

External behavior is unchanged: same TTL grace, same deletion semantics,
same metric semantics — only the detection source changes.

## Scope

### In Scope

**A. Shared accounting helper (`oceanfs-durability/src/gc`)**

- A small shared pass over the registry + f1's `list_dead_chunk_records_all`
  computing `dead_bytes: HashMap<SegmentId, u64>` for **aged** records only
  (`now_ms − record.captured_at > tombstone_ttl_ms`), so GC and the orphan
  reaper implement D3/D4 once. Prefer extending
  `gc/liveness_tracker.rs` (the natural home) over a new module.

**B. GC liveness from accounting (D3)** — `gc/garbage_collector.rs`

- `process_tombstones` (`:453-545`) reworked:
  - Phase 1 unchanged in spirit: `registry.for_each` registers every live
    segment — but with the durable total:
    `tracker.register_segment(id, entry.metadata.total_bytes)`
    (`:467-471`), instead of `register_segment(id, 0)` + later counting.
  - Phase 2 replaced: iterate `metadata.list_dead_chunk_records_all()` (the
    f1 enumeration) instead of `list_objects_all_with_bucket()` (`:523`).
    Classify each record:
    - `kind: Tombstone`, aged → today's behavior: insert
      `(bucket, key)` into `eligible_keys`, `tracker.mark_dead(chunks)`,
      remember the record for post-compaction cleanup;
    - `kind: Supersede`, aged → `tracker.mark_dead(chunks)` **only**. A
      supersede key is LIVE — it must never enter `eligible_keys` (the
      compaction dead-object filter) and never trigger a row delete;
      remember the record for post-compaction cleanup.
  - The old Phase 2 "re-PUT race" arm (`:525-536`, which treated a surviving
    row whose key matched an eligible tombstone as dead and queued it for
    row deletion) is deleted: f1's re-PUT already migrates the cleared
    tombstone's chunks into a supersede and clears the plain tombstone, so a
    live re-PUT row is never in `eligible_keys` to begin with.
- The `TombstoneResult` shape (`:78-79`) extends to carry, per compacted
  segment, the **supersede records** (bucket, key, version) alongside the
  existing tombstone keys, so the cleanup loop can delete dead-chunk records
  whose bytes a compaction reclaimed.
- Post-compaction cleanup (`run_cycle` result loop, `:388-404`):
  - plain-tombstone keys: unchanged (delete tombstone, then the GC-driven
    row delete `delete_object(bucket, key, Hlc::zero())` — D6 "GC-driven
    delete of a tombstoned key");
  - supersede records whose chunks referenced the compacted segment: delete
    the dead-chunk record only (via a new store method, see D) — **never** a
    row delete (the object is live).
- `LivenessTracker` (`gc/liveness_tracker.rs`) reworked:
  - `register_segment(id, total_size)` keeps initializing
    `live_bytes = total_size` — now fed the real f3 total;
  - `mark_dead(chunk)` unchanged (saturating subtract from live);
  - `add_live_bytes` (`:36-39`) is deleted (it existed to accumulate the
    object scan);
  - `liveness_ratio`/`compaction_candidates`/`dead_bytes_for` unchanged —
    live = total − dead falls out of the existing math. `dead_bytes_for` now
    reflects **aged captures only**, matching the reclaimable-by-TTL
    semantics GC always had.
- Stats parity: `stats.segments_scanned = tracker.known_segments.len()`,
  `dead_bytes`/`live_bytes` sums (`:255-257`) unchanged in meaning.

**C. Orphan reaper = fully-dead detection (D4)** — `gc/orphan_reaper.rs`

- `run_cycle` (`:120-262`) reworked:
  1. Build `dead_bytes` over the registry + aged captures via the shared
     helper (A). O(live segments) + O(aged dead-chunk records) — both
     bounded; no objects-CF access.
  2. For each registry entry (`:136-147`), `orphan` iff
     `dead_bytes(S) ≥ entry.metadata.total_bytes` **and** the segment is past
     the TTL grace (`now_ms − sealed_at > ttl_ms`) — the same grace as today.
     Track `(segment_id, pool_id)` as today.
  3. **Phase 2b deleted**: the on-disk unregistered-`.dat` sweep
     (`:149-176`) goes away — after ADR-0031/0032, `.dat` files without a
     registry entry cannot exist in the normal path, and sweeping "files the
     registry does not know" is the legacy-mode assumption this epic retires.
     `store.list_segment_files()` is no longer part of the reaper's cycle.
  4. The double-check loop (`:197-256`) drops the referenced-set re-scan; the
     delete-before-unlink flow through `lifecycle.request_delete` then
     `store.delete_shards_with_pool` is unchanged (ADR-0024 invariant 3). A
     bounded re-check of the *cycle snapshot* (dead ≥ total for that segment)
     may remain — never a store rescan.
- `build_referenced_set` (`:294-313`) deleted; the `[review][architectural][high]`
  marker at `:297` is closed.
- `metadata.list_objects_all()` has no remaining caller in the reaper.

**D. Dead-chunk record deletion (`oceanfs-storage` + storage-api)**

For post-compaction supersede cleanup the store needs a targeted delete:

- `RocksDbMetadataStore::delete_dead_chunk_record(bucket, key, version: Hlc)`
  — deletes the exact versioned supersede key (reconstructs the key from f1's
  encoding). Concrete store method + trait default in
  `oceanfs-storage-api::MetadataStore` (`delete_dead_chunk_record` defaulting
  to `Ok(())`) so GC test doubles stay minimal.
- (Plain tombstones continue to use `delete_tombstone`; the f1 `DeadChunkRecord`
  for supersedes carries the `version` needed to reconstruct the key.)

**E. Test rework (D4/D6 ownership)**

- Orphan-reaper tests pinned to the old detection source are reworked:
  - `referenced_set_contains_segment_ids` (`orphan_reaper.rs:945`),
    `referenced_set_empty_for_no_objects` (`:980`),
    `double_check_correctly_identifies_referenced_segments` (`:673`) —
    replaced by fully-dead accounting tests (delete/overwrite all objects →
    captured dead == total → orphan after TTL);
  - `sweeps_unregistered_on_disk_segments` (`:726`) — deleted with phase 2b;
  - the non-default-bucket liveness test (`object_in_non_default_bucket_keeps_segment_alive`,
    `:465`) is preserved in spirit (a live object in any bucket keeps its
    segment alive under accounting — captures never reach total).
- GC tests asserting Phase-2 object-count behavior are reworked to assert
  accounting parity (live = total − aged dead), keeping the metric semantics
  tests intact.

### Out of Scope (for this feature)

- Compactor discovery (`find_objects_in_segment` / membership list) — f3.
- The compaction-remap notification key list and healing `repoint_objects`
  — f4.
- Startup compaction-recovery `StoreObjectLookup` (`compaction_recovery.rs`)
  — startup-only, per marked unit; not one of the four ADR-0034 scans.
- Supersede-capture write side — f1 (must be in before this feature consumes
  its records).
- Any change to the tombstone TTL config or the delete grace.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `gc/garbage_collector.rs` (process_tombstones from accounting; cleanup loop for supersede records; pass totals), `gc/liveness_tracker.rs` (register from totals; delete `add_live_bytes`), `gc/orphan_reaper.rs` (fully-dead detection; delete `build_referenced_set` + phase 2b), tests |
| `oceanfs-storage` | `metadata/store.rs` (new `delete_dead_chunk_record`), tests |
| `oceanfs-storage-api` | New default `MetadataStore::delete_dead_chunk_record` |
| `oceanfs-node` | Reaper wiring unchanged (same constructor/cycle); integration tests reworked |

## Interface (Public API)

- `oceanfs_storage_api::MetadataStore::delete_dead_chunk_record(&self, bucket:
  &BucketId, key: &ObjectKey, version: Hlc) -> std::io::Result<()>` (default
  `Ok(())`) — deletes one versioned supersede record.
- `RocksDbMetadataStore::delete_dead_chunk_record` — concrete impl.
- Behavior contract (public semantics, unchanged):
  - `GarbageCollector::run_cycle` computes `segments_scanned`/`dead_bytes`/
    `live_bytes`/`segments_compacted` with the same meaning as today;
  - `OrphanReaper::run_cycle` keeps the same TTL grace and deletion flow;
    a fully-dead (but not TTL-aged) segment is left alone, exactly as today.

## Data Flow

```
GC cycle (garbage_collector.rs:232)
  Phase 1  registry.for_each → tracker.register_segment(id, total_bytes)   [f3 total]
  Phase 2  metadata.list_dead_chunk_records_all()                          [f1 records]
             │  plain Tombstone, aged → eligible_keys + mark_dead + cleanup note
             │  Supersede,    aged → mark_dead + cleanup note (never eligible,
             │                        never a row delete)
             ▼
  tracker:  live = total − dead (saturating); compaction_candidates(threshold)
  compact → after compaction: delete plain tombstone + GC row delete (today),
             and delete the aged supersede records that referenced the segment
             (delete_dead_chunk_record) — the live row is untouched.

Orphan reaper cycle (orphan_reaper.rs:120)
  dead_bytes = shared accounting over registry + aged captures
  for each registry entry: orphan iff dead(S) ≥ total_bytes(S) AND past grace
  phase 2b unregistered-.dat sweep: REMOVED
  reclaim: lifecycle.request_delete → delete_shards_with_pool (unchanged)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`,
      `oceanfs-storage`, `oceanfs-storage-api`, `oceanfs-node`. `grep -rn
      "list_objects_all_with_bucket\|list_objects_all" crates/oceanfs-durability/src/gc
      crates/oceanfs-durability/src/healing_service.rs` shows **no** remaining
      call on the GC/reaper/remap paths; `build_referenced_set` is gone.
      <!-- REVIEW: verified 2026-09-06 — builds pass for all four crates;
      the literal grep returns nothing (only the trait/impl declarations
      remain in storage(-api), with zero callers anywhere in crates/);
      build_referenced_set exists only as historical doc prose in
      healing_service.rs:73 and coordinator.rs:985. -->
- [x] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      (PIPELINE.md §4.6) pass, adding:
      - D6 "DELETE then idle": delete an object → captured dead bytes == old
        chunk bytes and the segment's liveness drops past the threshold once
        the tombstone ages;
      - D6 "PUT overwrite (old on segment A, new on B)": after the supersede
        ages, A's dead bytes equal the superseded chunk bytes and B stays
        live — no leak;
      - D6 "GC-driven delete of a tombstoned key": the GC cleanup path
        deletes the tombstone and the row for a plain-tombstoned key and
        does **not** double-count its chunks;
      - supersede cleanup: after compacting a segment, the aged supersede
        records referencing it are deleted and the (live) object row for the
        key survives;
      - reaper fully-dead: all objects in a segment deleted AND TTL-aged →
        orphan found; a partially-dead segment (dead < total) is never
        orphaned even when nothing references its dead region; a
        non-default-bucket object keeps its segment alive;
      - reaper phase-2b: the on-disk unregistered sweep is gone —
        `sweeps_unregistered_on_disk_segments` deleted, no remaining
        `list_segment_files` call on the reaper cycle;
      - parity pins: for fixtures with only plain tombstones, `GcStats`
        (`segments_scanned`/`dead_bytes`/`live_bytes`) and reaper
        `orphans_found` match the pre-f2 values.
      Then `cargo test -p oceanfs-node --test orphan_reaper -- --test-threads=1`
      and `cargo test -p oceanfs-node --test gc_compaction -- --test-threads=1`.
      <!-- REVIEW: verified 2026-09-06 (iteration 2) — durability lib
      (258), storage lib (458), node lib (66), durability gc_compaction
      (5), durability orphan_reaper (7), node orphan_reaper (8), node
      gc_compaction (7), data_pool_placement
      two_data_pool_node_roundtrip_gc (1), and segment_replication (3)
      all pass under --test-threads=1. The formerly flaky
      supersede_cleanup_after_compaction_deletes_record_keeps_live_row
      (garbage_collector.rs:1183) is now deterministic: the fixture sleeps
      5 ms after the v2 overwrite (garbage_collector.rs:1268) so the
      now_ms()-stamped supersede capture always satisfies the strict
      `now − captured_at > ttl` aged filter (TTL-0 config) before
      run_cycle; re-verified 5/5 isolated and green inside two full
      258-test lib runs. The new GC-side unknown-total pin
      unknown_total_segment_never_a_compaction_candidate
      (garbage_collector.rs:1332) passes: asserts dead_bytes > 0 (aged
      capture observed) with segments_compacted == 0 and the registry
      entry untouched. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `LivenessTracker`,
      `process_tombstones`, and `OrphanReaper::run_cycle` docs describe the
      accounting model (live = total − dead; orphan = dead ≥ total).
      <!-- REVIEW: verified — `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
      -p oceanfs-durability -p oceanfs-node -p oceanfs-storage
      -p oceanfs-storage-api` passes; all four lib.rs deny(missing_docs);
      the three named items carry accounting-model doc comments
      (liveness_tracker.rs:11-26, garbage_collector.rs:493-509,
      orphan_reaper.rs:123-140). -->
- [x] **ADR:** ADR-0034 D3 (no `list_objects_all` on the GC path; liveness
      from registry + aged captures) and D4 (reaper detects fully-dead from
      accounting, `build_referenced_set` deleted, phase-2b retired) satisfied;
      ADR-0025 registry remains the segment set; ADR-0032 unified store
      respected (no new store instances).
      <!-- REVIEW: verified — GC registers every registry entry with
      entry.metadata.total_bytes and consumes only the shared aged feed
      (garbage_collector.rs:527-537); reaper derives orphanhood from the
      same feed + registry totals and its cycle never calls
      list_segment_files (orphan_reaper.rs:145-272); startup
      compaction-recovery is membership-driven StoreObjectLookup
      (compaction_recovery.rs:100-160) and both former
      list_objects_all_with_bucket sites in gc/ are gone; no new store
      instances (reaper consumes the injected shared data_store;
      OrphanReaper::new lost pool_roots at all 6 sites). -->
- [x] **Perf:** GC and the reaper are O(live segments) + O(aged dead-chunk
      records) per cycle — no objects-CF scan, no full-`Vec` materialization
      of the objects CF; registry lock holds stay per-entry (perf 7.1).
      <!-- REVIEW: verified — both cycles are registry.for_each +
      collect_aged_dead_chunk_records (one deletions-CF enumeration,
      bounded by aged records); the objects CF is never scanned (zero
      list_objects_all callers in crates/); records are enumerated before
      the registry iteration so no registry lock spans store I/O;
      per-entry work is map arithmetic only. -->
- [x] **Integration:** a full delete→overwrite→GC→reaper sequence on one node
      converges to the same reclaimed-bytes totals as the pre-ADR-0034 path on
      the same fixture; the D6 matrix rows owned by f2 run green with
      `--test-threads=1`.
      <!-- REVIEW: verified 2026-09-06 (iteration 2) — node orphan_reaper
      (8), node gc_compaction (7), durability gc_compaction (5),
      durability orphan_reaper (7), data_pool_placement
      two_data_pool_node_roundtrip_gc (1), and segment_replication (3)
      all run green under --test-threads=1; the D6 overwrite row is
      deterministic since the supersede GC pin fix (see Tests REVIEW
      above). The former MEDIUM residue-sweep gap is closed by design: a
      once-per-boot registry-residue `.dat` sweep in modules/storage.rs
      startup (storage.rs:658-722), running after the compaction-recovery
      action loop, lists each data pool root and unlinks any `.dat` whose
      lifecycle entry is Missing or Deleted (idempotent
      delete_shards_with_pool) — covering reaper orphan-reclaim residue,
      compactor fully-dead residue, AND compaction-recovery residue
      (including the SweepOldDat pure-residue case, which can no longer
      resolve a pool id). The reaper run_cycle comment
      (orphan_reaper.rs:211-219) states this accurately; run_cycle never
      lists the disk (list_segment_files appears only in the startup sweep
      and a test-local mock). -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).

## Implementation notes (2026-09-06)

Decisions and deviations recorded during implementation (ADR-0034 D3/D4).

- **D1 — `OrphanReaper` drops `pool_roots`.** Phase-2b retirement (D4)
  removes the only per-root disk-listing consumer; the constructor loses
  the `pool_roots` argument (public-API change at 6 sites — the one
  production caller is `crates/oceanfs-node/src/modules/durability.rs`).
  The reclaim unlink keeps using the registry entry's `pool_id` through
  the shared `data_store`; `run_cycle` never calls `list_segment_files`.
- **D2 — shared accounting feed in `gc/liveness_tracker.rs`:**
  `collect_aged_dead_chunk_records(metadata, now_ms, ttl_ms)` — the one
  `MetadataStore::list_dead_chunk_records_all` enumeration + one TTL
  filter (`now − captured_at > ttl`). GC's `process_tombstones` and the
  reaper's fully-dead pass both consume it, so GC and the reaper agree on
  what "aged/reclaimable" means.
- **D3 — unknown-total rule:** a Sealed entry with `total_bytes == 0`
  (row-3 adopt / repair copies, f3 notes) is registered and scanned but
  is NEVER a compaction candidate (belt-and-braces pre-spawn skip in
  `run_cycle` mirrors the membership filter) and NEVER an orphan. The
  `segments_scanned` stat counts all registry entries.
- **D4 — the reaper's double-check** is a bounded cycle-snapshot
  re-verification (`dead ≥ total` per candidate from the same map) — no
  store rescan. Delete-before-unlink through `request_delete` +
  `delete_shards_with_pool` is unchanged.
- **D5 — tests reworked to accounting fixtures.** Old-model unit tests
  (`referenced_set_*`, `double_check_*`, `sweeps_unregistered_on_disk_segments`,
  `process_tombstones_respects_ttl`, and the "no-capture orphan" fixtures)
  are deleted; the reaper suites now seed real `total_bytes`, real object
  rows, and chunk-carrying aged captures (`put_tombstone` with an ancient
  `deletion_time` — the deterministic `delete_object`-shaped capture).
  The node `orphan_reaper.rs`/`gc_compaction.rs` fixtures are direct-wiring
  and were adapted the same way; real write-path gates (`data_pool_placement`,
  `segment_replication`) pass unchanged because real seals carry totals and
  real deletes carry chunks.
- **D6 (revised per review) — no scan remains in `gc/`.** The startup
  compaction-recovery `ObjectLookup` (`gc/compaction_recovery.rs`) is now
  **membership-driven**: `StoreObjectLookup(registry, metadata)`
  point-reads the queried segment's own seal-time contained list
  (ADR-0034 D5/f3) instead of `list_objects_all_with_bucket`. The
  crash-harness `StoreLookup` test impl was deleted with the scan. Both
  earlier `list_objects_all_with_bucket` sites in `gc/` are gone — the
  epic's literal grep over `gc/` + `healing_service.rs` returns nothing.
- **Residue backstop moved to startup (not left to the reaper).** The
  old phase-2b sweep was the periodic backstop for `.dat` residue whose
  registry entry is gone or `Deleted` (a crash between `DeleteEvent` and
  the unlink — from the reaper's orphan reclaim, the compactor's
  fully-dead path, or a compaction recovery action). That backstop now
  lives in `modules/storage.rs`'s startup path as a once-per-boot
  **registry-residue `.dat` sweep**: it lists each data pool root and
  unlinks any `.dat` whose lifecycle entry is `Missing`/`Deleted`
  (`delete_shards_with_pool`, idempotent). Bounded by the on-disk file
  count, no objects-CF access, no periodicity — the ADR-0034 D4 shape.
  Reaper `run_cycle` never lists the disk.
- **Post-compaction cleanup (D3/D6):** plain-tombstone keys keep today's
  cleanup (`delete_tombstone` + GC row delete); aged supersede records
  referencing a compacted segment are deleted **by version** via the new
  `MetadataStore::delete_dead_chunk_record` (default `Ok(())`; RocksDB
  impl reconstructs the exact versioned supersede key) — the key's LIVE
  row is never deleted. A GC unit test
  (`supersede_cleanup_after_compaction_deletes_record_keeps_live_row`)
  and a store unit test
  (`delete_dead_chunk_record_removes_only_the_versioned_supersede`) pin it.
  On iteration 2 the GC unit test was made deterministic (it had flaked
  under `--test-threads=1`): the fixture now sleeps 5 ms after the v2
  overwrite (`garbage_collector.rs:1268`) so the `now_ms()`-stamped
  supersede capture always clears the strict `now − captured_at > ttl`
  aged filter (TTL-0 config) before `run_cycle`.
