---
feature: "Remove the Segments + Deleted-Markers Column Families; Move Consumers to the Machine"
epic: "refactoring/segment-event-log-lifecycle/segments-cf-removal"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: lifecycle-registry-coordinator
    epic: refactoring/segment-event-log-lifecycle/segment-lifecycle-machine
    reason: The machine's API (get/for_each/request_delete) is the consumers' new home; it must be stable before any consumer moves
  - feature: lifecycle-read-path
    epic: refactoring/segment-event-log-lifecycle/segment-lifecycle-machine
    reason: The read path must already resolve through the machine; after CF removal there is no CF-based fallback
  - feature: event-wal-format
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: WAL retention moves from CF-set membership to event positions; the event log must exist and be the coordinator's durable write first
  - feature: event-wal-recovery
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: Recovery is fold-based before the CF mirror is deleted; dual-read verification is the safety net this phase removes
  - feature: event-wal-checkpoint
    epic: refactoring/segment-event-log-lifecycle/segment-event-log
    reason: The checkpoint is the durable snapshot that replaces the CF's role; consumers and startup must have it before the CF disappears
adr:
  - 0025-segment-lifecycle-state-machine
  - 0024-segment-event-log
  - 0018-durability-wal-consolidation
  - 0023-metadata-store-native-replacement-path
  - 0009-storage-crate-split
perf:
  - "2.3 parking_lot::RwLock everywhere"
  - "7.1 Minimize lock hold duration"
  - "11.1 Atomic counters on hot paths"
created: 2026-08-17
updated: 2026-08-17
---

# Remove the Segments + Deleted-Markers Column Families; Move Consumers to the Machine

## Summary

Delete the `segments` and deleted-markers column families from RocksDB and
move every consumer onto the machine (ADR-0025 Decision 3, migration
phase 3). RocksDB keeps only `objects` + `deletions` (objects stay in
RocksDB — confirmed scope). Consumers move: WAL retention
(`cleanup_old_wal_files` / `file_contains_live_entries`,
`crates/oceanfs-storage/src/wal/replay.rs:309,426`) to event positions;
GC liveness to registry enumeration; scrub to the machine's `Sealed`
`merkle_root`; anti-entropy's startup incremental-tree rebuild to a machine
scan (superseding ADR-0018 Decision 1's segments-CF scan); the orphan
reaper to `request_delete` through the coordinator. The derived CF mirror
and its dual-read verification are deleted. This is the phase where the
three pre-phase defects' last storage home disappears: there is no CF left
to downgrade, no CF scan left to leak from, and no CF-derived state for
recovery to disagree with.

## Evidence/Motivation

The 2026-08-16/17 campaign's four defects were all ordering failures
between the segment WAL and the segments CF (ADR-0024 §Context). Phases 1–2
made the CF a derived mirror; this phase removes it:

- **Phantom-downgrade race / WAL leak (~3.8 GB/hour)** — the leak's
  protection decisions came from the CF-derived `durable_or_deleted` set
  (`replay.rs:349-465`). With the CF gone, protection comes from the event
  log/checkpoint by position; a "downgraded sealed entry" has no store to
  live in. The measured regression (protected 17 → 45 in 30 min) becomes a
  soak assertion: `protected` is flat.
- **Idle-seal gap** — the same leak mechanism; the machine's idle driver
  (phase 1) plus position-based sweep (phase 2) make an unsealed-forever
  segment unable to pin files.
- **Metadata-only compaction / BadDigest repack** — the compactor's
  side-effect ordering is pinned by `compaction-state-machine` (this
  feature moves its consumers: the reaper that finishes its crash rows, and
  the scrub/AE that verify its outputs against the machine's roots).

The CF has exactly one job — answer "is this segment sealed/deleted?"
durably — and the checkpoint does it in our own format, without RocksDB
(ADR-0023 direction; ADR-0025 §Context). ADR-0018 Decision 1's startup
"rebuild the Merkle tree from a segments-CF scan" is superseded here by a
machine scan (ADR-0025 §References).

## Scope

### In Scope

- Remove `segments` + deleted-markers CFs from the RocksDB schema and
  opening (`crates/oceanfs-storage/src/metadata/store.rs`, cf.rs); remove
  `put_segment`/`get_segment`/`list_segments`/deleted-marker methods and
  their RocksDB storage. The `MetadataStore` trait's segment methods are
  deleted (ADR-0009 boundary stands for objects); consumers use the
  machine's `get`/`for_each`/`request_delete` through the existing
  trait-in-consuming-crate boundary (ADR-0025 Decision 1).
- Move consumers:
  - **WAL retention:** `cleanup_old_wal_files` / `file_contains_live_entries`
    use the machine + `data_wal_pos` positions (entry garbage iff
    `SealEvent.data_wal_pos ≥ entry pos` or deleted) — no CF scan, no
    `durable_or_deleted` set.
  - **GC liveness:** enumeration via `registry.for_each` (was
    `list_segments()`); `garbage_collector.rs` / `liveness_tracker.rs`
    otherwise unchanged.
  - **Scrub:** `merkle_root` from the machine's `Sealed` entries (was
    `SegmentMetadata.merkle_root` from the CF).
  - **Anti-entropy:** startup incremental-tree rebuild from the machine
    scan (was the segments-CF scan from ADR-0018 Decision 1); continuous
    `on_segment_sealed` wiring unchanged.
  - **Orphan reaper:** requests `request_delete` through the coordinator
    (was deleted-marker CF writes); `.dat` orphan sweeps unchanged.
- Delete the dual-read verification (phase 2's CF-mirror comparison) and
  the mirror writes in the coordinator.
- The full nine-row crash-window fault-injection matrix is green **with the
  CF removed** (rows 1–6 from `event-wal-recovery`, rows 7–9 from
  `compaction-state-machine`) — the matrix no longer has a mirror to mask
  errors.
- RocksDB opens with `objects` + `deletions` only; the SIGABRT /
  `--test-threads=1` caveat (PIPELINE.md §4.6) is unchanged until ADR-0023's
  broader replacement.

### Out of Scope

- Compaction as a state machine (feature `compaction-state-machine`).
- Startup rebuild wiring and deleting the adoption heuristic (feature
  `startup-rebuild-from-machine`).
- Objects/inline-payload storage — stays in RocksDB (ADR-0023 Phase 2
  scope is not this ADR's slice).
- Any dual-write reconciliation layer — the event log is the source of
  truth; the CF is gone, not synced.

## Known remaining CF writers (phases 1–2; absorbed by this phase)

Phase 1 (`lifecycle-registry-coordinator`) routes the write path's
phantom registration, the seal-complete metadata write, and the orphan
reaper's deleted-marker write through the coordinator — the only writer
of segment lifecycle state. **Two CF writers intentionally remain
outside the coordinator during phases 1–2** and must be absorbed (or
deleted) by this phase:

1. **GC compactor** (`crates/oceanfs-durability/src/gc/segment_compactor.rs`):
   `delete_segment` for fully-dead segments and
   `batch_write(PutSegment(new) + DeleteSegment(old))` for repacks. This
   is the `compaction-state-machine` feature's migration surface; until
   then the compactor's new segments have **no registry entry** — the
   orphan reaper's `request_delete` on them returns `Missing`, which the
   reaper treats as "durable deletion already happened" (unlink proceeds
   without a marker; the CF entry lingers). The reaper's
   `Missing`-handling must be revisited when the compactor moves onto
   the machine.
2. **Startup interrupted-seal adoption**
   (`crates/oceanfs-node/src/node.rs`, the `.dat`-orphan scan):
   `put_segment` with recomputed Merkle root. This is deleted by
   `startup-rebuild-from-machine` (fold-based recovery replaces the
   heuristic). Until then the adopted segments are also registry-less;
   the node's phase-1 startup **seed** (`seed_from_metadata_store`)
   covers pre-existing entries at boot, but adoption runs after the
   seed, so adopted segments stay registry-less until this phase.
3. **Heal worker's post-repair metadata refresh**
   (`crates/oceanfs-durability/src/heal/worker.rs`): re-saves a sealed
   segment's metadata with `merkle_root: None` (invalidate the anchor
   until rebuilt). This is a metadata refresh, **not** a lifecycle
   transition — `sealed_at` is preserved, no state change, no
   downgrade — and no phase-1 coordinator transition exists for it
   (`seal` is Reserved-only, the segment is already Sealed). Absorbed
   here by routing the refresh through the machine (the machine's
   entry metadata is the scrub/AE anchor in this phase).

Mutation/grep note: the phase-1 DoD's "every `put_segment` /
deleted-marker CF write outside `segment/lifecycle.rs` is gone" applies
to the three in-scope writers; the three remaining writers above are
the documented exceptions and each must be **gone** by the end of
this phase's "CF gone (grep-verifiable)" DoD item.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `metadata/store.rs` + `metadata/cf.rs`: segments + deleted-markers CFs removed, segment methods deleted; `wal/replay.rs`: retention via machine; `segment/lifecycle.rs`: no mirror writes |
| `oceanfs-durability` | `gc/`, `scrub.rs`, `anti_entropy/`, `gc/orphan_reaper.rs`: consumers re-pointed at the machine (trait boundary unchanged) |
| `oceanfs-storage-api` | `MetadataStore` trait: segment methods removed |
| `oceanfs-server` | Verify only |
| `oceanfs-node` | Composition root: no CF mirror; reaper/GC/scrub/AE wired to the machine |

## Interface (Public API)

- Removed: `MetadataStore::{put_segment, get_segment, list_segments}` and
  deleted-marker methods (breaking change contained to `oceanfs-storage-api`
  consumers — the five consumers in scope, plus tests).
- Added (on the machine, already shipped by phase 1 features):
  `SegmentLifecycleRegistry::{get, for_each, len}`, coordinator
  `request_delete`. No new public surface in this feature beyond the trait
  removal.

## Data Flow

```
WAL rotation (was: CF scan)               → machine: entry garbage iff sealed_pos ≥ entry_pos | deleted
GC liveness (was: list_segments)          → registry.for_each
Scrub (was: CF merkle_root)               → machine Sealed entries
AE startup rebuild (was: CF scan)         → machine scan (supersedes ADR-0018 D1)
Orphan reaper (was: deleted-marker write) → coordinator.request_delete
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets`, `cargo fmt --check`,
      `cargo clippy --lib -- -D warnings` pass in all affected crates;
      `#![deny(missing_docs)]` passes.
<!-- REVIEW: build (all 5 affected crates) and fmt pass; missing_docs denied everywhere. clippy FAILS on 2 errors introduced by this feature: crates/oceanfs-storage/src/segment/pool.rs:296-297 (`clippy::empty_line_after_doc_comments` — leftover doc line from the `try_seal_idle` removal in 41b6b42, now glued onto `install_replacement`) and crates/oceanfs-server/src/admin.rs:634-637 (`clippy::expect_used` on `state.lifecycle_registry` in production code, added by 2f78a34; the sibling fields use `if let Some`, only the registry uses `.expect()`). Fix: delete the orphaned doc lines in pool.rs; replace the expect with an `if let`/`ok_or` error response in admin.rs. RUSTDOCFLAGS="-D warnings" cargo doc also fails with 5 errors, 3 introduced here: lifecycle.rs:29 broken link to removed `seal_idle_segments`, lifecycle.rs:2076 private link `SegmentLifecycleRegistry::writer_join`, pool.rs:964 broken link `append_with_hook_async`; 2 pre-existing (buffer.rs:195, lifecycle.rs:1461). -->
- [x] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`,
      `-p oceanfs-durability`, `-p oceanfs-server`, `-p oceanfs-node` green
      (PIPELINE.md §4.6), with the pre-existing CF-dependent tests migrated
      to machine-backed fixtures.
<!-- REVIEW: verified independently: storage lib 317/317, server lib 218/218, node lib 32/32 (+2 ignored), durability lib 233/233, storage-api 0; all integration tests green (node 74, storage ~70, durability ~34, server ~30). Crash matrix rows 1-6 (segment/crash_matrix.rs) and rows 7-9 (gc/compaction_crash.rs) green. One load-induced flake observed: storage lib `io::segment_flush::tests::group_commit_batches_concurrent_registrations` failed once while the server suite compiled/runs concurrently on the same machine (asserts batches <= 2 on a 100 ms group-commit window); passes solo and 5/5 in isolation. -->
- [x] **Invariant — CF gone (grep-verifiable):** no `segments` CF, no
      deleted-markers CF, no `put_segment`/`get_segment`/`list_segments`
      anywhere in the workspace; the RocksDB open lists exactly
      `objects` + `deletions`. Mutation check: re-adding a segment CF write
      must fail a test (there is no store to write to).
<!-- REVIEW: verified by grep — all matches are comments/docs/tests; the trait (oceanfs-storage-api/src/metadata_store.rs) has no segment methods; BatchOp has only object/tombstone variants; RocksDB opens objects+deletions only (metadata/store.rs:241-244; cf.rs). Mutation check holds structurally (no trait method exists to call). Minor leftover: `Error::MirrorDivergence` variant (error.rs:141) is dead code — never constructed, only referenced by a stale doc (lifecycle.rs:1467). -->
- [x] **Invariant — event log is the only durable writer of segment
      state (final form):** the coordinator's `request_*` methods append
      events; no other durable segment-state store exists. Verified by the
      absence of the CF (above) plus the crash matrix.
<!-- REVIEW: coordinator holds no metadata-store handle (lifecycle.rs:1133-1141); event_wal is Option and every request_* (reserve/seal/delete/refresh/seal_finalized_batch) rejects with TransitionError::DurableWriteFailed when unwired (lifecycle.rs:1760, 1829-1846, 1899-1908, 1937-1942, 2003-2010) with tests at 2760/2893. -->
- [x] **Invariant — WAL retention without CF scan:** the leak regression —
      a soak test drives reserve→seal→sweep churn and asserts the WAL
      `protected` file set stays flat (the 3.8 GB/hour class: previously
      17 → 45 in 30 min); `cleanup_old_wal_files` performs no metadata
      store lookup.
<!-- REVIEW: cleanup_old_wal_files takes an is_entry_garbage closure (wal/replay.rs:336-435) backed by entry_is_garbage (lifecycle.rs:306); no metadata-store lookup. record_data_wal_pos is max-monotonic and updates Reserved AND Sealed entries (lifecycle.rs:902-934; tests 3054-3090); the recovery pass overlays the last WAL position for every segment incl. Sealed (lifecycle.rs:1556-1601). e2e/tests/wal_retention.rs passes (2/2) against a binary rebuilt from 41b6b42 — NOTE: an earlier stale pre-commit release binary failed it (26 files, no convergence); after `cargo build --release` from the current commit it passes (peak 9, converges to 4). Test nit: the DURING-load peak assertion is vacuous (wal_retention.rs:99 `let peak = initial_files;` is never reassigned); the post-load convergence gate (<= initial+6) is the effective assertion. -->
- [x] **Invariant — consumers read the machine:** GC liveness, scrub
      roots, AE rebuild, and reaper deletes are exercised by their existing
      suites with the CF removed; scrub's root now comes from the machine
      entry (test: sealed segment's root in the registry equals the scrub
      anchor).
<!-- REVIEW: GC run_cycle/start_background take &SegmentLifecycleRegistry + for_each liveness (gc/garbage_collector.rs:166,343,385); scrub run_cycle enumerates registry Sealed entries and the worker's anchor is registry.get(id).metadata.merkle_root (scrub.rs:706-727, 507-509); AE rebuild_from_segment_scan scans the registry (merkle/incremental_tree.rs:409-427); orphan reaper scans registry + request_delete with delete-before-unlink (gc/orphan_reaper.rs:128-153, 147-174); heal worker requests request_refresh_metadata (heal/worker.rs:417-418); healing_service reads the registry (healing_service.rs:30); admin /admin/segments wired via with_lifecycle_registry (admin.rs:422, node.rs:1230). -->
- [x] **Crash matrix with no mirror:** the full nine-row matrix passes with
      the CF removed — no dual-read check exists to mask a fold error; rows
      1–6 (from `event-wal-recovery`) and 7–9 (from
      `compaction-state-machine`) all green in one suite.
<!-- REVIEW: rows 1-6 green in segment/crash_matrix.rs (storage lib) and rows 7-9 green in gc/compaction_crash.rs (durability lib); no dual-read code remains. "One suite" is satisfied functionally (both suites green with no mirror); the rows remain physically split across the two crates as the DoD's own provenance line implies. -->
- [ ] **Perf:** no hot-path trait boundary added (registry is in-crate for
      storage consumers; durability consumers keep their existing trait
      boundary — ADR-0025 Decision 1); the machine scan is O(live segments)
      and the AE rebuild benchmark stays within the ADR-0018 rebuild cost
      envelope (O(N) scan, ~1 s per 1M segments).
<!-- REVIEW: registry is in-crate; shards use parking_lot::RwLock (perf 2.3); lock bodies contain no I/O (7.1); writer_count uses relaxed atomics (11.1); machine scan is O(live) by construction. Two gaps: (1) NO AE rebuild benchmark exists anywhere in benches/ — the "stays within the ADR-0018 rebuild cost envelope" claim is unverifiable; (2) durability consumers take the concrete SegmentLifecycleRegistry/Coordinator from oceanfs-storage rather than a trait defined in the consuming crate — deviation from the letter of ADR-0025 Decision 1's "trait-in-consuming-crate" wording (no hot path crosses the boundary; the machine is consumed only by background tasks). -->
- [ ] **Integration:** a full node cycle — write, seal, delete, restart,
      GC run, scrub run, AE run — with the CF removed, all assertions from
      the pre-removal suites preserved (objects CF untouched).
<!-- REVIEW: all 27 other e2e test files pass (write/seal/read/report via segment_lifecycle, restart via wal_recovery + cluster_lifecycle t42, GC, scrub, AE, orphan reaper, cluster paths). load_sustained FAILS on the dev machine: memory_bounded as claimed (run 2: RSS 1312116736 > 2x initial 593522688; run 1 passed — borderline), AND crash_recovery failed 2/2 of this reviewer's runs (1 of ~102-110 pre-crash objects "unreachable" — transport error on GET after SIGKILL+restart, keys load-test/hot-17 then hot-57), whereas the implementer's 07:53 run passed it. The crash_recovery claim is not reproducible on this machine; needs investigation or re-validation on the SUT VM. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --all-targets -D
> warnings` test-code warnings and `ignore`-tagged doc examples are
> structural hygiene tracked separately (guidelines/coding.md §9.2.1).
