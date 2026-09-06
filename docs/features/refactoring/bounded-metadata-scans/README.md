---
feature: "Bounded Metadata Accounting (ADR-0034) — Program Coordination"
epic: "refactoring/bounded-metadata-scans"
status: done
priority: critical
owner: ""
dependencies:
  - epic: refactoring/store-unification
    reason: ADR-0034 consumers (GC compactor, orphan reaper, healing service) run on the ADR-0032 unified SegmentDataStore + single composition-root instance; the reaper's data-access and the compactor's read path must already be unified before their scan elimination lands (ADR-0034 references store unification throughout; wave-2 ⑥ sits behind ②)
  - epic: refactoring/legacy-mode-removal
    reason: ADR-0031 removes the legacy single-data-dir stores and breaks the event-WAL/checkpoint formats (legacy-mode-removal/f3). The f3 seal-time record extension bumps the same event-WAL/checkpoint formats and must land after the legacy-format break; the orphan reaper's phase-2b unregistered-`.dat` path is retired because ADR-0031/0032 enforcement makes unregistered writers unreachable
adr:
  - 0034-bounded-metadata-accounting
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf:
  - "1.3 pre-size collections with known capacity"
  - "1.4 SmallVec for small metadata structures"
  - "2.2 dashmap for concurrent maps"
  - "6.3 #[repr(C)] for all on-disk / on-wire structures"
  - "7.1 minimize lock hold duration"
  - "11.1 atomic counters on hot paths"
created: 2026-09-04
updated: 2026-09-06
---

# Bounded Metadata Accounting — Program Coordination

> **EPIC COMPLETE (2026-09-06):** f1, f3, f4, and f2 all landed with
> independent review PASS and each feature doc is `done`. The four
> O(all-objects) consumer scans are eliminated (see the status board in
> `review-2026-09-orchestration.md`). This document remains the map.

> **This is the coordination document for the ADR-0034 epic (review triage
> Theme 4, wave 2 ⑥).** If you are implementing any feature under
> `refactoring/bounded-metadata-scans/`, read this first — it tells you where
> your work sits in the whole, what must exist before you start, and what
> must not regress while you work. The per-feature docs (`f1-*` … `f4-*`) are
> the authority for each feature; this document is the map.

## Summary

ADR-0034 (accepted 2026-09-04) eliminates the four **O(all-objects) scans**
the 2026-08-25/09-03 review found on the durability hot paths — three per
cycle (orphan reaper, GC liveness, GC compactor) and one per event (healing
remap). It replaces object *counting* with byte *accounting* and inverts
every recurring query into a point or per-segment operation. No reverse-index
CF, no self-describing `.dat` format, no new RocksDB surface.

| Consumer | Site today | Query | Eliminated by |
|---|---|---|---|
| Orphan reaper | `gc/orphan_reaper.rs:294-313` (`build_referenced_set` → `list_objects_all`) | "which segments are referenced?" | **f2** (fully-dead detection from accounting) |
| GC liveness | `gc/garbage_collector.rs:523` (`process_tombstones` Phase 2 → `list_objects_all_with_bucket`) | "per-segment live bytes" | **f2** (live = total − dead) |
| GC compactor | `gc/segment_compactor.rs:541-560` (`find_objects_in_segment`) | "which objects reference segment S?" | **f3** (seal-time membership list + point lookups) |
| Healing remap | `healing_service.rs:654-716` (`repoint_objects` → `list_objects_all_with_bucket`) | "which objects re-point on remap?" | **f4** (remap carries the object-key list) |

## The accounting invariant (D1)

For every sealed segment S:

```
logical_total(S) := SegmentMetadata.total_bytes   (recorded at seal — f3)
dead_bytes(S)    := Σ chunk.length over every captured dead-chunk record referencing S   (f1 capture + f2 aging)
live_bytes(S)    := logical_total(S) − dead_bytes(S)
orphan(S)        := dead_bytes(S) ≥ logical_total(S)
```

**Capture rule:** every chunk-ref that stops being referenced by a live object
row MUST be captured into a dead-chunk record **atomically with the row
change**. Delete capture already exists (`delete_object`); **overwrite capture
is the missing half and ships in f1**.

## The Epic at a Glance

```
refactoring/bounded-metadata-scans/
├── README.md                 ← this document (map)
├── f1-supersede-capture.md   [critical]  atomic supersede capture at put_object_in_bucket (D2)
├── f2-accounting-liveness.md [critical]  GC liveness = total−dead; reaper = fully-dead (D3+D4)
├── f3-seal-membership-list.md[critical]  seal-time total_bytes + contained-objects record (D1+D5/2a)
└── f4-remap-object-keys.md   [high]      remap notification carries the object-key list (D5/2b)
```

| Feature | Kills (by construction) | Delivers |
|---|---|---|
| **f1** | The overwrite-orphan hole (old version's chunks vanish with no dead record) and the only remaining need for the reaper's object scan | Atomic supersede dead-chunk capture in `RocksDbMetadataStore::put_object_in_bucket` (single WriteBatch: put new row + clear plain tombstone + write versioned supersede record) plus a classified dead-chunk enumeration |
| **f2** | `process_tombstones`'s `list_objects_all_with_bucket`; the `LivenessTracker` counting model; `build_referenced_set`; the reaper's phase-2b unregistered-`.dat` sweep | GC liveness from `registry.for_each` (totals) + aged dead-chunk captures; orphan reaper as fully-dead detection over the registry |
| **f3** | `find_objects_in_segment`'s objects-CF scan; the missing per-segment logical total | A seal-time per-segment record (logical `total_bytes` + contained-objects `(bucket, key)` list) persisted on the event-WAL + checkpoint path (ADR-0024/25), propagated with segment metadata to replicas, and consumed by the compactor |
| **f4** | `repoint_objects`'s objects-CF scan; the `[review][performance][critical]` at `healing_service.rs:671` | The compaction-remap fan-out (g3) and its receiving handler carry the re-pointed object-key list; holders re-point via per-key point lookups |

## Dependency Graph (implementation order)

```
epic preconditions (wave 2, must already be green):
  ① composition-root c1 → ② store-unification f1..f3 (ADR-0032)
  and ⑤ legacy-mode-removal f1..f3 (ADR-0031 — incl. f3 event-WAL/checkpoint format break)
                                     │
             ┌───────────────────────┴───────────────────────┐
             ▼                                               ▼
   f1-supersede-capture                         f3-seal-membership-list
   (no feature deps inside this epic)            (needs the legacy-format break;
                                                 the ADR-0032 store/registry shape)
              │                    (f1 and f3 touch disjoint files —
              │                     may land in parallel)
              │                                               │
              │                          ┌────────────────────┘
              │                          ▼
              │              f4-remap-object-keys
              │              (needs f3's membership-bound object set)
              │
              └──────────────────────────┬
                                         ▼
                               f2-accounting-liveness
                               (needs f1 captures + f3 total_bytes)
```

`f2` and `f4` are otherwise independent and may land in either order once
`f3` is green.

Ordering rules:

1. **Epic preconditions first.** This epic is wave-2 ⑥: it builds on the
   store-unification (ADR-0032) and legacy-mode-removal (ADR-0031) epics.
   Nothing here introduces a second data-access path or a legacy-mode
   branch. In particular **f3 modifies the same event-WAL/checkpoint
   formats that `legacy-mode-removal/f3-format-break-and-test-rework`
   already changes** — it must land after that break so there is one format
   migration, not two.
2. **f1 and f3 are independent and may land in parallel.** f1 touches the
   RocksDB metadata store + storage-api trait; f3 touches the segment
   lifecycle/seal/event-WAL/checkpoint/compactor. They touch disjoint files.
3. **f2 lands after BOTH f1 and f3.** Its accounting model is
   `live = logical_total − dead`: `logical_total` only exists once f3
   records `SegmentMetadata.total_bytes` at seal (ADR-0034 D1 — today the
   registry stores no total, and GC seeds `register_segment(id, 0)`); the
   `dead` half only exists once f1 captures superseded chunks. This is why
   the "natural" f1→f2 order is actually f1→f3→f2.
4. **f4 needs f3** so the owner's re-pointed object set is the segment's
   membership (bounded) rather than a scan result; it is otherwise
   independent of f2 and may land in parallel with it.
5. Each step lands green (build + tests + clippy + fmt per PIPELINE.md)
   before the next. PIPELINE.md §4.6: RocksDB-touching crates run with
   `--test-threads=1`.

## Sequencing vs the roadmap and other epics

- **Wave 2 ② (store-unification) and ⑤ (legacy removal) precede this epic**
  (orchestration doc §Wave order: ⑥ is gated behind ②, ⑤). Do not build the
  f3 format work on top of the pre-ADR-0031 event-WAL shapes.
- **This epic gates `durability-scheduler/f3-keyspace-sharding`**
  (`f3-keyspace-sharding.md` §Out of Scope: sharding GC/orphan requires a
  segment-scoped metadata API or the ADR-0034 accounting). After f2 the
  per-cycle cost is bounded, and sharding the *registry/captures* iteration
  becomes meaningful.
- **This epic gates g7 (wal-loss-recovery)**: the membership list is the
  natural seed for g7's catch-up enumeration and for ADR-0029 D7's deferred
  self-description when g8 lands (ADR-0034 D5). Wave 3 must not start before
  this epic's f2/f3 land.
- **Do not regress** the scheduler's f3 constraint (GC/orphan stay full-pass
  until this epic removes the O(objects) phase).

## Key Design Decisions to Respect (do not re-litigate)

- **Capture completeness is the invariant.** Correctness now rests on every
  dead byte being captured at row-change time (ADR-0034 Consequences). The
  single choke point is `RocksDbMetadataStore::put_object_in_bucket`
  (`metadata/store.rs:403`) — the one concrete write behind the
  `MetadataStore` trait funnel (write coordinator, hint-apply, replica-apply,
  node `MetadataStoreAdapter`). No path may write an object row without the
  capture logic. `batch_write`'s `PutObject` (used by the compactor remap and
  the healing re-point) is a **physical** chunk re-point of the same logical
  version and does NOT capture.
- **Supersede records are not tombstones of the key.** The object still
  exists (new version). The f1 encoding keeps them versioned so they
  (a) coexist with the live row, (b) age under the tombstone TTL discipline,
  (c) attribute to the segments they reference, and (d) are never interpreted
  as a delete of the new version — ADR-0034 D2 constraints (a)–(d). The
  encoding recommendation (versioned keys in the deletions CF) and the open
  alternative (dedicated CF — rejected because ADR-0025 Decision 3 keeps
  "objects + deletions only" and ADR-0034 opens with "no new RocksDB
  surface") are laid out in `f1-supersede-capture.md`. **This is the one open
  implementation choice of the epic; f1 does not silently pick a design that
  violates (a)–(d).**
- **The membership record lives with segment metadata, not in the `.dat`.**
  ADR-0034 D5/2a + ADR-0029 D7: nothing about contained objects is encoded in
  the segment binary. f3 stores the list on the event-WAL/checkpoint path.
- **No reverse index, no new RocksDB CF, no write-path amplification.** If an
  implementation step looks like it needs a `segment_refs` CF, stop and
  re-read ADR-0034's Considered Alternatives.
- **Legacy/corrupt shapes degrade, never scan.** A legacy tombstone with no
  chunks, a membership-less sealed segment (WAL-replayed), or a
  not-yet-accounted segment falls back to the bounded accounting path — the
  orphan reaper can still reap fully-dead segments — and never reintroduces a
  full objects-CF scan.

## What an Implementer Should Do When Picking Up a Feature

1. Read this document (you are here).
2. Read the feature doc's `adr:` frontmatter and the cited ADR-0034
   sections (D1–D6) plus the precondition epics' READMEs.
3. Identify your inputs (features in `dependencies:` — done) and outputs
   (who consumes you). The D6 fault-injection matrix in ADR-0034 is the
   acceptance bar for the whole epic; each feature's DoD lists the rows it
   owns.
4. Land green: build, tests (with `--test-threads=1` for RocksDB-touching
   crates per PIPELINE.md §4.6), clippy, fmt.

## Epic-level DoD (ADR-0034 acceptance)

- [ ] **No full-object scan remains on the four consumer paths.** `grep -rn
      "list_objects_all_with_bucket\|list_objects_all" crates/oceanfs-durability/src/gc
      crates/oceanfs-durability/src/healing_service.rs` returns nothing.
      `build_referenced_set` and `repoint_objects` are deleted.
- [ ] **Capture completeness (D2):** every production object-row write funnels
      through the supersede-capturing `put_object_in_bucket`; no row
      replacement path (S3 PUT overwrite, hint-apply, replica metadata apply,
      re-PUT after delete) drops a superseded version's chunks without a
      dead-chunk record in the same WriteBatch.
- [ ] **Accounting (D3/D4):** GC liveness and orphan detection derive from
      `registry.for_each` + captured dead-chunk records aged by TTL; no
      consumer consults the objects CF to decide liveness or orphanhood.
- [ ] **Seal-time record (D5):** sealed segments carry `total_bytes` + a
      contained-objects list on the event-WAL/checkpoint path; the compactor
      enumerates a segment's objects from that list (plus point lookups), not
      from a scan; the compaction remap notification carries the object-key
      list.
- [ ] **D6 fault-injection correctness matrix passes.** Each row below maps
      to feature DoDs and unit/integration tests that run green with
      `--test-threads=1` (PIPELINE.md §4.6):

| Scenario | Assertion | Owned by |
|---|---|---|
| DELETE then idle | captured dead bytes == old chunk bytes; segment liveness drops | f2 |
| PUT overwrite (old on segment A, new on B) | A's bytes captured; B live; no leak | f1 + f2 |
| DELETE → re-PUT same key | supersede of re-PUT's predecessor captured; re-PUT live; no double-dead | f1 |
| Multipart object spanning N segments, then overwrite | all N segments' bytes captured exactly once | f1 |
| Hint-apply that supersedes an existing key | capture fires on the apply path | f1 |
| Replica metadata apply overwriting a row | capture fires on the replica path | f1 |
| Crash between row write and capture | impossible by construction (same WriteBatch); test asserts atomicity | f1 |
| GC-driven delete of a tombstoned key | already-tombstoned bytes not double-counted | f2 |
| Supersede of a tombstoned-but-re-PUT key | no delete of the live row; correct aging | f1 |
| Corrupt/partial tombstone (legacy, no chunks) | degrades to orphan-reaper accounting, never to a full scan | f2 |

- [ ] **Behavioral parity:** GC's and the reaper's externally visible behavior
      is unchanged (same TTL grace, same deletion, same metrics semantics);
      `gc_dead_bytes_total`/`orphan_*` metric semantics preserved.
- [ ] **Green:** `cargo build --all-targets`; `cargo test -p oceanfs-storage
      --lib -- --test-threads=1`, `-p oceanfs-durability --lib --
      --test-threads=1`, `-p oceanfs-server --lib -- --test-threads=1`, `-p
      oceanfs-node --lib -- --test-threads=1`; node integration tests
      `--test orphan_reaper`, `--test gc_compaction`, `--test
      loss_announcement` (each with `--test-threads=1`).
- [ ] **Review markers closed:** the `[review]` blocks at
      `orphan_reaper.rs:297`, `garbage_collector.rs:521`,
      `segment_compactor.rs:537-540`, `healing_service.rs:671` are annotated
      resolved or removed.
- [ ] **Docs:** `#![deny(missing_docs)]` passes in all touched crates; every
      new `pub` item has `# Examples`.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).

## References

- ADR-0034 (this epic's decision; D1–D6), ADR-0032 (store unification —
  precondition), ADR-0031 (legacy removal — precondition + phase-2b
  retirement), ADR-0025 (lifecycle registry = segment set; the event log is
  the durable segment-state path), ADR-0024 (segment event log), ADR-0029 D7
  (deferred self-description — the membership list is its precursor),
  ADR-0017 (scheduler — this epic unblocks its f3 sharding)
- Roadmap: `docs/features/refactoring/review-2026-09-roadmap.md` (wave 2 ⑥),
  orchestration doc (`review-2026-09-orchestration.md` §Global dependency
  chain / §Wave order)
- Precondition epics: `refactoring/store-unification/README.md`,
  `refactoring/legacy-mode-removal/README.md`
- Downstream: `refactoring/durability-scheduler/f3-keyspace-sharding.md`
- Review anchors cited in ADR-0034 §References
