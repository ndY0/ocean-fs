---
feature: "Session Handoff — 2026-09-06 (bounded-metadata-scans f1 + f3 landed)"
epic: "refactoring"
status: active
priority: critical
owner: ""
created: 2026-09-06
updated: 2026-09-06
---

# Session Handoff — 2026-09-06

Continuation notes for the next implementer session on the 2026-09 review
program's **bounded-metadata-scans** epic
(`docs/features/refactoring/bounded-metadata-scans/README.md`, ADR-0034),
wave-2 ⑥ behind the orchestration doc
(`docs/features/refactoring/review-2026-09-orchestration.md`). Read this
together with the epic README, ADR-0034, and the four feature docs.

## Program position

Preconditions were already green at session start (store-unification EPIC
COMPLETE `3186176`; legacy-mode-removal EPIC COMPLETE). This session
implemented **f1** and **f3** of the epic end-to-end (code → independent
reviewer PASS → spec-writer sync → pushed).

| Commit | What |
|---|---|
| `1c38dfb` | **f1 done** — atomic supersede-capture on overwrite (ADR-0034 D2). Reviewer PASS (iter 2). |
| `6cb7958` | f3 core types: `SegmentMetadata.total_bytes`, `ContainedObject` (+134-literal sweep) |
| `9dfc197` | f3 event-WAL + checkpoint carry `total_bytes`/contained tail (checkpoint version 1); `request_seal_with_contained` |
| `58f8b19` | f3 sealer/flush thread membership; `meta.total_bytes = data.len()` |
| `58be716` | f3 `SegmentPool.record_object_key` + seal-pipeline membership + coordinator append-hook capture |
| `2b1f242` | f3 compactor consumes membership (point lookups); GC skips membership-less |
| `d8ca796` | f3 propagation: `PushSealedSegmentRequest.contained_objects` + replicator + receiver |
| `8abadb4` | f3 format round-trip tests |
| `068e906` / `526eb86` | f3 review-gap fixes (durability gc_compaction fixture, all-targets compile, asserts) |
| `0556dec` / `c376d5d` | f3 DoD complete (reviewer PASS iter 3) + spec-writer notes |

Status board:

| Epic | Status |
|---|---|
| review-wave-0-1 | done |
| composition-root-decomposition | EPIC COMPLETE |
| store-unification | EPIC COMPLETE |
| legacy-mode-removal | EPIC COMPLETE |
| **bounded-metadata-scans** | **f1 done · f3 done · f4 not started · f2 not started** |
| durability-scheduler, manifest-aware-repair | docs written, not started |
| g7/g8 healing | blocked on wave 2 |

Working tree is **clean at `c376d5d`**. All pushed.

## Key decisions recorded this session (do not re-litigate)

- **V1** supersede encoding = **versioned keys in the existing `deletions`
  CF** (no new CF, no third design).
- **V2** capture predicate in `put_object_in_bucket`: capture only when a
  segment-stored predecessor exists AND `meta.hlc > existing.hlc`, OR on a
  re-PUT over a tombstoned key with chunks (migrate, preserving
  `deletion_time` so TTL aging doesn't reset). Same/older-HLC physical
  repairs never capture. `batch_write(PutObject)` (compactor/heal re-point)
  unchanged, never captures.
- **V6** concurrent same-key safety = sharded per-key `parking_lot` stripe
  in `RocksDbMetadataStore`; **4096** stripes from
  `stripe_count_for_writers(64)` (documented birthday-bound formula; helper
  in place for a future config-driven bound); per-store `RandomState`
  (SipHash) so client keys can't align to a stripe. `delete_object` shares
  the stripe and commits row-delete+tombstone in one WriteBatch.
- **V3** `total_bytes` lives on `SegmentMetadata`; **amended (user
  option 1): it ALSO rides the `SealEvent` fixed fields** — checkpoint-only
  storage would lose it on event replay. Replay fold reconstructs it.
- **Checkpoint version is 1, not 3/4** (user directive): pre-production
  formats do not accumulate; any differing version refused/skipped. v2
  pre-pool refused with `UnsupportedPrePoolDataDir`.
- **V4** propagation: owner→holder `PushSealedSegmentRequest` carries
  membership. ACCEPTED DEVIATION: repair/heal replica-rebuild fetch paths
  (`repair.rs execute_repair`, heal worker) still seal membership-less
  (`request_seal(None)`) — re-readable/reapable, non-compactable (safe).
- f1 doc-format details, dev notes, and all accepted deviations are
  recorded in the f1/f3 feature docs' "Implementation notes" sections.

## What the code looks like now (key facts for f4/f2)

- **f1 choke point** (`oceanfs-storage`): `put_object_in_bucket` is a
  read→decide→single-WriteBatch under `with_key_lock`;
  `list_dead_chunk_records_all()` (concrete + `MetadataStore` default)
  is the ONLY enumeration exposing supersedes; plain tombstone ops never
  see them. `RocksDbMetrics` counters
  `supersede_captured_total`/`supersede_dead_bytes_total`.
- **f3**: `LifecycleEntry.contained_objects: Option<Arc<[ContainedObject]>>`;
  `request_seal_with_contained`; checkpoint Sealed entries persist
  `total_bytes` + a length-prefixed contained tail; `seal_from_data_with_contained`
  (`pub(crate)`, wrapper `seal_from_data` keeps the public shape);
  compactor `compact_segment(segment_id, &meta, contained: Option<&[ContainedObject]>, &dead_keys)`
  enumerates via membership point lookups and **refuses** membership-less;
  GC `run_cycle` skips membership-less candidates before spawning; the
  repacked segment seals `request_seal_with_contained(live membership)`.
- `PushSealedSegmentRequest.contained_objects` (proto + regenerated stubs);
  receiver converts empty→`None`.

## NEXT STEP — f4 (remap carries the object-key list)

`docs/features/refactoring/bounded-metadata-scans/f4-remap-object-keys.md`.
Needs f3 (done). Serial order chosen: **f4 then f2** (both touch
`garbage_collector.rs`/`segment_compactor.rs`).

- `CompactionRemapFn` (`gc/garbage_collector.rs:18`) → arity 4: add the
  ordered/deduped `(bucket,key)` list of repacked live objects. Notify site
  `segment_compactor.rs:447-459` fires it (the membership-enabled
  `live_objects` are already in scope at that point — no extra owner reads).
- Node fan-out closure lives in `modules/durability.rs:217-285`
  (NOT node.rs — composition root moved); `announce_segment_remap`
  (`oceanfs-node/src/announce.rs:348-415`) gains the key list; per-target
  `SegmentRemap` request build `:380-385` carries it.
- `healing_service.rs`: `announce_remap` `:1491-1596` decodes keys beside
  the chunk table (`:1508-1516`); `repoint_objects` `:641-703` rewritten to
  per-announced-key `get_object_metadata` point lookups (delete the
  `list_objects_all_with_bucket` scan at `:662` and the
  `[review][performance][critical]` marker `:658-661`); legacy empty-key
  guard degrades to alias+g4, never a scan.
- Proto: `proto/oceanfs/healing.proto` `SegmentRemap` `:263-276` gains a
  repeated `(bucket,key)` field — no existing composite message; add
  `RemappedObject { oceanfs.common.BucketId bucket; oceanfs.common.ObjectKey key }`.
  Regenerate via `cargo build -p oceanfs-durability` (build.rs rewrites
  committed `src/generated/oceanfs.healing.rs`); commit the diff.
- DOC DEVIATION to record: f4's DoD cites `node/tests/loss_announcement.rs`
  as the integration gate, but that file has NO remap content today. The
  real remap-propagation gate is `node/tests/segment_replication.rs`
  (`compacted_segments_are_readable_from_every_node` `:343-516`) plus
  compactor unit `compaction_fires_remap_notifier_with_chunk_table`
  (`segment_compactor.rs` tests). Fix the doc reference when landing.
- Owner-side helper test anchors are in the f4 research (see the epic's
  per-feature docs).

## NEXT STEP after f4 — f2 (accounting liveness + fully-dead reaper)

`docs/features/refactoring/bounded-metadata-scans/f2-accounting-liveness.md`.
Needs f1 (done) + f3 (done). This is the payoff feature and the biggest
behavioral-parity risk. Key current anchors (post-f3):

- GC `process_tombstones` `garbage_collector.rs:428-520`: Phase 1 still
  `register_segment(id, 0)` (`:442-446`) — switch to
  `entry.metadata.total_bytes`; Phase 2 consumes
  `metadata.list_dead_chunk_records_all()` instead of
  `list_objects_all_with_bucket()` (`:498`); classify Tombstone (→
  `eligible_keys` + `mark_dead` + cleanup note) vs Supersede (→
  `mark_dead` only, NEVER a row delete); DELETE the re-PUT-race arm
  (`:502-511`); extend `TombstoneResult` (`:78-79`) to carry supersede
  records; cleanup loop `:363-379` deletes aged supersedes referencing
  compacted segments via a new store method `delete_dead_chunk_record`
  (concrete + trait default `Ok(())`) — never a row delete for supersedes.
- `LivenessTracker` (`liveness_tracker.rs`): `register_segment(id,total)`,
  delete `add_live_bytes` (`:35-39`) and its only caller (`:514`).
- Orphan reaper `orphan_reaper.rs`: `run_cycle` `:130-296` — orphan iff
  `dead_bytes(S) ≥ total_bytes(S)` and past the sealed-at TTL grace; delete
  `build_referenced_set` (`:328-347`) + `[review][architectural][high]`
  (`:331-334`); delete phase-2b unregistered-`.dat` sweep (`:160-210`) and
  its `list_segment_files` call; double-check stays a bounded snapshot
  re-check, never a store rescan.
- **`total_bytes == 0` on a Sealed entry means "unknown"** — GC/reaper must
  SKIP it (never treat as fully-dead, never orphan everything).
- Supersede migration preserves `captured_at` (f1), so TTL aging does not
  reset on DELETE→re-PUT.
- V5 (user-accepted): the shared accounting helper counts only AGED
  captures (`now_ms - captured_at > ttl`) for both GC and the reaper —
  reaping may be later than the old referenced-set model, never earlier.
- Reaper tests to replace/delete are named in the f2 doc (they moved post
  refactors; current lines in the durability file: `referenced_set_*`
  ~`:1039/1074`, `double_check_…` ~`:706`, `sweeps_unregistered_…` `:759`,
  `object_in_non_default_bucket_keeps_segment_alive` `:498`).

## Epic close (after f2)

Walk the epic README DoD: grep
`list_objects_all_with_bucket\|list_objects_all` over
`oceanfs-durability/src/gc` + `healing_service.rs` returns nothing
(f2 handles gc/, f4 handles healing_service.rs); `build_referenced_set`
gone; capture-completeness funnel check; full D6 matrix; behavior-parity
pins; node gates `orphan_reaper`, `gc_compaction`, `loss_announcement`,
`segment_replication`. Then update the orchestration doc's status board
(bounded-metadata-scans → done), which unblocks
`durability-scheduler/f3-keyspace-sharding` and g7.

## Environment / verification recipes

- RocksDB suites always `-- --test-threads=1` (PIPELINE.md §4.6):
  `cargo test -p oceanfs-storage --lib -- --test-threads=1` (expect 457),
  `-p oceanfs-durability --lib` (263) and `--test gc_compaction` (5),
  `-p oceanfs-server --lib` (245), `-p oceanfs-node --lib` (66) + node
  `--test gc_compaction` (7), `--test segment_replication` (3),
  `--test orphan_reaper` (8), `--test loss_announcement` (1).
- `cargo clippy -p <crate> --lib -- -D warnings`, `cargo fmt -- --check`,
  `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` on touched crates.
- NEVER run e2e/load suites locally (PIPELINE.md §6).
- After touching `.proto`: `cargo build` on the owning crate regenerates the
  committed stubs; commit the generated diff.
- Reviewer + spec-writer loop per feature; feature docs get `status: done`
  + "Implementation notes" + in-place REVIEW annotations; re-index changed
  docs via `doc-graph_index_document`.
- Commit/push milestones as you go (repo style:
  `feat(refactoring): bounded-metadata-scans fN — …`).

## Known pre-existing issues (not ours, verified at HEAD `c376d5d`)

- oceanfs-durability `hint_wal.rs:848` unused test fn warning under
  test-target builds (pre-existing).
- e2e/load suites remain cloud-only; node functional suites are the local
  gates.

## Files most likely touched next

- f4: `oceanfs-durability/src/healing_service.rs`,
  `gc/garbage_collector.rs`, `gc/segment_compactor.rs`,
  `oceanfs-node/src/announce.rs`, `oceanfs-node/src/modules/durability.rs`,
  `proto/oceanfs/healing.proto`, regenerated stubs, node integration tests.
- f2: `oceanfs-durability/src/gc/garbage_collector.rs`,
  `gc/liveness_tracker.rs`, `gc/orphan_reaper.rs`,
  `oceanfs-storage/src/metadata/store.rs` (`delete_dead_chunk_record`),
  `oceanfs-storage-api/src/metadata_store.rs`, node integration tests
  (`orphan_reaper`, `gc_compaction`).
