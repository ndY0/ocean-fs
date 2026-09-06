---
feature: "f4: Compaction Remap Carries the Object-Key List"
epic: "refactoring/bounded-metadata-scans"
status: done
priority: high
owner: ""
dependencies:
  - feature: f3-seal-membership-list
    epic: refactoring/bounded-metadata-scans
    reason: The owner's re-pointed object set (the keys the remap notification must carry) is derived from the compactor's live-object enumeration, which f3 bounds via the seal-time membership list instead of a full scan; f4 lands on the f3 compactor shape
  - epic: refactoring/store-unification
    reason: The healing service and remap fan-out run on the ADR-0032 unified store / single composition-root instance (the fan-out closure this feature changes lives in the post-c1 wiring, `modules/durability.rs`)
adr:
  - 0034-bounded-metadata-accounting
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf:
  - "1.4 SmallVec for small metadata structures"
  - "7.1 minimize lock hold duration"
created: 2026-09-04
updated: 2026-09-06
---

# f4: Compaction Remap Carries the Object-Key List

## Summary

The fourth ADR-0034 scan is the **healing remap**: when the owner compacts
`S → S'`, each peer that holds a stale copy of S must re-point its own object
rows. Today the peer's `repoint_objects`
(`crates/oceanfs-durability/src/healing_service.rs`) discovers which rows to
re-point by scanning its entire objects CF
(`list_objects_all_with_bucket` — a `[review][performance][critical]`).
ADR-0034 D5/2b deletes that scan: the owner already knows exactly
which objects it repacked (the live objects from its membership list, f3), so
the **remap notification carries the object-key list** alongside the existing
old→new chunk table, and each holder re-points exactly those keys via point
lookups.

This feature changes the notification contract end to end — the
`CompactionRemapFn` type, the compactor's notify site, the node fan-out
closure (`modules/durability.rs`) and `announce.rs`, the `SegmentRemap`
proto/announcement, and the healing-service `announce_remap` handler — and
rewrites `repoint_objects` to a per-key point lookup. It lives in
`oceanfs-durability` (`gc/garbage_collector.rs`, `gc/segment_compactor.rs`,
`healing_service.rs`, the generated healing stub), the repo `proto/` tree
(remap message), and `oceanfs-node` (`modules/durability.rs`, `announce.rs`).

## Scope

### In Scope

**A. Notification contract gains the object-key list**

- `CompactionRemapFn` (`gc/garbage_collector.rs:18`):
  `Arc<dyn Fn(SegmentId, SegmentId, Vec<RemappedChunk>, Vec<ContainedObject>)
  + Send + Sync>` — the 4th argument is the repacked object-key list
  (`ContainedObject { bucket, key }` per live object, reusing the f3 core
  type). `object_keys` = the `(bucket, key)` of every live object the owner
  repacked (the compactor's `live_membership`, post-f3 membership
  enumeration), ordered and deduped.
- Compactor notify site (`gc/segment_compactor.rs`, after the `ObjectsMoved`
  milestone): build and fire the key list from the already-materialized
  `live_membership` alongside the `chunk_table`. The doc comment and the
  GAP-1 rationale are updated: a receiver re-points exactly the announced
  keys; keys it does not hold locally are no-ops.
- Fan-out (`crates/oceanfs-node/src/modules/durability.rs`): the
  `.with_compaction_remap_notifier` closure and its inner `tokio::spawn` pass
  the key list into `announce_segment_remap`.
- `announce_segment_remap` (`crates/oceanfs-node/src/announce.rs`):
  signature gains `objects: &[ContainedObject]`; the per-target
  `SegmentRemap` request carries them.
- Proto (`proto/oceanfs/healing.proto`): `message SegmentRemap` gains a
  `repeated oceanfs.segment.ContainedObject objects = 5` field — reuse of the
  existing object-ref wire message f3 added to `PushSealedSegmentRequest`
  (`oceanfs.common.BucketId` + `oceanfs.common.ObjectKey` inside). Regenerate
  the stub crate.

**B. Healing-service receive side** (`healing_service.rs`)

- `announce_remap`: decode the announced object keys alongside
  the chunk table and pass them to the re-point step. Steps 1
  (holder+origin verification), 2 (alias + chunk table), and 4 (stale-replica
  delete) are unchanged — the alias still translates late chunk refs at write
  time.
- `repoint_objects` (`healing_service.rs`) rewritten:
  ```
  table = {(old_offset, length) → new_offset} from chunk_table      // unchanged
  for (bucket, key) in announced_object_keys:                        // bounded
      if let Ok(Some(obj)) = metadata.get_object_metadata(bucket, key):
          if obj references old_segment:
              translate its old-segment chunks through table
              → BatchOp::PutObject (only if changed)
  batch_write(ops)
  ```
  No objects-CF scan (the old `list_objects_all_with_bucket` call and its
  `[review][performance][critical]` marker are gone). Rows absent locally are
  skipped (this holder does not own them). A chunk absent from the table stays
  untouched (tombstoned object the compactor filtered out) — same rule as
  today.
- Guard: a legacy/empty `object_keys` list (a peer on an older binary in a
  mixed-version window — not expected, but cheap) degrades to "re-point
  nothing and rely on the alias + g4 reconciliation", never to a full scan.

**C. Tests rework**

- `healing_service.rs` unit tests
  (`announce_remap_repoints_objects_and_records_alias`,
  `announce_remap_rejects_unheld_or_spoofed`) are extended: the re-point
  assertion seeds announced keys and verifies ONLY those keys' rows change;
  announced-but-absent keys are skipped; announced keys whose chunk is absent
  from the table keep their old ref; unannounced keys referencing the old
  segment are left untouched.
- The remap-propagation integration gate is `node/tests/segment_replication.rs`
  (`compacted_segments_are_readable_from_every_node` — the owner's remap
  fan-out + the peers' re-point through real nodes). `node/tests/
  loss_announcement.rs` exercises ONLY loss announcements (no remap content)
  and stays unchanged aside from compiling against the new wiring.

### Out of Scope (for this feature)

- The membership-list machinery (f3) — f4 consumes its object set.
- The remap **alias** semantics, GAP-1 translation at write time, or g4
  reconciliation — unchanged.
- Any change to the read-repair or hint paths.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `gc/garbage_collector.rs` (`CompactionRemapFn` signature), `gc/segment_compactor.rs` (notify site carries keys), `healing_service.rs` (`repoint_objects` → point lookups; `announce_remap` decodes keys), generated `healing_rpc` stub (regenerated `src/generated/oceanfs.healing.rs`), tests |
| `oceanfs-node` | `modules/durability.rs` fan-out closure, `announce.rs` `announce_segment_remap`, integration gates (`segment_replication`, `gc_compaction`, `loss_announcement`) |

## Interface (Public API)

- `CompactionRemapFn` — signature change (4th argument: `Vec<ContainedObject>`);
  all constructors/wiring sites updated.
- `announce_segment_remap(...)` — new `objects: &[ContainedObject]` parameter.
- `HealingGrpcService::announce_remap` — wire behavior change: the request
  may carry object keys (`repeated oceanfs.segment.ContainedObject objects`);
  a receiver re-points announced keys via point lookups. `RemapAck.applied`
  semantics unchanged.
- `repoint_objects` — private helper; rewritten, no signature exposure.
- Behavior contract: after this feature a holder that receives a remap never
  scans its objects CF (`grep -n list_objects_all_with_bucket` returns nothing
  in `healing_service.rs`).

## Data Flow

```
Owner compactor (segment_compactor.rs, after the ObjectsMoved milestone)
  live_membership (post-f3; sorted + deduped)
  remap_notifier(old S, new S', chunk_table, [ContainedObject of every live object])

modules/durability.rs fan-out closure
  targets = storage_locations(old S) − self
  announce_segment_remap(origin, old, new, chunk_table, objects, targets,…)
    → SegmentRemap{ …, chunks, objects: [{bucket,key}…] } per target (announce.rs)

Peer healing service announce_remap
  1 verify holder+origin (unchanged)
  2 alias.insert(old, new, chunk_table)                     (unchanged)
  3 repoint_objects(old, new, chunk_table, object_keys)
      for each announced (bucket,key): point-read local row;
      if it references old S → translate chunks → batch PutObject
  4 delete stale replica (unchanged)
  ack applied=true
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`,
      `oceanfs-node`, and the proto stub crate. `grep -rn
      "list_objects_all_with_bucket" crates/oceanfs-durability/src/healing_service.rs`
      returns nothing; `repoint_objects`'s scan body and the `:671` review
      marker are gone.
      <!-- REVIEW: independently verified — cargo build --all-targets PASSES in oceanfs-durability and oceanfs-node; the healing.proto stub regenerates via oceanfs-durability/build.rs into src/generated/oceanfs.healing.rs (field `objects` tag 5 present); `grep -rn "list_objects_all_with_bucket" healing_service.rs` returns NOTHING (also no plain list_objects_all variant) and no `[review][performance][critical]` marker remains anywhere in the file (repoint_objects now iterates the announced-key list at healing_service.rs:672-713). -->
- [x] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `cargo test -p oceanfs-node --lib -- --test-threads=1` (PIPELINE.md
      §4.6) pass, adding:
      - the remap notifier fires with the exact live-object key list after
        the ObjectsMoved milestone (compactor unit test);
      - `announce_remap` re-points exactly the announced keys: rows for
        announced keys referencing the old segment are translated through the
        chunk table; rows for unannounced keys referencing the old segment are
        untouched; announced keys absent locally are skipped without error;
      - a key whose chunk is absent from the chunk table (tombstoned object)
        keeps its old ref — unchanged rule;
      - spoof/non-holder rejection still returns `applied=false` before any
        re-point.
      Then `cargo test -p oceanfs-node --test segment_replication --
      --test-threads=1` (the remap-propagation gate: the owner compacts a
      segment held by peers, the remap re-points the peers' rows, reads of the
      re-pointed objects succeed) and `cargo test -p oceanfs-node --test
      gc_compaction -- --test-threads=1`; `cargo test -p oceanfs-node --test
      loss_announcement -- --test-threads=1` (loss-only suite, unchanged).
      <!-- REVIEW: independently re-verified — durability lib 264/264, node lib 66/66, segment_replication 3/3 (incl. compacted_segments_are_readable_from_every_node), gc_compaction 7/7, loss_announcement 1/1; durability gc_compaction 5/5. Test coverage confirmed present: compaction_fires_remap_notifier_with_chunk_table (segment_compactor.rs:801) asserts notifier fires exactly once with object_keys == membership(["remapped.txt"]); announce_remap_repoints_objects_and_records_alias (healing_service.rs:2363) seeds announced/chunk-absent/unannounced/absent keys and asserts only `k` is re-pointed while `stale` keeps old ref, `unannounced` is untouched, and `absent` is skipped; announce_remap_rejects_unheld_or_spoofed (:2515) asserts applied=false with no alias; announce_remap_empty_object_keys_degrades_to_alias_only (:2562) asserts alias recorded + row untouched. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `CompactionRemapFn`, the
      compactor notify-site doc, `announce_segment_remap`, and
      `repoint_objects` document the object-key contract.
      <!-- REVIEW: independently verified — RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p oceanfs-durability -p oceanfs-node PASSES; both crates deny missing_docs and every changed pub item (CompactionRemapFn garbage_collector.rs:26, announce_segment_remap announce.rs:357, repoint_objects healing_service.rs:648) carries an object-key-contract doc comment. -->
- [x] **ADR:** ADR-0034 D5/2b satisfied — the remap carries the object-key
      list so each holder re-points exactly those keys via point lookups; no
      holder scans. Remap fan-out growth is bounded by objects in the repacked
      segment × RF (ADR-0034 Consequences).
      <!-- REVIEW: independently verified — ADR-0034 D5/2b: owner attaches the repacked object-key list (segment_compactor.rs:510 uses the already-materialized sorted+deduped live_membership built at :400-409); receiver repoint_objects (healing_service.rs:672-713) is strictly per-announced-key get_object_metadata point lookups with NO objects-CF scan (ADR-0034 References healing_service.rs:671 review closed). Rejected alternatives not reintroduced: no reverse-index CF / segment_refs, no self-describing .dat, no bounded-scan fallback (empty keys return Ok(0) at :666-668, never a scan). -->
- [x] **Perf:** the receive-side re-point is O(announced keys) point lookups —
      no objects-CF scan per remap event; the key list is built from the
      compactor's already-materialized `live_membership` (no extra store reads
      on the owner); proto objects reuse the existing object-ref wire message
      (perf 1.4 small-vec discipline at the call sites).
      <!-- REVIEW: independently verified — the notifier fires with the already-computed live_membership Vec (no added store reads on the owner); receiver-side ops Vec is pre-sized to object_keys.len() (healing_service.rs:670) and per-object chunk lists use SmallVec<[ChunkRef;4]> (perf 1.4); announce.rs maps ContainedObject → existing proto message (announce.rs:386-392) per perf 1.4; fan-out remains storage_locations(old) − self (modules/durability.rs:236-247). -->
- [x] **Integration:** a two-node fixture where the owner compacts a segment
      held by a peer converges: the peer's rows referencing the old segment
      are re-pointed to the new one without a full scan, reads of the
      re-pointed objects succeed, and `RemapAck.applied` is true.
      <!-- REVIEW: independently verified — the remap-propagation gate is node/tests/segment_replication.rs::compacted_segments_are_readable_from_every_node (:344): after DELETE + GC compaction on the owner, reads of the surviving objects return byte-identical bodies through A, B, AND C within the convergence deadline; loss_announcement.rs contains no remap content (only a loss-only comment at :44) and was NOT modified by this feature. Unit tests pin applied=true (healing_service.rs:2454, :2633) and applied=false for spoofs (:2554). -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).

## Implementation notes (2026-09-06)

Final implemented state — independent reviewer PASS, 0 gaps; all DoD items
checked with `<!-- REVIEW: … -->` evidence in place. Recorded decisions and
corrections supersede or refine the prose above.

- **Design decision Q1=A — the remap payload is a `Vec<ContainedObject>`.**
  `CompactionRemapFn` (`gc/garbage_collector.rs:26`) is
  `Arc<dyn Fn(SegmentId, SegmentId, Vec<RemappedChunk>, Vec<ContainedObject>)
  + Send + Sync>`. The 4th argument reuses the f3 **core** object-ref type
  (`oceanfs_core::ContainedObject { bucket, key }`) rather than introducing a
  new `(BucketId, ObjectKey)` tuple pair or a dedicated struct — the compactor
  already materializes exactly this type for the seal-time membership
  (`segment_compactor.rs:402-409`), so the notifier moves that Vec out with no
  conversion.
- **Design decision Q2=A — the wire field reuses the f3 object-ref message.**
  `proto/oceanfs/healing.proto` `message SegmentRemap` carries
  `repeated oceanfs.segment.ContainedObject objects = 5` — the same
  `oceanfs.segment.ContainedObject` message f3 added to
  `PushSealedSegmentRequest` (`oceanfs.common.BucketId` +
  `oceanfs.common.ObjectKey` inside). No new `RemappedObject`/`ObjectRef`
  message was introduced; the generated healing stub (`oceanfs.healing.rs`,
  `objects` field tag 5) and `announce.rs:386-392` map the core type straight
  onto it.
- **Fan-out closure location — the doc's original 2026-09-04 anchors were
  stale.** The `.with_compaction_remap_notifier` closure and its inner
  `tokio::spawn` (which calls `announce_segment_remap`) live in
  `crates/oceanfs-node/src/modules/durability.rs` (lines ~220-259), the
  post-c1 composition root on the ADR-0032 unified store — NOT `node.rs`
  (the doc originally pinned `node.rs:1140-1208`). The line-pinned
  `repoint_objects` (`:654-716`) and `announce_remap` (`:1483-1589`) ranges
  were likewise stale; all of them are now **un-anchored module references**
  throughout §Summary/§Scope/§Data-Flow so the prose does not rot against a
  moving line base.
- **Integration gate correction.** The DoD test bullet now names
  `crates/oceanfs-node/tests/segment_replication.rs` →
  `compacted_segments_are_readable_from_every_node` as the remap-propagation
  gate (owner fan-out + peers' re-point through real nodes, pinned at
  segment_replication.rs:344). `node/tests/loss_announcement.rs` contains NO
  remap content (loss-only suite; the earlier "updated for the new
  signature" wording was wrong) and is unchanged aside from compiling against
  the new wiring.
- **Behavior notes (accepted in review, all covered by tests):**
  - An empty `object_keys` list (a legacy/mixed-version owner that cannot
    know membership) degrades to **alias-only**: the receiver records the
    alias + chunk table and re-points nothing, relying on the g4
    reconciliation failsafe — never a scan. Pinned by
    `announce_remap_empty_object_keys_degrades_to_alias_only`
    (healing_service.rs:2562).
  - `RemapAck.applied` semantics are unchanged: `true` when the alias is
    recorded and any announced keys this holder owns are re-pointed; `false`
    for spoof/non-holder requests before any re-point
    (`announce_remap_rejects_unheld_or_spoofed`, healing_service.rs:2515).
  - **Fully-dead compactions never fire the notifier**: a segment with zero
    live objects returns early on the delete path
    (`segment_compactor.rs:224-240`) and never reaches the notifier site at
    `:498-510` — there is nothing to re-point and no `S → S'` pair exists.
- **Verification summary (independent reviewer):** durability lib
  264/264; node lib 66/66; node doctests 38/38; durability doctests 24/24;
  node `segment_replication` 3/3 (incl.
  `compacted_segments_are_readable_from_every_node`); node `gc_compaction`
  7/7; node `loss_announcement` 1/1; durability integration `gc_compaction`
  5/5. `cargo clippy`/`rustfmt`/`cargo doc` clean (`RUSTDOCFLAGS="-D
  warnings"`); `grep -n "list_objects_all_with_bucket"`
  `crates/oceanfs-durability/src/healing_service.rs` returns NOTHING (no
  plain `list_objects_all` variant either).
