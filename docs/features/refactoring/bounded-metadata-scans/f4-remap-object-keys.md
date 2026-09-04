---
feature: "f4: Compaction Remap Carries the Object-Key List"
epic: "refactoring/bounded-metadata-scans"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: f3-seal-membership-list
    epic: refactoring/bounded-metadata-scans
    reason: The owner's re-pointed object set (the keys the remap notification must carry) is derived from the compactor's live-object enumeration, which f3 bounds via the seal-time membership list instead of a full scan; f4 lands on the f3 compactor shape
  - epic: refactoring/store-unification
    reason: The healing service and remap fan-out run on the ADR-0032 unified store / single composition-root instance (the node.rs closure this feature changes lives in the post-c1 wiring)
adr:
  - 0034-bounded-metadata-accounting
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf:
  - "1.4 SmallVec for small metadata structures"
  - "7.1 minimize lock hold duration"
created: 2026-09-04
updated: 2026-09-04
---

# f4: Compaction Remap Carries the Object-Key List

## Summary

The fourth ADR-0034 scan is the **healing remap**: when the owner compacts
`S → S'`, each peer that holds a stale copy of S must re-point its own object
rows. Today the peer's `repoint_objects`
(`crates/oceanfs-durability/src/healing_service.rs:654-716`) discovers which
rows to re-point by scanning its entire objects CF
(`list_objects_all_with_bucket`, `:675` — the `[review][performance][critical]`
at `:671`). ADR-0034 D5/2b deletes that scan: the owner already knows exactly
which objects it repacked (the live objects from its membership list, f3), so
the **remap notification carries the object-key list** alongside the existing
old→new chunk table, and each holder re-points exactly those keys via point
lookups.

This feature changes the notification contract end to end — the
`CompactionRemapFn` type, the compactor's notify site, the node.rs fan-out
closure and `announce.rs`, the `SegmentRemap` proto/announcement, and the
healing-service `announce_remap` handler — and rewrites `repoint_objects` to
a per-key point lookup. It lives in `oceanfs-durability`
(`gc/garbage_collector.rs`, `gc/segment_compactor.rs`,
`healing_service.rs`), `oceanfs-core`/proto (remap message), and
`oceanfs-node` (`node.rs`, `announce.rs`).

## Scope

### In Scope

**A. Notification contract gains the object-key list**

- `CompactionRemapFn` (`gc/garbage_collector.rs:17`):
  `Arc<dyn Fn(SegmentId, SegmentId, Vec<RemappedChunk>, Vec<(BucketId,
  ObjectKey)>) + Send + Sync>` — or a single `CompactionRemap { old, new,
  chunk_table, object_keys }` struct if the closure arity gets unwieldy.
  `object_keys` = the `(bucket, key)` of every live object the owner repacked
  (the compactor's `live_objects`, post-f3 membership enumeration), ordered
  and deduped.
- Compactor notify site (`gc/segment_compactor.rs:439-452`): build and fire
  the key list from `live_objects` alongside the `chunk_table`. The doc
  comment (`:77-96`) and the GAP-1 rationale are updated: a receiver re-points
  exactly the announced keys; keys it does not hold locally are no-ops.
- Fan-out (`crates/oceanfs-node/src/node.rs:1140-1208`): the
  `.with_compaction_remap_notifier` closure and its inner `tokio::spawn` pass
  the key list into `announce_segment_remap`.
- `announce_segment_remap` (`crates/oceanfs-node/src/announce.rs:348-415`):
  signature gains `object_keys: &[(BucketId, ObjectKey)]`; the per-target
  `SegmentRemap` request (`:380-385`) carries them.
- Proto (`proto/oceanfs/healing.proto`): `message SegmentRemap` (`:263-279`)
  gains a repeated `RemappedObject`/`ObjectRef` field
  (`bucket`, `key` — reuse the existing common object-key message shape if
  one exists, else add a minimal pair). Regenerate the stub crate.

**B. Healing-service receive side** (`healing_service.rs`)

- `announce_remap` (`:1483-1589`): decode the announced object keys alongside
  the chunk table (`:1500-1508`) and pass them to the re-point step. Steps 1
  (holder+origin verification), 2 (alias + chunk table), and 4 (stale-replica
  delete) are unchanged — the alias still translates late chunk refs at write
  time.
- `repoint_objects` (`:654-716`) rewritten:
  ```
  table = {(old_offset, length) → new_offset} from chunk_table      // unchanged
  for (bucket, key) in announced_object_keys:                        // bounded
      if let Ok(Some(obj)) = metadata.get_object_metadata(bucket, key):
          if obj references old_segment:
              translate its old-segment chunks through table
              → BatchOp::PutObject (only if changed)
  batch_write(ops)
  ```
  No `list_objects_all_with_bucket`. Rows absent locally are skipped (this
  holder does not own them). A chunk absent from the table stays untouched
  (tombstoned object the compactor filtered out) — same rule as today.
- The `[review][performance][critical]` block at `:671-674` is removed with
  the scan.
- Guard: a legacy/empty `object_keys` list (a peer on an older binary in a
  mixed-version window — not expected, but cheap) degrades to "re-point
  nothing and rely on the alias + g4 reconciliation", never to a full scan.

**C. Tests rework**

- `healing_service.rs` unit tests (`announce_remap_repoints_objects_and_records_alias`
  at `:2294`, `announce_remap_rejects_unheld_or_spoofed` at `:2378`) are
  extended: the re-point assertion now seeds announced keys and verifies only
  those keys' rows change; unannounced keys referencing the old segment are
  left untouched.
- Node `loss_announcement.rs` integration test is updated for the new
  signature and asserts the receiver re-points via the announced keys.

### Out of Scope (for this feature)

- The membership-list machinery (f3) — f4 consumes its object set.
- The remap **alias** semantics, GAP-1 translation at write time, or g4
  reconciliation — unchanged.
- Any change to the read-repair or hint paths.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `gc/garbage_collector.rs` (`CompactionRemapFn` signature), `gc/segment_compactor.rs` (notify site carries keys), `healing_service.rs` (`repoint_objects` → point lookups; `announce_remap` decodes keys), tests |
| `oceanfs-core` | `proto` remap message field (via the stub crate) |
| `oceanfs-node` | `node.rs` fan-out closure, `announce.rs` `announce_segment_remap`, `loss_announcement.rs` integration test |

## Interface (Public API)

- `CompactionRemapFn` — signature change (4th argument: `Vec<(BucketId,
  ObjectKey)>` or a struct); all constructors/wiring sites updated.
- `announce_segment_remap(...)` — new `object_keys` parameter.
- `HealingGrpcService::announce_remap` — wire behavior change: the request
  may carry object keys; a receiver re-points announced keys via point
  lookups. `RemapAck.applied` semantics unchanged.
- `repoint_objects` — private helper; rewritten, no signature exposure.
- Behavior contract: after this feature a holder that receives a remap never
  scans its objects CF (`grep` returns nothing in `healing_service.rs`).

## Data Flow

```
Owner compactor (segment_compactor.rs:439-452)
  live_objects (post-f3 membership; dead filtered)
  remap_notifier(old S, new S', chunk_table, [(bucket,key) of every live object])

node.rs fan-out closure (:1146)
  targets = storage_locations(old S) − self
  announce_segment_remap(origin, old, new, chunk_table, object_keys, targets,…)
    → SegmentRemap{ …, chunks, objects: [{bucket,key}…] } per target (announce.rs)

Peer healing service announce_remap (:1483)
  1 verify holder+origin (unchanged)
  2 alias.insert(old, new, chunk_table)                     (unchanged)
  3 repoint_objects(old, new, chunk_table, object_keys)
      for each announced (bucket,key): point-read local row;
      if it references old S → translate chunks → batch PutObject
  4 delete stale replica (unchanged)
  ack applied=true
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`,
      `oceanfs-node`, and the proto stub crate. `grep -rn
      "list_objects_all_with_bucket" crates/oceanfs-durability/src/healing_service.rs`
      returns nothing; `repoint_objects`'s scan body and the `:671` review
      marker are gone.
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
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
      Then `cargo test -p oceanfs-node --test loss_announcement --
      --test-threads=1` and `cargo test -p oceanfs-node --test gc_compaction
      -- --test-threads=1`.
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `CompactionRemapFn`, the
      compactor notify-site doc, `announce_segment_remap`, and
      `repoint_objects` document the object-key contract.
- [ ] **ADR:** ADR-0034 D5/2b satisfied — the remap carries the object-key
      list so each holder re-points exactly those keys via point lookups; no
      holder scans. Remap fan-out growth is bounded by objects in the repacked
      segment × RF (ADR-0034 Consequences).
- [ ] **Perf:** the receive-side re-point is O(announced keys) point lookups —
      no objects-CF scan per remap event; the key list is built from the
      compactor's already-materialized `live_objects` (no extra store reads on
      the owner); proto objects reuse the existing small-vec discipline
      (perf 1.4).
- [ ] **Integration:** a two-node fixture where the owner compacts a segment
      held by a peer converges: the peer's rows referencing the old segment
      are re-pointed to the new one without a full scan, reads of the
      re-pointed objects succeed, and `RemapAck.applied` is true.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
