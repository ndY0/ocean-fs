# ADR-0034: Bounded Metadata Accounting — Eliminate Full-Object Scans

**Status:** Accepted
**Date:** 2026-09-04
**Deciders:** OceanFS architecture team

---

## Context

The 2026-08-25/09-03 review (triage Theme 4) found four background paths
that scan the whole objects column family — O(all objects) per run:

| Consumer | Site | Query | Recurrence |
|---|---|---|---|
| Orphan reaper | `gc/orphan_reaper.rs:294-313` (`build_referenced_set` → `list_objects_all`) | "which segments are referenced by any live object?" | every cycle |
| GC liveness | `gc/garbage_collector.rs:521` (`process_tombstones` Phase 2 → `list_objects_all_with_bucket`) | "per-segment live bytes" | every GC cycle |
| GC compactor | `gc/segment_compactor.rs:541-558` (`find_objects_in_segment`) | "which objects reference segment S?" | per compaction candidate |
| Healing remap | `healing_service.rs:654-716` (`repoint_objects` → `list_objects_all_with_bucket`) | "which objects reference the old segment (to re-point)?" | per compaction-remap event |

At millions of objects these are unbounded CPU + full `Vec` materializations
(`list_objects_all[_with_bucket]` collect the whole CF). Three further facts
shape the design:

1. **Segments are not self-describing.** `SegmentIndexEntry {
   offset, length, blob_key_hash }` (`oceanfs-storage/src/segment/index.rs`)
   stores a *hash* of a blob key, not `(bucket, key)`. The only
   object→chunk→segment mapping is the forward objects CF.
2. **Delete already captures chunks.** `RocksDbMetadataStore::delete_object`
   reads the object row's chunks before removing it and writes them into a
   `Tombstone` (`metadata/store.rs:472-485`). GC marks dead bytes from these
   tombstones today (`garbage_collector.rs:480-500`).
3. **Overwrite does NOT capture.** The PUT path ("create **or overwrite**",
   `s3_handler/handlers.rs:60`) builds fresh `ObjectMetadata` and calls
   `put_object_in_bucket`, which blindly replaces the old row and clears its
   tombstone. The old version's chunks — the previous segment's bytes —
   vanish from the objects CF **without any dead record**. Today the orphan
   reaper's full object scan is the only mechanism catching these bytes.

ADR-0023/0029 steer the project **away from RocksDB coupling** toward a
native store; ADR-0029 D7 already defers a "segment self-description" idea
for g8 metadata-loss recovery.

## Decision

**Eliminate the full-object scans by inverting the algorithms: replace
object *counting* with byte *accounting*, and make every recurring query a
point or per-segment operation. No reverse index is added; no new RocksDB
surface is introduced.**

### D1. The accounting invariant

For every sealed segment S:

```
logical_total(S)   := stored on SegmentMetadata (total_bytes) at seal
dead_bytes(S)      := Σ chunk.length over every captured dead-chunk record referencing S
live_bytes(S)      := logical_total(S) − dead_bytes(S)
orphan(S)          := dead_bytes(S) ≥ logical_total(S)   (no live referencing object)
```

**Capture rule (the invariant that makes scans unnecessary): every
chunk-ref that stops being referenced by a live object row MUST be captured
into a dead-chunk record — atomically with the row change.**

### D2. Capture completeness (supersede-capture on overwrite)

- **Delete** already captures (`delete_object` writes the tombstone with the
  old chunks). Preserved.
- **Overwrite (PUT on an existing key)** must capture the superseded
  version's chunks. Implemented in `RocksDbMetadataStore::put_object_in_bucket`
  (the single concrete choke point behind the trait — the write coordinator,
  hint-apply, replica-apply, and the node `MetadataStoreAdapter` all funnel
  here): read the existing row; if present, fold its chunks into a
  **supersede dead-chunk record** in the same RocksDB `WriteBatch` as the new
  row. Atomicity is guaranteed by the batch.
- **Supersede records must not collide with, nor delete, the new live row.**
  A supersede is NOT a tombstone of the key: the object still exists (new
  version). Plain `(bucket,key)` tombstone keys are therefore unusable —
  `put_object_in_bucket` clears tombstones for the now-live key. Encoding
  decision (implementation-level, constrained as follows): supersede records
  carry the superseded chunks + a version discriminator (e.g., the
  superseded object's HLC / creation time) so they (a) coexist with the live
  row, (b) age under the same TTL discipline as tombstones, (c) are
  attributable to the segments they reference, and (d) are never interpreted
  as a delete of the new version. Whether they live in the deletions CF under
  versioned keys or in a dedicated dead-chunks CF is an implementation
  choice; both must satisfy (a)-(d).
- **Every row-replacement path** must capture: S3 PUT overwrite, hinted-handoff
  apply that supersedes, replica metadata apply that supersedes, and
  multipart finalization that replaces an existing object. The single choke
  point makes this tractable.

### D3. GC liveness = accounting, not scanning

`process_tombstones` (GC) derives per-segment live bytes from:
`registry.for_each` (O(live segments), in-memory, ADR-0025) + the dead-chunk
captures (tombstones + supersedes, aged by TTL). **No `list_objects_all`
call remains on the GC path.** The `LivenessTracker` registers segments from
the registry and marks dead from captures; live = total − dead.

### D4. Orphan reaper = fully-dead detection

`orphan(S)` is now derivable from accounting: a segment whose captured dead
bytes reach its logical total has no live referencing object. The reaper
iterates the registry + on-disk `.dat` set (both bounded) and applies the
TTL gate, **without building a referenced-set from all objects**
(`build_referenced_set` deleted). The historical phase-2b "unregistered
`.dat`" path is obsolete once lifecycle registration is enforced everywhere
(the receiver already registers; ADR-0031's legacy purge removes the last
unregistered writer) and is removed.

### D5. Compactor + remap use a seal-time per-segment object membership list

- **Seal-time membership record (the 2a decision):** the write coordinator
  knows `(bucket, key)` for every chunk it appends. Record a compact
  **contained-objects list** for the segment at seal time (stored with the
  segment's metadata/checkpoint, not inside the `.dat` binary). Storage cost
  is O(objects-per-segment), written once at seal, deleted with the segment.
- **Compactor** (`find_objects_in_segment` deleted): read S's own membership
  list, filter dead via the dead-keys/accounting set, point-read the
  survivors. No scan.
- **Compaction remap to peer holders** (`repoint_objects` deleted): the owner
  already repacks specific live objects; the remap notification carries the
  **object-key list** alongside the existing old→new chunk table, so each
  holder re-points exactly those keys via point lookups. No holder scans.
- The membership list also serves the g7 catch-up enumeration and is the
  natural seed for ADR-0029 D7's deferred self-description when g8 lands.

### Out of scope

- Reverse-index CF (`segment_refs`) — rejected (Option 2): write-path
  amplification + RocksDB coupling + still needs byte accounting for GC.
- Self-describing `.dat` binary format (Option 3) — deferred to g8
  (ADR-0029 D7); the seal-time membership record is the metadata-level
  precursor.
- The scheduler `keyspace_fraction` sharding (`durability-scheduler/f3`) —
  enabled by this ADR but a separate epic.
- Any change to the Merkle/anti-entropy protocol.

## Consequences

### Positive

- Four O(all-objects) scans become O(bounded): registry/`.dat` iteration +
  point lookups. `list_objects_all[_with_bucket]` full-`Vec` materializations
  disappear from the durability hot paths.
- Removes the largest obstacle to scheduler keyspace sharding and to g7/g8
  at scale.
- No new RocksDB CF, no reverse-index maintenance cost, no write-path
  amplification — aligned with ADR-0023/0029's anti-RocksDB direction.
- Overwrite-orphaned bytes (a real leak today, only caught by the reaper's
  scan) become captured at write time — a correctness improvement on its own.

### Negative

- **Correctness now rests on capture completeness.** GC/orphan detection
  trusts that every dead byte is captured. Any path that replaces an object
  row without capture silently leaks until a full scan runs — and there will
  no longer be one. Mitigated by: single choke point, atomic batch capture,
  and the fault-injection matrix (D6).
- **Accounting soundness edge cases must be proven:** delete→re-PUT,
  overwrite where the old version lives on a *different* segment than the
  new, multipart objects spanning many segments, concurrent overwrite racing
  a delete, supersede of a tombstoned-but-re-PUT key, GC-driven deletes.
- Seal-time membership storage grows segment metadata (O(objects-per-segment),
  written once, bounded per segment) and needs event-WAL/checkpoint plumbing
  (it rides the same durable path as the segment's metadata).
- Remap notification grows (object-key list) — bounded by objects in the
  repacked segment, RF-sized fan-out.

### Neutral

- GC's liveness model changes from "count surviving objects" to "trust
  accounting" — the metrics semantics stay the same.
- The orphan reaper's external behavior is unchanged (same TTL grace, same
  deletion), only its detection source changes.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Reverse-index CF (`segment_refs`)** | Arbitrary segment→objects queries via prefix scans; simple mental model | Write-path amplification (N reverse rows per object); deepens RocksDB coupling (ADR-0023 counter-direction); rebuild path on corruption; still needs byte accounting for GC live/dead totals | Rejected: Option 2. Costs the most on the hot path and does not remove the accounting need |
| **Self-describing `.dat` (per-chunk object identity in the binary)** | Index travels with replicas; serves g8 directly | On-disk format change (v3 header/index); list goes stale on delete/overwrite (must filter via point lookups); heavier than needed now | Rejected for this ADR; deferred to g8 (ADR-0029 D7). The seal-time metadata membership list (D5) is the lightweight precursor |
| **Keep scans, bound them (iterator, no Vec materialization)** | Smallest change | Still O(all objects) CPU/IO per cycle; does not fix the review's complaint at millions of objects; per-event `repoint_objects` remains unbounded | Rejected: bounding memory without bounding time does not meet the requirement |
| **Pure delete-accounting, no supersede capture** | Simplest accounting | Overwrite-orphaned bytes leak (Hole #1) — exactly the case the reaper's scan catches today; reintroduces a correctness hole | Rejected: capture completeness (D2) is mandatory |

## D6. Fault-injection correctness matrix (acceptance bar)

The epic's DoD must include a crash/fault matrix proving capture
completeness and accounting soundness:

| Scenario | Assertion |
|---|---|
| DELETE then idle | captured dead bytes == old chunk bytes; segment liveness drops |
| PUT overwrite (old on segment A, new on B) | A's bytes captured; B live; no leak |
| DELETE → re-PUT same key | supersede of re-PUT's predecessor captured; re-PUT live; no double-dead |
| Multipart object spanning N segments, then overwrite | all N segments' bytes captured exactly once |
| Hint-apply that supersedes an existing key | capture fires on the apply path |
| Replica metadata apply overwriting a row | capture fires on the replica path |
| Crash between row write and capture | impossible by construction (same WriteBatch); test asserts atomicity |
| GC-driven delete of a tombstoned key | already-tombstoned bytes not double-counted |
| Supersede of a tombstoned-but-re-PUT key | no delete of the live row; correct aging |
| Corrupt/partial tombstone (legacy, no chunks) | degrades to orphan-reaper accounting, never to a full scan |

## References

- Review comments: `gc/orphan_reaper.rs:297`, `gc/garbage_collector.rs:521`,
  `healing_service.rs:671`, `anti_entropy/engine.rs:184,199`,
  `reconcile.rs:148`
- ADR-0025 (lifecycle registry = segment set), ADR-0031 (legacy removal),
  ADR-0032 (store unification), ADR-0017 (scheduler — sharding enabled by
  this ADR), ADR-0023 (native-store direction), ADR-0029 D7 (deferred
  self-description)
- Roadmap: `docs/features/refactoring/review-2026-09-roadmap.md` (wave 2 ⑥),
  orchestration doc (wave 2 ⑥)
