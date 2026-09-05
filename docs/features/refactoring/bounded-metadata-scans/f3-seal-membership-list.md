---
feature: "f3: Seal-Time Per-Segment Record (total_bytes + contained-objects)"
epic: "refactoring/bounded-metadata-scans"
status: done
priority: critical
owner: ""
dependencies:
  - epic: refactoring/store-unification
    reason: The compactor that consumes the membership list runs on the ADR-0032 unified data store and lifecycle-coordinated writes; ADR-0034 D5 presupposes that shape
  - epic: refactoring/legacy-mode-removal
    reason: This feature bumps the same event-WAL Seal record and checkpoint formats that legacy-mode-removal/f3-format-break-and-test-rework changes (it removes the no-flag pool-id Seal arms and the v2 checkpoint decode). f3 must land after that break so there is one format migration; the same "no production data, we refactor, refuse old dirs" stance applies
adr:
  - 0034-bounded-metadata-accounting
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf:
  - "1.4 SmallVec for small metadata structures"
  - "6.3 #[repr(C)] for all on-disk / on-wire structures"
  - "7.1 minimize lock hold duration"
created: 2026-09-04
updated: 2026-09-06
---

# f3: Seal-Time Per-Segment Record (total_bytes + contained-objects)

## Summary

ADR-0034 needs two per-segment facts that today's machine does not store:

1. **D1 — the logical total.** GC liveness and orphan detection are
   `live = logical_total − dead`; the total must be durable on the segment's
   metadata (`SegmentMetadata.total_bytes`, recorded at seal). Today the
   registry stores no total and GC seeds `register_segment(id, 0)`
   (`gc/garbage_collector.rs:467-471`).
2. **D5/2a — the contained-objects membership list.** The compactor's
   `find_objects_in_segment` (`gc/segment_compactor.rs:541-560`) is an
   O(all-objects) scan because segments are not self-describing. The write
   coordinator knows `(bucket, key)` for every chunk it appends, so it can
   record a compact **contained-objects list at seal time** — stored with the
   segment's metadata on the event-WAL + checkpoint path (ADR-0024/25), NOT
   inside the `.dat` binary (the ADR-0034 boundary; ADR-0029 D7's deferred
   self-description stays deferred).

This feature records both facts at seal, persists them through the event
WAL + checkpoint (so they survive restart), propagates them with segment
metadata to replica holders, and switches the compactor's object discovery
from the full scan to a **membership-list read + per-key point lookups**.
It lives in `oceanfs-storage` (lifecycle/event-wal/checkpoint/sealer),
`oceanfs-server` (write coordinator append-key recording + seal worker +
sealed-segment push), `oceanfs-core` (the contained-object type and the
`SegmentMetadata.total_bytes` field), and `oceanfs-durability` (compactor).

## Scope

### In Scope

**A. Core types (`oceanfs-core`)**

- `SegmentMetadata` gains `pub total_bytes: u64` (ADR-0034 D1; `#[serde(default)]`
  for JSON tolerance — bincode legacy handling is a checkpoint-version
  concern, see B). A sealed segment's `total_bytes` = the data-section byte
  length of its `.dat` at seal (= Σ blob lengths, and = the `size` field the
  sealer already writes in the segment header, `sealer.rs:343-362`).
- New `ContainedObject { bucket: BucketId, key: ObjectKey }` (or
  `ContainedObjects` newtype over an ordered/deduped `Vec`). The list is
  **deduplicated by `(bucket, key)`** and **sorted** so its serialization is
  deterministic (an object split across chunks in one segment appears once).
- `# Examples`, `#[derive]`, `#![deny(missing_docs)]` per coding.md.

**B. Durable record: SealEvent extension + checkpoint v4**

The membership list and total must survive a restart whose fold replays
events after the last checkpoint, so the list rides the **SealEvent record
itself** (one atomic durable record — a separate event could be lost between
the seal and the membership append).

- `event_wal.rs` `SealEvent` (`:197-225`): add `total_bytes: u64` to the
  fixed fields (or keep it solely on `SegmentMetadata` in the checkpoint —
  see below) and add an **optional, length-prefixed contained-objects tail**:
  a new flags byte `SEAL_FLAG_CONTAINED_OBJECTS`; when set, the payload
  carries `[u32 LE blob_len][bincode(Vec<ContainedObject>)]` after the fixed
  fields. Encoder (`SegmentEvent::to_record_bytes`, `:315-343`) and decoder
  arms (`decode_payload`, `:452-…`) gain the tail; the framing doc
  (`:24-53`) and size constants are updated. This mirrors exactly how the
  `pool_id` and `repacked_from` extensions were framed (and how
  `legacy-mode-removal/f3` reworks the same arms) — perf 6.3 byte-explicit
  layout.
  - Recommended split to limit churn: keep `total_bytes` on
    `SegmentMetadata` (checkpoint meta payload) **and** in the SealEvent's
    fixed fields? No — record it once. **Recommendation:** put
    `total_bytes` on `SegmentMetadata` only (D1's wording) and carry the
    variable `contained_objects` tail on the SealEvent. The checkpoint
    stores `SegmentMetadata` (now including `total_bytes`) and, for Sealed
    entries, the contained-objects tail next to the `repacked_from` field.
- `event_checkpoint.rs`: bump `CHECKPOINT_VERSION` (`:84`) from 3 to **4**.
  Sealed entry format gains the `total_bytes` in the bincode metadata payload
  and a length-prefixed contained-objects payload after `repacked_from`.
  Following ADR-0031 D3/D4's stance (no production data, we refactor): v3
  checkpoints are refused with the explicit "unsupported checkpoint version"
  error, not decoded with defaults — the same `Error` classification seam
  legacy-mode-removal/f3 used for v2. Update the snapshot format doc
  (`:18-52`) and `LegacySegmentMetadata`-style decode helpers accordingly.
- `lifecycle.rs`: `LifecycleEntry` (`:123-163`) gains
  `contained_objects: Option<Arc<[ContainedObject]>>` for Sealed entries
  (runtime mirror of the durable record; `None` for Reserved/Deleted and for
  membership-less Sealed entries). The fold (SealEvent → registry) populates
  it. `SegmentLifecycleCoordinator::request_seal` (`:1975-…`) grows a way to
  pass the list — recommended: an extended
  `request_seal_with_contained(id, metadata, repacked_from, contained:
  Option<&[ContainedObject]>)` with the existing `request_seal` delegating
  `None`, so current callers and tests stay put; the production seal path and
  the compactor call the extended form.
- Registry memory bound note (ADR-0025 Decision 5): per-entry RAM grows by
  O(objects-per-segment) for Sealed entries (≈ 64 entries for a standard
  4 MiB segment at 64 KiB chunks; Multi-tier segments hold more). Keep the
  registry-size gauge visible; ADR-0034 accepts this (written once at seal,
  deleted with the segment).

**C. The write coordinator records `(bucket, key)` per append**
(`oceanfs-server/src/write/coordinator.rs`)

- `record_blob_entry` (`:1496-1499`) currently records only
  `SegmentIndexEntry { offset, length, blob_key_hash }`. Add a parallel
  per-segment key record — recommended: a second
  `DashMap<SegmentId, Vec<(BucketId, ObjectKey)>>` populated alongside
  `segment_entries` (`:205`) from the same append hooks (`:694, :747, :807`
  in `put()`, `:1076, :1112` in `apply_hinted_object`) — the append-hook
  closures capture `req.bucket`/`req.key` (or the hint's bucket/key).
  `SegmentIndexEntry` itself is **not** extended (it is the in-`.dat` blob
  index; nothing new goes inside the `.dat`).
- The seal worker (`start_seal_worker`, `:1515-…`) drains the key map beside
  `segment_entries` (`:1554-1555`), dedupes to `Vec<ContainedObject>`, and
  passes the list into `seal_from_data` (new parameter) so it rides the flush
  registration into `request_seal_with_contained`.
- WAL-replayed segments (entries drained empty — the comment at `:1557-1573`
  explains this is legitimate) seal with **no** contained-objects list
  (`None`): the data-WAL does not carry keys, and reconstructing membership
  from the objects CF would be the very scan being eliminated. Such segments
  are non-compactable (see D) but remain fully re-readable and reapable when
  fully dead.

**D. Compactor consumes the membership list**
(`oceanfs-durability/src/gc/segment_compactor.rs`)

- `find_objects_in_segment` (`:541-560`) is deleted. `compact_segment`
  (`:183-…`) receives the segment's `contained_objects` (GC `run_cycle` reads
  `registry.get(segment_id)` at `garbage_collector.rs:351-360` — pass
  `entry.contained_objects` alongside `segment_meta`) and enumerates:
  ```
  for (bucket, key) in contained:
      if let Ok(Some(obj)) = metadata.get_object_metadata(bucket, key):
          if obj.chunks references segment_id: include (bucket, obj)
  ```
  O(objects-per-segment) point lookups; the `dead_object_keys` filter
  (`:193-199`) is unchanged.
- **Membership-less guard:** a Sealed segment whose `contained_objects` is
  `None` (WAL-replayed; pre-feature) is **skipped** by compaction — GC filters
  such candidates before spawning (`garbage_collector.rs:353-360`) and
  `compact_segment` also refuses if handed one. Never scan to recover it.
- The **repacked new segment's seal** (`:343-371`) calls
  `request_seal_with_contained` with the membership built from `live_objects`
  (the compactor knows `(bucket, key)` of every object it repacks).
- The remap notification site (`:439-452`) — see f4 — passes the same
  `live_objects` keys.

**E. Propagation to replica holders**

A replica holder runs the same GC machinery over segments it holds, so its
registry entry needs the list too. Extend the segment-metadata push payloads
that rebuild `SegmentMetadata`/register a replica on the receiver:

- `PushSealedSegmentRequest` (sealed-segment replication;
  `grpc/segment_service.rs:761-…`): add a repeated `ContainedObject`-shaped
  field (proto: `bucket`, `key`) and seed
  `request_seal_with_contained` on the receiver. (`total_bytes` needs no wire
  change — the receiver already computes the data length from the stream,
  `:772-801`.)
- The healing-service sealed-replica push path that registers a received
  segment carries the same metadata — extend it identically.

### Out of Scope (for this feature)

- f2's accounting consumers (GC liveness from `total_bytes`, orphan reaper
  fully-dead) — they consume what this feature records.
- f4's remap key-list — the compactor site this feature already touches is
  f4's seam, but the notification signature change is f4's.
- Rewriting the startup compaction-recovery `ObjectLookup`
  (`compaction_recovery.rs:94-105` — one `list_objects_all_with_bucket` per
  **marked unit at startup**, not one of ADR-0034's four per-cycle/per-event
  scans). Note in the module docs that it can later be driven off membership
  + accounting; it stays as-is in this epic.
- Any `.dat` binary format change (ADR-0034 Out of scope; ADR-0029 D7).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | `SegmentMetadata.total_bytes` field; new `ContainedObject` type |
| `oceanfs-storage` | `segment/event_wal.rs` (SealEvent contained-objects tail + framing + tests), `segment/event_checkpoint.rs` (v4 + per-entry contained payload), `segment/lifecycle.rs` (`LifecycleEntry.contained_objects`, extended seal request, fold), `segment/sealer.rs` (accept + forward the list), error variant for unsupported checkpoint version |
| `oceanfs-server` | `write/coordinator.rs` (per-append `(bucket,key)` map + seal-worker drain + `seal_from_data` arg), `grpc/segment_service.rs` (push metadata field), proto bump |
| `oceanfs-durability` | `gc/segment_compactor.rs` (membership-based discovery; membership-less skip; seal-with-membership), `gc/garbage_collector.rs` (pass `contained_objects`; skip membership-less candidates) |
| `oceanfs-node` | Composition-root construction of the compactor/push (signature-only) |

## Interface (Public API)

- `oceanfs_core::SegmentMetadata.total_bytes: u64` — the seal-time logical
  total (ADR-0034 D1).
- `oceanfs_core::ContainedObject { bucket: BucketId, key: ObjectKey }` — one
  object contained in a sealed segment.
- `SegmentLifecycleRegistry`/`LifecycleEntry.contained_objects:
  Option<Arc<[ContainedObject]>>` — the per-segment membership (Sealed only).
- `SegmentLifecycleCoordinator::request_seal_with_contained(...)` — extended
  seal request; `request_seal` delegates `None` (signature-compatible).
- Compactor `compact_segment` gains the contained-objects argument.
- Behavior contract: every segment sealed after this feature carries
  `total_bytes` and (unless WAL-replayed) a contained-objects list; GC
  compaction never full-scans to discover a segment's objects.

## Data Flow

```
PUT (owner)                              PUT /bucket/key → coordinator.rs
  append → record_blob_entry(seg, off, len, hash, bucket, key)   // key recorded
  ... seal work item
seal worker (coordinator.rs:1515)
  drain entries + (bucket,key) map → dedupe → Vec<ContainedObject>
  sealer.seal_from_data(..., contained)          sealer.rs:270
    flush registration → lifecycle.request_seal_with_contained(id, meta{
      total_bytes = data.len(), .. }, repacked_from, contained)
      → SealEvent(+ SEAL_FLAG_CONTAINED_OBJECTS tail) [durable]
      → registry entry { Sealed, contained_objects: Some(..) }  (ADR-0025 fold)
      → checkpoint v4 snapshots the entry (list survives restart)

GC compactor (oceanfs-durability)            gc/garbage_collector.rs:351-360
  registry.get(S) → segment_meta + contained_objects (skip if None)
  compactor.compact_segment(S, meta, contained, dead_keys)
    for (bucket,key) in contained: point-read object row; repack live ones
    new seal → request_seal_with_contained(new, live-object membership)
    remap notifier(S, S', chunk_table, live object keys)      → f4

Replica holder (segment_service.rs push; healing-service push)
  .dat stream + metadata(+ contained field) → reserve → request_seal_with_contained
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`,
      `oceanfs-core`, `oceanfs-server`, `oceanfs-durability`, `oceanfs-node`.
<!-- REVIEW: re-verified post-fix — `cargo build --all-targets` PASSES on the five DoD crates (3m02s, exit 0) and on `oceanfs` + `e2e` (exit 0). The grpc_services.rs E0063 is fixed (crates/oceanfs-server/tests/grpc_services.rs:289 now carries `contained_objects: vec![]`). -->
- [x] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      passes with new coverage:
      - a sealed segment's `total_bytes` equals its data-section length and
        its contained-objects list round-trips through the SealEvent record
        byte-exact (encoder/decoder + the framing-doc size constants updated,
        mirroring legacy-mode-removal/f3's record-size tests);
      - checkpoint v4 round-trips a Sealed entry with `total_bytes` +
        contained objects across a coordinator restart (open event-WAL →
        fold → registry has `contained_objects`); a v3 checkpoint fails
        `load_checkpoint` with the explicit unsupported-version error;
      - dedupe: one object with three chunks in one segment appears exactly
        once in the list; list is sorted/deterministic;
      - a WAL-replayed segment (empty entries) seals with `None` membership;
      - compactor: `compact_segment` over a membership list re-packs exactly
        the live contained objects and skips dead keys (point-lookup
        semantics); a membership-less segment is skipped, never scanned;
      - compactor's new-segment seal carries the correct membership.
      Then `cargo test -p oceanfs-durability --lib -- --test-threads=1`,
      `cargo test -p oceanfs-server --lib -- --test-threads=1`, `cargo test
      -p oceanfs-node --lib -- --test-threads=1` (PIPELINE.md §4.6).
<!-- REVIEW: FINAL re-verified on 526eb86 — `cargo test -p oceanfs-storage --lib -- --test-threads=1` 457/457 PASS and `cargo test -p oceanfs-durability --test gc_compaction -- --test-threads=1` 5/5 PASS. All previously flagged coverage gaps are now CLOSED and independently verified: (1) seal_worker_seals_replayed_segment_with_empty_entries (coordinator.rs:2858-2863) asserts entry.contained_objects == None after the re-seal; (2) contained_object_sorted_dedup_is_deterministic_and_dedupes (core types/metadata.rs:382) pins the 3-chunk dedupe + sorted determinism; (3) sealer.rs::seal_from_data_with_parity_writes_v2_section (:1146-1151) now asserts hdr.size == 512 AND entry.metadata.total_bytes == 512, pinning total_bytes == data-section length at seal; (4) the stale event_wal.rs record-size comments are corrected to 92 (:1606, :2309); (5) gc_compaction.rs::full_gc_cycle_compacts_segment's throwaway first registry is removed — a single fresh registry is reserved and sealed-with-membership at :214/:237. Suites: storage 457/457, durability 263/263 + gc_compaction 5/5, server 245/245, node 66/66 + gc_compaction 7/7 + segment_replication 3/3, core lib 233/233 + doc 63/63 (--test-threads=1). Accepted deviation (documented, safe): CHECKPOINT_VERSION stays 1 (event_checkpoint.rs:93) instead of the doc's bump-to-4 — a v2 pre-pool checkpoint is refused with the explicit UnsupportedPrePoolDataDir error (event_checkpoint.rs:274-287; test v2_checkpoint_is_refused_not_decoded :717), and other stale versions are rejected by decode_snapshot (:528); the v3/v4 sub-bullet text above is superseded by that directive. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; the event-WAL framing doc,
      checkpoint snapshot doc, and `LifecycleEntry`/`SegmentMetadata` docs
      describe the new fields; `find_objects_in_segment`'s review note
      (`segment_compactor.rs:537-540`) is removed with the function.
      <!-- REVIEW: FINAL re-verified on 526eb86 — RUSTDOCFLAGS="-D warnings" cargo doc --no-deps is clean on all six f3 crates; core doctests 63/63 pass; event-checkpoint + event-WAL framing docs describe the version-1 layout + total_bytes + contained tail. The last stale test-module comments were corrected in 526eb86: event_wal.rs:1606 now reads "seal = 92" matching SEAL_RECORD_SIZE = 92 (:1608), and event_wal.rs:2309 now reads "The resumed file (92 bytes)". No remaining doc gaps. -->
- [x] **ADR:** ADR-0034 D1 (total at seal) and D5/2a (membership at seal on
      the metadata/checkpoint path, not in the `.dat`) satisfied; the
      membership list is written once at seal and dies with the segment's
      DeleteEvent. No reverse-index CF (ADR-0034 Alternatives). No `.dat`
      format change.
      <!-- REVIEW: re-verified post-fix — total_bytes on SegmentMetadata + SealEvent fixed fields (event_wal.rs:270, framed :423) + checkpoint meta payload; the event carries total_bytes so a post-checkpoint replay fold reconstructs it (lifecycle.rs:1537-1539, comment :105-109); ContainedObject tail on SealEvent (SEAL_FLAG_CONTAINED_OBJECTS) + checkpoint Sealed entries (flag+len, event_checkpoint.rs:457-477); RocksDB CFs remain objects+deletions only (store.rs:314-315); header.rs/index.rs untouched across the whole f3 commit range (no .dat change); ADR-0031 D3 pool-id-always-encoded invariant and the pre-pool boot classifier preserved. Accepted deviations confirmed safe: (a) checkpoint version stays 1 (see Tests comment); (b) membership-less registrations are confined to production paths that cannot know membership — adopted row-3 copies (lifecycle.rs:1789-1794, sealed via request_seal(id, meta, None) :1823), re-replication/repair copies fetched from a peer (repair.rs:489, total_bytes left 0), the write-coordinator's WAL-replayed seals (the drain passes None — pinned by coordinator.rs:2858-2863), and the push receiver when the peer sent an empty membership (segment_service.rs:884-887) — all stay re-readable/reapable but non-compactable; heal rewrites already-registered segment data and never registers a new copy. GC skips all such candidates pre-spawn (garbage_collector.rs:331-340). -->
- [x] **Perf:** the compactor's per-segment discovery is O(objects-per-segment)
      point lookups — no objects-CF scan; GC spawn loop skips membership-less
      candidates before any store call; the membership Vec is `SmallVec`-friendly
      (perf 1.4) and registry lock holds stay per-entry (perf 7.1).
      <!-- REVIEW: verified — find_objects_in_segment deleted; compact_segment enumerates membership + get_object_metadata point lookups (segment_compactor.rs:199-211) and refuses membership-less (:190-195); GC skips membership-less candidates pre-spawn before any store call (garbage_collector.rs:331-340); checkpoint encode copies entries under short read guards then serializes lock-free (event_checkpoint.rs:390-411). -->
- [x] **Integration:** a node writes objects spanning multiple segments,
      restarts, and compacts a low-liveness segment using only its
      membership list + point lookups; a replica pushed to a second node
      carries the membership and that node can compact its copy. Run
      `cargo test -p oceanfs-node --test gc_compaction -- --test-threads=1`
      and `cargo test -p oceanfs-node --test segment_replication --
      --test-threads=1`.
<!-- REVIEW: FINAL re-verified on 526eb86 — `cargo test -p oceanfs-node --test gc_compaction -- --test-threads=1` 7/7 and `--test segment_replication` 3/3 PASS; `cargo test -p oceanfs-durability --test gc_compaction -- --test-threads=1` 5/5 PASS — full_gc_cycle_compacts_segment (gc_compaction.rs:165) seeds the Sealed candidate with seal-time membership via registry.seal_with(seg_id, meta, None, Some(obj0..obj4)) on a single fresh registry (:211-237, no throwaway first registry — the dead setup removed in 526eb86) and asserts segments_compacted == 1. oceanfs-durability compaction_crash + oceanfs-storage crash_matrix unit suites cover restart → membership → compaction. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
