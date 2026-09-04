---
feature: "f1: Atomic Supersede-Capture on Overwrite"
epic: "refactoring/bounded-metadata-scans"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring/store-unification
    reason: ADR-0034's consumers (f2/f3/f4) run on the unified store; this feature itself only touches the RocksDB metadata store and the storage-api trait, but the epic must not widen the multi-store sprawl ADR-0032 removes
adr:
  - 0034-bounded-metadata-accounting
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf:
  - "1.3 pre-size collections with known capacity"
  - "1.4 SmallVec for small metadata structures"
  - "6.3 #[repr(C)] for all on-disk / on-wire structures"
  - "11.1 atomic counters on hot paths"
created: 2026-09-04
updated: 2026-09-04
---

# f1: Atomic Supersede-Capture on Overwrite

## Summary

ADR-0034 D2 ("capture completeness") finds one half of the capture rule
already implemented — `RocksDbMetadataStore::delete_object`
(`crates/oceanfs-storage/src/metadata/store.rs:460-493`) reads the object
row's chunks before deleting it and folds them into a `Tombstone` — and the
other half missing: **overwrite does not capture**. The PUT path ("create or
overwrite", `s3_handler/handlers.rs:60`) builds fresh `ObjectMetadata` and
calls `put_object_in_bucket` (`metadata/store.rs:403-418`), which blindly
`put_cf`s the new row and clears the key's tombstone. The superseded
version's chunks — the previous segment's bytes — vanish from the objects CF
with no dead record; today only the orphan reaper's full object scan catches
those bytes.

This feature makes every overwrite at the single concrete choke point —
`RocksDbMetadataStore::put_object_in_bucket` — read the predecessor
(existing row, or the plain tombstone of a deleted-then-re-PUT key) and fold
its chunks into a **versioned supersede dead-chunk record in the same RocksDB
`WriteBatch`** as the new row and the tombstone clear. Atomicity is
guaranteed by the batch; the supersede record coexists with the (new) live
row, ages under the tombstone TTL discipline, is attributable to the segments
it references, and is never interpreted as a delete of the new version —
ADR-0034 D2 constraints (a)–(d). It lives in `oceanfs-storage`
(`metadata/store.rs` + `metadata/cf.rs`), `oceanfs-core` (the dead-chunk
record types), and `oceanfs-storage-api` (the classified enumeration the
accounting consumers use in f2).

> **This feature is the write side of capture.** The accounting consumers
> (GC/orphan/compactor liveness) land in f2/f3. While only this feature is in,
> the existing durability behavior must be **byte-for-byte unchanged**: the
> plain-tombstone scan API (`list_tombstones_all`/`list_tombstones`) keeps
> returning only plain tombstones, and supersede records are written but not
> yet consumed.

## The supersede-capture encoding (recommendation — ADR-0034 D2's open choice)

ADR-0034 D2 deliberately leaves the encoding open: versioned keys in the
deletions CF **or** a dedicated dead-chunks CF. Both must satisfy
constraints (a)–(d). **Recommended: versioned keys in the existing
`deletions` CF**, because

- ADR-0025 Decision 3 fixes RocksDB to "objects + deletions only"; a new CF
  would require amending ADR-0025 and contradicts ADR-0034's opening
  "no new RocksDB surface is introduced".
- Plain tombstone lookups are **exact-key** operations
  (`has_tombstone`, `get_tombstone`, `delete_tombstone`,
  `metadata/store.rs:616-674`) — they can never match a versioned supersede
  key, so the F3/t19 read-repair gate and the LWW delete-vs-write logic are
  untouched.
- The deletions CF is already the TTL-aged dead-byte home; supersedes belong
  to the same aging discipline.

A dedicated dead-chunks CF remains the fallback if an implementer finds the
versioned-key parse untenable — but choosing it requires an ADR-0025
amendment and is out of scope for this feature. **This is the one open
implementation choice of the epic; do not silently pick a third design.**

### On-disk layout (deletions CF)

Keep the plain tombstone key and value **byte-identical**:
`encode_object_key(bucket, key)` = `{bucket}\0{key}` (`metadata/cf.rs:22-28`),
value = bincode(`Tombstone`).

A supersede record is written under a longer key so it sorts within the same
bucket prefix and is self-describing (the object key length makes the parse
exact even though object keys may contain arbitrary bytes after the first
NUL):

```text
supersede key = encode_object_key(bucket, key)
              ++ [0x00]                separator
              ++ [kind: u8 = 0x02]     SUPERSEDE marker (0x01 reserved; plain
                                       tombstones have NO suffix — exact-key
                                       ops and pre-feature records unchanged)
              ++ [key_len: u16 BE]     object key byte length (self-check)
              ++ [version: 12]         superseded version discriminator:
                                       hlc.wall_time u64 LE ++ hlc.logical u32 LE
```

- Value: the existing bincode(`Tombstone`) shape reused as a dead-chunk
  payload — `deletion_time` = capture time (`now_ms`, drives TTL aging),
  `hlc` = the superseded version's HLC, `chunks` = the superseded chunks.
  Reusing the shape means **no tombstone value format change** and no new
  value codec; the kind lives in the key only.
- Decode/classification: `decode_object_key` is extended with a
  `decode_deletions_key(&[u8]) -> Option<DeletionsKey>` returning
  `Plain { bucket, key }` or `Supersede { bucket, key, version }`. A key is a
  supersede iff its tail parses as `\0 0x02 key_len version` AND
  `key_len == remainder.len() − 15` (self-checking). Object keys arriving on
  the HTTP path are URL path segments and cannot contain the `\0`+marker
  sequence in practice; the self-check makes a misparse require a crafted
  key.
- `has_tombstone`/`get_tombstone`/`delete_tombstone` continue to read/delete
  the exact plain key only — they structurally ignore supersedes.

### Why not re-use the plain key

A plain `(bucket,key)` record is cleared by `put_object_in_bucket` for the
now-live key (`delete_tombstone`, store.rs:415), and semantically means "the
object is deleted" for the read-repair gate (`has_tombstone`). A supersede of
a **live** key at the plain key would be wiped by the next PUT and would
wrongly reject read-repair pushes. The versioned key is what satisfies (a)-(d).

## Scope

### In Scope

**A. Key encoding + classification (`oceanfs-storage/src/metadata/cf.rs`)**

- Add `SUPERSEDE_KIND: u8 = 0x02`, `encode_supersede_key(bucket, key, version:
  Hlc) -> Vec<u8>`, and `decode_deletions_key(&[u8]) -> Option<DeletionsKey>`
  with the `Plain`/`Supersede` variants. Unit-test the round-trip and the
  exact-key non-interference (a plain tombstone key never classifies as a
  supersede and vice versa).

**B. Dead-chunk record type (`oceanfs-core`)**

- Add `DeadChunkKind { Tombstone, Supersede }` and
  `DeadChunkRecord { kind: DeadChunkKind, captured_at: i64, hlc: Hlc, chunks:
  SmallVec<[ChunkRef; 4]> }` to `oceanfs-core` (beside `Tombstone` in
  `types/metadata.rs`, or `types/dead_chunk.rs`). This is the typed read-side
  view of a deletions-CF record; it is **not** a new on-disk format (the
  stored value stays the `Tombstone` shape). `# Examples` + `#[derive]` per
  coding.md.

**C. Choke-point capture (`oceanfs-storage/src/metadata/store.rs`)**

Rewrite `put_object_in_bucket` (`:403-418`) as a read-modify single
`rocksdb::WriteBatch`:

```
existing   = get_cf(objects,   encode_object_key(bucket, key))     // decode tolerant
plain_ts   = get_cf(deletions, encode_object_key(bucket, key))     // exact key only
superseded = existing.chunks            if the existing row is segment-stored
          ∪ plain_ts.chunks            if the existing row is absent and the
                                       tombstone predates this write (deleted →
                                       re-PUT: migrate the delete capture so it
                                       is not lost when the tombstone is cleared)
if superseded is non-empty →
    batch.put_cf(deletions, encode_supersede_key(bucket, key, version),
                 bincode(Tombstone{ deletion_time: now_ms,
                                     hlc: <superseded row hlc, else tombstone hlc>,
                                     chunks: superseded }))
batch.delete_cf(deletions, encode_object_key(bucket, key))          // unchanged: clear stale tombstone
batch.put_cf(objects, encode_object_key(bucket, key), bincode(meta)) // the new live row
db.write(batch)
```

- The capture reads happen **before** the batch; the batch itself carries
  put-row + clear-tombstone + supersede, so a crash between row-write and
  capture is impossible by construction (D6 "Crash between row write and
  capture").
- Inline objects (`chunks` empty) capture nothing.
- A **no-op overwrite** (identical row, e.g. the compactor/heal physical
  re-point) still must not produce a spurious supersede; the decision is made
  on `existing.chunks` before the batch, exactly as specified. Note: the
  compactor remap and healing re-point use `batch_write(PutObject)` (a
  physical re-point of the same logical version) — they intentionally do NOT
  capture; leave `batch_write` (`:834-879`) unchanged and document why.

**D. Supersede-safe existing enumeration (`oceanfs-storage` + `oceanfs-storage-api`)**

- `list_tombstones` (`:677-715`) and `list_tombstones_all` (`:722-745`) route
  their key decode through `decode_deletions_key` and **skip** `Supersede`
  keys, so every pre-f2 consumer sees byte-identical output to today (GC,
  caches, read-repair gate).
- New typed enumeration for the accounting consumers:
  - concrete `RocksDbMetadataStore::list_dead_chunk_records_all() ->
    Vec<Result<(BucketId, ObjectKey, DeadChunkRecord)>>` returning plain
    tombstones (`kind: Tombstone`) and supersedes (`kind: Supersede`);
  - trait default `MetadataStore::list_dead_chunk_records_all()` returning
    `Vec::new()` in `oceanfs-storage-api/src/metadata_store.rs` so test
    doubles stay minimal (mirrors the existing scan-method defaults); the
    RocksDB impl overrides it. f2 consumes this; while f1 alone is in, no
    production caller uses it.
- Optional but recommended: an atomic counter on the store
  (`supersede_captured_total`, via the existing `RocksDbMetrics` pattern)
  recording supersede captures and dead bytes captured — the "capture rule is
  firing" signal the D6 matrix asserts on.

### Out of Scope (for this feature)

- GC/orphan/compactor consumption of supersede records (f2/f3) — the
  enumeration lands here, the accounting lands there.
- `batch_write` capture semantics (compaction remap/healing re-point are
  physical re-points of the same logical version; capturing them would
  double-account the old segment's bytes on its own delete).
- The tombstone-TTL aging loop for supersedes (f2, alongside all capture
  aging).
- The seal-time record (`total_bytes`/membership) — f3.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `metadata/cf.rs` (supersede key encode/decode + `DeletionsKey`), `metadata/store.rs` (`put_object_in_bucket` → read-capture-WriteBatch; `list_tombstones*` supersede-skip; `list_dead_chunk_records_all`), tests reworked/extended |
| `oceanfs-core` | New `DeadChunkKind` + `DeadChunkRecord` types |
| `oceanfs-storage-api` | New default `MetadataStore::list_dead_chunk_records_all` method |
| `oceanfs-server` | None (the S3/hint/replica paths funnel into `put_object_in_bucket` unchanged) |
| `oceanfs-durability` | None in f1 (consumers land in f2/f3) |

## Interface (Public API)

- `oceanfs_core::DeadChunkKind { Tombstone, Supersede }` — record kind.
- `oceanfs_core::DeadChunkRecord { kind, captured_at, hlc, chunks }` — typed
  dead-chunk view.
- `oceanfs_storage_api::MetadataStore::list_dead_chunk_records_all(&self) ->
  Vec<std::io::Result<(BucketId, ObjectKey, DeadChunkRecord)>>` (default:
  empty) — f2's accounting feed.
- `RocksDbMetadataStore::put_object_in_bucket` — behavior change: an
  overwrite (or a re-PUT over a tombstoned key) now also writes a versioned
  supersede dead-chunk record **in the same WriteBatch** as the row write.
  Signature unchanged.
- No change to `Tombstone`, the plain tombstone key layout, or the
  `deletions` CF column family set.

## Data Flow

```
PUT /bucket/key (overwrite)                 S3 handler handlers.rs:145 (chunked)
  or inline metadata write                  coordinator.rs:671
  or hint apply                             coordinator.rs:1061,1168
  or replica metadata (read-repair) push    segment_service.rs:740
       └─ trait MetadataStore::put_object ─ MetadataStoreAdapter (node.rs:126-130)
              └─ RocksDbMetadataStore::put_object_in_bucket (store.rs:403)
                     ├─ read existing objects row  (+ chunks)
                     ├─ read plain tombstone (exact key)
                     ├─ [if superseded chunks] put versioned SUPERSEDE record
                     ├─ delete plain tombstone              ┐ one RocksDB
                     └─ put new live row                    ┘ WriteBatch
        → crash between any two batch ops: impossible (atomic commit)

GC/orphan/compactor (f2/f3)
   └─ MetadataStore::list_dead_chunk_records_all()  → Plain tombstone +
        Supersede records, each {kind, captured_at, hlc, chunks} for TTL-aging
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`,
      `oceanfs-core`, `oceanfs-storage-api`.
- [ ] **Tests:** `cargo test -p oceanfs-storage --lib -- --test-threads=1`
      and `cargo test -p oceanfs-core` pass, adding at minimum:
      - key round-trip: `encode_supersede_key`/`decode_deletions_key`; a
        plain tombstone key and a supersede key with the same `(bucket,key)`
        classify distinctly; `has_tombstone`/`get_tombstone`/
        `delete_tombstone` on the plain key never observe a supersede;
      - D6 "PUT overwrite (old on A, new on B)": v1 chunked on segment A,
        PUT v2 on segment B → a Supersede record holds v1's chunk refs, the
        live row references B only, no plain tombstone exists;
      - D6 "DELETE → re-PUT same key": delete captures v1 chunks in the
        tombstone, re-PUT migrates them into a Supersede record and clears
        the plain tombstone — the live row survives and the old chunks are
        accounted exactly once (no double-dead);
      - D6 "Multipart object spanning N segments, then overwrite": one object
        whose chunks span N segments is overwritten → every chunk appears in
        the supersede record exactly once (dedupe by chunk ref);
      - D6 "Hint-apply that supersedes an existing key": seed an existing row,
        run `apply_hinted_object` for the same key → capture fires;
      - D6 "Replica metadata apply overwriting a row": a read-repair push
        (`put_object` with a newer HLC) over an existing row → capture fires
        on the replica path;
      - D6 "Supersede of a tombstoned-but-re-PUT key": no delete of the live
        row (row present, supersede record present, `has_tombstone` false);
      - D6 "Crash between row write and capture": assert there is no API path
        that commits the row without the capture (single `db.write(batch)`);
      - `list_tombstones_all` output is unchanged for a store holding only
        plain tombstones, and supersede records are invisible through it;
      - `list_dead_chunk_records_all` returns both kinds with correct
        `kind`/`captured_at`/`hlc`/`chunks`.
- [ ] **Docs:** every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the cf.rs key-layout comment documents the supersede suffix.
- [ ] **ADR:** ADR-0034 D2 constraints (a)–(d) satisfied by the versioned-key
      encoding; capture is atomic with the row change; every production
      row-replacement path funnels through the choke point. ADR-0025's
      "objects + deletions only" CF set is preserved (no new CF). If the
      implementer instead picks a dedicated dead-chunks CF, this DoD is NOT
      met — that choice requires an ADR-0025 amendment and an epic-level
      review.
- [ ] **Perf:** supersede write path adds one exact-key get (existing row) +
      one exact-key get (plain tombstone) + one batch put per overwrite —
      bounded, no scan; the supersede chunk `SmallVec` is pre-sized from the
      existing row's chunk count (perf 1.3/1.4); no new allocation on the
      first-PUT path beyond today's.
- [ ] **Integration:** the D6 rows above run against a real
      `RocksDbMetadataStore` in `oceanfs-storage` tests; `cargo test -p
      oceanfs-server --lib -- --test-threads=1` (write coordinator + S3
      handler suites) and `cargo test -p oceanfs-durability --lib --
      --test-threads=1` stay green — proving pre-f2 consumers see no
      behavior change.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
