# ADR-0024: Segment Event Log — Dedicated Event WAL as the Source of Truth for Segment Lifecycle

**Status:** Proposed
**Date:** 2026-08-17
**Deciders:** OceanFS architecture team

---

## Context

Segment lifecycle state — *reserved (phantom), sealed, deleted* — is
currently stored in the `segments` column family of RocksDB
(`crates/oceanfs-storage/src/metadata/store.rs`), while the segment's
*data* is written to the project-owned segment WAL
(`crates/oceanfs-storage/src/wal/writer.rs`). These are **two independent
stores with no shared order**, and a large fraction of the system's
correctness work exists solely to reconcile them:

- the phantom-before-WAL registration
  (`crates/oceanfs-server/src/write/coordinator.rs:661-693`) exists so the
  WAL cleanup never mistakes an in-flight segment's entries for garbage;
- the seal-aware WAL retention (`cleanup_old_wal_files`,
  `crates/oceanfs-storage/src/wal/replay.rs:349-465`) re-derives "is this
  segment durable?" from the CF on every rotation;
- the interrupted-seal adoption at startup
  (`crates/oceanfs-node/src/node.rs:995-1067`) heals `.dat`-without-CF
  entries;
- the seal-window data set (ADR-0021) and read-from-active-buffers
  (ADR-0020) exist to cover the read gap across the same boundary.

### Observed failures (2026-08-16/17 load-test campaign)

Every one of the four data-integrity/WAL bugs fixed during the campaign
was an ordering defect between these two stores, not an isolated logic
error:

1. **Phantom-downgrade race** — the write path registers the phantom
   (`sealed_at: None`) *after* the append that may have filled the
   segment and enqueued its seal; the seal worker (separate task) can
   persist `sealed_at: Some` before the phantom write lands, and the
   phantom then **downgrades the sealed entry back to unsealed**. Nothing
   ever re-seals it; the WAL cleanup protects every file holding its
   entries forever. Measured leak: `protected` grew 17 → 45 over a
   30-min run (~1.5 files/min × 64 MB ≈ 3.8 GB/hour — disk-full in under
   a day on a 75 GB SUT).
2. **Missing idle seal** — the pool sealed segments only on `is_full()`;
   a partially-filled segment that stopped receiving writes stayed
   registered-unsealed forever, pinning its WAL files (same leak
   mechanism). The sealer's `seal_timeout_ms` logic existed but was never
   driven (`crates/oceanfs-storage/src/segment/sealer.rs:162`).
3. **Metadata-only compaction** — the GC compactor created a new segment
   ID and remapped object chunks to it **without ever writing the new
   segment's `.dat`** (`crates/oceanfs-durability/src/gc/segment_compactor.rs`),
   because the compactor had no data store and no lifecycle discipline;
   crash-recovery mismatches resulted.
4. **Compression-ref corruption on repack** — the same compactor
   hardcoded `compressed: false` on repacked `ChunkRef`s, so reads of
   repacked compressed objects returned raw compressed bytes
   (`BadDigest` after restart).

All four are the same disease: **segment state transitions are not
first-class, ordered, durable events; they are ad-hoc writes to a
secondary store, reconciled by convention.**

### The insight

The `segments` CF is *not* a general-purpose store — it exists to answer
exactly one question durably: *"what is this segment's lifecycle state?"*
That question is answered far more naturally by an **ordered event log**
that shares the write pipeline's own ordering discipline. The data WAL
stays as a seekable pool of in-flight blob bytes; the event log becomes
the source of truth for what those bytes mean.

### Forces

- **Correctness is the product.** Metadata is the source of truth; every
  replica runs the same store, so a state bug is silent divergence with
  no safety net. Ordering bugs have already caused real data-integrity
  failures in load testing.
- **The system must run for days.** Any monotonic leak (WAL files,
  protected entries) is catastrophic, not cosmetic. "Narrow the race" is
  not a viable strategy; the ordering must be structural.
- **The existing WAL machinery is proven.** Group-commit fsync
  (`WalSyncGroup`, `crates/oceanfs-storage/src/wal/sync.rs`), rotation,
  and replay are hardened. A dedicated event log reuses this machinery —
  it does not invent a new durability domain (contrast ADR-0018, which
  *removed* WAL domains; this ADR replaces a RocksDB CF with a
  project-owned log, netting -1 external durability domain).
- **Production scale is TBs across multiple drives.** The design must
  not be derived from the load-test box (75 GB SUT); memory and disk
  costs must be bounded at TB scale (see ADR-0025 for the machine's
  O(live segments) memory bound).
- **ADR-0023 pre-positions this direction.** It already records RocksDB's
  structural costs and a phased path toward a native store; this ADR is
  the *segment-state slice* of that path, deliberately narrower than the
  full native-store replacement (objects stay in RocksDB for now).

---

## Decision

### Decision 1: Introduce a dedicated segment event WAL

A new, project-owned, append-only **event WAL** (plain files, checksummed
records, group-commit fsync — reusing the `WalWriter`/`WalSyncGroup`
discipline) becomes the **single source of truth for segment lifecycle
transitions**. The data WAL is demoted to a **seekable pool of blob
bytes** — its entries are not the reconstruction source; they are
replayed only for segments the event log says were *reserved but not yet
sealed*.

**Event records** (all reference a `segment_id`; the event log is the
only place these transitions are recorded):

```
ReserveEvent { segment_id, tier, ec_k, ec_m }          // replaces the phantom CF write
SealEvent    { segment_id, tier, ec_k, ec_m, merkle_root, data_wal_pos }
                                                       // replaces sealed_at CF write
DeleteEvent  { segment_id }                            // replaces the deleted-marker CF write
```

`data_wal_pos` is the position (file sequence + offset) of the segment's
**last data entry** in the data WAL. It makes the data WAL *seekable*:
recovery knows exactly which entries belong to a reserved-unsealed
segment, and the retention logic knows exactly when a segment's data
entries became garbage (SealEvent position > last data position).

**Ordering invariants (by construction — the ADR-0025 machine enforces
them; see that ADR for the enforcement mechanism):**

| Transition | Required order | Rationale |
|---|---|---|
| Reserve | `ReserveEvent` appended **before** the first `DataEntry` | A data entry must never exist without its reserve (the reserve is what makes the entry meaningful at recovery) |
| Seal | last `DataEntry` → `.dat` fsync → `SealEvent` | The seal event is *causally after* the data by construction: the seal worker cannot append the event before the fsync returns; `data_wal_pos` makes it verifiable |
| Delete | `DeleteEvent` (durable) → `.dat` unlink | The event must be durable before the data disappears; otherwise recovery would fold "sealed" with a missing `.dat` |
| Compaction | new `.dat` → `SealEvent(new)` → `PutObject(new refs)` → `DeleteEvent(old)` → unlink old `.dat` | Objects must point at a sealed new segment before the old segment is deleted; every crash window is safe (see ADR-0025 §Crash windows) |

**Retention rules:**

- **Data WAL**: an entry is garbage iff its segment has a durable
  `SealEvent` (or `DeleteEvent`) whose position is at/after the entry's
  position. Sweeping never needs a CF scan — it consults the event log
  (or the checkpoint; see Decision 3).
- **Event WAL**: retained until checkpointed (Decision 3). `ReserveEvent`
  without any data and no `SealEvent` is garbage after the reserve is
  superseded (idle-seal of an empty segment never happens; recovery drops
  empty reserves).

### Decision 2: `data_wal_pos` position references (not a shared sequence)

Ordering between the two logs is established by **position references
carried in the events**, not by a global sequence number issued by a
coordinator. Rationale:

- A shared sequence requires a single issuance point for *both* logs —
  a coordination bottleneck on the write path.
- Position refs make the data WAL seekable: recovery seeks directly to a
  reserved segment's entries instead of scanning.
- The causal order is already enforced by the seal worker's own
  operation sequence (fsync before event); the position ref makes it
  *checkable* at recovery rather than trusted.

### Decision 3: Checkpoint the event log (the event log's own GC)

The event log grows with every seal (≈ 1.4M events/day at sustained
load ≈ tens of MB/day — bounded per-day but unbounded over weeks).
**Checkpointing** is therefore a first-class mechanism, designed in from
day one:

- A **checkpoint** is an atomic snapshot of the folded registry
  (temp file + rename + fsync), triggered by a **byte threshold** on
  the event log (see Decision 4 — the threshold is the *only* trigger).
- On checkpoint: events older than the snapshot are truncated;
  `DeleteEvent`s make their segment's entire history garbage.
- Startup: load latest checkpoint (ms) → append-fold any events after
  it → machine ready.

The checkpoint file is the on-disk state snapshot **in our own format** —
it is what replaces the `segments` CF, without RocksDB (see ADR-0025).

### Decision 4: The event log has its own fsync group

The event log does **not** share the data WAL's `WalSyncGroup`. Each
log owns a private group-commit fsync domain:

- **Isolation of durability points.** The data WAL's group commit is
  sized and tuned for the write path (140 MB/s, sub-ms batches). Event
  appends are ~50 B and sparse (seal/delete/compaction cadence); mixing
  them into the data group would either delay data fsyncs (waiting on
  the event batch window) or force the events to ride an unrelated
  durability point.
- **Independent batch windows.** The event group's `fsync_batch_timeout`
  is tuned for *event* latency (a seal is already paying a `.dat` fsync;
  its `SealEvent` must be durable promptly, but the batch window can be
  wider than the data path's).
- **Independent backpressure.** A stall or overload on one log cannot
  block the other. The data path's write latency never depends on event
  log availability, and vice versa.
- **Reuse, not reinvention.** The event group is the same
  `WalSyncGroup` machinery (`crates/oceanfs-storage/src/wal/sync.rs`)
  instantiated per log — one more instance of a proven component, not a
  new fsync discipline.

The group-commit batch window for the event log is a configuration knob
(`event_wal_fsync_batch_timeout_ms`), defaulting to a wider window than
the data WAL's since events are sparse and a seal's `.dat` fsync already
precedes its `SealEvent`.

**Why a byte threshold, not rotation, for checkpointing.** Rotation is a
time/volume proxy that fits the data WAL (whose files are written at a
steady rate and whose retention is cadence-shaped). The event log's
write rate is *bursty and workload-shaped* — a quiet cluster generates
almost no events, a delete-heavy or compaction-heavy one generates
thousands. A byte threshold makes checkpoint cost a direct function of
*replay cost*: the event log is checkpointed before it can grow past
`event_wal_checkpoint_bytes` (default e.g. 64 MB, configurable), so
startup replay after checkpoint is bounded by the threshold regardless
of how the workload arrived there. At TB scale this matters: rotation
would checkpoint on wall-clock cadence even when the log is tiny (wasted
I/O) or, worse, defer checkpointing past the bounded-replay guarantee
when events spike. The threshold is the only trigger; there is no
time-based fallback.

### Scope

- **In:** event WAL format, position references, retention rules,
  checkpoint mechanism (byte-threshold trigger), event log's own
  fsync group (Decision 4), recovery algorithm (see ADR-0025 for the
  machine + recovery fold).
- **Out:** object metadata (stays in RocksDB — confirmed),
  inline-payload replay (ADR-0023's store concern, not this ADR's),
  multi-drive placement (the machine addresses segments by ID; placement
  stays under `DiskSegmentStore`-style directory abstraction).

---

## Consequences

### Positive

- **Ordering becomes structural.** The phantom-downgrade race, the
  idle-seal gap, and the compaction ordering hazards become
  unrepresentable: transitions are events with a defined causal order,
  enforced by the machine (ADR-0025).
- **The WAL leak class is eliminated.** Protection/sweep decisions
  derive from the event log (or checkpoint), never from a CF scan; no
  monotonic accumulation is possible.
- **Deterministic recovery.** `state = fold(events)`; no more
  "adopt interrupted seal commit" heuristics. The data WAL is a pool,
  not a reconstruction source.
- **One less external durability domain.** RocksDB's `segments` CF
  (and its deleted-markers CF) disappear from the durability story,
  consistent with ADR-0023's direction.
- **Reuses proven machinery.** `WalWriter`, `WalSyncGroup`, rotation,
  and replay patterns are reused; no new fsync discipline is invented.

### Negative

- **A new WAL domain.** ADR-0018's thrust was *fewer* WAL domains. This
  ADR adds one — justified because it *replaces* a RocksDB CF (net
  external durability domains: -1) and because the event log is not a
  parallel data path but the single ordering authority. This must be
  stated explicitly wherever ADR-0018 is referenced.
- **Recovery now depends on two logs' consistency.** The position
  references must be correct; a torn event record or a wrong
  `data_wal_pos` corrupts recovery. Mitigated by checksums, the
  crash-window table (ADR-0025), and fault-injection tests.
- **Event log growth requires the checkpoint mechanism** to be
  implemented and correct; checkpointing is itself a crash-recovery
  surface (snapshot-vs-WAL ordering) that must be tested.

### Neutral

- The data WAL's hot write path is untouched by this ADR; the event log
  rides the seal/delete/compaction paths, not the per-write path.
- The `MetadataStore` trait's segment methods become consumers of the
  machine (ADR-0025), not writers.

---

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Single mixed WAL (data + events in one log)** | One log = one order, no position refs, no two-log consistency | Retention is a compromise (data is transient, events are persistent); replaying events means scanning hundreds of 64 MB data files; event appends contend with data appends on the hot path | Rejected — the two streams have opposite lifecycles and 7 orders of magnitude of rate difference; separation is the point |
| **Keep the CF, write events to it too** | No new log format; RocksDB gives atomicity | Two sources of truth for the same transition (CF + events) that can disagree — the phantom-downgrade disease relocated; deepens RocksDB coupling, against ADR-0023 | Rejected — dual-write reconciliation is the exact bug class being eliminated |
| **Shared global sequence number across logs** | Single order, no position math | Requires a single issuance point for both logs — a write-path coordination bottleneck; does not make the data WAL seekable | Rejected in favor of position refs (Decision 2) |
| **No event log; keep CF + narrow the races** | Zero new machinery | Demonstrated insufficient: the leak persisted through two "narrowing" fixes; the system must run for days | Rejected — probabilistic narrowing is not a production strategy |
| **Event log backed by RocksDB** | Battle-tested durability | The tool the project is trying to escape (ADR-0023); overkill for a tiny ordered log | Rejected — plain files with the project's own WAL discipline are simpler and align with the de-RocksDB direction |

---

## References

- ADR-0025 (Segment Lifecycle State Machine — companion ADR; the
  machine, enforcement, crash-window table, migration)
- ADR-0023 (Metadata Store native replacement path — this ADR is the
  segment-state slice of its Phase 2)
- ADR-0018 (Durability WAL consolidation — the "fewer WAL domains" rule
  this ADR deliberately extends with a replacing, not additive, domain)
- ADR-0020 / ADR-0021 (read-from-active + seal-window — the read-path
  mechanisms the machine absorbs)
- `crates/oceanfs-server/src/write/coordinator.rs:661-693` —
  `register_phantom_before_wal` (the downgrade race)
- `crates/oceanfs-storage/src/segment/sealer.rs:150-172` — `try_seal`
  with unused `seal_timeout_ms` (the idle-seal gap)
- `crates/oceanfs-storage/src/wal/replay.rs:349-465` —
  `cleanup_old_wal_files` / `file_contains_live_entries` (today's
  CF-derived protection)
- `crates/oceanfs-durability/src/gc/segment_compactor.rs` — the
  metadata-only compaction defect
- `crates/oceanfs-node/src/node.rs:995-1067` — interrupted-seal
  adoption (replaced by deterministic fold)
- `crates/oceanfs-storage/src/wal/{writer,sync}.rs` — the reused WAL
  machinery
- Spec §4.2 (Pipeline Parallelism) — async sealing requirement preserved
