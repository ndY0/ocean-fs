# ADR-0037: Metadata Anti-Entropy Detection — Rotating Bounded Scan with Key-Ordered Fingerprint Comparison

**Status:** Superseded by [ADR-0038](0038-metadata-change-journal.md)
**Date:** 2026-09-12
**Deciders:** OceanFS architecture team (proposal; pending architecture-owner approval)

**Related:** [ADR-0034 bounded metadata accounting](../adr/0034-bounded-metadata-accounting.md)
(the constraint), [ADR-0015 anti-entropy Merkle protocol](../adr/0015-anti-entropy-merkle-protocol.md)
(the segment-plane pattern), [ADR-0023 native-store path](../adr/0023-metadata-store-native-replacement-path.md)
(anti-RocksDB-coupling posture), [ADR-0027 hinted-handoff ownership](../adr/0027-hinted-handoff-ownership-model.md)
(§D5 data-bearing backstop), [ADR-0028 membership plane](../adr/0028-membership-plane-full-swim-gossip.md),
[ADR-0029 storage pools](../adr/0029-storage-pools-disk-resilience.md) (§D3/§D7),
[ADR-0030 target-pull](../adr/0030-re-replication-target-pull.md),
[ADR-0033 manifest-aware peer selection](../adr/0033-manifest-aware-peer-selection.md),
[ADR-0017 durability task abstraction](../adr/0017-durability-task-abstraction.md).
Design capture: [`docs/features/metadata-anti-entropy/design-draft.md`](../features/metadata-anti-entropy/design-draft.md).

> **Superseded 2026-09-12 by [ADR-0038](0038-metadata-change-journal.md).**
> This ADR's Option 3 — a durable rotating bounded scan — is rejected by the
> architecture owner's governing principle: *the system must never perform
> recurring full-metadata scans; performance is the center of the design.*
> Detection must cost ∝ changes, never ∝ rows. The gap analysis, the
> three-state/HLC/tombstone correctness rules, and the reuse inventory are
> carried into ADR-0038; the detection and transfer mechanism is replaced by
> a co-durable change journal with pull-based catch-up and triggered,
> range-bounded bootstrap. (ADR-0038 also deliberately reverses this ADR's
> D4 local-segment-presence precondition: rows and bytes live on independent
> rings, so foreign chunk refs are normal and are the byte plane's concern.)
> This document is kept intact as the record of the rejected alternative.

---

## Context

### The gap (verified)

The index plane has no eventual-repair path. Segment reconciliation restores
**bytes** by holder set (`crates/oceanfs-durability/src/reconcile.rs`), and
hinted handoff is the only metadata-**row** backfill — read repair explicitly
delegates the remote-newer case to it
(`crates/oceanfs-server/src/read/coordinator.rs:1142-1149`). The 2026-09-10
churn session recorded the residual as *"anti-entropy does not backfill
missing object rows"* (`review/cluster-churn-resolution-2026-09-10.md:72`).

The f0 hints gate (admission honesty) bounds **new** divergence; it cannot
heal divergence already created by debt lost with a hints device, bounded
give-up windows, or crash windows. Nothing re-derives a row whose hint is
gone. The f3/f5 artifacts show the observed shape: whole keys
`absent from every alive node` after hint death / outage windows
(`docs/features/fleet-degradation/artifacts/f3-control-dirty-20260911.json:36-77`,
6 of 94 keys; `…/f3-full-run3-20260911.json:36-65`, 4 of 103 keys). f4's P4
is explicitly the AE acceptance harness and will record the post-f0 residual
as the baseline (`docs/features/fleet-degradation/f4-pool-degradation-under-load.md:179-184`,
`:309-311`); **f4 is paused (2026-09-12)** and no P4 divergence counts exist
yet — all sizing below is explicitly assumption-based.

### Why ADR-0015's segment-plane Merkle does not transfer directly

ADR-0015 maintains an incremental Merkle tree over **sealed segments** — a
small (≤10 000, `continuous_max_segments`), append-oriented, registry-bounded
set (`crates/oceanfs-durability/src/anti_entropy/`; ADR-0015 §1-3). The
metadata plane is structurally different:

1. **Cardinality is O(all objects)** — millions per node, not thousands
   (ADR-0023:84-86: "millions of objects × ~100–300 B").
2. **Rows mutate in place** on every PUT/DELETE, not append.
3. **Storage order ≠ ring order.** RocksDB orders rows by
   `{bucket}\0{key}` (`crates/oceanfs-storage/src/metadata/cf.rs:58-67`);
   ownership is by `SHA-256(bucket/key)` (`crates/oceanfs-routing/src/hash.rs:15`,
   `Ring::ranges_owned_by`, `crates/oceanfs-routing/src/ring.rs:226`).
   Any hash-ordered comparison must either reorder (a derived hash-ordered
   surface) or compare in raw key order.
4. **The existing range stream cannot be the recurring detector.** The g8
   server-side lister `MetadataStoreRangeLister` visits **both CFs in full**
   and filters by key hash (`crates/oceanfs-node/src/modules/metadata_recovery.rs:42-89`);
   that is acceptable once at boot (g8) and unacceptable per AE cycle under
   ADR-0034.

### Constraints

- **C1 — ADR-0034 bounded-metadata discipline.** No recurring unbounded
  full-object scans; "no new RocksDB surface is introduced" (ADR-0034
  Decision) without explicit justification; any new persisted state needs an
  accounting statement (size × rows, tombstones included). ADR-0034's own
  §3 accepts *multi-cycle* bounded passes (keyspace_fraction) as the model
  for spreading work — a bounded rotating cursor is that model taken to its
  conclusion, not a per-event scan.
- **C2 — ADR-0023 posture.** Do not deepen RocksDB coupling; a native
  replacement (memory-first store + WAL, ADR-0023 Decision Phase 2) must be
  able to provide the same AE capability. A new derived **RocksDB CF** is
  the hardest thing to re-implement under that path.
- **C3 — hot-path sensitivity.** Every PUT/DELETE already runs a
  read→decide→single-`WriteBatch` critical section at the choke point
  `put_object_in_bucket` (`crates/oceanfs-storage/src/metadata/store.rs:529-602`)
  with ADR-0034 D2 capture. Any detector that writes on the mutation path
  adds cost to the hottest metadata path and a new capture-completeness
  obligation for paths that bypass the choke point (`batch_write` at
  `store.rs:1133-1178`, used by compaction remap/healing re-points).
- **C4 — kill-switch / pre-f4 rollout.** The mechanism must be inert when
  disabled (`metadata_ae.enabled = false`, default off): no loops, no tasks,
  no new CF writes, no behavior change. f4's P4 must run twice in one fleet
  session (AE-off baseline, then AE-on acceptance).
- **C5 — correctness requirements** (from the design draft): three distinct
  states — `(no row)` ≠ `(object row)` ≠ `(tombstone row)`; HLC-aware;
  tombstone-safe (no resurrection); row repair must not create dangling
  references to absent segment bytes.

### Sizing assumptions (estimates; to be re-based on f4 P4 data)

| # | Assumption | Value | Basis |
|---|---|---|---|
| A1 | rows per node `R` | 10³ (f4 fleet) / 10⁶ near-term / 10⁷ horizon | f4 manifest counts ~10²; ADR-0023:84-86 "millions" |
| A2 | average stored row | ~200 B | ADR-0023:84-86 (100-300 B) |
| A3 | topology | N=3, RF=3 (full replication) in f4; N≫RF at scale | metadata_recovery.rs:236-244 comment; ADR-0026/0027 |
| A4 | write rate | ~43 PUT/s (f5: 12 792 puts / 300 s) | f5-rerun-full-20260912.json:7-9 |
| A5 | fingerprint wire cost | ~85 B/key (key ~40 B + state 1 + HLC 12 + row hash 32) | derived |

---

## Options Considered

All three options detect the same thing; they differ in **where the
comparison order lives** (persisted hash-ordered index / in-memory derived
state / the scan itself) and therefore in resource cost. A Merkle *tree* is
optional in every case; what is mandatory is an enumerable partition that
both sides can compute (see "why a tree is not the crux" below).

### Option 1 — New hash-ordered fingerprint column family

A compact CF keyed by `truncated_hash(bucket/key) → (state, HLC, row_hash)`,
maintained in the same `WriteBatch` as every row mutation; ranges are ring
arcs; per-range digests become cheap.

| Dimension | Estimate |
|---|---|
| Detection | Exact, localized; freshness bounded only by compare cadence (minutes or better) |
| Disk | ~45-80 B/row (16-32 B key + ~29-45 B value) → 45-80 MB @1M; **450-800 MB @10M** (~20-40% of a 2 GB metadata CF) |
| Write path | +1 small row per PUT/DELETE in the same batch; ~+2-4 KB/s at A4; roughly **+5-10% metadata write CPU/compaction**; every mutation path (incl. `batch_write`) must maintain it |
| I/O | Compare reads 45-80 B/row per rotation (≈0.1× the full-row scan) |
| Boot | None (persisted); one-time backfill scan when enabled on a store with existing rows |
| Crash safety | Strong (same batch) |
| Build | **Highest** (~2.5-4k LOC): new CF + encoding + all mutation paths + backfill + native-store parity + protocol |
| Constraint fit | Directly contrary to C1's "no new RocksDB surface" and C2's native-store path; the value (cheap recurring compare) does not remove the write-path/accounting cost |

### Option 2 — In-memory incremental Merkle, one-time boot scan, change-fed

Per-key fingerprints/digests held in memory (hash-ordered), fed from the
mutation choke point; a boot scan rebuilds them; keyspace sharding bounds
each node's shard. This is ADR-0015 lifted to rows.

| Dimension | Estimate |
|---|---|
| Detection | Exact, localized; continuous freshness |
| Memory | ~70-85 B/row (16 B hash + ~29 B fp + map/node overhead) → 70-85 MB @1M; **700-850 MB @10M/node**; no reduction at N=RF (A3) |
| Disk | None (or a MerkleWal journal → new persisted surface + replay machinery) |
| Write path | In-process feed + lock/atomic per mutation (small, but on the hot path) |
| Boot | Full `R`-row scan per cold boot without a journal (R=10M: ~10-60 s of CF iteration), off the critical path but node-startup coupled; with a journal, a new WAL surface |
| Crash safety | Journal required for no-rescan recovery; otherwise rebuild |
| Build | High (~2-3.5k LOC) + keyspace-sharding dependency |
| Constraint fit | Bounded-metadata*ish*, but memory scales with object count — exactly the resource the metadata plane is trying to bound; localization requires per-key state (hash-bucket aggregates alone cannot enumerate a divergent bucket without a hash-ordered store) |

### Option 3 — Rotating bounded scan cursor (key-ordered)

No derived state. A durable cursor walks the objects CF then the deletions
CF in bounded batches; for each contiguous key interval the node compares
**fingerprint batches** with a co-holder of those keys and pulls differing
rows (target-pull). Wraps around; convergence is bounded by the rotation
period.

| Dimension | Estimate |
|---|---|
| Detection | Exact per rotation (no sampling, no false negatives); freshness = rotation period |
| Memory | Batch buffer only (~512 × 85 B ≈ 44 KB) + cursor |
| Disk | **None** (a ~40 B cursor file; no new CF) |
| Write path | **Zero** — read-only scan, no choke-point or `batch_write` changes |
| I/O per rotation | Local scan `R` × ~200 B ≈ 200 MB @1M / **2 GB @10M**; comparable responder lookup work; ~85 MB @1M / 850 MB @10M wire per compared peer (one peer per rotation; RF−1 rotations cover all holders) |
| Per-tick pacing | Bounded by config: e.g. 4 096 keys / 4 MiB per 30 s cycle → 1M keys in ~2 h, 10M in ~20 h; f4 (10²-10³ keys) completes in one cycle; budgets tunable for a ≤1 h near-term target |
| Boot | **None** — no rebuild, nothing on the critical path |
| Crash safety | Cursor file is atomic temp+rename; lost/corrupt ⇒ rotation restarts (safe) |
| Build | Moderate (~1.5-2.5k LOC incl. tests): cursor + compare/fetch RPCs + ownership filter + apply path + metrics; reuses `MetadataRow` and the existing HLC-guarded apply primitives |
| Constraint fit | The only option with no new persisted surface, no hot-path cost, and trivial kill-switch inertness; the accepted ADR-0017 §3 keyspace_fraction model bounds per-tick work |

### Hybrid candidates

- **2 (hot/owned set) + 3 (cold safety net).** A *bounded* change-fed hot
  set (e.g. last 64k-256k mutated keys, ~8-32 MB RAM) shortens detection
  latency for actively-written keys; Option 3 guarantees complete coverage.
  The hot set cannot know about mutations this node *missed* (the actual gap
  — the writer that lost the hint is elsewhere), so it does not change the
  cold-key bound; it only cheapens/freshens re-comparison of locally-active
  keys. Build cost is Option 3 plus ~0.5-1k LOC. **Deferred, not rejected**
  — the compare/fetch protocol below is agnostic to how the initiator
  produced its fingerprint batch, so the hot set can be added later behind
  the same RPC without an ADR-level change.
- **1 + 3.** Rejected with Option 1: paying the write-path/disk cost *and*
  the cursor.

### Comparison summary

| Criterion | O1 fingerprint CF | O2 in-memory Merkle | O3 rotating cursor |
|---|---|---|---|
| Exactness / coverage | Exact; full | Exact; full | Exact; full per rotation |
| Freshness (cold keys) | minutes or better | minutes or better | rotation period (target ≤1 h near-term, ≤24 h horizon) |
| Freshness (hot keys) | minutes or better | continuous | rotation period (hybrid can improve later) |
| RAM @10M rows | ~0 (RocksDB cache) | 700-850 MB | ~44 KB |
| Disk @10M rows | 450-800 MB | 0 (+journal) | ~0 |
| Write-path cost | +5-10% | small, hot path | 0 |
| Boot cost | 0 (+backfill once) | full scan or journal | 0 |
| Crash safety | strong | journal or rescan | cursor only |
| ADR-0034/0023 fit | poor | mixed | strong |
| Build effort | highest | high | moderate |

### Why a Merkle *tree* is not the crux

For a bounded batch (hundreds of keys) the full fingerprint list is small
enough to exchange directly; a tree/digest only compresses a payload that
does not need compressing at that size. The tree becomes valuable only when
the comparison unit is a whole hash arc (Option 1/2). Option 3's comparison
unit is the batch, so it needs no tree — and, importantly, **key-ordered
comparison sidesteps the hash-order enumeration problem** that forces
Options 1/2 to carry a hash-ordered surface.

---

## Decision

**Adopt Option 3: a durable rotating bounded scan cursor over the metadata
column families, comparing key-ordered fingerprint batches per co-owned key
and repairing differences by target-pull.** Do **not** add a derived
fingerprint CF or an in-memory per-key index now. The compare/fetch protocol
is defined so a bounded change-fed hot set (Option 2 restricted) can be
added later as an accelerator behind the same RPCs without changing the
apply/repair path; a hash-ordered fingerprint CF (Option 1) is revisited
only on measured rotation-latency evidence (see Open Questions).

### D1. Mechanism

One node-local worker, `MetadataAntiEntropy`, owned by `oceanfs-durability`
and wired by the composition root. Each cycle it:

1. Resumes the cursor at `(phase, last_key)`: phase `objects` walks the
   objects CF; phase `deletions` walks the deletions CF. Order is raw
   RocksDB key order (`{bucket}\0{key}`), not hash order.
2. Accumulates up to `batch_keys` locally-held rows into a batch; computes a
   fingerprint per row (D2).
3. Selects, per key, one comparison peer from the key's current replica set
   (`Ring::lookup(SHA-256(bucket/key))`), filtered by manifest health
   (ADR-0033 D1). One peer is used for the whole rotation (rotating across
   rotations), so each key is compared against a different co-holder each
   rotation until all RF−1 peers have been covered. Keys for which this node
   is not a current holder are not compared (stale non-owner extras are a
   GC/relocation concern, not an AE concern).
4. Sends one `CompareMetadata` RPC per batch (D3) and, for each difference
   where the local side is behind, issues `FetchMetadataRows` and applies the
   rows (D4). Where the peer is behind, the peer issues its own fetch to
   this node — data always moves by pull (ADR-0030).
5. Advances the cursor past `min(local batch end, responder truncation
   point)` and persists it. Compare success advances the cursor even if a
   repair is deferred; deferred repairs are re-detected next rotation (and,
   for missing bytes, handled by the segment re-replication worker).

A full pass covers every locally-held key. **The draft's concern that a
rotating cursor "misses cold keys" does not hold once the cursor is
durable**: cold keys are precisely what the rotation re-visits; the cost is
latency, not coverage. Coverage degrades only if the rotation is
intentionally sampled — which this ADR does not do.

> Implementation note: because the two CFs are walked separately, a key in
> `objects` and its absence in `deletions` are observed in different phases.
> Comparison and repair are always against the peer's **logical state for
> the key** (object row or plain tombstone — looked up across both CFs), so
> the phase is a scan device, not a semantic one.

### D2. What is compared (fingerprints, states, CFs)

- **Three states must be distinguishable**: `(no row)` / `(object row)` /
  `(plain tombstone row)`. Absence is never offered as an entry; the protocol
  reports it (D3), and `(no row)` vs `(tombstone)` is a real mismatch.
- **`(object row)` fingerprint** = `state=object` + `hlc` +
  `row_hash = BLAKE3(serialized objects-CF value)`. The row hash covers the
  full logical row: size, blake3 hash, inline payload, and the chunk-ref list
  (segment id, offset, length, compressed, logical length) — this is what
  closes the HLC-blind-divergence class (`put_object_in_bucket` HLC-blind
  residual, `review/cluster-churn-resolution-2026-09-10.md:73`).
- **`(plain tombstone)` fingerprint** = `state=tombstone` + `hlc` only.
  Tombstone `chunks` and `deletion_time` are **GC accounting**, not logical
  state: two replicas with the same delete HLC are in sync even if their
  captured chunk lists differ (they capture their own local copies).
- **Supersede records are excluded from comparison.** They are
  version-keyed dead-chunk captures (`cf.rs:26-56`), not logical state; they
  are produced locally by the local apply path (D4) and never pulled. This
  avoids false divergences and keeps repair from importing foreign chunk
  refs (see D4's accounting rule).
- **HLC participates**: same key with different HLC is a mismatch that
  LWW resolves. Equal HLC with different `row_hash` is a tie broken
  deterministically by **max row_hash** (the same rule is applied
  symmetrically on both sides, so convergence is stable).
- **Keys are compared, not hashed, inside the fingerprint**: the exchange is
  key-ordered, so the key itself travels in the entry. This is what makes
  localization free.

### D3. Comparison protocol (new RPCs, not `MerkleExchange`)

Add to `HealingRpc` (`proto/oceanfs/healing.proto:67-128`) — the service is
already implemented in `oceanfs-durability/src/healing_service.rs:799` and
wired by the node:

- `CompareMetadata(MetadataCompareRequest) → MetadataCompareResponse`
  (unary, batched, bounded);
- `FetchMetadataRows(MetadataFetchRequest) → stream MetadataRow`
  (server-streaming; reuses the existing `MetadataRow` message,
  `healing.proto:386-406`).

Illustrative shape (field layout finalized at spec time):

```proto
message MetadataFingerprint {
  bytes key = 1;                      // full CF key {bucket}\0{key}
  uint32 state = 2;                   // 0 = absent marker, 1 = object, 2 = plain tombstone
  oceanfs.common.HlcTimestamp hlc = 3;
  bytes row_hash = 4;                 // BLAKE3(value); empty for tombstone/absent
}
message MetadataCompareRequest {
  bytes start_key = 1;                // inclusive cursor position
  bytes end_key = 2;                  // inclusive local batch end (empty = open)
  repeated MetadataFingerprint held = 3;  // initiator's rows in [start,end]
  uint32 max_entries = 4;             // responder work cap
}
message MetadataCompareResponse {
  repeated MetadataFingerprint differing = 1;   // responder state for offered keys that differ
  repeated MetadataFingerprint absent = 2;      // offered keys the responder does not hold
  repeated MetadataFingerprint extras = 3;      // co-owned keys the responder holds that were not offered
  bytes truncated_at = 4;             // responder-local bound when max_entries hit
}
message MetadataFetchRequest { repeated bytes keys = 1; }  // bounded by the cycle budget
```

- **Why not extend `MerkleExchange`**: it is segment-id-partitioned
  (`healing.proto:35-64`) and its response trees are built for segments.
  Overloading it would conflate two partitions in one protocol. The
  ADR-0030 split (routing/diff intent vs data movement) is the template:
  compare carries intent, fetch carries rows.
- **Bounded peer work.** The responder answers from point gets for offered
  keys and one bounded scan of `[start_key, end_key]` for extras; `max_entries`
  caps the response and `truncated_at` tells the initiator where to advance
  the cursor. An initiator with a large key gap therefore still discovers
  peer-only keys in that gap (extras) at bounded cost, and never stalls.
- **Either side may discover it is behind**; the behind side pulls. Nothing
  is pushed, preserving ADR-0030 and ADR-0027 §D5's data-bearing rule.
- **Ownership filter**: both sides include a key only if the peer is in the
  key's current replica set, and the applying side re-checks co-ownership at
  apply time. Ring changes are read from the current snapshot per batch and
  per apply; AE never mutates or caches ownership, so join/leave/re-weight
  (d6 C2a, currently uniform — `docs/features/disk-resilience-scale/sketches.md:157-174`)
  create no new dependency and no repair storms.

### D4. Apply semantics — bytes before rows

**Recommended: local-segment-presence precondition, with ADR-0030 pairing
for the missing-bytes case.** A repaired row must never reference bytes the
node cannot read.

1. **Inline rows** (`chunks` empty) apply directly.
2. **Segment-stored rows**: before applying, verify every `ChunkRef.segment_id`
   is locally present (lifecycle registry entry Sealed and resolvable by the
   local data store). If all present → apply. If any missing → **do not apply
   the row**; dispatch re-replication for the missing segments through the
   existing ADR-0030 target-pull worker (`RequestReReplication`, reason
   `REPAIR_REASON_RECONCILIATION`) and remember the key in a small bounded
   in-memory `awaiting_bytes` set. When the segment lands, the pending row is
   applied; if the process restarts, the next rotation re-detects it. This is
   the "row + bytes" repair for the absent-holder case; row-only for the
   index-only case (the f4 fixture: bytes present, row missing).
3. **Plain tombstone rows apply unconditionally** (no chunks needed), but
   through a local-aware path: delete the local live row, capture the
   **local** row's chunks into the tombstone, preserve the pulled
   `deletion_time`/`hlc`, and **drop any pulled chunk refs for segments this
   node does not hold**. Importing foreign chunk refs would let a later
   segment re-replication inherit phantom dead-bytes accounting and
   mis-mark live data dead (ADR-0034 D3 trust-the-accounting).
4. **Apply through the mutation choke point**, not the byte-verbatim
   recovery fold: the incoming logical fields drive a capture-preserving
   apply (supersede capture on overwrite, tombstone migration on re-PUT),
   reusing the LWW/tombstone guards of `PutObjectMetadata`
   (`segment_service.rs:660-802`) and `rebuild_apply_*` (`store.rs:1569-1616`).
   The streamed value's `row_hash` is re-verified before apply.
5. **Ordering**: bytes before rows. A row whose bytes are missing leaves the
   key unapplied (read failover continues to serve from replicas) until the
   byte repair completes.

### D5. Tombstone TTL invariant (no resurrection)

A GC'd tombstone cannot win LWW, so after its TTL expires a stale live copy
on a node that missed the delete can be "repaired" back as a resurrection.
The safety invariant is the standard one:

> **The AE rotation period (plus worst-case clock skew and partition
> window) must be strictly less than the tombstone TTL**
> (`gc.tombstone_ttl_sec`, default 259 200 s = 3 days;
> `crates/oceanfs-durability/src/gc/config.rs:34-47`, aged at
> `gc/garbage_collector.rs:234-265,521`).

Enforcement:

- expose `oceanfs_metadata_ae_rotation_seconds` (gauge) so the margin is
  observable;
- when `metadata_ae.enabled`, validate at boot that
  `gc.tombstone_ttl_sec ≥ max_rotation_budget + skew_margin`, where
  `max_rotation_budget` is derived from `R`-independent config (budget ×
  cycles) rather than a live estimate — if the configured budget cannot
  guarantee it, **refuse to start the AE worker** (fail loud, leave the
  feature disabled) and log the mismatch;
- the default `metadata_ae` budgets are sized for a rotation well under the
  default TTL; an operator who raises the rotation budget past the margin
  gets the boot error, not silent resurrection risk.

### D6. Pacing and budget (Tier-1 under ADR-0017)

- The worker is a `DurabilityTask` registered with the `DurabilityScheduler`
  (name `metadata_anti_entropy`, `keyspace_fraction() == 1.0` — it owns a
  finer cursor than the scheduler's shard rotation, exactly as `AeTask`
  rejects a non-full window today, `crates/oceanfs-durability/src/scheduler/adaptors.rs:190-228`).
- Each cycle acquires one **Tier-1** housekeeping permit
  (`DurabilityBudget::acquire_housekeeping`), per the ADR-0017 amendment:
  metadata AE is clock-driven verification where minutes of delay are
  invisible to the durability contract. A cycle is bounded by config and
  releases the permit between cycles:
  - `interval_sec` (cycle cadence, default 30),
  - `batch_keys` (keys offered per compare RPC, default 512),
  - `max_batches_per_cycle` (default 8),
  - `max_bytes_per_cycle` (pulled rows + payload bytes, default 4 MiB),
  - `max_inflight_compares` (default 2).
- Priority model reuse: the worker itself is uniform (every key is
  eventually visited), but **repair** urgency is inherited from the existing
  reconcile/G4 priority when a missing row is under-replicated (single-copy
  ranges are seen first by the quickest peer rotation); missing **bytes**
  ride the Tier-0 ADR-0030 worker and its existing prioritization unchanged.
- No repair storm on rejoin: a returning node compares at its configured
  cadence; the other nodes' cursors encounter it on the next rotation and
  the per-cycle budget caps how much is pulled per tick.

### D7. Crash safety and boot cost

- **Persisted**: the enabled flag (config) and a ~40 B cursor
  (`(phase, last_key, rotation_index, peer_index)`) written atomically
  (temp + rename) under the metadata pool root, e.g.
  `<metadata_pool_root>/metadata_ae.cursor`.
- **Rebuilt**: nothing. There is no derived state. A missing/corrupt cursor
  restarts the rotation from the beginning — safe, never a correctness issue.
- **Worst-case boot cost: zero.** The worker starts after cluster readiness
  and resumes from the cursor; there is no full scan on the critical path
  (contrast Option 2's boot scan and g8's node-gating rebuild,
  `metadata_recovery.rs:210-291`).
- A metadata-pool replacement (g8) wipes the cursor with the pool; the
  rebuilt store simply starts a new rotation.

### D8. ADR-0034 accounting statement

**No new persisted metadata surface.** Specifically:

- no new column family (Option 1 rejected);
- no per-row or per-key derived rows; the only new persistent artifact is
  the ~40 B cursor file, constant-size, independent of row count;
- tombstones are **read** (and included in comparison) but never duplicated;
  supersede records are neither compared nor repaired;
- per-tick work is bounded (`max_batches_per_cycle × batch_keys` keys,
  `max_bytes_per_cycle`), and the rotation is a multi-cycle pass — the
  ADR-0017 §3 keyspace_fraction model, not a per-event `list_objects_all`
  scan. The design draft's ADR-0034 acceptance ("no recurring
  `list_objects_all`-class call") is met: the cursor never materializes a
  CF, never holds more than one batch, and resumes across cycles.

### D9. Metrics

Registered only when enabled (so a disabled node exposes no AE series):

| Metric | Type | Meaning |
|---|---|---|
| `oceanfs_metadata_ae_keys_compared_total` | counter | keys offered/compared |
| `oceanfs_metadata_ae_mismatches_total{state}` | counter | `object`/`tombstone`/`absent` differences found |
| `oceanfs_metadata_ae_rows_pulled_total` | counter | rows fetched for repair |
| `oceanfs_metadata_ae_rows_applied_total` | counter | rows applied through the choke point |
| `oceanfs_metadata_ae_rows_skipped_total{reason}` | counter | `lww`, `missing_segment`, `not_holder`, `hash_mismatch` |
| `oceanfs_metadata_ae_bytes_pulled_total` | counter | repair transfer volume |
| `oceanfs_metadata_ae_repair_lag_seconds` | histogram | now − pulled row HLC wall time at apply (convergence lag) |
| `oceanfs_metadata_ae_rotation_seconds` | gauge | duration of the last completed rotation |
| `oceanfs_metadata_ae_rotation_progress` | gauge | 0..1 estimated coverage of the current rotation |
| `oceanfs_metadata_ae_cursor_resets_total` | counter | cursor missing/corrupt → rotation restarted |
| `oceanfs_metadata_ae_pending_bytes_keys` | gauge | keys awaiting re-replicated bytes |

Naming follows the g8 durability-metric convention
(`oceanfs_metadata_rebuild_*`, `metadata_recovery.rs:106-140`) and avoids the
segment-plane `ae_*` counters (`ae_segments_compared_total`,
`ae_mismatches_found_total`, `anti_entropy/engine.rs:93-94`).

### D10. Kill-switch and rollout

**Config — exact names.** New section in `oceanfs.toml`:

```toml
[metadata_ae]
enabled = false                # kill-switch (default OFF)
interval_sec = 30              # Tier-1 cycle cadence
batch_keys = 512               # keys per compare RPC
max_batches_per_cycle = 8      # per-cycle scan budget
max_bytes_per_cycle = 4194304  # per-cycle transfer budget
max_inflight_compares = 2
```

- `MetadataAeConfig` lives in `crates/oceanfs-core/src/config/metadata_ae.rs`
  (same pattern as the pr1 `[durability] capacity_refresh_interval_sec`
  precedent, `crates/oceanfs-core/src/config/durability.rs:34-151`), is
  re-exported from `oceanfs_core`, and is added to `NodeConfig` beside
  `anti_entropy`/`durability` (`config/node.rs:220-230`) as
  `#[serde(default)] pub metadata_ae: crate::MetadataAeConfig`.
- `enabled = false` ⇒ the composition root does **not** construct/register
  the worker, does **not** spawn any loop, does **not** read/write the
  cursor, and the `CompareMetadata`/`FetchMetadataRows` handlers return
  `unavailable` (an enabled peer skips the node for that rotation). Zero
  behavior change; no new CF writes (there is no CF).
- `enabled = true` ⇒ the worker registers with the scheduler as a Tier-1
  task; the compare/fetch handlers serve; metrics register.

**Pre-f4 implementation and the P4 two-pass protocol.** Land the ADR-0037
feature with `enabled = false` before f4 resumes. When the P4 scenario runs
on the fleet:

1. **Pass A (AE off):** run P4 unchanged; the report records the residual
   cold-key divergence (the AE acceptance baseline; f4 DoD
   `f4-pool-degradation-under-load.md:309-311`). This is the only sizing
   data that replaces assumptions A1-A5.
2. **Pass B (AE on):** set `metadata_ae.enabled = true` on all SUT configs
   and restart the oceanfs service (same fleet session; no re-provisioning);
   rerun the same P4 scenario. f4 must observe: the residual divergence
   detected (`oceanfs_metadata_ae_mismatches_total > 0`), repaired
   (`..._rows_pulled_total`/`..._rows_applied_total > 0`), and manifest
   verification back to full coverage within the rotation bound for the P4
   key count; tombstone-safety — the missed DELETE is not resurrected
   (the node that missed it converges to the tombstone); and no regression
   of the bounded discipline (per-cycle keys/bytes stay within budget).
   Exact assertion blocks are the spec-writer's deliverable; the ADR fixes
   only the observables.

### Out of scope

- Segment-plane reconciliation / holder-set repair (bytes are already
  owned there).
- Read-path backfill and debt-replay repair (dropped 2026-09-11).
- C2b proactive row migration; AE may later be reused by it, but this ADR
  does not decide C2b.
- Repair of **extra** (non-owner) stale rows; GC/ownership handles those.
- Sampling mode: coverage is complete per rotation by design.
- Changing the hint delivery contract (ADR-0027 as amended) or the f0 gate.

---

## Consequences

### Positive

- **The gap closes with no derived state and no hot-path cost.** A missing
  row or missed delete is eventually re-derived from a live co-holder; f0
  and AE together are admission honesty + convergence.
- **ADR-0034/0023 alignment**: no new CF, no per-row surface, no recurring
  materialization; the only new persistent artifact is a 40-byte cursor.
  The native-store path (ADR-0023 Phase 2) needs no fingerprint store.
- **Kill-switch inertness is structural**, not flag-checked on the hot path:
  disabled means no worker, no RPC traffic, no cursor I/O.
- **Correctness by construction where the draft demanded it**: three-state
  digest, HLC-aware, supersede-excluded, tombstone chunk filtering, bytes-
  before-rows ordering, deterministic equal-HLC tie-break.
- **Reuses existing machinery**: `MetadataRow` transport, the g8 apply
  guards, the HLC-LWW choke point, the ADR-0030 re-replication worker for
  bytes, the ADR-0033 manifest-aware peer filter, the ADR-0017 scheduler.

### Negative

- **Detection latency is the rotation period.** At 10⁷ rows/node with the
  default budget this is ~20 h; the ADR's near-term target is ≤1 h at 10⁶
  rows, ≤24 h at 10⁷. Cold-key healing is bounded, not prompt.
- **The scan re-reads the metadata CF every rotation.** At 10M rows that is
  ~2 GB of iteration per pass plus peer-side lookups — real background I/O,
  bounded per tick but non-zero. It is housekeeping, not free.
- **Per-key ownership filtering and peer-only-key discovery add protocol
  complexity** (extras, truncation, per-key co-holder selection) that a
  hash-ordered index would not need.
- **The tombstone-TTL invariant is operational**: if operators shrink the
  GC TTL below the rotation budget, resurrection risk exists (mitigated by
  the boot validation in D5, but it must be implemented correctly).
- **No latency benefit for hot keys** until the deferred hot-set
  accelerator lands.

### Neutral

- Segment-plane `[anti_entropy]` (Merkle/continuous/sampling) is untouched;
  `metadata_ae` is a separate feature with separate config and metrics.
- The g8 `ListObjectsInRange` (hash-range, full-CF-filter) remains the
  boot-rebuild tool; AE adds a key-range path rather than changing g8.
- The design draft's Open Questions are answered by this ADR (detection,
  protocol, pairing, sizing method, TTL); the draft's status can move from
  `deferred` to "ADR decided, spec pending" once accepted.

---

## Risks

1. **Rotation latency at horizon scale becomes an operational SLA problem.**
   Mitigation: observable rotation metrics; budget knobs; the sanctioned
   accelerator path (bounded hot set → fingerprint CF) with an explicit
   trigger; the ADR can be amended with data.
2. **Tombstone resurrection via TTL/rotation inversion or long partitions.**
   Mitigation: D5 boot validation + metrics + the resurrection hazard being
   explicitly tested (tombstone-safety scenario in f4 P4 and a dedicated
   in-process test).
3. **Dangling references from repaired rows.** Mitigation: D4 precondition,
   bytes-before-rows ordering, `missing_segment` skip counter, and the
   pending-bytes hand-off to ADR-0030.
4. **Peer RPC work amplification under a skewed keyspace** (one peer with a
   huge range of extras). Mitigation: `max_entries`/`truncation_at` caps and
   `max_bytes_per_cycle`; the cursor advances even when truncated.
5. **False diffs from non-deterministic row serialization** (legacy JSON vs
   bincode, `store.rs:1663-1677`). Mitigation: deterministic equal-HLC
   tie-break (max row_hash) converges; the `hash_mismatch` skip counter
   surfaces anomalies.

## Open Questions

- **Rotation budget defaults at horizon scale**: is ~20 h at 10⁷ rows
  acceptable, or should the first implementation ship the bounded hot-set
  accelerator? (Trigger: f4/f5 rotation metrics + first production-scale
  row counts.)
- **Compare RPC payload tuning**: is a key-ordered full-fingerprint batch
  (default 512) the right unit, or should a digest (Merkle-style) compress
  the equal case for large batches? (Decide at spec time with a micro-
  benchmark; the protocol shape supports either.)
- **Ownership during in-flight joins**: ADR-0028's lossless boundaries are
  assumed; the implementation must confirm that a key whose arc moved
  mid-rotation is either compared against the new holder or skipped without
  a false repair (re-checked at apply time per D3).
- **Cursor placement under the metadata pool**: confirm the pool-root file
  location against ADR-0029/0031 path rules and the g8 replacement path.
- **Tie-break semantics for equal-HLC content divergence**: max row_hash is
  proposed; verify against the hint/read-repair paths so the whole system
  uses one deterministic rule.

## If a Different Option Is Chosen — Feature-Spec Deltas

- **Option 1 (fingerprint CF):** the feature spec adds a new column family
  to `MetadataStore` (open/migrate/backfill), a capture-completeness matrix
  for every mutation path (mirroring ADR-0034 D6) covering
  `put_object_in_bucket`, `delete_object`, and `batch_write` remaps, an
  enable-time backfill scan, native-store parity as an explicit residual,
  and a hash-arc comparison protocol instead of key-ordered batches.
  Kill-switch inertness must additionally guarantee no CF creation/writes
  when disabled.
- **Option 2 (in-memory Merkle/journal):** the feature spec adds a
  per-key in-memory digest structure with a hard memory cap and keyspace
  sharding as a dependency, a boot rebuild path (or `MerkleWal`-style
  journal + replay), a mutation choke-point feed with its own tests, and
  boot-warming observability; the compare protocol can stay range-digest
  based, but localization requires the per-key state.
- **Hybrid (hot set + cursor):** the spec is this ADR's spec plus a bounded
  change-fed hot-set section (memory cap, eviction, feed wiring, its own
  metrics); the compare/fetch/apply paths are unchanged.

## References

- Gap and design capture: `docs/features/metadata-anti-entropy/design-draft.md`
- Residual statement: `review/cluster-churn-resolution-2026-09-10.md:72-73`
- AE acceptance harness (paused): `docs/features/fleet-degradation/f4-pool-degradation-under-load.md:179-184,309-311`
- Baseline data: `docs/features/fleet-degradation/artifacts/f3-control-dirty-20260911.json:36-77`,
  `…/f3-full-run3-20260911.json:36-65`, `…/f5-rerun-full-20260912.json:36-41`
- Choke point / capture: `crates/oceanfs-storage/src/metadata/store.rs:529-602,650-692,1133-1178,1460-1616`
- CF layout: `crates/oceanfs-storage/src/metadata/cf.rs:14-16,26-56,58-148`
- Apply/LWW semantics: `crates/oceanfs-server/src/grpc/segment_service.rs:655-803`,
  `crates/oceanfs-server/src/write/coordinator.rs:1128-1149`
- g8 pull/rebuild: `crates/oceanfs-node/src/modules/metadata_recovery.rs:42-90,210-291,332-397`
- Wire: `proto/oceanfs/healing.proto:35-64,67-128,369-406`
- Scheduler/budget: `crates/oceanfs-durability/src/scheduler/adaptors.rs:183-228`,
  `crates/oceanfs-durability/src/reconcile.rs:53-99,152-162,200-250`
- Ring/ownership: `crates/oceanfs-routing/src/hash.rs:15`, `crates/oceanfs-routing/src/ring.rs:150-167,226-269`
- Tombstone TTL: `crates/oceanfs-durability/src/gc/config.rs:34-47`, `gc/garbage_collector.rs:234-265,521`
- Config precedent: `crates/oceanfs-core/src/config/durability.rs:34-151`, `config/node.rs:27-58,220-230`
- Metrics conventions: `crates/oceanfs-core/src/metrics.rs:263-269`, `metadata_recovery.rs:99-148`
- ADRs: 0015, 0017 (incl. 2026-09-06 amendment), 0023, 0027, 0028, 0029, 0030, 0033, 0034
- d6 ownership sketch: `docs/features/disk-resilience-scale/sketches.md:157-174`
