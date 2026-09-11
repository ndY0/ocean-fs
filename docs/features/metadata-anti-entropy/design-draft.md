---
feature: "Metadata Anti-Entropy — Design Draft (Deferred)"
epic: "metadata-anti-entropy"
status: deferred
priority: high
owner: ""
dependencies:
  - feature: fleet-degradation/f4-pool-degradation-under-load
    reason: The f4 rerun measures residual cold-key divergence and is this feature's acceptance harness; sizing is informed by its first-run data
  - feature: fleet-degradation/f0-hints-durability-gate
    reason: The gate bounds NEW divergence (no ack without durable debt); AE heals EXISTING divergence from any cause — complementary, not alternatives
adr:
  - 0034-bounded-metadata-accounting
  - 0015-anti-entropy-merkle-protocol
  - 0029-storage-pools-disk-resilience
  - 0027-hinted-handoff-ownership-model
  - 0030-re-replication-target-pull
perf: []
created: 2026-09-11
updated: 2026-09-11
---

# Metadata Anti-Entropy — Design Draft (Deferred)

> **Status: DEFERRED design capture (d6 pattern).** This is not a committed
> feature; it is the record of a verified gap and the design question it
> raises, so the work is never forgotten. It is written as a design draft
> (not a full feature) because its **detection mechanism is an open
> architecture question that requires an ADR before the feature can be
> specified** ([the hard design question](#the-hard-design-question-must-be-an-adr-input)
> below). Acceptance is the **f4 rerun** after f0 lands.
>
> **Why its own directory.** Metadata anti-entropy is a product/durability
> workstream, not a fleet-degradation test feature; giving it a topic-
> addressed home (`docs/features/metadata-anti-entropy/`) keeps it
> discoverable and lets the eventual full feature doc land beside this
> draft. It is cross-linked from the
> [`fleet-degradation`](../fleet-degradation/epic.md) epic (which gates it)
> and from `disk-resilience-capacity`.

## Summary

The **index plane has no eventual-repair path.** Three verified facts define
the gap:

1. **Hints are the only metadata-row backfill for a missed replica.** Read
   repair explicitly delegates the absent/remote-newer case to hinted handoff
   (`crates/oceanfs-server/src/read/coordinator.rs:1142-1149`), and the churn
   session recorded the residual in plain terms: "anti-entropy does not
   backfill missing object *rows*"
   (`review/cluster-churn-resolution-2026-09-10.md:72`).
2. **The repair plane heals segments by holder sets, not the key→segment
   index.** Reconciliation compares live replica counts over the lifecycle
   registry / `HolderIndex` (`crates/oceanfs-durability/src/reconcile.rs`)
   and restores **bytes**; segments are not self-describing, so it cannot
   know which object rows reference them (ADR-0034 Context: "The only
   object→chunk→segment mapping is the forward objects CF").
3. **Debt can still be lost.** f0 closes the *admission* hole (no ack
   without durable debt), but divergence can already exist from: debt lost
   with a hints device before the gate existed, bounded hint give-up
   windows, crash windows between row apply and hint truncation, and the
   hint WAL's own recovery limits. Once a row is missing and the hint is
   gone, **nothing ever re-derives it**; a cold key can stay
   under-replicated forever.

**Relationship to f0 — complementary, not alternatives.** The gate is
*admission honesty*: it stops new divergence at the ack boundary. AE is
*convergence*: it heals divergence from any cause, including the one f0
cannot prevent (device loss erases recorded debt). Segment reconcile +
hints gate + metadata AE is the locked target architecture; read-path
backfill and debt-replay repair were dropped as half-measures.

## Existing machinery to reuse (inventory, verified 2026-09-11)

| Piece | Where | Reuse |
|---|---|---|
| HLC-aware, tombstone-safe row push RPC `PutObjectMetadata` | `crates/oceanfs-server/src/grpc/segment_service.rs:660-802` | The **apply primitive**: HLC mandatory; a tombstoned key rejects pushes unless strictly newer (legitimate resurrection); LWW rejects pushes older than the local row. |
| Row-range stream `ListObjectsInRange` / `MetadataRangeLister` | `proto/oceanfs/healing.proto:127`; `crates/oceanfs-durability/src/healing_service.rs:113-135` | The **transfer primitive** (pull). Server side scans objects+deletions CFs and filters by key hash (`crates/oceanfs-node/src/modules/metadata_recovery.rs:42-89`). **Caveat:** it visits both CFs in full — fine for the one-time g8 boot rebuild, NOT usable as recurring AE under ADR-0034 (see options). |
| g8 metadata-pool rebuild consumer | `crates/oceanfs-node/src/modules/metadata_recovery.rs:332-379` | The **pull-and-fold loop** and its per-row apply (`rebuild_apply_object_row`), plus the metrics shape (`oceanfs_metadata_rebuild_*`). AE is "g8, but recurring, incremental, and range-scoped". |
| Hint-apply HLC-LWW semantics | `crates/oceanfs-server/src/write/coordinator.rs:1029+` (`apply_hinted_object`, `:1052`) | The **ordering semantics** repairs must preserve (HLC-LWW; tombstone-safe). Note hint-apply also appends segment data; AE row repair does not (see apply-semantics question below). |
| Segment-plane anti-entropy (Merkle roots, incremental trees) | `crates/oceanfs-durability/src/anti_entropy/`, ADR-0015 | The **pattern** for change-fed incremental digests — but per *segment* (thousands), not per *row* (millions). |
| Reconciliation work queue / `HolderIndex` | `crates/oceanfs-durability/src/reconcile.rs:186` | The **scheduling** pattern: bounded, paced, prioritized, retried; Tier-1 under the durability budget (ADR-0017). |
| Target-pull discipline | ADR-0030 / `RequestReReplication` | The **directionality**: the behind node pulls; the holder serves. Keeps the sender out of the data path. |

## The hard design question (must be an ADR input)

**How does a node detect that its metadata rows differ from a replica's,
under ADR-0034's bounded-metadata discipline (no full-object scans, no new
RocksDB surface without explicit justification)?**

ADR-0015 solved this for the segment plane with incremental Merkle trees
over *sealed segments* (a small, append-oriented, registry-bounded set).
The metadata plane is different: **rows are O(all objects)**, churn on
every PUT/DELETE, and the existing range lister is a scan-and-filter. Any
detection mechanism must state its bound on CPU, memory, I/O, and rebuild
cost. Options, each with honest trade-offs:

1. **New hash-ordered metadata index CF maintained on write/delete.**
   A compact derived CF keyed by `hash(bucket/key) → row fingerprint`
   (e.g. HLC + tombstone flag + chunk-set digest), updated in the same
   RocksDB `WriteBatch` as the row mutation. Ranges then support bounded
   prefix iteration (`hash` order), replacing the full-CF scan; per-range
   digests become cheap and recurring AE is O(changed keys + compared
   ranges).
   *Trade-offs:* duplicates metadata (space), adds write amplification to
   the hottest path, and is a new RocksDB surface at a time when
   ADR-0023/ADR-0034 steer **away** from RocksDB coupling. It does make
   detection exact and cheap forever. Accounting implications must be
   stated (fingerprint size × rows; tombstones included).
2. **Change-fed in-memory incremental Merkle, one-time boot scan.**
   The ADR-0015 pattern lifted to rows: maintain per-range Merkle trees in
   memory (`hash → version/tombstone leaf`), updated from the same
   mutation choke point as ADR-0034 D2's capture; on boot, one full scan
   rebuilds the trees (off the critical path, like g8's rebuild gate),
   then change-feed maintenance keeps them fresh.
   *Trade-offs:* row cardinality memory is the risk — at millions of keys
   × 32 B leaves × tree overhead the in-memory set is hundreds of MB per
   node and scales with the object count, not the segment count (contrast
   ADR-0015's 10 k-segment cap). Requires keyspace-fraction sharding
   (the scheduler sharding ADR-0034 explicitly enables) to bound each
   shard; crash-safe derived state needs a MerkleWal-like journal or
   accepts the boot re-scan; the boot scan is a full scan, once.
3. **Rotating bounded scan cursor.**
   No derived index at all: a persistent cursor walks the objects +
   deletions CFs in bounded batches (key-ordered), computing and exchanging
   per-range digests with a peer, wrapping around on a schedule. The
   existing `ListObjectsInRange` lister is the seed, but the scan must be
   incremental (resume from a durable cursor) rather than a full visit per
   range.
   *Trade-offs:* cheapest to build, no new CF, no in-memory state, uses
   the existing stream; convergence is slow and proportional to full-
   keyspace scan time (cold keys are exactly the ones a short window
   misses), and per-range digest exchange still needs a comparison
   protocol. Best fit as the **safety net** under **sampling** semantics,
   not as the primary detector at scale.

**What the ADR must decide:** the option (or hybrid — e.g. option 2 with
keyspace sharding for hot/owned ranges plus option 3 for cold/full
coverage), the digest content and ordering, the comparison RPC, the
initiator direction, and the accounting/space impact with numbers.

### Tombstone-safe hashing (a correctness requirement, not a detail)

- The digest must cover **both objects and deletions CFs**. An object on
  one node and a tombstone on another are *different* states; hashing only
  live objects would report "in sync" while a stale copy survives.
- HLC must participate: same key, different version is a mismatch that
  HLC-LWW resolves; the digest must distinguish versions the apply path
  treats as distinct.
- Tombstone **TTL/aging** interacts with AE: a tombstone GC'd before a
  stale copy is healed can allow resurrection on the stale node. The ADR
  must state whether AE reads unaged tombstones or whether TTL coverage
  alone is sufficient (today's delete path relies on tombstones for
  ordering; AE must not weaken that).
- **Missing/absent** must be a distinct digest state from **tombstoned**:
  `(no row)` vs `(object row)` vs `(tombstone row)` are three states, and
  the first two are a mismatch.

### Apply semantics and the dangling-reference wrinkle

The row push RPC applies a row that references segment chunks. A row
restored on a node that does not hold the referenced segment is a dangling
reference (reads fail over, but the row is not truly repaired).
Recommended: AE row repair treats **local segment presence** as a
precondition or a paired action (segment reconcile restores bytes by holder
set; AE restores the index). The ADR must state the ordering: whether AE
applies only rows whose chunks resolve locally (verify via the lifecycle
registry / local reader), or whether a missing segment triggers a segment
fetch first. Hinted handoff solved this by fetching + appending data and
row together; AE may need an equivalent "row + bytes" repair for the
absent-holder case, while staying row-only for the index-only divergence
case.

### Comparison, direction, pacing, crash safety, metrics

- **Bidirectional diff:** the behind node pulls (target-pull discipline,
  ADR-0030). The comparison must be symmetric enough that either side can
  discover it is behind; the *repair* direction is always pull.
- **Pacing/budget:** Tier-1 scheduled background work (ADR-0017), bounded
  per tick, prioritized by under-replication risk (single-copy ranges
  first, reusing the reconcile priority model). No repair storm when a
  node rejoins after a long outage.
- **Crash-safe derived state:** digests/index rows are derived; the ADR
  states what is persisted, what is rebuilt, and the worst-case rebuild
  cost on boot — never a full scan on the critical path (ADR-0015's
  constraint).
- **Metrics:** ranges compared, mismatches found, rows pulled/applied,
  rows skipped (stale), convergence lag, cursor position/coverage — all
  needed to define the acceptance bound.
- **Ownership:** metadata rows are placed on the ring successors of
  `hash(bucket/key)` (the ring is the discovery index, d6 measurement
  finding). AE compares within owned/held ranges; ring changes stay
  lossless at empty boundaries (ADR-0028 / d6 finding), so AE does not
  create a new ownership dependency.

## Acceptance criteria (placeholder — filled by the f4 rerun data)

- After the f4 hints-death + peer-outage scenario, with f0 landed: measured
  **cold-key coverage returns to RF within a stated bound** (bound derived
  from the f4 first-run divergence counts; no bound is asserted until that
  data exists).
- The AE path repairs a row whose hint was demonstrably lost (the f4
  fixture), verified by read-back from the recovered replica.
- Tombstone-safety scenario: a missed DELETE is not resurrected by AE
  (stale object row on a node that missed the tombstone converges to the
  tombstone, not the reverse).
- No regression of the bounded-metadata discipline: no recurring
  `list_objects_all`-class call on the AE path (ADR-0034 acceptance).

## Non-goals

- **Read-path backfill** — dropped (locked 2026-09-11).
- **Debt-replay repair** — dropped (locked 2026-09-11); AE makes it
  redundant.
- **Segment-plane changes** — reconciliation/holder sets already own bytes;
  AE owns rows only (with the paired-action exception above if the ADR
  requires it).
- **Changing the hint delivery contract** (ADR-0027 as amended) or the f0
  gate.
- **C2b-style proactive row migration** — the `disk-resilience-capacity`
  backlog decision; if C2b is ever built, it will likely reuse AE's
  digests/range iteration, but AE does not decide C2b.

## Cross-links

- [`fleet-degradation` epic](../fleet-degradation/epic.md) — owns f0 and
  f4; the f4 rerun is this feature's acceptance harness.
- [`f0-hints-durability-gate`](../fleet-degradation/f0-hints-durability-gate.md)
  — admission honesty; this draft is its convergence complement.
- [`disk-resilience-capacity`](../disk-resilience-scale/epic.md) and
  [`d6-capacity-weighted-ownership`](../disk-resilience-scale/d6-capacity-weighted-ownership.md)
  — C2a/C2b residual; a row-migration attempt would likely reuse AE
  digests.
- ADR-0034 (bounded metadata accounting — the constraint), ADR-0015
  (segment-plane Merkle pattern), ADR-0029 §D3/D7 (hints/pool semantics;
  metadata loss recovery), ADR-0027 (hinted handoff delivery), ADR-0030
  (target-pull), ADR-0017 (durability scheduling).

## Open Questions

- **The detection option** (the ADR input above) — no feature spec until
  decided.
- **Digest protocol:** new RPC vs extending `MerkleExchange` to metadata
  ranges; who initiates; how ranges map to ring ownership.
- **Row+bytes pairing:** does AE ever fetch segment bytes, and if so, does
  that reuse the re-replication worker (ADR-0030) rather than a new path?
- **Sizing:** the f4 first run's divergence counts set the scan/repair
  budgets; this draft deliberately contains no numbers until then.
- **Tombstone TTL coverage:** how AE interacts with tombstone expiry on a
  partitioned/stale node.
