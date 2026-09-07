# ADR-0036: Phase C — Cluster Capacity Operations (Drain / Detach / Retirement)

**Status:** Proposed
**Date:** 2026-09-07
**Deciders:** Stakeholder (architecture owner) + Implementer, design session 2026-09-07

**Related:** [ADR-0029 storage pools](../adr/0029-storage-pools-disk-resilience.md)
(Phase A/B/C scope), [ADR-0030 re-replication target-pull](../adr/0030-re-replication-target-pull.md)
(the C1b mover), [ADR-0031 pools mandatory](../adr/0031-remove-single-datadir-legacy-mode.md)
(obsoletes ADR-0029 §D8 zero-config language), [ADR-0032 store unification](../adr/0032-unify-segment-data-access.md),
[ADR-0033 manifest-aware selection](../adr/0033-manifest-aware-peer-selection.md),
[ADR-0034 bounded metadata scans](../adr/0034-bounded-metadata-accounting.md),
[ADR-0017 durability task abstraction](../adr/0017-durability-task-abstraction.md),
[ADR-0025 segment lifecycle](../adr/0025-segment-lifecycle-state-machine.md),
[ADR-0035 replicated lifecycle state](../adr/0035-replicated-segment-lifecycle-state.md)

---

## Context

### Where the disk-aware scope stands

Phase A (`disk-resilience` epic, f1–f8) and Phase B (`disk-resilience-healing`
epic, g1–g8) have landed. Pools are mandatory at boot (ADR-0031). A node can
attach a new pool at runtime (f8), route around degraded/dead pools, announce
losses, reconcile under-replication, re-replicate to capacity-aware targets
(ADR-0030), and recover from WAL-pool and metadata-pool loss (g7/g8, ADR-0035).

The 2026-08-25/09-03 whole-project review produced a structural refactor that
is now complete: composition-root decomposition (module builders under
`oceanfs-node/src/modules/`), one unified `DiskSegmentStore` (ADR-0032),
pools-only data access (ADR-0031), accounting-based bounded metadata scans
(ADR-0034), the durability scheduler with a two-tier budget (ADR-0017
amendment), and manifest-aware AE/scrub/repair peer selection (ADR-0033).

What is missing is the **scale-ops half of the pool model**: the ability for
an operator to *change a node's or the cluster's capacity over time* —
remove a disk, shrink a node, retire a node, or redistribute what a topology
change leaves behind. Today:

- There is no `detach` (only f8 `attach`); a pool can be added but never
  cleanly removed.
- `pool_id` is immutable per segment (stamped at seal, cached in the reader);
  there is no durable path to move a sealed segment to another pool.
- Graceful leave is `leave(None)` + peer healing (the leave-handler streaming
  was deleted in the refactor); retiring a node therefore means stopping it
  and letting the cluster re-replicate its whole footprint — a heal storm
  rather than a paced operation.
- The ring is uniform `vnodes_per_node` with no capacity weighting; pool
  weight/free-capacity influence placement and repair-target choice only.

The 2026-08-22 brainstorm (`disk-resilience-pools.md`) scoped this as
"Phase C — Scale ops" with three items (C1 drain/rebalance, C2 capacity-
weighted vnodes, C3 segment self-description). The design session on
2026-09-07 re-scoped C1–C3 against the post-refactor code. Several premises
changed:

1. **No legacy mode** (ADR-0031): the "config migration from single
   `data_dir`" framing and the zero-config-fallback convenience argument in
   ADR-0029 §D8 and brainstorm §5-Q6 are obsolete. Phase C is topology change
   on live, pool-configured nodes — nothing else.
2. **Metadata-loss recovery no longer re-replicates** (g8): it streams
   `ObjectRow`s + `DeletionRow`s from a peer over owned ring ranges
   (`ListObjectsInRange`). The original C3 rationale — "segment
   self-description so metadata-loss recovery needs no re-replication
   traffic" — is **logically wrong** now: even a self-describing `.dat`
   cannot reconstruct inline objects or deletions/supersedes records, which
   the peer stream exists to restore.
3. **Drain ≠ rebalance.** "Drain" as an operator-intent, terminal operation
   (empty this source) and "rebalance" as a continuous skew-chasing loop are
   different animals with different risk profiles. The design session
   separated them explicitly.

### Forces

- **Cluster capacity is a whole, not a per-node sum.** Operators must be able
  to take one node's disks out of service (or shrink/retire the node) and
  have its data move to the *rest of the cluster* — not just to a sibling
  disk on the same node.
- **Large nodes make synchronous retirement impossible.** A node may hold
  terabytes; streaming them during a shutdown is unacceptable (the review's
  conclusion, reaffirmed). Retirement must be a paced background operation
  run *before* the node is stopped, not a shutdown-path transfer.
- **Re-replication machinery already exists** (ADR-0030 target-pull,
  `ReRepWorker`, `ManifestRepairTargetSelector`). Drain must reuse it, not
  invent a second data-movement path.
- **One durable writer.** Segment state changes must ride the event-WAL +
  checkpoint fold (ADR-0025/ADR-0024) — the same discipline that carried the
  `storage_locations` refresh payload (ADR-0030).
- **Background work is scheduled, not free-spawned.** Post-review, every
  background loop is a `DurabilityTask` under the ADR-0017 scheduler; the
  two-tier budget protects Tier-0 repair/heal from Tier-1 housekeeping.
- **No destructive failure.** A drain that cannot find an eligible target must
  block and explain itself, never delete data and never half-remove a pool.
- **Tests have not been run broadly for some time** (design-session note).
  The phase opens with a regression gate before any code lands — and the
  seven pre-existing failures documented across the Phase A/B feature docs
  are **fixed as a pre-epic**, not recorded-and-accepted (session decision
  2026-09-07; d0 of the epic carries the fix list).

---

## Decision

### D1. Phase C = cluster capacity operations. Scope:

**In scope (this ADR / the `disk-resilience-scale` epic):**

- **C1a — intra-node pool drain.** Empty a data pool by relocating its sealed
  segments to sibling data pools on the *same* node (no cluster traffic,
  `storage_locations` untouched). Precondition: ≥ 2 eligible data pools on the
  node; the operator workflow is attach → drain → detach.
- **C1b — cluster drain (pool-only and node-level).** Empty a *source* (one
  data pool on a node with no viable sibling, a node's whole footprint, or an
  operator-selected subset) by moving copies to **other nodes** through the
  existing ADR-0030 target-pull path, then removing the source from
  `storage_locations` and unlinking its local `.dat`. Terminal, paced,
  pausable, operator-initiated. Pool-only retirement and whole-node
  retirement are both supported. This is the operation that runs *before*
  shutdown so `leave(None)` (unchanged) has nothing left to heal.
- **Detach.** The inverse of f8 `attach`: remove an empty pool from the live
  registry, drop it from config/topology, rebuild + re-gossip the manifest.
  No restart.
- **Drain-state plumbing.** A registry-level `Draining` state distinct from
  `Degraded`/`Dead`: placement excludes the pool, reads still serve from it,
  the health monitor does not fight the drain, and the pool reports a blocked
  reason when no target is eligible.

**Out of scope (this epic):**

- **C2b — proactive rebalance.** A continuous/periodic controller that moves
  *healthy* segments to chase a capacity-weighted ideal distribution. Moved to
  the backlog (`disk-resilience-capacity`, together with C2a below if C2a is
  deferred). Rationale: open-ended background data movement, safety analysis
  on steady-state churn, and no proven skew problem yet at fleet scale.
- **C3 — segment self-description.** Dropped as logically wrong after g8
  (metadata loss is repaired by peer row-rebuild; g7 + ADR-0035 already cover
  wal-loss registry/accounting recovery). No `.dat` v3 self-describing format.
- **Graceful-leave redesign.** Dropped. Streaming a node's data during
  shutdown is incompatible with large nodes; `leave(None)` + peer healing is
  the intended model. Node *retirement* is served by C1b *before* shutdown,
  not by changing the shutdown path. Spec §13.2 ("streams owned segment
  shards to successors") is stale and should be corrected in a follow-up spec
  pass (recorded; no code change to leave).

**Optional late-stage feature (difficulty to be measured when reached):**

- **C2a — capacity-derived ring ownership.** Make a node's ring share track
  its healthy data-pool capacity so attach/drain/detach re-weight the ring.
  Tentatively the last feature of the epic; may be deferred to the backlog
  `disk-resilience-capacity` epic if it proves disruptive to the
  membership/ring convergence path (ADR-0028 territory). Requires hysteresis
  and deterministic convergence across nodes.

### D2. Durable segment relocation = a `pool_id` payload on `MetadataRefreshEvent`

The existing metadata-refresh path (extended for `storage_locations` in
ADR-0030, `oceanfs-storage/src/segment/lifecycle.rs`) gains an optional
`pool_id` section, following the same discipline:

- One event family, one durable writer (event-WAL + checkpoint fold), length-
  discriminated backward-compatible decode.
- The fold updates `SegmentMetadata.pool_id`; the change is durable before any
  file is unlinked.
- Reader caches that memoize the resolved pool root per segment
  (`DiskSegmentReader::pool_id_cache`, f5) are purged on the mutation.

### D3. Relocation ordering = copy → commit → unlink (crash-safe)

Under the unified store's per-segment write lock (ADR-0032 D3):

1. Copy `.dat` to the target pool root (atomic write path).
2. Commit the `pool_id` refresh event (durable).
3. Unlink the source copy.

Both crash windows are safe: pre-commit leaves the source authoritative (the
target copy is boot-reapable residue); post-commit makes the target
authoritative and the source a harmless duplicate. Reads resolve by registry
`pool_id`, so the switch is atomic at the commit. GC/reaper interplay is
specified in the feature (bounded-metadata discipline: drain enumerates the
lifecycle registry, never a disk scan).

### D4. The C1b mover reuses ADR-0030; source-release is the new primitive

- The drain controller runs on the source node and iterates its held
  segments from the lifecycle registry (no disk scan).
- For each segment: select a cluster target with the existing capacity-aware
  selector (other nodes, healthy data pools, not `node_unavailable`), issue
  `RequestReReplication` — the target *pulls* and its own placement picks the
  pool (ADR-0030 decision 1 preserved).
- After the target confirms (its `storage_locations` stamp lands), the source
  performs **source-release**: refresh its own registry entry to the new
  holder set **minus self** (the locations payload is a set replacement, so
  self-removal is expressible), then unlink the local `.dat`.
- The controller is a `DurabilityTask` under the ADR-0017 scheduler's
  Tier-1 budget; Tier-0 repair/heal/hint work is never blocked by a drain.
- Source-release durability/reconciliation/GC consequences (event-WAL fold,
  reconciliation no longer counting the source as a live holder, residue
  handling) are worked out in the feature spec.

### D5. Configurable per-task byte budget — no generic framework yet

The drain worker is paced by `max_bytes_per_tick` in its task config, under
the same config family as the scheduler knobs. Every other byte-budgeted task
keeps its own configurable knob (explicitly **not** a shared byte-budget
abstraction in this epic; if a second consumer appears, the shared helper is
factored then). Drain runs under Tier-1; an operator may pause/resume.

### D6. Drain state machine (no-destructive-failure rule)

```
Idle ──(operator: POST drain)──▶ Draining ──(registry empty)──▶ Detachable
  ▲                                │
  └────────(pause/resume)──────────┘
       Draining + no eligible target = BLOCKED (metric + status reason)
```

- Placement excludes a `Draining` pool from new segment targets immediately.
- Reads keep serving from the pool until the last copy moves (read-while-
  draining).
- A pool whose drain cannot complete (no eligible sibling *and* no eligible
  cluster target — e.g., single-node cluster) stays `Draining` and reports
  `oceanfs_pool_drain_blocked_reason`. Nothing is deleted; nothing is
  half-removed. (With C1b in scope, "no target" is the honest residual, not
  the common case.)
- Detach is only accepted on an empty (`Detachable`) pool.

### D7. Topology/config language correction

ADR-0029 §D8's zero-config fallback and brainstorm §5-Q6's "migration is
non-urgent because no pools = today's behavior" language are **obsolete** per
ADR-0031 and are corrected by this ADR: Phase C exists to change topology on
live pool-configured nodes (attach → drain → detach; retire node/pool), not to
migrate from a legacy single-directory layout. Feature docs and the
`disk-resilience-pools.md` brainstorm carry the correction note.

### D8. Detach is persistent — a removed-pool overlay (`config − removed`)

Detach survives restart. The pool set at boot is `config − removed`, where
`removed` is a small node-local persistent record of detached pools keyed by
**name + root** (never pool id — ids are config-order and shift). This was
decided over the two alternatives at the 2026-09-07 session:

- **Rejected: ephemeral registry overlay (symmetric with f8 attach).** f8's
  attach is live-session-only unless the operator edits the config file, and
  an ephemeral detach would resurrect an empty pool on the next restart where
  placement would silently refill it — a footgun in exactly the
  disk-replacement workflow detach exists for. Config-file-rewrite was also
  rejected (rewriting user/fleet-managed `oceanfs.toml` is config magic).
- **Adopted:** a boot-consulted removed-pool marker written crash-safely
  (temp → fsync → rename) under the node's state directory, in the same
  family as g7's wal-pool replacement marker (ADR-0035 D4) and
  `membership_state.toml`. Reconciliation rules: a record suppresses a
  config-declared pool only while both name and root match; re-attaching the
  same name+root via the admin API clears the record; a stale record whose
  name/root no longer matches any config pool is inert. Boot applies
  `config − removed` **before** role/cardinality validation (ADR-0031's ≥1
  data pool check sees the post-overlay set), so a restart of a node that has
  drained + detached its **last** data pool refuses boot — the documented
  retirement sequence is detach → leave, never restart-fully-detached.

---

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **C1 only intra-node (drain to sibling pools)** | Smallest scope, zero cluster traffic, easy crash-safety | A node with one data pool could never drain a disk; cluster capacity as a whole stays stuck node-locally; pool-only retirement of the last data pool impossible | Rejected: C1b is the value (stakeholder decision 2026-09-07) |
| **Full C2 (weighted vnodes + proactive rebalance) in this epic** | Completes the brainstorm's "heterogeneous scale" story | Open-ended steady-state data movement; new skew controller; high review/ test burden right after a long refactor with no broad test run | Rejected: C2b backlog; C2a optional late-stage, difficulty measured at that point |
| **Node retirement via graceful-leave streaming (spec §13.2 model)** | Symmetric with old design docs | Terabytes streamed during shutdown; shutdown held open; contradicts the review's large-node conclusion | Rejected: leave stays `leave(None)`; retirement is C1b *before* shutdown |
| **Self-describing `.dat` for metadata-loss recovery (C3)** | Local rebuild without peer traffic | Cannot reconstruct inline objects or deletions/supersedes; g8 peer row-rebuild already covers metadata loss; g7 + ADR-0035 cover wal-loss accounting | Rejected: logically wrong after g8; dropped |
| **Drain by marking pool dead and letting g3/g4 heal** | Zero new machinery | Loss semantics (announcement, RF dip, possible storm), not operator-paved; no partial/shrink control; no pause; treats a healthy disk as dead | Rejected: drain is planned, paced, terminal — healing is the safety net, not the tool |

## Consequences

### Positive

- **Cluster capacity becomes mutable**: attach → grow, drain → shrink/replace,
  detach → remove, retire → release a node's footprint to the rest of the
  cluster.
- **Paced, observable retirement**: operators drain before shutdown;
  `leave(None)` stays trivial; no shutdown-path terabyte stream, no heal storm
  at stop time.
- **One mover**: C1b reuses the reviewed ADR-0030 target-pull machinery and
  the capacity-aware selector; no second data-movement path to audit.
- **One durable writer**: `pool_id` mutation extends the existing refresh
  event; the event-WAL/checkpoint discipline is unchanged.
- **No destructive failure**: blocked drains explain themselves; detach only
  on empty.
- **Detach is durable (D8)**: `config − removed` keeps a disk-replacement
  workflow honest across restart; no footgun of an empty pool being silently
  refilled; config file stays untouched.
- **Scope is deliberate**: the risky, open-ended pieces (C2b rebalance, C3
  self-description, graceful-leave redesign) are excluded with recorded
  rationale; the epic opens with a regression gate given the long gap since a
  broad test run — and the seven known pre-existing failures are fixed as a
  pre-epic (d0), not accepted as baseline (session decision 2026-09-07).

### Negative

- Drain of a large source is long-running; operators must plan capacity headroom
  on targets (or the drain blocks and reports).
- C1b's source-release changes `storage_locations` on live segments; its
  reconciliation/GC interaction needs careful feature-level analysis and tests.
- Without C2a, freed capacity is reclaimed only through new writes and
  repair-target selection, not by ring re-weighting — acceptable interim
  behavior, revisited when C2a is attempted.
- D8 adds a node-local persistent overlay (`removed` record) and its
  reconciliation rules — a second, bounded source of topology truth that boot
  must consult (mitigated: keyed by name+root, cleared on re-attach, g7-marker
  precedent).
- Backlog carries C2b and possibly C2a; the "heterogeneous scale pays off"
  story is not yet complete.
- d0 is real work before any Phase-C code (seven fixes), but it de-risks every
  later feature DoD.

### Neutral

- No wire/proto change for the intra-node half (`pool_id` is node-local; each
  holder keeps its own pool numbers). C1b adds an admin/control surface, not a
  data-plane protocol.
- Spec §13.2 and brainstorm Phase-C tables need correction notes (recorded,
  not code changes).

## References

- Brainstorm: `docs/brainstorm/disk-resilience-pools.md` (Phase C tables; rev.
  3 addendum records this ADR).
- ADR-0029 §D1/D8 (pool model; zero-config language now superseded by ADR-0031
  and corrected here), ADR-0030 (target-pull mover, D4 consequences),
  ADR-0031, ADR-0032 (unified store, per-segment locks), ADR-0033,
  ADR-0034 (bounded scans; drain enumerates registry, not disk),
  ADR-0017 (scheduler/Tier budgets), ADR-0035 (replicated lifecycle state).
- Feature deferrals that named "Phase C": `runtime-attach` (detach/drain),
  `data-pool-placement` (pool_id immutability), `placement-policy` (ring-level
  weighting), `routing-manifests` (pool-granular write routing),
  `wal-loss-recovery` and `metadata-loss-recovery` (drain / C3 out-of-scope
  notes), ADR-0030 Decision 4 (migration-plane isolation — remains a recorded
  future consequence).
