---
feature: "Cluster Drain (C1b — Off-Node Mover + Source-Release)"
epic: "disk-resilience-scale"
status: proposed
priority: high
owner: ""
dependencies: ["d2-segment-relocation", "d3-intra-node-drain"]
adr: [0036, 0017, 0030, 0033, 0034, 0035]
perf: []
created: 2026-09-07
updated: 2026-09-07
---

# Cluster Drain (C1b — Off-Node Mover + Source-Release)

## Summary

The value feature of the epic (ADR-0036 C1b): a paced, pausable, terminal
**controller** that empties a **source** — one data pool with no viable
sibling, an operator-selected pool set, or a node's whole footprint — by
moving copies to **other nodes** through the **existing ADR-0030
target-pull machinery**, then **source-releasing** the local copy. Per held
segment (registry-enumerated, ADR-0034): select a cluster target with the
existing `ManifestRepairTargetSelector` (capacity-aware, healthy data
pools, not `node_unavailable`), issue the existing `RequestReReplication`
RPC — the **target pulls**, and its own placement picks the pool. When the
target's stamp lands, the source refreshes its own registry entry to the
holder set **minus self** (the `storage_locations` payload is a set
replacement, so self-removal is expressible) and unlinks the local `.dat`.
This is the new **source-release** primitive; reconciliation (g4) counts
holders from `storage_locations`, so a released source stops being counted
immediately after the durable refresh and no heal loop re-adds it. The
controller is a Tier-1 task (paced, pausable, terminal at empty), supports
pool-only retirement even when the node has a single data pool, and is the
documented pre-step before node `leave(None)`. ADR-0036 D4 concentrates the
epic's risk here: source-release's durability / reconciliation / GC story.

## Scope

### In Scope

- **Drain controller (source-node side; `oceanfs-node` + reuses the
  durability repair surface).**
  - Enumerates the source's held segments from the **lifecycle registry**
    (`SegmentLifecycleRegistry::for_each`,
    `crates/oceanfs-storage/src/segment/lifecycle.rs:793`), filtered to
    entries whose holder set includes self and whose local `pool_id` is in
    the drain source set. Never a disk scan (ADR-0034).
  - Drain-source selection:
    - **pool-only**: one `Draining` data pool with no viable sibling —
      allowed even when it is the node's **only** data pool (the mover is
      off-node; ADR-0036 D1 / epic admin surface);
    - **node-level**: the node's whole footprint (all data pools), the
      documented pre-`leave(None)` retirement step.
  - Runs as a `DurabilityTask` under the ADR-0017 scheduler's **Tier-1**
    budget (`crates/oceanfs-durability/src/scheduler/budget.rs:72`;
    Tier-0 repair/heal/hint work is never blocked by a drain — ADR-0036
    D4). Paced by its own configurable `max_bytes_per_tick` (ADR-0036 D5 —
    per-task knob, no shared budget abstraction). Pausable via
    `pause`/`resume`; terminal when the source holds zero segments (or the
    node-level watermark — see Open Questions).
  - **Node keeps serving reads until empty.** During a node-level drain the
    node's data pools are `Draining`, so local placement cannot create new
    segments; reads of still-held segments continue from the local `.dat`
    until each is released. The node stops being a *write* target once its
    manifest shows no healthy data pool (existing manifest-aware peer
    selection — ADR-0033); the drain must not require new writes to
    complete. (Local writes for ranges the draining node still owns will
    fail placement / 503 once no healthy data pool remains — encode this
    expected behavior, see Open Questions.)
- **Target selection: reuse ADR-0030/ADR-0033, no new selector.**
  - For each held segment the controller dispatches through the same
    shape as the g5 repair dispatcher
    (`crates/oceanfs-node/src/repair.rs` — `ManifestRepairTargetSelector`
    at `:73`, its `pick_repair_target` impl at `:85-130`; the dispatcher
    filters live holders, excludes self/holders, requires ≥1 healthy
    non-write-degraded data pool on the target, prefers most free
    capacity). Because the selector already excludes self and the existing
    holders, passing `holders = [source]` yields a cluster target that does
    not currently hold the segment.
  - Issue the existing **`RequestReReplication` RPC**
    (proto `oceanfs.healing`; handler
    `crates/oceanfs-durability/src/healing_service.rs:1648`; the target's
    `ReRepWorker` at `crates/oceanfs-durability/src/repair.rs:156` pulls
    the full segment from a live holder and writes through its own
    pool-aware store — its placement picks the pool, ADR-0030 decision 1
    preserved).
  - **Reason field**: add `RepairReason::Drain` (a new variant alongside
    `Announcement`/`Reconciliation`, `crates/oceanfs-durability/src/repair.rs`
    / the proto reason) so drain dispatches are distinguishable in
    metrics/logs and so a target can apply drain-appropriate pacing.
    `<!-- TODO(spec): verify anchor -->` confirm whether the existing
    `RequestReReplicationRequest.reason` wire enum suffices with an added
    `Drain` value (a small proto change) or whether the epic's noted
    "drain RPC" is actually required — the sketch's open question: does the
    controller need to tell a target "pull this for a drain" vs. repair?
    Recommend the reason field only.
- **Source-release (the new primitive — ADR-0036 D4; the epic's risk
  center).**
  - Completion signal: the target confirms when its own
    `storage_locations` stamp lands and the source-side converge appends
    the target to the source's holder view (the g5 holder-side handoff,
    `converge_holder_registry`, `crates/oceanfs-node/src/repair.rs` ~`:530`;
    the source then sees the target in its own entry's holder set).
  - Source-release step, per segment, **after the target copy is durable**:
    1. `request_refresh_metadata(id, None, Some(holders − self), None)` —
      the refresh payload is a **set replacement**, so self-removal is
      expressible (`request_refresh_metadata`,
      `crates/oceanfs-storage/src/segment/lifecycle.rs:2100`;
      `MetadataRefreshEvent.storage_locations`,
      `segment/event_wal.rs:277`; durable event-WAL fold, ADR-0025);
    2. unlink the local `.dat` (source pool root).
  - **Reconciliation interaction**: g4's `HolderIndex` counts live holders
    from `storage_locations` (`crates/oceanfs-durability/src/reconcile.rs:185`),
    so after the durable refresh the source no longer counts itself; no
    heal loop re-adds it. Peers whose cached views still list the source
    are harmless: reads fail over on error (the stale-cache error path,
    ADR-0029 §D5) and membership removal after `leave` drops the stale
    holder from live-count filters.
  - **GC/residue**: after source-release the source holds no file and no
    registry claim; the target copy is a normally-registered holder. A
    crash mid-source-release (refresh durable, unlink pending) leaves the
    source's `.dat` as an unregistered file → boot-reapable residue (same
    rule as d2's post-commit window). A crash after dispatch but before the
    target lands leaves the source still holding (its registry unchanged) —
    the drain re-runs idempotently on restart.
  - **No-destructive-failure (ADR-0036 D6)**: no eligible target anywhere
    (e.g. single-node cluster, or all other nodes lack healthy data-pool
    capacity) ⇒ controller parks, `set_drain_blocked("no eligible cluster
    target")` (d1), pool stays `Draining`, **nothing is deleted**. With C1b
    in scope this is the honest residual case, not the common one.
- **Watermark / terminality.**
  - Pool-only drain: terminal when the registry holds no entry with
    `pool_id == source` → `set_pool_empty` (d1) → `Detachable` (d5).
  - Node-level drain: terminal when the node holds no segments (see Open
    Questions for the "empty vs. leave-N-replicas-for-owned-ranges"
    alternative). Afterwards `leave(None)` is a no-op drain (epic DoD).
- **Boot/recovery interplay (mid-drain restart).**
  - Relocations are crash-safe and idempotent (d2); source-release is
    crash-safe (windows above). The registry fold after restart reflects
    completed relocations/releases; in-flight ones re-run.
  - The operator `Draining` intent itself is **runtime registry state,
    rebuilt from config at boot** (pools boot Healthy unless the health
    monitor says otherwise; f8 attach is ephemeral too). Recommendation to
    record: a mid-drain restart drops the operator flag; the operator
    re-issues the drain; no drain-intent persistence is added in this epic
    (the g7/g8 recovery paths rebuild *segment* state, not operator pool
    intent — ADR-0035). See Open Questions.
  - A node that restarts mid-drain still owns the *source* segments it
    held at the crash; its registry fold recovers them and the re-issued
    drain re-runs them.
- **Admin surface (finalized here for C1b; d5 finalizes detach).**
  - `POST /admin/pools/{id}/drain` with target mode `cluster` (pool-only
    retirement — valid even with one data pool);
  - `POST /admin/nodes/{node}/drain` (or `POST /admin/nodes/{node}/retire`)
    — node-level drain-to-empty, the pre-`leave` step; the node keeps
    serving reads until empty;
  - `POST /admin/pools/{id}/drain/pause` · `/resume` (pacing control);
  - status: per-source progress (segments/bytes remaining, released count),
    `Draining`/`Detachable`/`Draining(blocked: reason)`, metrics
    `oceanfs_pool_drain_*` (d1) plus drain-throughput counters.
- Tests:
  - unit: source-set selection (pool-only, node-level; single-data-pool
    pool-only allowed);
  - unit: target selection reuses the selector — excludes self/holders,
    excludes nodes without healthy data pools (draining-target exclusion
    via manifest status), prefers capacity;
  - unit: source-release — a registry entry refresh to `holders − self`
    round-trips durably and g4's holder-count logic stops counting self
    immediately (unit against `HolderIndex`);
  - unit: no-destructive-failure — no eligible target anywhere ⇒ parked +
    blocked reason + pool stays `Draining` + zero deletes;
  - unit: crash windows for source-release (refresh-durable/unlink-pending
    residue reaped; dispatch-pending re-run idempotent);
  - integration (local 3-node, RF=2): PUT data → drain node A's only data
    pool (cluster mode) under live read load → copies land on B/C
    (capacity-aware), A source-releases (its registry `storage_locations`
    drops self; local `.dat` gone), reconciliation never re-adds A, reads
    of every key succeed from B/C, A's pool becomes `Detachable`, `leave`
    afterwards is a no-op — zero data loss;
  - integration (blocked variant): single-node cluster (or all targets
    full) drain request ⇒ pool stays `Draining`, blocked reason surfaced,
    nothing deleted;
  - integration (restart variant): kill A mid-drain → restart → re-issue →
    drain completes idempotently (relocated segments skipped, remaining
    ones moved).

### Out of Scope (for this feature)

- Proactive rebalance (C2b) and ring re-weighting (d6/C2a) — freed
  capacity is reclaimed via new writes + repair targeting until C2a; the
  ring-share consequence of shrinking a node is documented **expected
  interim behavior** (ADR-0036 Negative), not implemented here.
- Graceful-leave redesign / shutdown streaming — leave stays `leave(None)`;
  C1b runs *before* it (epic non-goal, ADR-0036 D1).
- Migration-plane isolation (ADR-0030 D4) — recorded future consequence.
- Intra-node drain (d3/C1a) — the sibling-pool mover; d4 reuses its mover
  *pattern* but is the off-node controller.
- The durable relocation primitive (d2) and drain-state plumbing (d1) —
  consumed.
- Segment self-description (C3), fleet/load-test phase-4 scenarios
  (harness epic) — out of scope.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | Drain controller (pool-only + node-level); admin drain routes (mode cluster + node retire + pause/resume); source-release orchestration over the lifecycle refresh + store unlink; selector/dispatcher reuse wiring |
| `oceanfs-durability` | `RepairReason::Drain` (+ proto reason value) if the reason-field route is chosen; any drain pacing/backpressure hook into the ReRepWorker bounded queue |
| `oceanfs-storage` | Source-release refresh call (`request_refresh_metadata` holder-set replacement — already expressible, no new event); residue/boot-sweep interplay tests; (no new primitive beyond d2) |
| `oceanfs-server` | Admin HTTP surface for the C1b verbs |

## Interface (Public API)

- Drain controller (node-side): `run_drain_cycle(source: DrainSource,
  …) -> DrainCycleStats { dispatched, released, blocked_reason, remaining }`
  wrapped in a `DurabilityTask` adaptor (name `"drain_cluster"`) — same
  seam decision as d3.
- `pub enum DrainSource { Pool(u32), Node, PoolSet(SmallVec<[u32; N]>) }`
  — pool-only, node-level, operator-selected subset.
- `request_refresh_metadata(…, storage_locations: Some(holders − self), …)`
  — source-release is an **ordinary holder-set refresh**; the doc's default
  answer to the sketch's open question is that no distinct event/reason is
  needed for the refresh itself (the *dispatch* reason gets
  `RepairReason::Drain` for observability). `<!-- TODO(spec): verify
  anchor -->` confirm no coordinator validation rejects a locations set
  that drops self.
- New metrics: `oceanfs_drain_dispatched_total`,
  `oceanfs_drain_released_total`, `oceanfs_drain_remaining{source}`,
  reusing `oceanfs_pool_drain_blocked_reason` (d1).
- Admin routes listed in Scope (final verb wording for `drain`/`retire`
  and the pause/resume paths lands here; d5 adds `detach`).

## Data Flow

```
operator ──▶ POST /admin/nodes/{node}/drain   (or pool {id}/drain mode=cluster)
   └─ mark source pools Draining (d1) ──▶ manifest (no healthy data pool) ──▶ peers route writes elsewhere
   └─ controller (Tier-1 DurabilityTask, own max_bytes_per_tick):
        enumerate lifecycle registry → held segments (pool_id ∈ source set)
        per segment (within budget):
          target = ManifestRepairTargetSelector.pick_repair_target(id, holders=[self])
          ├─ None ──▶ set_drain_blocked("no eligible cluster target") → park; nothing deleted
          └─ Some(T) ──▶ RequestReReplication{reason: Drain} ──▶ T's ReRepWorker
               ├─ pull full segment from a live holder (holders − self)
               ├─ write via T's pool-aware store (T's placement picks the pool)
               ├─ register reserve+seal in T's lifecycle
               └─ stamp T into storage_locations
               └─ source-side converge appends T to source's holder view (g5 handoff)
        source-release (T durable):
          request_refresh_metadata(storage_locations = holders − self)  ← set replacement, durable
          unlink local .dat (source pool root)
        g4 reconcile: HolderIndex counts from storage_locations → released source not counted → no re-add
        pool empty? ──▶ set_pool_empty → Detachable ──▶ d5 detach
        node empty? ──▶ done ──▶ leave(None) is a no-op drain
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-node`,
      `oceanfs-durability`, `oceanfs-storage`, `oceanfs-server` (+ proto
      regen if the reason field changes).
- [ ] **Tests:** `cargo test -p oceanfs-node -p oceanfs-durability -p
      oceanfs-storage -p oceanfs-server --lib -- --test-threads=1` passes;
      the Scope scenario list is green — including: pool-only retirement of
      a node's **single** data pool; node-level drain to capacity-aware
      targets under live reads; **source-release** (self dropped from the
      source's `storage_locations`, local `.dat` gone, g4 never re-adds);
      **no-destructive-failure** (no eligible target anywhere ⇒ pool stays
      `Draining`, blocked reason surfaced, nothing deleted); crash-window
      restarts mid-drain (idempotent re-run, no orphaned-then-lost copy);
      and the 3-node RF=2 integration scenario ending in a no-op
      `leave(None)`.
- [ ] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the interim ring-share behavior (no C2a re-weighting) is
      documented on the controller.
- [ ] **ADR:** ADR-0036 C1b + D4 satisfied (off-node mover reuses
      ADR-0030; source-release durability/reconciliation/GC consequences
      worked out and tested; Tier-1 pacing; Tier-0 never blocked), D5
      (configurable `max_bytes_per_tick`, no shared framework), D6
      (blocked ⇒ stays Draining + reason; nothing deleted) satisfied;
      ADR-0030 (target-pull preserved), ADR-0033 (manifest-aware target
      selection), ADR-0034 (registry enumeration only), ADR-0017
      (scheduled Tier-1 task), ADR-0035 (boot recovery rebuilds segment
      state; mid-drain restart handled per the recorded rule) satisfied.
- [ ] **Perf:** frontmatter `perf: []`; prose constraints: enumeration is
      a registry `for_each`, never a disk scan; the byte budget bounds
      per-tick dispatch; target backpressure reuses the ReRepWorker's
      bounded queue (`tokio::sync::mpsc`, perf rule 2.6) + semaphore (perf
      rules 2.7/8.5) — a drain must not bypass the queue that bounds
      re-replication; no unbounded fan-out when many segments drain at once
      (see Open Questions).
- [ ] **Integration:** integration test at the cluster boundary exercises
      the full pool-only + node-level drain → source-release → detach /
      no-op-leave workflow with zero data loss and continuous reads. **No
      load suite is run locally** (PIPELINE §6).

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **Distinct event/reason for source-release.** The sketch asks whether
  source-release needs a distinct event reason or is an ordinary
  holder-set refresh. Default in this doc: the refresh is ordinary (set
  replacement already expresses self-removal; d2 keeps one event family);
  the *dispatch* gains `RepairReason::Drain` for observability. Confirm no
  coordinator validation rejects a holder-set refresh that drops self.
- **Target backpressure.** Many segments drain at once: the controller
  must respect the ReRepWorker's bounded queue + semaphore (perf 2.6/2.7,
  8.5) and not outpace the cluster's repair budget; drain pacing =
  min(own `max_bytes_per_tick`, target queue capacity). Verify the
  dispatcher's existing pending/park sweep absorbs drain overcapacity the
  same way it absorbs repair overcapacity.
- **Node-drain watermark.** Empty vs. "leave N replicas for still-owned
  ranges": with C2a out of scope the ring still maps ranges to the
  draining node until `leave`. This doc's default is **drain to empty then
  `leave(None)`** (the epic's DoD wording), with the node kept alive and
  read-serving until empty; confirm the write-routing consequence for the
  draining node's still-owned ranges (see next question) and record.
- **Write behavior during node-level drain.** A retiring node whose last
  data pool is `Draining` cannot durably store new local segments; peers
  already route around it (no healthy data pool in its manifest). But
  writes for ring ranges it still owns will hit its coordinator and fail
  local placement / 503 once no healthy pool remains. Decide and record
  whether this is acceptable interim behavior (operator drains off-peak,
  before `leave`) or the drain must also drop/transfer the node's ring
  ownership (that is d6/C2a territory — likely out of scope).
- **Drain-intent persistence across restart.** Pool `Draining` is runtime
  registry state rebuilt from config at boot; a mid-drain restart drops the
  operator flag and the operator must re-issue the drain. Confirm this
  ephemeral-intent rule (consistent with f8 attach being ephemeral) vs.
  persisting a drain marker; g7/g8 (ADR-0035) rebuild segment state, not
  operator intent.
- **Metadata/read failover after release.** The retiring node's metadata
  (objects CF) still maps released objects → segments it no longer holds;
  reads that land on it must fail over to the remaining holders via the
  existing stale-cache/error path. Verify the read path uses the
  released holder set (`storage_locations − self`) and fails over rather
  than erroring on the stale local row.

## Deviations (accepted)

None yet — this document is proposed. Expected-deviation candidates (each
recorded with its resolution at implementation): the source-release reason
answer, the RepairReason::Drain proto value, the node-drain watermark and
write-routing rule, and the drain-intent-persistence rule. The interim
ring-share consequence (no C2a) is an accepted, documented behavior, not a
deviation.
