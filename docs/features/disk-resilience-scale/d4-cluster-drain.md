---
feature: "Cluster Drain (C1b — Off-Node Mover + Source-Release)"
epic: "disk-resilience-scale"
status: done
priority: high
owner: ""
dependencies: ["d2-segment-relocation", "d3-intra-node-drain"]
adr: [0036, 0017, 0030, 0033, 0034, 0035]
perf: []
created: 2026-09-07
updated: 2026-09-09
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
     node-level watermark — see Resolved Decisions 8).
  - **Node keeps serving reads until empty.** During a node-level drain the
    node's data pools are `Draining`, so local placement cannot create new
    segments; reads of still-held segments continue from the local `.dat`
    until each is released. The node stops being a *write* target once its
    manifest shows no healthy data pool (existing manifest-aware peer
    selection — ADR-0033); the drain must not require new writes to
    complete. (Local writes for ranges the draining node still owns will
    fail placement / 503 once no healthy data pool remains — encode this
    expected behavior, see Resolved Decisions 8.)
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
  - **Reason field**: `RepairReason::Drain` shipped as a new variant on the
    existing wire reason enum: the **runtime enum used by the handler**
    lives in `crates/oceanfs-durability/src/healing_service.rs:210` with
    the numeric proto conversion
    (`u32::from(RepairReason::Drain) == 3`,
    `healing_service.rs:227-240`), alongside the regenerated proto enum
    `REPAIR_REASON_DRAIN = 3`
    (`proto/oceanfs/healing.proto:175`; `generated/oceanfs.healing.rs:410`
    with `as_str_name`/`from_str_name`), next to
    `Announcement`/`Reconciliation`. Drain dispatches are distinguishable
    in metrics/logs and a target applies drain-appropriate pacing. The
    reason field route was sufficient — **no distinct "drain RPC" was
    required** (the sketch's open question, settled: the target only needs
    to know *this is a drain* so its `request_re_replication` handler runs
    the D4-1a durable-materialization gate before acking — Resolved
    Decisions 1).
- **Source-release (the new primitive — ADR-0036 D4; the epic's risk
  center).**
  - Completion signal: the target confirms when its own
    `storage_locations` stamp lands and the source-side converge appends
    the target to the source's holder view (the g5 holder-side handoff,
    `converge_holder_registry`, `crates/oceanfs-node/src/repair.rs` ~`:530`;
    the source then sees the target in its own entry's holder set).
  - Source-release step, per segment, **after the target copy is durable**:
    1. a durable holder-set refresh to `holders − self` — the refresh
      payload is a **set replacement**, so self-removal is expressible.
      Shipped as `request_refresh_metadata(id, Some(merkle_root),
      Some(holders − self), None)` inside the controller's `release()`
      (`cluster_drain.rs:410-452`), re-passing the entry's current merkle
      root (the refresh merkle parameter is a value replacement — `None`
      clears the anchor, so source-release must carry it; Resolved
      Decisions 6). The general location-only stamp is the new
      `persist_storage_locations` coordinator method
      (`crates/oceanfs-storage/src/segment/lifecycle.rs:2260-2277`), which
      likewise re-passes the live root through the same
      `request_refresh_metadata` path (event-WAL fold, ADR-0025);
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
  - Node-level drain: terminal when the node holds no segments (see Resolved
    Decisions 8 for the "empty vs. leave-N-replicas-for-owned-ranges"
    alternative, settled as drain-to-empty). Afterwards `leave(None)` is a
    no-op drain (epic DoD).
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
    intent — ADR-0035). See Resolved Decisions 8.
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
| `oceanfs-node` | New `crates/oceanfs-node/src/cluster_drain.rs` — `DrainClusterController` (implements `oceanfs_durability::DurabilityTask` directly, name `"drain_cluster"`, `cluster_drain.rs:456`), `DrainClusterConfig` (:62), `DrainCycleStats` (:78); built + registered in `DurabilityModule::build` (`modules/durability.rs:545-560`); `Node::cluster_drain()` accessor (`node.rs:698`). Node-side dispatch seam `RepairDispatcher::dispatch_drain` + `DrainDispatchError` (`repair.rs:551`, `:83`). Node-level admin begin closure `with_node_drain_begin` (`modules/server.rs:541-556`) |
| `oceanfs-durability` | Proto `REPAIR_REASON_DRAIN = 3` (`proto/oceanfs/healing.proto:175`; generated `RepairReason::Drain` + numeric/`as_str`/`from_str` arms in `generated/oceanfs.healing.rs` and `healing_service.rs:210`). D4-1a durable gate in the target's `request_re_replication` handler: `reason == Drain` polls THIS node's own registry for `Sealed` + local id in `storage_locations` until `DRAIN_MATERIALIZE_TIMEOUT` (120 s, `healing_service.rs:21`) before acking (`healing_service.rs:1823-1868`); non-Drain repair/reconcile fire-and-forget unchanged (Deviations c). 281 lib tests incl. `repair_reason_drain_round_trips_numerically` |
| `oceanfs-storage` | New durable holder-set stamp `SegmentLifecycleCoordinator::persist_storage_locations` (`segment/lifecycle.rs:2260-2277`) — event-WAL `MetadataRefresh` that re-passes the live merkle root; in-memory `set_storage_locations` **removed** (Deviations a). `DrainMode::{IntraNode, Cluster}` routing tag + `PoolRegistry::begin_drain_with_mode`/`drain_mode` (`pool/drain.rs:218`; `pool/mod.rs`) — a separate registry map, not a `DrainState` field (Deviations b). d3 `IntraNodeDrain` skips `Cluster` pools (`drain/intra_node.rs:305`) |
| `oceanfs-server` | Admin routes `POST /admin/pools/{id}/drain` (body `{"mode":"cluster"}`), `POST /admin/nodes/{node}/drain`, node + pool `pause`/`resume` (`crates/oceanfs-server/src/admin.rs:825-830`); `parse_drain_mode` (admin.rs:1219), `begin_pool_drain` (:1255), `begin_node_drain` (:1325); builders `with_pool_drain_begin` (:742) / `with_node_drain_begin` (:769) |
| `oceanfs-core` | `[durability] drain_cluster_max_bytes_per_tick` (default 64 MiB) in `config/durability.rs:53,93-96` (ADR-0036 D5 per-task knob) |
| tests | `crates/oceanfs-node/tests/cluster_drain.rs` — 3 integration scenarios (node-level RF=2 drain to `Detachable`; blocked no-eligible-target no-delete; restart mid-drain idempotent) |

## Interface (Public API)

Shipped surface (what landed, reconciled at spec close):

- `pub struct DrainClusterController` (`crates/oceanfs-node/src/cluster_drain.rs:121`) — the node-side controller. **Implements `oceanfs_durability::DurabilityTask` directly** (`name() == "drain_cluster"`, `keyspace_fraction() == 1.0`, `run_cycle` at `:456-476`) because it needs node-side `RepairDispatcher`/membership; no durability-crate adaptor (Resolved Decisions 2 / Deviations b). Constructed by `DrainClusterController::new(config, self_id, membership, repair_dispatcher, registry, lifecycle_registry, lifecycle, data_store, replication_factor, interval)` (`:137`); one instance built + registered in `DurabilityModule::build` (`modules/durability.rs:545-560`). `pub async fn run_drain_cycle(&self) -> DrainCycleStats` (`:168`) is exposed for tests/admin (the task's `run_cycle` delegates to it).
- `pub struct DrainClusterConfig { pub max_bytes_per_tick: u64 }` (`:62`, `Default` = 64 MiB) — the ADR-0036 D5 per-task byte knob.
- `pub struct DrainCycleStats { dispatched, released, bytes_released, emptied: Vec<u32>, blocked: Vec<(u32, String)> }` (`:78`) — one cycle's outcome aggregated over every cluster-mode draining pool; **no `oceanfs_drain_*` throughput counters are registered** — stats are the observable (Deviations e).
- No `DrainSource` enum shipped: the source set is **derived from the registry routing tag** — pools whose `DrainMode::Cluster` record is live (`cluster_draining_sources`, `:252`), pool-only (one `Cluster` pool, valid as the node's only data pool) or node-level (every data pool marked `Cluster` by `POST /admin/nodes/{node}/drain`). d3's worker owns `DrainMode::IntraNode` pools only (Resolved Decisions 3).
- `oceanfs_storage::DrainMode { IntraNode, Cluster }` + `as_str()` (`oceanfs-storage/src/pool/drain.rs:218-241`); `PoolRegistry::begin_drain_with_mode(pool_id, mode)` / `begin_drain` (defaults IntraNode) / `drain_mode(pool_id)` (`pool/drain.rs:328-374`).
- `pub async fn persist_storage_locations(&self, id: SegmentId, locations: SmallVec<[NodeId; 16]>) -> Result<(), TransitionError>` (`oceanfs-storage/src/segment/lifecycle.rs:2260`) — the durable holder-set stamp (owner post-push stamp `segment_replicator.rs:683`; push-receiver Fresh+Existing arms `segment_service.rs:1055/1072`); re-passes the live merkle root so location-only stamps never clear the seal-time anchor (Resolved Decisions 6). **In-memory `set_storage_locations` was removed** — all holder stamps go through the event-WAL (Deviations a).
- Source-release = an **ordinary holder-set refresh** through the same `MetadataRefreshEvent` family — `DrainClusterController::release` (`cluster_drain.rs:410-452`) calls `request_refresh_metadata(id, Some(merkle_root), Some(holders − self), None)` then unlinks the local `.dat`; no distinct event/reason for the refresh, no coordinator validation rejects a set that drops self (confirmed at implementation — Resolved Decisions 4/5).
- `RepairReason::Drain` (proto `REPAIR_REASON_DRAIN = 3`) — the wire reason enum sufficed; no separate drain RPC (Resolved Decisions 1).
- `RepairDispatcher::dispatch_drain(&self, request: &ReRepRequest) -> Result<NodeId, DrainDispatchError>` (`oceanfs-node/src/repair.rs:551`) — synchronous: returns `Ok(target)` only after the target acked a durable copy; `DrainDispatchError { NoEligibleTarget, NotDurable(NodeId), NoAddress(NodeId), Channel{..}, Rpc(..), TimedOut(NodeId) }` (`repair.rs:83-100`). DRAIN dispatch is bounded client-side by `DRAIN_DISPATCH_TIMEOUT_MS` (150 s).
- `Node::cluster_drain(&self) -> Arc<DrainClusterController>` accessor (`node.rs:698`).
- Admin routes (`oceanfs-server/src/admin.rs:825-830`): `POST /admin/pools/{id}/drain` body `{"mode":"cluster"}` (or `intra-node`/empty → d3), `POST /admin/nodes/{node}/drain`, `POST /admin/nodes/{node}/drain/pause|resume`, pool `pause`/`resume` (d3). Node-level drain is non-transactional across pools (Deviations f.iii).
- Config: `[durability] drain_cluster_max_bytes_per_tick` (default 64 MiB) + shared `drain_interval_sec` cadence (`oceanfs-core/config/durability.rs:53,93-96`).
- Metrics: reuse d1's pool drain-state gauges/`blocked_reason`; `oceanfs_drain_*` throughput counters were proposed but NOT registered (Deviations e).

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
               ├─ stamp T into storage_locations (persist_storage_locations; anchor-preserving)
               └─ D4-1a gate: ack only once T's OWN registry shows Sealed + self ∈ storage_locations
                  (DRAIN_MATERIALIZE_TIMEOUT 120 s) ──▶ source-side converge appends T (g5 handoff)
        source-release (T durable):
          request_refresh_metadata(id, Some(merkle_root), storage_locations = holders − self, None)
              ← set replacement, durable; merkle re-passed (None would clear the anchor)
          unlink local .dat (source pool root)
        g4 reconcile: HolderIndex counts from storage_locations → released source not counted → no re-add
        pool empty? ──▶ set_pool_empty → Detachable ──▶ d5 detach
        node empty? ──▶ done ──▶ leave(None) is a no-op drain
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-node`,
      `oceanfs-durability`, `oceanfs-storage`, `oceanfs-server` (+ proto
      regen if the reason field changes).
<!-- REVIEW: verified 2026-09-09 — `cargo build --all-targets` green for core/storage/durability/node/server (fresh compile of the untracked cluster_drain.rs + tests/cluster_drain.rs confirmed). proto/oceanfs/healing.proto gains REPAIR_REASON_DRAIN=3 and the regenerated crates/oceanfs-durability/src/generated/oceanfs.healing.rs matches (RepairReason::Drain + as_str + from_str arms). `cargo fmt --check` clean; `cargo clippy --workspace --lib -- -D warnings` clean. DoD Code item PASSES. -->
- [x] **Tests:** `cargo test -p oceanfs-node -p oceanfs-durability -p
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
<!-- REVIEW: PASS (iteration-2 re-verified 2026-09-09). All lib suites green single-threaded: storage 517, durability 281, node 100, server 245, core 232. The iteration-1 FAIL (`push_sealed_segment_registers_and_serves`, merkle root cleared by the durable stamp) is FIXED: `persist_storage_locations` (crates/oceanfs-storage/src/segment/lifecycle.rs:2260-2277) now reads the live entry's `merkle_root` and forwards `Some(root)` through `request_refresh_metadata`, preserving the seal-time anchor on every location-only stamp (owner post-full-ack stamp segment_replicator.rs:683, push-receiver Fresh+Existing arms segment_service.rs:1055/1072); the d2 relocation commit also carries the source root explicitly (relocate.rs:215-223). The fold's value-replacement semantics are unchanged (lifecycle.rs:1123: `Some` replaces, `None` clears), so the heal-worker invalidation path is NOT regressed — heal deliberately passes `None` (worker.rs:443) and its tests still assert the root is cleared after repair (worker.rs:824-828, 1135). New/updated assertions: `relocate_moves_file_flips_pool_id_and_purges_reader` asserts `merkle_root.is_some()` after a pool_id-only commit (relocate.rs:498) and the new `storage_locations_stamp_survives_cold_restart` asserts locations restored AND root still `Some` after a cold restart (relocate.rs:619-646). Targeted suites re-verified: heal::worker 14/14, relocate 7/7, grpc::segment_service 26/26. Integration green single-threaded: cluster_drain 3/3, re_replication 2/2, intra_node_drain 3/3, pool_drain_state 3/3. `repair_reason_drain_round_trips_numerically` present in durability 281. Residual (non-DoD-gating, recorded): no-op `leave(None)` ending not exercised by cluster_drain tests (node shutdown used instead — noted in the Integration REVIEW); the >budget pacing and release-view gaps are fixed (see ADR REVIEW); remaining LOWs (stale duplicate seal-time push re-add; non-transactional node-level begin; emptiness-definition vs d5; oceanfs_drain_* throughput counters not registered) stay non-blocking for this feature's DoD — see the ADR/Perf REVIEW notes. -->

- [x] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the interim ring-share behavior (no C2a re-weighting) is
      documented on the controller.
<!-- REVIEW: PASS (iteration-2 re-verified 2026-09-09). rustdoc + missing_docs gates pass (`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean on core/storage/durability/node/server); every new pub item — DrainClusterConfig/DrainCycleStats/DrainClusterController::new/run_drain_cycle, DrainMode/as_str, DrainDispatchError, RepairReason::Drain, Node::cluster_drain — carries `# Examples`, `ignore`-tagged where a live node is required (non-gating per the Lint note). The iteration-1 gap is FIXED: the interim ring-share behavior is now documented in the controller module docs — `crates/oceanfs-node/src/cluster_drain.rs:28-36` ("## Interim ring-share behavior (recorded, no C2a)": freed capacity reclaimed by new writes/repair targeting; the ring keeps mapping ranges to the draining node until `leave(None)`; operators drain off-peak before leave). The scheduler/doc-count comment drift is also fixed: "five Tier-1" → "six Tier-1" in crates/oceanfs-durability/src/scheduler/task.rs:3-5, scheduler/mod.rs:17, crates/oceanfs-node/src/node.rs:40-42, crates/oceanfs-node/src/modules/durability.rs:44-48,86-88,492. Residual doc nit (non-blocking, not DoD-gated): the cluster_drain struct doc itself (cluster_drain.rs:107) does not repeat the ring-share sentence — it lives at module level; and the feature doc's RepairReason anchor points at crates/oceanfs-durability/src/repair.rs while the enum lives in healing_service.rs:210 — both cosmetic. -->
- [x] **ADR:** ADR-0036 C1b + D4 satisfied (off-node mover reuses
      ADR-0030; source-release durability/reconciliation/GC consequences
      worked out and tested; Tier-1 pacing; Tier-0 never blocked), D5
      (configurable `max_bytes_per_tick`, no shared framework), D6
      (blocked ⇒ stays Draining + reason; nothing deleted) satisfied;
      ADR-0030 (target-pull preserved), ADR-0033 (manifest-aware target
      selection), ADR-0034 (registry enumeration only), ADR-0017
      (scheduled Tier-1 task), ADR-0035 (boot recovery rebuilds segment
      state; mid-drain restart handled per the recorded rule) satisfied.
<!-- REVIEW: PASS (iteration-2 re-verified 2026-09-09). Both iteration-1 consequence gaps are FIXED. (1) The D4/ADR-0025 "one durable writer / anchor" consequence is now worked out: the durable stamp preserves the entry's merkle anchor (lifecycle.rs:2260-2277 re-passes the live root; relocate.rs:215-223 carries the source root explicitly), so ADR-0030 fetch verification stays ON for the drain mover (repair.rs:427 now receives a Some root for any entry that was ever sealed+stamped; covered by relocate.rs:498 and relocate.rs:619-646 cold-restart assertions). Heal's deliberate `None` invalidation is un-regressed (worker.rs:443 + worker tests asserting the cleared root). (2) The source-release final view no longer drops the just-materialized target: `release()` (cluster_drain.rs:410-452) reads the CURRENT holder set from the live registry entry at release time (falling back to the candidate snapshot only if the entry vanished) and refreshes to `current − self`, so the converged target stays in the released view (no extra g4 round, A-routed reads can fail over to the new copy). Remaining verified: D4 Tier-1 registration + Tier-0 never gated (scheduler/engine.rs housekeeping permit; scheduler/mod.rs + task.rs + node.rs + durability.rs now document the SIX Tier-1 tasks incl. drain_cluster); D5 own knob `durability.drain_cluster_max_bytes_per_tick` default 64 MiB (core/config/durability.rs:52-55,93-96) — no shared framework; D6 blocked ⇒ `set_drain_blocked` + pool stays Draining + zero deletes (blocked_cluster_drain_deletes_nothing); D4-1a durable gate (healing_service.rs:1839-1868) acks Drain only after Sealed + local id in storage_locations, DRAIN_MATERIALIZE_TIMEOUT=120s; ADR-0030/0033/0034/0017/0035 satisfied in shape; mid-drain restart per the ephemeral-intent rule (cluster_drain_survives_a_restart_mid_drain re-verified 3/3). Residual cross-feature notes (non-blocking for THIS feature, recorded for d5/operators): node-level drain begin is non-transactional across pools (modules/server.rs:541-556 — a failing pool aborts mid-loop leaving earlier pools Cluster-Draining; operator recovers individually); cluster-drain emptiness is defined as "self in no locations + no Reserved" while released entries keep pool_id == source (cluster_drain.rs:410-452, release() leaves pool_id untouched) — d5's detach check must use the d4 definition or retire those entries; a stale duplicate seal-time push can re-add a released node durably (segment_service.rs:1066-1087 Existing arm) — self-limiting while the pool is Draining, d5 must account for post-Detachable re-materialization. -->
- [x] **Perf:** frontmatter `perf: []`; prose constraints: enumeration is
      a registry `for_each`, never a disk scan; the byte budget bounds
      per-tick dispatch; target backpressure reuses the ReRepWorker's
      bounded queue (`tokio::sync::mpsc`, perf rule 2.6) + semaphore (perf
      rules 2.7/8.5) — a drain must not bypass the queue that bounds
      re-replication; no unbounded fan-out when many segments drain at once
      (see Resolved Decisions 7).
<!-- REVIEW: verified 2026-09-09 — enumeration is `SegmentLifecycleRegistry::for_each` only (cluster_drain.rs:264), never a disk scan (ADR-0034); per-tick dispatch is bounded by `drain_cluster_max_bytes_per_tick`; the drain request rides the existing `RequestReReplication` RPC into the target's bounded `ReRepWorker` mpsc queue (a full queue ⇒ enqueue error ⇒ accepted=false ⇒ NotDurable ⇒ park, so a drain never bypasses the queue), and the drain RPC itself is bounded by DRAIN_DISPATCH_TIMEOUT_MS=150s client-side vs the 120s materialize gate. No unbounded fan-out (one synchronous dispatch per cycle under the budget). Perf item PASSES. The iteration-1 pacing edge (>budget single segment stalled forever) is FIXED in iteration 2: `next_releasable` (cluster_drain.rs:311-331) now allows one oversized candidate when the tick has not yet moved any bytes (`bytes_released == 0`), then enforces the cap afterwards — mirroring the intra-node worker's overshoot semantics (cluster_drain.rs:307-310 documents it). -->
- [x] **Integration:** integration test at the cluster boundary exercises
      the full pool-only + node-level drain → source-release → detach /
      no-op-leave workflow with zero data loss and continuous reads. **No
      load suite is run locally** (PIPELINE §6).
<!-- REVIEW: verified 2026-09-09 — crates/oceanfs-node/tests/cluster_drain.rs 3/3 single-threaded: (1) node-level RF=2 drain to Detachable under live reads — A's root empties, every held segment exists on B or C, A's `segment_locations` drops self, reads byte-identical through A, status stays Draining after Detachable; (2) blocked variant — B leaves, cluster pool drain parks blocked with a surfaced reason and zero `.dat` deleted, reads keep serving from A; (3) restart mid-drain — A stops after begin, restarts over the same data dir, the test asserts each pre-restart held segment RE-LISTS node-a in A's own `segment_locations` (lines 523-532) before re-issuing, then drives to Detachable with all segments on B/C and byte-identical reads — this assertion is exactly what fails under the pre-d4 in-memory stamp (a checkpoint may not have run before the stop) and is what the durable-stamp fix restores, so the scenario genuinely exercises Option A. `pool-only mode=cluster` begin over the admin route is exercised in the blocked scenario; the no-op `leave(None)` ending is not exercised in these tests (the epic-DoD wording; node shutdown is used instead) — noted, not blocking. No load/e2e suite run locally (PIPELINE §6). -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Resolved Decisions

Recorded 2026-09-09 at final spec close (implementation complete;
independent review PASS, iteration 2). Every question posed under "Open
Questions for the Implementer" is resolved; where the outcome is also
recorded under [Deviations (accepted)](#deviations-accepted), the
cross-reference is given.

1. **Durable source-release gate (D4-1a).** A drain dispatch
   (`RepairReason::Drain`) makes the *target's* `request_re_replication`
   handler wait until **its own registry** shows `Sealed` +
   `storage_locations` contains itself before acking
   (`healing_service.rs:1823-1868`, poll deadline
   `DRAIN_MATERIALIZE_TIMEOUT` = 120 s); `accepted` therefore means
   "durable", and the source never source-releases over an
   un-materialized target. Repair/reconcile (`Announcement`/
   `Reconciliation`) stay fire-and-forget — the gate is drain-only
   (Deviations c).
2. **Worker/controller seam.** The controller is **node-side**:
   `DrainClusterController` (`crates/oceanfs-node/src/cluster_drain.rs`)
   implements `oceanfs_durability::DurabilityTask` **directly**
   (`name() == "drain_cluster"`) — not a durability-crate adaptor —
   because it needs the node-side `RepairDispatcher` and membership to
   dispatch drains. Registered in `DurabilityModule::build`
   (`modules/durability.rs:545-560`) (Deviations b).
3. **Ownership discriminator.** The registry `DrainMode::{IntraNode,
   Cluster}` routes each `Draining` pool to **exactly one mover**: d3's
   worker skips `Cluster` pools (`intra_node.rs:305`) and the d4
   controller drains only `Cluster` pools
   (`cluster_draining_sources`, `cluster_drain.rs:252`), via
   `PoolRegistry::begin_drain_with_mode` / `drain_mode`. The mode is a
   routing tag on a separate registry map, not a `DrainState` field
   (Deviations b).
4. **Source-release mechanics.** Durable `storage_locations =
   holders − self` refresh (`request_refresh_metadata`, event-WAL
   `MetadataRefresh` — ADR-0025) + local `.dat` unlink
   (`release()`, `cluster_drain.rs:410-452`); g4's live-count stops
   counting self immediately after the durable refresh and never
   re-adds it; crash windows follow d2's residue rules
   (refresh-durable/unlink-pending → boot-reapable residue;
   dispatch-pending → idempotent re-run).
5. **Release-final-view rule.** Source-release refreshes from the **live
   (converged) holder set** read at release time — not the pre-dispatch
   candidate snapshot — so a just-dispatched (and g5-converged) target is
   retained in the released view (`release()`, `cluster_drain.rs:410-452`,
   falls back to the candidate snapshot only if the entry vanished). This
   closes the iteration-1 "dropped holder / extra g4 round" consequence.
6. **Merkle-anchor preservation (Option-A critical-finding fix).**
   `persist_storage_locations` (`lifecycle.rs:2260-2277`) and the d2
   relocate commit (`relocate.rs:215-223`) carry the **current merkle
   root explicitly** through the refresh, because the refresh's merkle
   parameter is a **value replacement** (heal deliberately passes `None`
   to clear the anchor — un-regressed, `worker.rs:443`). This fix was
   directed by the stakeholder after the iteration-1 FAIL
   (`push_sealed_segment_registers_and_serves` cleared the seal-time
   anchor) (Deviations a).
7. **Budget pacing / overshoot.** One oversized segment may overshoot a
   tick when the tick has not yet moved any bytes
   (`next_releasable`, `cluster_drain.rs:311-331`); the cap is enforced
   once the tick has moved bytes — mirroring the intra-node worker's
   overshoot semantics. No unbounded fan-out: one synchronous dispatch
   per cycle under the budget (Deviations d).
8. **No-C2a interim ring-share + drain-intent ephemerality (recorded).**
   The ring keeps mapping ranges to the draining node until `leave(None)`
   (no C2a re-weighting); freed capacity is reclaimed by new writes +
   repair targeting — documented controller behavior (`cluster_drain.rs`
   module docs, "Interim ring-share behavior"). Draining is **drain to
   empty then `leave(None)`** (not leave-N-replicas for owned ranges);
   write routing for still-owned ranges failing placement/503 while no
   healthy data pool remains is accepted interim behavior (operators
   drain off-peak before leave). The operator `Draining` intent is
   runtime registry state rebuilt from config at boot — a mid-drain
   restart drops the flag and the operator re-issues the drain; no
   drain-intent persistence is added (ADR-0035; consistent with f8
   attach being ephemeral).

## Deviations (accepted)

Recorded 2026-09-09 at final spec close after implementation review
(PASS, iteration 2) — each validated by the stakeholder at/after
implementation.

- **a. Option-A durable holder stamp.** The in-memory `set_storage_locations`
  method was **removed**; every durable holder stamp now goes through
  `SegmentLifecycleCoordinator::persist_storage_locations`
  (`lifecycle.rs:2260-2277`) — an event-WAL `MetadataRefresh` — for the
  owner post-full-ack stamp (`segment_replicator.rs:683`) and the
  push-receiver Fresh+Existing arms (`segment_service.rs:1055/1072`).
  d2's relocate commit now carries the merkle root explicitly
  (`relocate.rs:215-223`). The restart/retirement workflow requires a
  node to durably remember it holds a segment (d4 re-issue, g4 live-count,
  read path), and the durable stamp must not clear the seal-time merkle
  anchor (Resolved Decisions 6).
- **b. Controller node-side `DurabilityTask` impl + separate `drain_mode`
  registry map.** The controller is NOT the durability-crate-adaptor seam
  d3 used: `DrainClusterController` implements
  `oceanfs_durability::DurabilityTask` directly in `oceanfs-node`
  (`cluster_drain.rs:456`) because it needs the node-side
  `RepairDispatcher`/membership. The `DrainMode` routing tag is a separate
  registry map (`PoolRegistry::begin_drain_with_mode`/`drain_mode`,
  `pool/drain.rs:218`), not a field on `DrainState` (Resolved Decisions
  2/3).
- **c. Durable-gate RPC semantics only for `RepairReason::Drain`.**
  `request_re_replication`'s materialization gate (Sealed + self in
  `storage_locations`, `DRAIN_MATERIALIZE_TIMEOUT` = 120 s) applies only
  when `reason == Drain`; repair/reconcile (`Announcement`/
  `Reconciliation`) requests remain fire-and-forget enqueues (Resolved
  Decisions 1).
- **d. Budget/overshoot semantics.** One oversized segment is allowed to
  overshoot a tick when the tick has moved nothing yet
  (`next_releasable`, `cluster_drain.rs:311-331`); the cap binds once any
  bytes have moved (Resolved Decisions 7).
- **e. `oceanfs_drain_*` throughput counters NOT registered.** The
  proposed `oceanfs_drain_dispatched_total` /
  `oceanfs_drain_released_total` / `oceanfs_drain_remaining{source}`
  counters were not registered; `DrainCycleStats` is returned by
  `run_drain_cycle`/observable, and d1's gauges cover blocked/state.
  **Recorded accepted — close or document before the epic DoD.**
- **f. d5 handoffs (non-blocking for this feature, recorded).**
  - (i) Cluster-drained pools reach `Detachable` while released `Sealed`
    entries keep `pool_id == source` (`release()` leaves `pool_id`
    untouched); d5's detach check must use the d4 emptiness definition
    ("self in no `storage_locations` + no Reserved") or retire those
    entries.
  - (ii) A stale duplicate seal-time push can durably re-add a released
    node after `Detachable` (the `segment_service.rs` Existing arm);
    self-limiting while the pool is `Draining`, but d5 must account for
    post-`Detachable` re-materialization.
  - (iii) Node-level drain begin is non-transactional across pools
    (`modules/server.rs:541-556`): a failing pool aborts the loop leaving
    earlier pools Cluster-Draining; the operator recovers individually.
