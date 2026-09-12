---
feature: "Degraded-Pool Semantics & Detector Configuration (pre-f4 Bug-Fix Gate)"
epic: "fleet-degradation"
status: done
priority: critical
owner: ""
dependencies:
  - feature: f1-volume-backed-fleet-topology
    reason: The acceptance rerun executes on the real-volume fleet; the f3 failures were produced on this topology
  - feature: f2-remote-fault-injectors
    reason: The acceptance rerun drives the f3 suite's SSH injections (vm_kill, latency, disk_fill, segment corruption) unchanged
  - feature: f3-load-degraded-fleet
    reason: The existing load_degraded suite + run-phase4.sh are the acceptance harness; f3's fleet runs are the bug evidence, and f3 stays open until this feature lands
adr:
  - 0027-hinted-handoff-ownership-model
  - 0029-storage-pools-disk-resilience
  - 0030-re-replication-target-pull
  - 0033-manifest-aware-peer-selection
perf: []
created: 2026-09-11
updated: 2026-09-12
---

# Degraded-Pool Semantics & Detector Configuration (pre-f4 Bug-Fix Gate)

## Summary

Fix the product defects the f3 `load_degraded` fleet validation exposed on
2026-09-11 (ADR-0026 volume-backed fleet), as a new bug-fix feature inserted
**pre-f4**. Four workstreams, all **product code + config** — no test-only
hooks, no new scenarios:

1. **D1 — Degraded is a preference, not an exclusion.** Restore two-tier
   read/write candidate handling: `preferred` (Healthy data pool, not
   `write_degraded`) vs `fallback` (reachable but Degraded). Reads try
   preferred then fallback; the write replication fan-out includes fallback
   targets so W=2 can be met. `write_degraded` (Dead WAL) stays a hard local
   write exclusion.
2. **D2 — Degraded is not a faithful copy.** A node with no Healthy data
   pool is non-copy for reconciliation; repair target selection becomes
   healthy-preferred, degraded-fallback.
3. **D3 — Hint-cap drops become bounded repair intents.** On
   `max_delivery_attempts` exhaustion, convert the drop into a per-segment
   repair intent fanned out to the segment's RF holders (bounded by RF,
   deduped by segment), and bound the reconciliation work queue with an
   overflow metric. `hinted_handoff_hints_dropped_total` stays the trigger
   counter.
4. **D4 — Detectors are configurable.** Add node-global `[storage.health]`
   defaults plus the existing per-pool `health = { ... }` overrides,
   covering the hard-coded detector knobs.

The work lives in `oceanfs-node` (routing cache, repair target selection,
health wiring), `oceanfs-server` (read candidate ordering, write fan-out),
`oceanfs-durability` (reconcile accounting/queue, hint delivery),
`oceanfs-core` (config surface), and `oceanfs-storage` (configurable health
monitor + any placement fallback).

**Sequencing.** The epic order is now `f3 → f5 → f4`. The f3 suite stays
open until this feature lands and its fleet rerun passes; the f3
fleet-run DoD item is **overruled as a closure gate** — the failing run is
bug evidence, not a feature gate (user decision 2026-09-11, recorded in
[f3 Fleet Run Evidence and DoD Overrule](f3-load-degraded-fleet.md#fleet-run-evidence-and-dod-overrule-2026-09-11)).
Acceptance is the **existing** f3 `load_degraded` full run plus a
`--no-injections` control; no new scenario is added unless the rerun proves
one cannot observe a fixed gate.

**Correctness only — no performance claims.** The acceptance runs are
volume-backed and not comparable to local-disk runs; no assertion adds a
latency or throughput bound (epic rule).

## Root Causes & Evidence (verified 2026-09-11)

### R1 — Degraded is hard-avoided instead of preferred

`crates/oceanfs-node/src/routing_cache.rs`: `healthy_data_pools` (`:280`)
counts only `"healthy"`; `exclude_read_candidate` (`:352-368`) and
`exclude_write_target`/`can_accept_writes` (`:347-349`, `:370-381`) treat
`"degraded"` like `"dead"`. Read candidate sets are then filtered with no
fallback (`crates/oceanfs-server/src/read/fetch.rs:549-553`, `:780-788`);
the write replication fan-out is filtered with no fallback
(`crates/oceanfs-server/src/write/coordinator.rs:908-912`), while only the
forward target has an `or_else(alive)` fallback (`:543-547`). Strict
exclusion was born in commit `e3f5333` (2026-08-23, f7 routing cache) and
never relaxed; it became observable when W=2 was actually honored
(`269aa4a`, 2026-09-10) and when f0 added the durable-debt gate (`b9f3f6a`,
2026-09-11).

**Evidence.** All-node 503 stretches (full runs 2/3: S3 baseline writes
`0/0`); victim HTTP 500 for 60s for a key node 0 served (run3 S4); instant
HTTP 000/ECONNREFUSED under CPU pressure; run1
`s3_reads_served_correct_bytes_during_fill: 8/15` and
`s3_recovery_write_after_fill_removal: Ok(503)`.

### R2 — Degraded counts as a faithful copy, so repair never fires

`crates/oceanfs-durability/src/reconcile.rs`: `live_copy_count`
(`:137-147`) counts a node unless it is membership-Dead or **all** its data
pools are `dead` (`membership_snapshot`, `:494-528`). A node whose data
pools are all `degraded` still counts as live → `live >= RF` → no repair.

**Evidence.** `oceanfs_repair_enqueued_total = 0` in every full run; keys
`hot-71/42/90/76` absent cluster-wide for 6-8+ minutes and reconverging
only after pools recovered; `oceanfs_ranges_under_replicated` never moved.
Final manifest gaps: 1/101 (run1), 1/102 (run2), 4/103 (run3), 6/94
(control-dirty).

### R3 — Hint debt is destroyed at the delivery cap

`crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs:826-856`:
after `max_delivery_attempts` (default 10, `hinted_handoff/mod.rs:71-89`)
a hint is dropped and only `hinted_handoff_hints_dropped_total` is
incremented — the debt ledger (ADR-0027 D2) forgets the copy, which then
feeds R2 (no repair). Mitigating fact: `HintRecord` carries `segment_id`
(`hinted_handoff/mod.rs:66-70`), so the drop can be converted into a
per-segment repair intent.

**Evidence.** 2 / 8 / 17 drops across the full runs (2026-09-11 run
investigation; `f3-control-dirty` records 3 at final scrape); run3's
`s1_hints_reconverged` recorded `pending=13` still draining after 90s.

### R4 — Detector knobs are hard-coded; defaults misfire on cloud volumes

All six `PoolHealthConfig` fields are configurable per pool via inline
`health = { ... }` (`crates/oceanfs-core/src/config/storage.rs:202-235`),
but the decision machinery is hard-coded in
`crates/oceanfs-storage/src/pool/health.rs`: trend doubling factor/rule and
minimum series length (`:243-253`), which series and percentiles feed trends
(`:128-130`, `:195-201`), SMART growth rules per tech (`:201-230`,
`:257-266`), absolute fast-path operators (`:826-901`), confirmed-loss
classification (`:307-318`), the 1s base ticker (`:698`), and the history
clamp 4..64 (`:752`). `HealthMonitorConfig` (`tick_interval`,
`event_capacity`, `:375-390`) is always `::default()` at the composition
root (`crates/oceanfs-node/src/modules/storage.rs:539-543`); the hints
probe divisor is hard-coded 6
(`crates/oceanfs-node/src/modules/durability.rs:797-805`).

**Evidence.** Data pools latched `degraded` for 10+ minutes after load
stopped; a `--no-injections` control run that passed the manifest still
recorded 8 hint drops (run investigation) — the defaults are not tuned for
network-block-storage latency profiles.

### Evidence artifacts (read-only — do not modify)

- [f3-full-run1-20260911.json](artifacts/f3-full-run1-20260911.json) — fail
- [f3-full-run2-20260911.json](artifacts/f3-full-run2-20260911.json) — fail
- [f3-full-run3-20260911.json](artifacts/f3-full-run3-20260911.json) — fail
- [f3-control-dirty-20260911.json](artifacts/f3-control-dirty-20260911.json)
  — fail (`--no-injections`)
- [f3-control-pass-20260911.json](artifacts/f3-control-pass-20260911.json)
  — pass (`--no-injections`)

Every injection that actually ran recorded `success=true` (five records per
full run: `vm_kill`, `latency`, `latency_remove`, `disk_fill`,
`disk_fill_remove`; the S4 `segment_corrupt` injection never ran because the
blob write failed first). The failures are product-side
(reads/writes/manifest), not injector-side.

## Scope

### In Scope

**D1 — Two-tier candidate handling (Degraded = fallback, never exclusion).**

- `oceanfs-node::routing_cache`: classify each node manifest into
  `preferred` (≥1 Healthy data pool, not `write_degraded`, not
  `node_unavailable`), `fallback` (reachable, not `write_degraded`, not
  `node_unavailable`, no Healthy data pool but at least one non-Dead data
  pool), and hard `excluded` (`node_unavailable`; all data pools Dead or
  absent; `write_degraded` on the write path only). Extend the
  `RoutingHint` implementation so coordinators can obtain the tiers, not
  just a single exclude bit.
- `oceanfs-server::read`: read candidate sets try `preferred` first, then
  `fallback`; they fail only when both are exhausted (and the existing EC
  path fails). Applies to the data-shard path and the parity-shard path.
- `oceanfs-server::write`: the replication fan-out attempts `preferred`
  targets first, then `fallback` targets, so W=2 can be met while a data
  pool is Degraded. Debt/hints accrue only for targets that cannot accept
  the copy: membership Dead/absent, `write_degraded`/`node_unavailable`
  hard exclusions, and attempted-but-failed deliveries — never merely
  because a data pool is Degraded. The forward-target `or_else(alive)`
  fallback is subsumed by the shared selector.
- The f0 honest-debt contract is unchanged: any N-member that cannot
  accept a copy is either acked or owed, and the hints-pool gate still
  applies (`write/coordinator.rs:554-568`).
- `write_degraded` (Dead WAL) stays a hard local **write** exclusion
  (ADR-0029 §D3 role consequence); it is not a read exclusion.
- The I/O-error fallthrough stays the guarantee (ADR-0029 §D5): a tier is
  an ordering, never a correctness dependency.

**D2 — Degraded is not a faithful copy.**

- `oceanfs-durability::reconcile`: a node with no Healthy data pool is
  **non-copy** for `live_copy_count` / under-replication accounting (flip
  `membership_snapshot`'s `all_data_dead` test to "no Healthy data pool",
  preserving the existing empty-data-pool guard); over-replication after a
  pool recovers is acceptable.
- `oceanfs-node::repair::ManifestRepairTargetSelector`: healthy-preferred,
  degraded-fallback repair target selection, so repair can land while the
  fleet is degraded (ADR-0030 dispatcher; ADR-0033 manifest-aware
  selection).
- `oceanfs-storage` (only if required): allow a repair copy to land on a
  Degraded pool when the selected target has no Healthy pool —
  healthy-preferred, degraded-fallback in pool placement; hard Dead
  exclusion unchanged.
- `oceanfs_ranges_under_replicated` must reflect the corrected accounting
  (it never moved during f3).

**D3 — Hint-cap drop ⇒ bounded per-segment repair intent.**

- On `max_delivery_attempts` exhaustion, convert the drop into **one
  repair intent per distinct `segment_id`** (deduped; never per-key),
  addressed to the segment's current RF holder set (≤ RF recipients). The
  receiving side's existing ADR-0030 target-pull dispatch (or the local
  reconciliation queue) decides and executes the repair.
- The conversion is wired into the existing give-up branch;
  `hinted_handoff_hints_dropped_total` remains the trigger counter.
- Bound the reconciliation work queue (`reconcile.rs:337`, currently an
  unbounded `BinaryHeap`) with a configurable `max_queue_depth`; on
  overflow the intent is not enqueued (or the least-urgent item is evicted)
  and an overflow metric increments. Overflow is safe because holders see
  degraded manifests (D2) and the drift scan remains the completeness
  fallback.
- Existing per-segment queue dedup stays (a segment already in flight is
  not enqueued twice).

**D4 — Detectors are configurable.**

- New node-global defaults section `[storage.health]` in
  `oceanfs-core::config::storage`, covering the existing
  `PoolHealthConfig` fields plus the currently hard-coded knobs: trend
  doubling factor, minimum trend series length, trend percentile
  selection, per-tech SMART-growth counter selection, base monitor tick,
  event capacity, history clamp, and the hints probe divisor.
- The existing per-pool inline `health = { ... }` overrides win over the
  global defaults (merge, not replace).
- Validation for all new fields (ranges/enums) with clear config errors;
  docs (`# Examples`, field docs, config reference).
- The composition root passes the resolved `HealthMonitorConfig` (no more
  unconditional `::default()`), and the hints probe reads the divisor.

**Tests and acceptance.**

- Unit tests for every changed gate (list in [Definition of Done](#definition-of-done)).
- No regression in the existing `oceanfs-node`, `oceanfs-durability`,
  `oceanfs-server`, `oceanfs-storage`, `oceanfs-core` suites.
- Fleet acceptance: the existing f3 `load_degraded` full run (all four
  scenarios, 0 manifest mismatches, all injections `success=true`) plus a
  `--no-injections` control, run on the cloud fleet via `run-phase4.sh`
  (PIPELINE §6 — never locally). New artifacts recorded under
  `docs/features/fleet-degradation/artifacts/`.

### Out of Scope (for this feature)

- **f4's pool-role hard-failure matrix and dynamic ops** — f4 starts only
  after this gate lands.
- **Metadata anti-entropy** — deferred product workstream, gated on the f4
  rerun.
- **Re-calibrating detector defaults** — D4 exposes the knobs; choosing
  production default values (especially for cloud volumes) is a separate
  calibration decision. Existing defaults are unchanged unless a
  validation constraint forces it.
- **Removing or redesigning the hint delivery cap / give-up policy** — the
  cap stays; only the drop consequence changes.
- **Confirmed-loss classification and its error-kind mapping** — fixed
  semantics (ADR-0029 §D3); this feature exposes numeric thresholds and
  knobs, not a reclassification surface.
- **New scenarios or injectors** — unless the rerun proves the existing
  suite cannot observe a fixed gate; then the minimal assertion change is
  recorded as a deviation.
- `dm-flakey` / `dm-delay`, network partitions, clock skew; any test-only
  product hook; any performance assertion.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | `routing_cache.rs`: tiered manifest classification + `RoutingHint` implementation; `repair.rs`: `ManifestRepairTargetSelector` healthy-preferred/degraded-fallback; `modules/storage.rs`: build `HealthMonitorConfig` from config; `modules/durability.rs`: configurable hints probe divisor + hint-drop→repair-intent bridge |
| `oceanfs-server` | `routing_hint.rs`: tier-aware candidate classification contract; `read/fetch.rs`: preferred→fallback read candidate ordering (data + parity paths); `write/coordinator.rs`: preferred→fallback replication fan-out + debt classification |
| `oceanfs-durability` | `reconcile.rs`: `live_copy_count`/`membership_snapshot` accounting, bounded queue + overflow metric; `hinted_handoff/hint_delivery.rs`: drop→repair-intent conversion; `hinted_handoff/mod.rs`: intent dedup/emission; `healing_service.rs`: intent/reason shape if extended |
| `oceanfs-core` | `config/storage.rs`: `[storage.health]` global defaults + per-pool override merge, validation, docs; re-exported config types |
| `oceanfs-storage` | `pool/health.rs`: configurable trend/SMART/tick/history knobs consumed by the monitor; placement fallback for a repair landing on a Degraded pool (only if the current policy hard-refuses it) |
| `e2e` | **No expected change** — f3's `load_degraded.rs` + `run-phase4.sh` are the acceptance harness unchanged. Any assertion adjustment needed to observe a fixed gate is recorded as a deviation (no new scenarios). |

## Interface (Public API)

### Config surface (sketch — exact schema is an implementer OQ)

```toml
# Node-global health defaults (NEW). Existing per-pool inline `health = { ... }`
# overrides win over these; the current PoolHealthConfig fields become the
# global defaults as well.
[storage.health]
tick_interval_secs = 1              # base monitor ticker (today hard-coded 1s)
event_capacity = 64                 # health status-event channel (perf 2.6)
trend_window_secs = 300             # existing PoolHealthConfig fields...
detection_window_secs = 30
recovery_window_secs = 300
error_rate_threshold = 0.001
min_errors = 3
latency_factor = 5.0
trend_doubling_factor = 2.0         # NEW — the `x[i] >= 2*x[i-1]` rule
trend_min_windows = 3               # NEW — minimum trend series length
trend_latency_percentile = "p99"    # NEW — p50|p95|p99 (percentile feeding trends)
history_max_windows = 64            # NEW — history clamp upper bound
hints_probe_divisor = 6             # NEW — hints probe cadence = detection_window / divisor

[storage.health.smart_growth]       # NEW — per-tech counters that trip the trend
hdd = ["reallocated_sectors", "pending_sectors"]
ssd = ["uncorrectable_ecc", "wear_level"]
nvme = ["uncorrectable_ecc", "wear_level"]
cloud_ephemeral = []

[[storage.pools]]
# ... existing fields unchanged ...
health = { latency_factor = 2.0, trend_min_windows = 4 }   # per-pool override
```

### Metrics

| Metric | Kind | Change |
|---|---|---|
| `oceanfs_routing_manifest_skips_total{path="read"}` | counter | Semantics: counts only hard exclusions; Degraded no longer increments |
| `oceanfs_routing_manifest_skips_total{path="write"}` | counter | Same |
| `oceanfs_routing_degraded_fallbacks_total{path}` | counter (new) | Fallback tier was consulted (`read`/`write`) — proposed name |
| `oceanfs_reconcile_queue_depth` | gauge (new) | Bounded queue occupancy (single urgency class; the proposed `{priority}` label was dropped — see Deviations) |
| `oceanfs_reconcile_queue_overflow_total` | counter (new) | Intents dropped at the bound — overflow is observable, never silent |
| `hinted_handoff_hints_dropped_total` | counter (existing) | Unchanged; remains the trigger counter for intent conversion |
| `oceanfs_repair_enqueued_total` | counter (existing) | Counts the reconciliation drift-scan enqueues AND the hint-drop bridge intents (shared counter handle) |
| `oceanfs_ranges_under_replicated` | gauge (existing) | Must move with the D2 accounting |

### Module-boundary signatures (sketch — names finalized in implementation)

- `oceanfs-server::routing_hint::RoutingHint`: add tier-aware candidate
  classification (e.g. `fn read_candidate_class(&self, node_id: &NodeId) ->
  CandidateClass` and `fn write_target_class(...)`, with `pub enum
  CandidateClass { Preferred, Fallback, Excluded }`), preserving
  `on_failover`. Trait stays in the consuming crate (`oceanfs-server`);
  the implementation stays `oceanfs-node::routing_cache::ManifestCache`
  (architecture §2.1).
- `oceanfs-node::routing_cache`: `pub fn data_pool_tiers(manifest:
  &NodeManifest) -> (usize, usize)` (preferred, fallback) or an equivalent
  tier enum; `healthy_data_pools` / `can_accept_writes` keep their hard-
  exclusion meaning.
- `oceanfs-durability::reconcile::ReconcileConfig`: add
  `pub max_queue_depth: usize` (default value OQ).
- `oceanfs-durability::hinted_handoff`: a drop sink injected into hint
  delivery, e.g. `pub trait HintDropSink: Send + Sync { fn
  on_hints_dropped(&self, dropped: &[HintDropRecord]) -> Result<(), String>; }`
  with `pub struct HintDropRecord { pub segment_id: SegmentId, pub
  intended_for: NodeId }`; the node-side bridge resolves the RF holder set
  and emits the intent through the ADR-0030 path (or `ReRepRequest` gains
  a `RepairReason::HintDrop` if that route is extended instead).
- `oceanfs-core`: `pub struct StorageHealthConfig` (global defaults) and
  the new `PoolHealthConfig` fields; `HealthMonitorConfig` is built from it
  at the composition root.
- `oceanfs-storage::pool::health`: `evaluate_trend` / `decide_transition`
  / `HealthMonitor` consume the configurable factor, min-windows,
  percentile, and SMART selection instead of hard-coded values; the
  history clamp and event capacity become config-driven.

## Data Flow

```
WRITE — before (broken)
PUT /{bucket}/{key}
  → coordinator: replica_set (ADR-0027 N-set)
      filter(!exclude_write_target)            # Degraded == excluded
  → remote_targets = preferred-only; no fallback
  → W=2 not met → 503, and/or unattempted Degraded members become hint
    debt that can be dropped at the cap (R3 → R2: never repaired)

WRITE — after
PUT /{bucket}/{key}
  → coordinator: classify each member
      preferred = ≥1 Healthy data pool ∧ ¬write_degraded ∧ ¬node_unavailable
      fallback  = reachable ∧ ¬write_degraded ∧ ¬node_unavailable
                  ∧ no Healthy data pool ∧ ≥1 non-Dead data pool
      excluded  = node_unavailable ∨ write_degraded ∨ all data pools Dead
  → fan-out preferred first, then fallback (both count toward W)
  → attempted-but-failed → hint debt (as today); Degraded members are
    attempted, so being Degraded no longer creates debt
  → write_degraded stays a hard local write exclusion
  → I/O error path is still the guarantee (ADR-0029 §D5)

READ — before (broken)
GET /{bucket}/{key}
  → replica_set → filter(!exclude_read_candidate)   # Degraded filtered out
  → if empty → routing error / HTTP 5xx, even when a Degraded replica is up

READ — after
GET /{bucket}/{key}
  → replica_set → preferred list + fallback list
  → try preferred in order; on I/O error → next preferred
  → preferred exhausted → try fallback in order
  → fail only when both tiers are exhausted (EC path unchanged)
  → on_failover() still records every error-driven fallthrough

HINT DROP — after (new path)
hint batch delivery → receiver retry_indices → attempts > max_delivery_attempts
  → dropped++ (hinted_handoff_hints_dropped_total — unchanged trigger)
  → dedupe dropped records by segment_id
  → one repair intent per segment (≤ RF recipients = the segment's holders)
      → receiver side: ADR-0030 target-pull dispatch / reconcile enqueue
  → reconcile queue bounded: overflow → intent not enqueued + overflow metric
  → drift scan remains the completeness fallback
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in every affected crate
      (`oceanfs-core`, `oceanfs-storage`, `oceanfs-node`, `oceanfs-server`,
      `oceanfs-durability`); no `unsafe`, no test-only product hooks.
- [x] **Tests (unit — each changed gate):**
      - Degraded read fallback: a Degraded candidate is consulted after the
        Preferred ones and serves the read; both-exhausted still fails;
      - Degraded replication fallback: a write reaches W with a Degraded
        fallback target; a Degraded target does not create hint debt, while
        a Dead/absent one does;
      - `write_degraded` / `node_unavailable` remain hard exclusions on
        their respective paths;
      - live-count accounting: a node whose data pools are all Degraded is
        non-copy (`live_copy_count` drops below RF → repair intent);
        metadata-dead/healthy-data behavior preserved;
      - repair target selection: healthy preferred, degraded fallback;
      - hint-drop conversion: cap exhaustion emits one intent per segment
        (deduped), bounded by RF holders, and keeps the dropped counter;
      - reconcile queue bound: enqueue beyond `max_queue_depth` is
        observable and does not grow the heap unboundedly;
      - config parsing/validation: `[storage.health]` + per-pool override
        merge, invalid values rejected, existing defaults unchanged.
- [x] **Tests (no regression):** existing suites pass —
      `cargo test -p oceanfs-node --lib -- --test-threads=1`,
      `-p oceanfs-durability`, `-p oceanfs-server`, `-p oceanfs-storage`,
      `-p oceanfs-core` (RocksDB-affected crates serialized per PIPELINE
      §4.6). No `load_*` suite on the dev machine (PIPELINE §6).
- [x] **Docs:** every new/changed `pub` item has `# Examples`;
      `#![deny(missing_docs)]` passes in the affected crates; the config
      reference documents `[storage.health]`, the merge/validation rules,
      and each new knob.
- [x] **ADR:** ADR-0029 §D3/D5 (Degraded = suspicion; the cache optimizes,
      the error path guarantees; `write_degraded` role consequence),
      ADR-0027 D2 (debt is bounded, never silent; the coordinator owns
      convergence), ADR-0030 (repair intents flow through the target-pull
      dispatcher), and ADR-0033 (manifest-aware selection) are satisfied;
      no new test-only hook.
- [x] **Perf:** `perf: []`; the reconciliation queue is bounded (perf 2.6),
      candidate tiering stays allocation-light on hot paths (perf 1.3/2.4
      lock-free manifest reads), and no fleet run asserts a perf bound.
- [x] **Integration (fleet acceptance):** on the cloud fleet via
      `run-phase4.sh` (PIPELINE §6), the **existing f3 `load_degraded` full
      run passes all four scenarios with 0 manifest mismatches and all
      injections `success=true`**, and a `--no-injections` control run
      passes; the new artifacts are recorded under
      `docs/features/fleet-degradation/artifacts/` (e.g. `f5-rerun-full-…`,
      `f5-rerun-control-…`) and linked from this doc. Any assertion change
      needed to observe a fixed gate is a recorded deviation (no new
      scenario).
- [x] **Integration (docs):** the epic DAG/table shows f5 as the pre-f4
      bug-fix gate; f3's fleet-run DoD item is annotated as overruled as a
      closure gate and f3 stays open until this feature's rerun passes; no
      artifact file is modified.
- [x] **Deviations:** implementation-shape choices (OQ answers), the
      queue-bound value, the intent message shape, the final config schema,
      and any rerun finding are recorded in this doc.
<!-- REVIEW (iteration 2, 2026-09-12): all ten review-1 gaps independently re-verified — (1) write-fallback test `degraded_peer_is_attempted_as_write_fallback_without_debt` (Fallback attempted+acked, no debt; Excluded still owed; fallback tier consulted exactly once); (2) live skip counters — `read_candidate_class`/`write_target_class` increment on `Excluded` only, are registered, and are reached from the production coordinator/fetch paths; (3) bridge test `hint_drop_bridge_dispatches_one_intent_per_held_segment`; (4) shared `oceanfs_repair_enqueued_total` handle (`ReconciliationLoop::repair_enqueued_counter` + `with_repair_enqueued_counter`, wired at `modules/durability.rs:504-513`); (5) Deviations section populated; (6) `{priority}` drop recorded (final gauge unlabeled); (7) the two read-tier fetch tests; (8) `# Examples` added for `HintDropRecord`, `HintDropRepairBridge`+`new`/`with_repair_enqueued_counter`, `StorageConfig` accessors, and the module doc now carries `[storage.health]`+merge; (9) `hint_delivery` test lints fixed (`StdMutex`, Copy clone); (10) frontmatter `updated: 2026-09-12`. Commands reproduced: `cargo build --all-targets` clean; `cargo fmt --all -- --check` clean; lib suites core 239 / storage 538 / server 264 / node 123 / durability 294 (0 failed, `--test-threads=1`); doctests pass; `RUSTDOCFLAGS="-D warnings" cargo doc` clean; `cargo clippy --lib -- -D warnings` clean in all five crates. Fleet artifact `f5-rerun-full-20260912.json`: 50/50 assertions, 0 manifest mismatches, 6/6 injections `success=true`; control 8/8. Non-blocking notes: `ReconciliationLoop::repair_enqueued_counter` (reconcile.rs:476) has no `# Examples` (trivial accessor, same as sibling `holder_index`; doc examples are explicitly non-gating per the note below); the integration red `routing_manifests::write_degraded_peer_is_routed_around` is pre-existing (reproduced at the f5 base `3081da5`; recorded in f0's review notes), not an f5 regression. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

Implementation-shape only — the product semantics above are decided and
must not be re-litigated.

- **Ordering representation.** Two explicit tier lists vs one scored or
  sorted candidate list; how unknown-manifest peers and ties are ordered;
  whether the fallback pass preserves the preferred pass's per-candidate
  timeout budget.
- **Reconciliation queue bound.** The `max_queue_depth` value and the
  overflow policy (drop-new intent vs evict least-urgent item); whether
  overflow is also logged/evented; the final metric names (`_depth` gauge
  plus `_overflow_total`, or a single gauge).
- **Repair-intent message shape.** Reuse `ReRepRequest` with a new
  `RepairReason::HintDrop` vs a narrower `HintDropRecord` + node-side
  bridge; the dedup scope (one per delivery cycle vs one per drop event);
  whether the intent fans out to the holders directly (≤ RF messages) or
  is enqueued locally and dispatched by reconciliation. Bound by RF and
  deduped by segment either way.
- **`[storage.health]` schema details.** Exact field names/types; the
  global-vs-per-pool merge rules; how the per-tech SMART rule is expressed
  (counter-name lists vs enums); percentile representation (enum vs
  numeric); how the existing `PoolHealthConfig` fields relate to the new
  global section; whether the placement fallback in `oceanfs-storage` is
  needed at all.
- **Metric label shape** for `oceanfs_routing_degraded_fallbacks_total`
  and the repair-intent reason label (the names above are proposals).

## Deviations (accepted)

Implementation-shape choices (the OQ answers) and the rerun findings:

- **D1 ordering representation.** Two explicit tier lists, built once per
  operation on the calling coordinator (`order_read_candidates` for reads;
  `preferred_targets`/`fallback_targets` in the write fan-out, Preferred
  first, input order preserved inside a tier, unknown-manifest peers =
  Preferred, hard exclusions dropped). The write tiering lives at the
  coordinator call site, not inside `RoutingHint`, because it must run on
  the same pass that collects failures for the hint ledger.
- **D1 skip counters.** `oceanfs_routing_manifest_skips_total{path="read"}`
  / `{path="write"}` are incremented from the tier
  classification itself (`read_candidate_class`/`write_target_class` in
  `ManifestCache`), so hard exclusions are counted exactly once per
  consulted candidate and Degraded fallbacks never increment them. The
  binary `exclude_*` trait methods delegate to the tier methods.
- **D2 storage placement.** No `oceanfs-storage` change was needed: the
  repair write path resolves the segment's recorded `pool_id` without a
  status gate, so a dispatched repair can already land on a Degraded pool.
  The `ManifestRepairTargetSelector` change supplies the
  healthy-preferred/degraded-fallback *node* choice.
- **D3 repair-intent shape.** `HintDropRecord { segment_id, intended_for }`
  plus a node-side `HintDropRepairBridge` emitting one `ReRepRequest`
  (`RepairReason::Reconciliation`) per held segment through the existing
  ADR-0030 dispatcher; segments the node does not hold are skipped. Dedup
  is per delivery cycle (upstream, by segment); the bridge fans out to the
  recorded holder set (≤ RF). `oceanfs_repair_enqueued_total` counts the
  emitted intents through a counter handle shared with the reconciliation
  loop.
- **D3 queue bound.** `ReconcileConfig::max_queue_depth = 10 000`,
  drop-new: the overflowing intent is not enqueued,
  `oceanfs_reconcile_queue_overflow_total` increments, and the segment
  stays discoverable by the drift scan / next holder event. Dedup is
  unchanged (a segment already in flight is not re-enqueued).
- **D3 queue-depth metric.** Final name `oceanfs_reconcile_queue_depth`, a
  single unlabeled gauge: the queue has one urgency class (live count), so
  the proposed `{priority}` label would be constant. The dispatcher's
  parked-repair gauges (`announcement`/`reconciliation`) remain the
  per-priority observability.
- **D4 config schema (OQ).** `[storage.health]` holds `PoolHealthOverride`
  (all fields optional) merged field-by-field over the hard-coded
  `PoolHealthConfig::default()`; a pool's inline `health` table merges over
  that resolved global (per-pool wins). Final new knobs:
  `trend_doubling_factor` (2.0), `trend_min_windows` (3),
  `trend_latency_percentile` (`p50|p99|p999`, default `p99`),
  `history_max_windows` (64), `smart_growth` (per-tech counter lists:
  hdd/ssd/nvme/cloud_ephemeral), `hints_probe_divisor` (6). Monitor-level
  keys (`monitor_tick_interval_secs`, `event_capacity`,
  `hints_probe_divisor`) are global-only and rejected on a pool table.
  Existing defaults reproduce the pre-f5 hard-coded behavior exactly.
- **Acceptance assertion A-change.** `s4_heal_failed_zero` →
  `s4_heal_failures_observed_evidence` (user decision A, 2026-09-11):
  recorded evidence of the heal-counter classification finding (benign
  stale-segment / no-local-shard races counted as permanent failures) is
  the correct S4 contract; exact repair-failure assertions belong to f4's
  pool-role scenarios. The green rerun is
  [f5-rerun-full-20260912.json](artifacts/f5-rerun-full-20260912.json).

### Pre-close findings (2026-09-11, f5 acceptance rerun)

- **Heal-counter classification (recorded for f4, user decision A).** The
  first f5 acceptance full run passed 49/50 assertions; the only failure
  was the legacy `s4_heal_failed_zero` import from the superseded phase4
  doc. Evidence: the heal for the *inserted* corrupted segment permanently
  failed after 3 retries with `EC decode failed: need at least 4 shards,
  got 0`, and a peer logged benign `segment not found` permanent heal
  failures, while the blob was served with correct bytes throughout, the
  second scrub pass was clean, and the manifest stayed intact. This is a
  heal-counter semantics issue (benign stale/no-local-shard races counted
  as permanent failures), not data loss; f3's S4 scope does not require
  `heal_failed == 0`. The assertion was replaced with recorded evidence
  (`s4_heal_failures_observed_evidence`) and precise repair assertions
  belong to f4's pool-role scenarios. Artifacts:
  [f5-rerun-full-20260911.json](artifacts/f5-rerun-full-20260911.json),
  [f5-rerun-control-20260911.json](artifacts/f5-rerun-control-20260911.json).
- **Fleet acceptance rerun green (2026-09-12, post-A).** The full
  `load_degraded` run on the freshly provisioned volume-backed fleet passed
  all four scenarios: **50/50 assertions, 0 manifest mismatches, 0 failed
  injections** (6 records: `vm_kill`, `latency`, `latency_remove`,
  `disk_fill`, `disk_fill_remove`, `segment_corrupt`), including
  `s4_heal_failures_observed_evidence` — the A-change replacement for
  `s4_heal_failed_zero` — and `cluster_healthy_at_end`. No perf assertion is
  present. Artifact:
  [f5-rerun-full-20260912.json](artifacts/f5-rerun-full-20260912.json)
  (seed 42, duration 341s). The `--no-injections` control from 2026-09-11
  (8/8 pass) remains valid: the A-change only touched a full-run assertion.
  The 2026-09-12 fleet was re-provisioned from scratch (the f3 fleet had
  been destroyed) via `vm-provision.sh --volume-pools`; the provisioning
  script was interrupted mid-run and completed manually, with the
  provisioning record reconstructed from the live account before the
  deploy — the acceptance run itself used the standard
  `run-phase4.sh --full` path.
