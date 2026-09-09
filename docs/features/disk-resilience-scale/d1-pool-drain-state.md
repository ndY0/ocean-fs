---
feature: "Pool Drain State (Registry Draining + Observability)"
epic: "disk-resilience-scale"
status: done
priority: high
owner: ""
dependencies: ["d0-regression-gate"]
adr: [0036, 0029, 0033]
perf: []
created: 2026-09-07
updated: 2026-09-08
---

# Pool Drain State (Registry Draining + Observability)

## Summary

Add the **drain-state plumbing** that every later Phase-C feature consumes
(ADR-0036 D1 "drain-state plumbing" / D6). A data pool an operator wants to
remove is marked with a registry-level `Draining` status — a **distinct**
`PoolStatus` variant, not a parallel flag — so every existing status
consumer reasons about it in one place: `PlacementPolicy` excludes it from
new segment targets immediately (it only selects `Healthy` pools today),
reads keep serving from it until the last copy moves, the health monitor
does not fight the operator's drain, the `NodeManifest` carries `"draining"`
so peers see it as not-a-placement-target, and a blocked-reason record
feeds the no-destructive-failure rule (drain blocked ⇒ pool stays
`Draining` + reason surfaced, nothing deleted). Lives in
`oceanfs-storage/src/pool/` (state) and `oceanfs-node` (manifest build +
wiring); the actual mover workers are d3 (C1a) and d4 (C1b), which consume
this state.

## Scope

### In Scope

- **`PoolStatus::Draining` as a distinct variant**
  (`crates/oceanfs-storage/src/pool/mod.rs:131`, today
  `Healthy | Degraded | Dead`):
  - extend `as_u8`/`pool_status_from_u8` (`pool/mod.rs:152,162`) and every
    in-crate exhaustive consumer. The `#[non_exhaustive]` attribute means
    *external* crates already compile with a wildcard arm — but Phase C
    must sweep the in-crate `match` sites that need a deliberate Draining
    arm (placement filter, health monitor, manifest builder, metric
    encoding) and must not silently fall into a `_ => Healthy` arm
    (`pool/mod.rs:166` maps unknown bytes to Healthy today — if `Draining`
    is stored as the status atomic byte, `pool_status_from_u8(3)` must map
    to `Draining`, not to Healthy).
  - **Key decision (settled — sketch): a distinct status, not a parallel
    flag.** Rationale (ADR-0036 D6): placement, routing, manifest, and
    health already branch on `PoolStatus`; a flag would need a second
    dimension in every consumer and would allow impossible combinations
    (e.g. `Dead` + draining).
  - Only `role == data` pools may be marked `Draining`; `wal`/`metadata`/
    `hints` pools are cardinality-1 and their replacement is the g7/g8
    boot/remount path, not drain (ADR-0031 role pinning; preserved from
    the epic's non-goals).
- **Immediate placement exclusion.** `PlacementPolicy::select_from_pools`
  already filters `pool.status() == PoolStatus::Healthy` &&
  `!write_degraded()` && `free_bytes > MIN_FREE_HEADROOM_BYTES`
  (`crates/oceanfs-storage/src/pool/placement.rs:168-178`). Because
  `Draining != Healthy`, a draining data pool is excluded from **new**
  segment targets with no policy change — the work is the test that pins
  this and the doc update on `select_data_pool`
  (`placement.rs:128`). The f5 sealer reserves through the same policy, so
  seal-time reservation cannot target a draining pool (this is what makes
  d3's "no segment mid-seal on the source" guarantee hold for *new*
  segments — see the open question for in-flight ones).
- **Read-while-draining.** Reads keep serving from the pool until the last
  copy moves. The read path resolves a segment's root from the lifecycle
  registry `pool_id` + pool registry (`DiskSegmentStore::resolve_pool`,
  `crates/oceanfs-storage/src/segment/data_store.rs:292`;
  `DiskSegmentReader::pool_root_cache`, `crates/oceanfs-storage/src/io/segment_reader.rs:170`)
  and does **not** gate on pool status, so a `Draining` data pool continues
  to serve reads naturally. The feature adds no status check to the read
  path; it adds tests that prove reads keep flowing from a draining pool
  (including the case where it is the node's last data pool — read-only
  retirement, d4's precondition).
- **Health monitor does not fight the drain.**
  (`crates/oceanfs-storage/src/pool/health.rs`: `HealthMonitor` at 451,
  `tick_pool` at 721, `decide_transition` at 817, `apply_role_consequences`
  at 780.) Drain-induced activity is relocation reads/writes plus normal
  read traffic, so I/O signals stay normal; but the monitor must never
  transition a `Draining` pool to `Degraded`/`Dead` from those signals —
  `decide_transition` treats `Draining` as an absorbing input (like the
  `Dead` absorbing rule at `health.rs:1308`), and `apply_role_consequences`
  skips Draining pools (a draining data pool must not trip the
  write-degraded / availability logic).
- **Manifest carries the state.**
  (`crates/oceanfs-node/src/pool_manifest.rs:74` —
  `pool_manifest_from_pool` maps `PoolStatus` → `"healthy" | "degraded" |
  "dead"`, `_ => "healthy"`.) Add the `Draining => "draining"` arm; peers
  then see the pool as not-a-placement-target. Existing consumers already
  interpret non-`"healthy"` as excluded:
  - repair/selector: `ManifestRepairTargetSelector::pick_repair_target`
    requires a manifest data pool with `status() == "healthy"` &&
    `!write_degraded()` (`crates/oceanfs-node/src/repair.rs:110-113`), so a
    node whose only data pools are draining is never selected as a repair
    or drain target (ADR-0033 manifest-aware selection — the draining
    status flows through the *existing* seam; no new peer-routing code is
    needed because peers never pick a node's pool, the node's own
    placement does).
  - write routing/peer selection: new replicas land on the node, and the
    node's local placement picks the pool — so remote coordinators need no
    per-pool change; a node with zero healthy data pools is already
    routed around as a write target via the existing manifest-derived
    gates (`oceanfs-node/src/routing_cache.rs:305` `is_write_degraded`,
    `peer_selection.rs`).
- **Registry/state surface for the drain lifecycle (ADR-0036 D6).** New
  `oceanfs-storage/src/pool/drain.rs` module (or equivalent registry
  methods) carrying the operator-visible drain record:
  - `begin_drain(pool_id)` — validates (data role, current status not
    Dead/not already draining), flips the pool's status to `Draining`,
    records `DrainState::Draining { blocked_reason: None }` (d3 later
    added `paused: false` to the variant);
  - `set_drain_blocked(pool_id, Option<&str>)` / `clear_drain_blocked` —
    written by the workers (d3/d4) when no eligible target exists;
  - `set_pool_empty(pool_id)` — the Draining→Detachable transition,
    invoked by the worker that emptied the pool (d3/d4);
  - read accessors: `drain_state(pool_id) -> DrainState`, plus a cheap
    "is draining" check used by placement/doc sites.
  - The **no-destructive-failure rule** is a test scenario here, not just
    prose: a pool whose drain is blocked stays `Draining` with the reason
    surfaced and **nothing deleted** (ADR-0036 D6); detach (d5) is only
    accepted on `Detachable`.
- **Observability.**
  - `oceanfs_pool_status{pool_id, role}` keeps its 0=Healthy/1=Degraded/
    2=Dead semantics; draining is exposed as a distinct series to avoid
    renumbering an existing gauge contract used by fleet assertions
    (`oceanfs_pool_draining{pool_id}` 0/1) — OR extend `as_u8` with
    `Draining = 3` and document the gauge now takes 3; **implementer
    decision**, flag in the DoD which encoding shipped.
  - `oceanfs_pool_drain_blocked_reason{pool_id}` — a labelled info/gauge
    set by `set_drain_blocked` and cleared on `Detachable` (ADR-0036 D6
    observability requirement). 
  - `oceanfs_pool_drain_state{pool_id}` gauge (0=Idle, 1=Draining,
    2=Detachable) for the operator dashboard.
- **Node wiring + minimal admin status.** A `Draining` transition re-runs
  the manifest build + re-gossip path exactly like f8 attach does:
  `build_node_manifest` (`crates/oceanfs-node/src/pool_manifest.rs:52`) →
  `Membership::set_self_manifest` (`crates/oceanfs-membership/src/membership/manager.rs:752`);
  the same `on_pool_attached`-style hook that server/admin.rs uses for f8
  (`crates/oceanfs-server/src/admin.rs:430,899`) is extended with a
  drain-state notifier. A read-only admin status route exposing per-pool
  `drain_state` + `blocked_reason` is part of d1 (the *mutation* routes
  `POST …/drain` are d3/d4 — d1 exposes state only).
- Tests:
  - unit: `Draining` status round-trips through `as_u8`/`pool_status_from_u8`
    and the metric/string encodings;
  - unit: placement excludes a `Draining` data pool immediately (existing
    `select_data_pool` returns a non-draining sibling); a pool that turns
    `Draining` mid-run stops receiving new reservations;
  - unit: `begin_drain` validation (data-role only, no Dead, no double
    drain); `set_drain_blocked` / `set_pool_empty` transition the record;
  - unit: `decide_transition(Draining, …)` stays `Draining` under
    degrading signals and clean windows (monitor does not fight the drain);
  - unit: manifest builder maps `Draining` → `"draining"`; repair-selector
    unit excludes a node whose only data pools are draining;
  - unit/metrics: `oceanfs_pool_drain_blocked_reason` set + cleared;
  - integration (local node, 2 data pools): PUT → mark pool 0 draining via
    the admin status surface → **GETs of objects whose segments live on
    pool 0 keep serving** (read-while-draining) → new sealed segments land
    only on pool 1 → manifest on the node shows `"draining"` → drain
    blocked with no sibling headroom ⇒ pool stays `Draining`, reason
    surfaced, no `.dat` deleted (**no-destructive-failure scenario**);
  - integration (single data pool variant, d4 precondition): mark the last
    data pool draining → reads still served, node manifests as having no
    healthy data pool (write-target exclusion for peers).

### Out of Scope (for this feature)

- The mover workers: d3 (C1a intra-node), d4 (C1b cluster, source-release).
  d1 provides state; it does not empty pools.
- The durable `pool_id` mutation primitive (d2).
- `PoolRegistry::detach` and config/topology drop (d5).
- Peer-routing filter changes: draining propagates through the existing
  manifest-aware seams (ADR-0033); no new remote-coordinator routing logic.
- Proactive rebalance (C2b), ring re-weighting (C2a/d6), graceful-leave
  redesign, segment self-description (C3) — epic non-goals, untouched here.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `PoolStatus::Draining` variant (`pool/mod.rs:131`); `as_u8`/`pool_status_from_u8`; new `pool/drain.rs` (DrainState record + registry methods); health-monitor carve-out (`pool/health.rs` decide/apply); placement doc + Draining-exclusion test (`pool/placement.rs`) |
| `oceanfs-node` | `pool_manifest.rs` `Draining => "draining"` mapping; manifest rebuild + re-gossip on drain-state change; admin status route |
| `oceanfs-server` | Admin status route + drain-state notifier hook (mirrors f8 `on_pool_attached`, `admin.rs:430`) |

## Interface (Public API)

- `PoolStatus::Draining` — new enum variant (`pub enum PoolStatus`, currently
  `#[non_exhaustive]`).
- `pub enum DrainState { Idle, Draining { blocked_reason: Option<String>,
  paused: bool }, Detachable }` — the operator-visible drain record
  (ADR-0036 D6 machine: Idle → Draining → Detachable, BLOCKED = Draining +
  reason). **d3 added the `paused` field** to the `Draining` variant for
  the operator pause/resume controls (see the d3 feature doc); d1 shipped
  the variant without it.
- `PoolRegistry::begin_drain(&self, pool_id: u32) -> Result<(), DrainStateError>`
  — validates + flips status to `Draining` + records state.
- `PoolRegistry::set_drain_blocked(&self, pool_id: u32, reason: Option<&str>)`
  — no-destructive-failure surfacing.
- `PoolRegistry::set_drain_paused(&self, pool_id: u32, paused: bool) ->
  Result<(), DrainStateError>` — **d3 addition**: operator pause/resume
  toggle; preserves the blocked reason, keeps the pool `Draining`.
- `PoolRegistry::set_pool_empty(&self, pool_id: u32)` — Draining → Detachable
  (called by d3/d4 workers when the registry holds no segment with this
  `pool_id`).
- `PoolRegistry::drain_state(&self, pool_id: u32) -> DrainState`.
- `PoolRegistry::is_draining(&self, pool_id: u32) -> bool` (or a
  `StoragePool::status()` read — a Draining pool reports the new variant).
- `GET /admin/pools` status payload gains per-pool `drain_state` +
  `blocked_reason` (read-only in d1) and, **from d3**, `drain_paused`;
  mutation verbs land in d3/d4/d5.
- Metrics: `oceanfs_pool_draining{pool_id}`,
  `oceanfs_pool_drain_blocked_reason{pool_id}`,
  `oceanfs_pool_drain_state{pool_id}`.

## Data Flow

```
operator ──▶ (admin) begin_drain(pool_id)                    [d3/d4 call this too]
   ├─ validate (data role; not Dead; not already Draining)
   ├─ registry: status = Draining + DrainState::Draining
   ├─ rebuild NodeManifest ("draining" row) ──▶ set_self_manifest ──▶ gossip
   │     └─ peers: node excluded as repair/drain target when no healthy data pool
   └─ d3/d4 worker picks the pool up

placement: select_data_pool ──▶ excludes Draining (Healthy-only filter) ──▶ no new segments
reads:     resolve_pool(pool_id) ──▶ reads keep serving (status-agnostic read path)
health:    tick_pool(Draining) ──▶ decide_transition keeps Draining ──▶ monitor does not fight

worker blocked (no sibling/headroom) ──▶ set_drain_blocked(reason) ──▶ metric + status reason
worker emptied the pool            ──▶ set_pool_empty ──▶ DrainState::Detachable ──▶ d5 may detach
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in
      `oceanfs-storage`, `oceanfs-node`, `oceanfs-server`; the d0 baseline
      stays green for the touched crates.
<!-- REVIEW: verified 2026-09-08 — build --all-targets green for storage/membership/node/server; storage lib 487, membership lib 100, node lib 100, server lib 245 all pass under --test-threads=1; fmt/clippy --lib clean; rustdoc -D warnings clean. -->
- [x] **Tests:** `cargo test -p oceanfs-storage -p oceanfs-node
      -p oceanfs-server --lib -- --test-threads=1` passes; new tests cover
      every `pub` API path, and the scenario list in Scope is green,
      **including**: (1) placement excludes a `Draining` pool immediately;
      (2) reads serve from a draining pool (read-while-draining, both
      multi-pool and single-last-data-pool variants); (3) the health
      monitor leaves `Draining` untouched across degrading signals and
      clean windows; (4) manifest `"draining"` round-trips and the
      repair-target selector excludes a node whose only data pools drain;
      (5) **no-destructive-failure**: blocked drain keeps the pool
      `Draining` with the reason surfaced and deletes nothing; (6) metric
      set/clear behavior for `oceanfs_pool_drain_blocked_reason`.
      Integration test at the crate boundary exercises the full
      PUT → mark-draining → read-through-drain → blocked/no-delete
      scenario.
<!-- REVIEW: verified 2026-09-08 — scenarios (1)-(6) pinned by pool/placement.rs draining_pools_are_excluded, pool/health.rs (decide_transition + monitor tests), pool_manifest.rs draining_pool_maps_to_draining_status_string, repair.rs manifest_selector_excludes_nodes_with_only_draining_data_pools, pool/mod.rs metrics test (gauge 3 + blocked set/clear), and oceanfs-node/tests/pool_drain_state.rs (3 integration scenarios). Metric encoding decision: Draining = 3 via as_u8 (oceanfs_pool_status renders 3), NOT a separate oceanfs_pool_draining series. -->
- [x] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes (note: `PoolStatus` gains a third variant — check its rustdoc
      example table at `pool/mod.rs:125` and the manifest status-string
      doc at `oceanfs-membership/src/manifest.rs:89`).
<!-- REVIEW: verified 2026-09-08 — rustdoc with RUSTDOCFLAGS="-D warnings" clean for storage/membership/node/server; PoolStatus::Draining=3 example at pool/mod.rs:171, manifest status-string doc updated at membership/src/manifest.rs:47-48; DrainState/DrainStateError + all PoolRegistry drain methods + Node::begin_pool_drain carry # Examples. -->
- [x] **ADR:** ADR-0036 D1 (drain-state plumbing) + D6 (state machine,
      no-destructive-failure, detach-only-on-empty precondition, blocked
      reason surfaced) satisfied; ADR-0029 §D2/D3/D5 (pool status model,
      manifest carries state, routing on manifests) and ADR-0033
      (manifest-aware target selection excludes draining) satisfied.
<!-- REVIEW: verified 2026-09-08 — distinct PoolStatus::Draining (not a flag); placement excludes via Healthy-only filter with no policy change; reads status-agnostic (resolve_pool/read path untouched); health decide_transition Draining arm absorbing (health.rs:875-881) + mirror reconcile (health.rs:742-744); ConfirmedLoss beats Draining → Dead (unit + monitor tests); DrainState Idle/Draining/Detachable + blocked_reason + no-delete; data-role-only begin_drain (wal/meta/hints rejected); "draining" flows through manifest + routing_cache + repair selector existing "healthy"-only seams (ADR-0029 D2/D5, ADR-0033). -->
- [x] **Perf:** frontmatter `perf: []`; prose constraints followed:
      registry status stays an atomic (`StoragePool.status: AtomicU8`,
      `pool/mod.rs:235` — no lock on the read path); `drain_state` reads
      are lock-free/short-lock; manifest rebuild is once-per-change
      (`pool_manifest.rs` module doc, perf rule 2.4); the blocked-reason
      path is a rare admin/worker event.
<!-- REVIEW: verified 2026-09-08 — StoragePool.status AtomicU8 (pool/mod.rs:258), is_draining atomic-only (drain.rs:471-473), drain_state()/mutations short RwLock on drain map with documented lock order (drain.rs:32-40, mod.rs:851-860), manifest rebuilt once per transition (node.rs:618-629), no lock held across scoring (placement.rs perf note). -->
- [x] **Integration:** integration test at the node boundary proves a
      draining pool is excluded from placement while reads keep serving,
      the manifest re-gossips `"draining"`, and a blocked drain neither
      deletes nor half-removes. **No load suite is run locally** (PIPELINE
      §6).
<!-- REVIEW: verified 2026-09-08 — oceanfs-node/tests/pool_drain_state.rs 3/3 pass (read-through-drain + exclusion + manifest; blocked drain no-delete + admin JSON + metrics; single-last-data-pool zero-healthy-manifest). No load/e2e suites invoked. Remaining LOW notes (non-blocking): Deviations section below still says "None yet" although the four open-question resolutions are recorded only in the Implementation Report; Node::begin_pool_drain skips manifest_cache.update(self) unlike the f8/health seams; narrow begin_drain validation TOCTOU vs concurrent ConfirmedLoss→Dead; no direct pool_status_from_u8 unit test. -->


> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Resolved Decisions

The four questions posed under "Open Questions for the Implementer" in the
proposal are resolved (2026-09-08) and recorded in full in
[Deviations (accepted)](#deviations-accepted).

1. **Metric encoding for draining.** `Draining = 3` via `as_u8` — a single
   source of truth shared by the atomic status byte and the
   `oceanfs_pool_status` gauge; the separate `oceanfs_pool_draining` series
   was dropped. (Deviation 1.)
2. **Health monitor vs. genuine failure mid-drain.** Genuine `ConfirmedLoss`
   beats `Draining`: a confirmed-loss transition takes the pool
   `Draining → Dead`, the Dead status event is emitted, and the d3/d4 drain
   worker observes it and parks; degrading/clean signals remain absorbing.
   (Deviation 2.)
3. **`Draining` on the f5 sealer snapshot.** The sealer refreshes its
   data-pool snapshot from the live registry per seal; the all-draining
   `data_pools[0]` fallback is preserved; local write acceptance with no
   healthy pool is deferred to d4. (Deviation 3.)
4. **In-flight active segments on a newly-Draining pool.** Resolved by
   documentation, no code: the segment finishes and seals on the source
   pool; its registry entry keeps `pool_id == source` and the d3 worker
   relocates it like any other sealed segment. (Deviation 4.)

## Deviations (accepted)

Recorded 2026-09-08 after implementation review (PASS). Seven accepted
deviations/resolutions, each validated by the stakeholder before
implementation:

### 1. Metric encoding: `Draining = 3` via `as_u8`

Resolves the *metric encoding for draining* open question. Chose
`as_u8: Draining = 3` (single source of truth); the atomic status byte and
the `oceanfs_pool_status` gauge both render `3` while a pool drains. Help
text, the rustdoc table, and the metric-registration tests were updated. The
alternative separate `oceanfs_pool_draining` series was dropped as redundant
with the new `oceanfs_pool_drain_state{pool_id}` (0=Idle, 1=Draining,
2=Detachable) gauge. No automated consumer asserted the old 0/1/2 value set
(verified).

### 2. Health monitor vs. genuine failure mid-drain: `ConfirmedLoss` beats `Draining`

Resolves the *health monitor vs. genuine failure mid-drain* open question.
Genuine `ConfirmedLoss` beats `Draining`: `decide_transition(Draining, …)`
is absorbing for degrading/clean signals but returns `Dead` on confirmed
loss (ENOENT/EIO/device-unplug kinds); the monitor emits the Dead status
event so the manifest re-gossips `"dead"`; and the d3/d4 drain worker
observes the Dead transition and parks. `tick_pool` reconciles its mirror to
`Draining` when the registry status is `Draining`, so a stale
`Healthy`/`Degraded` mirror can never fight the operator's drain. A `Dead`
transition does not clear the drain record (harmless in d1; no worker
exists yet).

### 3. Sealer all-pools-draining fallback (deferred boundary)

Resolves the *`Draining` on the f5 sealer snapshot* open question. The f5
sealer refreshes its data-pool snapshot from the live registry **per seal**,
so a pool turning `Draining` between selections cannot receive a new segment
while a healthy sibling exists. When **every** data pool is draining,
`sealer.rs` still falls back to `data_pools[0]` (pre-existing behavior for
"no eligible pool"). **Deferred to d4**: d4 decides local write acceptance
while the last pool drains (d4 expects zero new local data mid-drain); d1's
scenarios avoid writes after the last pool drains, and placement exclusion
is pinned by tests while a healthy sibling exists.

### 4. In-flight active segment on a newly-Draining pool (documented, no code)

Resolves the *in-flight active segments on a newly-Draining pool* open
question. A segment that was reserving/appending when its pool turned
`Draining` cannot be mid-seal "cancelled" into another pool (its id/geometry
are bound); it finishes and seals, its registry entry has
`pool_id == source`, and the d3 worker relocates it like any other sealed
segment.

### 5. Node-level seam shape: `Node::begin_pool_drain`

d1 ships the drain mutation as a node-layer seam
`Node::begin_pool_drain(pool_id)` — registry `begin_drain` +
once-per-change manifest rebuild + `set_self_manifest` (re-gossip) — rather
than a new `on_pool_drain_change` callback field on the server `AdminState`.
An AdminState hook would be dead code in d1 because the HTTP drain-mutation
routes (`POST /admin/pools/{id}/drain`) only land in d3/d4; the read-only
`GET /admin/pools` status route ships as specified. Note for d3/d4:
`begin_pool_drain` does not update the local `ManifestCache` self-entry
(gossip reaches peers; local placement reads the registry) — align with the
f8/health seams when the worker-driven transition path is added.

### 6. `set_drain_blocked` / `set_pool_empty` return `Result<(), DrainStateError>`

Richer than the doc's untyped sketch: the two setters return
`Result<(), DrainStateError>` so workers distinguish unknown / not-draining /
Dead pools instead of silent no-ops.

### 7. Status invariant on `Detachable`

After `set_pool_empty`, the pool's status stays `Draining` (never back to
`Healthy`) so placement cannot silently refill a pool the operator is
removing; d5's detach removes the pool from the registry.
