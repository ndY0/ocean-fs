---
feature: "Pool Drain State (Registry Draining + Observability)"
epic: "disk-resilience-scale"
status: proposed
priority: high
owner: ""
dependencies: ["d0-regression-gate"]
adr: [0036, 0029, 0033]
perf: []
created: 2026-09-07
updated: 2026-09-07
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
    records `DrainState::Draining { blocked_reason: None }`;
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
- `pub enum DrainState { Idle, Draining { blocked_reason: Option<String> },
  Detachable }` — the operator-visible drain record (ADR-0036 D6 machine:
  Idle → Draining → Detachable, BLOCKED = Draining + reason).
- `PoolRegistry::begin_drain(&self, pool_id: u32) -> Result<(), DrainStateError>`
  — validates + flips status to `Draining` + records state.
- `PoolRegistry::set_drain_blocked(&self, pool_id: u32, reason: Option<&str>)`
  — no-destructive-failure surfacing.
- `PoolRegistry::set_pool_empty(&self, pool_id: u32)` — Draining → Detachable
  (called by d3/d4 workers when the registry holds no segment with this
  `pool_id`).
- `PoolRegistry::drain_state(&self, pool_id: u32) -> DrainState`.
- `PoolRegistry::is_draining(&self, pool_id: u32) -> bool` (or a
  `StoragePool::status()` read — a Draining pool reports the new variant).
- `GET /admin/pools` status payload gains per-pool `drain_state` +
  `blocked_reason` (read-only; mutation verbs land in d4/d5).
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

- [ ] **Code:** `cargo build --all-targets` succeeds in
      `oceanfs-storage`, `oceanfs-node`, `oceanfs-server`; the d0 baseline
      stays green for the touched crates.
- [ ] **Tests:** `cargo test -p oceanfs-storage -p oceanfs-node
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
- [ ] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes (note: `PoolStatus` gains a third variant — check its rustdoc
      example table at `pool/mod.rs:125` and the manifest status-string
      doc at `oceanfs-membership/src/manifest.rs:89`).
- [ ] **ADR:** ADR-0036 D1 (drain-state plumbing) + D6 (state machine,
      no-destructive-failure, detach-only-on-empty precondition, blocked
      reason surfaced) satisfied; ADR-0029 §D2/D3/D5 (pool status model,
      manifest carries state, routing on manifests) and ADR-0033
      (manifest-aware target selection excludes draining) satisfied.
- [ ] **Perf:** frontmatter `perf: []`; prose constraints followed:
      registry status stays an atomic (`StoragePool.status: AtomicU8`,
      `pool/mod.rs:235` — no lock on the read path); `drain_state` reads
      are lock-free/short-lock; manifest rebuild is once-per-change
      (`pool_manifest.rs` module doc, perf rule 2.4); the blocked-reason
      path is a rare admin/worker event.
- [ ] **Integration:** integration test at the node boundary proves a
      draining pool is excluded from placement while reads keep serving,
      the manifest re-gossips `"draining"`, and a blocked drain neither
      deletes nor half-removes. **No load suite is run locally** (PIPELINE
      §6).

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **Health monitor vs. genuine failure mid-drain.** The sketch settles
  "monitor never transitions a Draining pool to Degraded/Dead *from
  drain-induced activity*", but leaves open: if a draining disk genuinely
  confirms loss (ENOENT on a held segment, fsync EIO — the `ConfirmedLoss`
  kinds at `pool/health.rs:281-289`) mid-drain, should the pool go Dead
  (drain aborts, loss-announcement/heal path takes over — the operator's
  disk was dying anyway) or stay `Draining` and let the drain block on
  unreadable segments? Recommend: genuine `ConfirmedLoss` beats `Draining`
  (do not mask a real loss behind an operator flag), the drain worker
  observes the Dead transition and parks; record the resolution in the
  Deviations section.
- **Metric encoding for draining.** `oceanfs_pool_status` is 0/1/2 today;
  either add `Draining=3` to `as_u8` (single source of truth, but changes
  an existing gauge's value set) or add a separate
  `oceanfs_pool_draining` series. Both are defensible; pick one and record
  it (the DoD "Tests" bullet includes the chosen encoding).
- **`Draining` on the f5 sealer snapshot.** Placement reads the registry
  per seal, but the sealer may hold a `Vec<Arc<StoragePool>>` snapshot
  between selections; confirm a pool that becomes `Draining` between the
  snapshot and the reservation cannot receive a new segment (worst case: a
  sealed segment lands on the draining pool and is simply relocated by d3
  later — correct, but wasteful; note whether the snapshot is refreshed
  per seal).
- **In-flight active segments on a newly-Draining pool.** A segment that
  was reserving/appending when the pool turned `Draining` cannot be
  mid-seal "cancelled" into another pool (its id/geometry are bound to the
  pool). Recommended resolution (surface, do not silently decide): let it
  finish and seal; the registry entry then has `pool_id == source` and the
  d3 worker relocates it like any other sealed segment. Document the
  recommended behavior in the Deviations section once confirmed.

## Deviations (accepted)

None yet — this document is proposed. Expected-deviation candidates from
the sketch's open questions: the health-monitor-vs-genuine-failure rule,
the draining metric encoding, the sealer-snapshot caveat, and the
in-flight-active-segment rule. Record each here with its resolution when
the feature is implemented.
