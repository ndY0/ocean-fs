---
feature: "Capacity-Weighted Ring Ownership (C2a) — OPTIONAL / LATE-STAGE"
epic: "disk-resilience-scale"
status: deferred
priority: medium
owner: ""
dependencies: ["d4-cluster-drain"]
adr: [0036, 0028, 0029]
perf: []
created: 2026-09-07
updated: 2026-09-09
---

# Capacity-Weighted Ring Ownership (C2a) — OPTIONAL / LATE-STAGE

> **Status: OPTIONAL and LATE-STAGE.** This feature is the last row of the
> `disk-resilience-scale` DAG and is explicitly **not committed**. ADR-0036
> D1 (Optional late-stage feature) says its difficulty is to be *measured
> when d1–d5 land*, and that it **may be deferred to the backlog epic
> `disk-resilience-capacity`** (which also carries C2b) if it proves
> disruptive to the freshly-reworked membership/ring path (ADR-0028
> territory). It is written at full feature depth so it is ready if
> attempted; the first act of the feature is the difficulty measurement,
> not code.
>
> **DEFERRED (2026-09-09):** the difficulty measurement is complete and the
> stakeholder decided to defer this feature (and C2a) to the backlog epic
> `disk-resilience-capacity`; only C2b will be considered later, gated on
> fleet/load-test data. See [Deferred (difficulty measurement,
> 2026-09-09)](#deferred-difficulty-measurement-2026-09-09) below.

## Summary

Make a node's **ring share track its healthy data-pool capacity** so the
topology operations d1–d5 enable (attach → grow, drain/detach → shrink,
retire → release) re-weight **ownership**, not just placement. Today the
ring is uniform — `RingConfig { vnodes_per_node }`
(`crates/oceanfs-core/src/config/ring.rs:18,20`) gives every node the same
share regardless of how much data-pool capacity it actually has; pool
weight/free capacity influence placement and repair-target choice only
(ADR-0036 Context). C2a derives a per-node weight from the gossiped
`NodeManifest` (`oceanfs-membership/src/manifest.rs:152`), plumbs it into
ring construction (`oceanfs-routing/src/ring.rs`), and requires
**deterministic convergence** across nodes (every node must build the same
weighted ring from the same membership view) plus **hysteresis** so
capacity jitter does not churn the ring. It deliberately does **not**
include proactive rebalance (C2b): new writes and repair targeting reclaim
freed capacity; the ring share only decides where *future* ownership
lands.

## Scope

### In Scope

- **Weight source (membership).** Derive a per-node ownership weight from
  the gossiped `NodeManifest` pools (ADR-0029 D2 — today
  `PoolManifest { id, role, status, write_degraded, capacity_free_bytes,
  weight }`, `oceanfs-membership/src/manifest.rs:38-54`):
  - candidates: **sum of healthy data-pool `weight`** (stable — `weight`
    is config or auto-derived from total capacity at pool creation,
    `pool/mod.rs` `auto_weight`; already on the wire, no manifest change),
    vs. **sum of total bytes of healthy data pools** (the semantically
    cleanest "capacity" but `total_bytes` is NOT currently gossiped — a
    manifest/proto addition), vs. **free capacity** (rejected for ring
    ownership: too volatile — see hysteresis).
  - Recommended default to evaluate first: sum of healthy data-pool
    `weight` (zero manifest/proto change; stable; proportional to total
    capacity under auto-weight). The sketch's open question (total vs.
    free vs. explicit weight) is a **decision point at feature start**.
  - Draining/degraded/dead pools are excluded from the weight (their node
    must shed share, not keep it).
- **Weighted ring construction (routing).** Replace the uniform
  `vnodes_per_node` with per-node vnode counts proportional to the derived
  weights (`oceanfs-routing/src/ring.rs` — vnode positions are hashed
  `[u8;32]` per (node, vnode index); only the *count* per node becomes
  weighted). A node with weight 0 (no healthy data pools) gets no vnodes.
  Ring construction remains a pure, deterministic function of the node set
  + weights so every node converges on the same ring (ADR-0028
  determinism).
- **Deterministic convergence across nodes.** All nodes derive the weight
  from the same `NodeManifest` gossip they already merge (ADR-0028 D3);
  the weighting rule must be a pure function of the *merged* membership
  view so transient gossip skew cannot produce divergent rings for the
  same view. Document the convergence rule and the stale-manifest window
  (weight changes propagate with the manifest version bump — same
  mechanism as f6/f7).
- **Hysteresis (anti-churn).** Free-capacity jitter must not re-weight the
  ring every gossip round. If weight derives from a stable signal (pool
  weight / total capacity), jitter is already bounded; add a minimum
  change threshold (e.g. re-weight only when the derived share moves by
  ≥ X% or when the healthy data-pool set changes) so attach/drain/detach
  events — not capacity refresh — drive re-weighting. Ring changes only on
  membership/topology events, never on per-tick capacity refresh.
- **Interaction with d1–d5.** After d5 detach / d4 drain, the freed node
  sheds ring share; newly attached pools (f8) grow it. This is what makes
  the "freed capacity reclaimed by new writes" interim behavior (ADR-0036
  Negative) eventually automatic for *ownership* — without C2b there is
  still no movement of existing healthy data to the ideal.
- **Difficulty measurement (the feature's first deliverable).** Before any
  code: assess blast radius against the freshly-reworked membership/ring
  path (ADR-0028 territory) and the routing consumers (write-path replica
  selection, read routing, repair-target ring cache, g8 metadata-loss
  row-rebuild over owned ranges). If the assessment shows disruption
  beyond a bounded change, **defer to the backlog `disk-resilience-capacity`
  epic** (which also carries C2b) and record the deferral — deferral is a
  *successful outcome* of this feature's measurement step.
- Tests:
  - unit: weight derivation (healthy data pools only; draining/dead
    excluded; weight 0 node gets no vnodes);
  - unit: ring construction is deterministic for the same (node set,
    weights) input and reproduces across independently-built instances;
  - unit: share proportionality — a node with 2× the data-pool weight of
    another receives ~2× vnodes (statistical bound over the hash ring);
  - unit: hysteresis — a small capacity/weight change below the threshold
    does not rebuild the ring; an attach/detach event does;
  - integration (local 3-node, heterogeneous pools): nodes with different
    data-pool capacities converge on the same weighted ring; detach one
    node's pool (after d5) and observe its share drop; new writes land on
    the re-weighted ownership without moving existing data (no C2b).

### Out of Scope (for this feature)

- **Proactive rebalance (C2b)** — moving existing healthy segments to
  chase the ideal distribution. Explicitly backlog
  (`disk-resilience-capacity`), per ADR-0036 D1. C2a only re-weights
  *future* ownership.
- Drain/detach/relocation mechanics — d1–d5; C2a consumes their topology
  effects as weight inputs.
- Placement and repair-target weighting — already capacity-aware via
  manifests (ADR-0033); C2a changes the ring, not the placement score.
- Graceful-leave redesign, segment self-description (C3),
  migration-plane isolation (ADR-0030 D4) — epic non-goals.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-membership` | Weight source derivation over merged `NodeManifest`s (or manifest addition of `total_bytes` if that option is chosen) |
| `oceanfs-routing` | Weighted ring construction in `oceanfs-routing/src/ring.rs`; `Ring`/`RingConfig` API for per-node vnode counts |
| `oceanfs-node` | Weight derivation wiring (healthy data-pool aggregate from the node's own registry/manifest); ring rebuild triggers on topology change |
| `oceanfs-core` | `RingConfig` extension only if a weighted mode config key is needed (e.g. `ring_weighting = "uniform" \| "capacity"` with hysteresis threshold) |
| `proto` | Only if the `total_bytes` weight source is chosen (PoolManifest addition) |

## Interface (Public API)

- `pub fn derive_node_ring_weight(manifest: &NodeManifest) -> u64` — sum of
  healthy data-pool weights (or chosen source), membership-side, pure.
- `Ring::new_weighted(nodes: &[(NodeId, u64 /* weight */)],
  base_vnodes: u32, …)` (or a weighted-mode extension of the existing
  `Ring::new(RingConfig)`) — replaces uniform `vnodes_per_node`
  (`crates/oceanfs-core/src/config/ring.rs:18`). 
  `<!-- TODO(spec): verify anchor -->` the exact current ring construction
  signature is in `crates/oceanfs-routing/src/ring.rs`; confirm how the
  membership layer builds the ring from node lists today and where the
  per-node count argument plugs in.
- `RingConfig` gains (if the config route is chosen) a weighting-mode enum
  + hysteresis threshold (or the threshold lives with the node's ring
  builder).
- New metric: `oceanfs_ring_share{node_id}` (gauge, 0..1) for
  observability of the re-weighted ownership.

## Data Flow

```
NodeManifest gossip (per-node pools; healthy data-pool weights)
   └─ merge (ADR-0028 D3) ──▶ membership view
   └─ derive_node_ring_weight per node (healthy data pools only)
   └─ hysteresis check (change ≥ threshold OR topology event)
        └─ Ring::new_weighted(nodes, weights) ──▶ deterministic weighted vnode ring
             └─ write replica selection / read routing / repair ring cache / g8 owned-range rebuild
topology events (attach f8 / drain d1–d4 / detach d5) ──▶ weight change ──▶ ring rebuild
no C2b: existing healthy data is NOT moved; new writes + repair reclaim freed capacity
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in
      `oceanfs-membership`, `oceanfs-routing`, `oceanfs-node` (+
      `oceanfs-core` / proto if the config or manifest route is chosen).
      **Gate: the difficulty measurement completed first** — the
      assessment of the ADR-0028 membership/ring path and the routing
      consumers is recorded in the feature's Deviations section, with the
      go/defer verdict. A deferral to `disk-resilience-capacity` closes
      this feature as "deferred", not "done".
- [ ] **Tests:** `cargo test -p oceanfs-membership -p oceanfs-routing -p
      oceanfs-node --lib -- --test-threads=1` passes; the Scope scenario
      list is green — including deterministic ring convergence across
      independently-built instances, share proportionality, hysteresis
      (capacity jitter does not rebuild the ring; topology events do), and
      the heterogeneous-node integration scenario with no data movement.
- [ ] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the chosen weight-source semantics (and the 
      "ring share ≠ stamped `storage_locations`" note) are documented on
      the derivation function.
- [ ] **ADR:** ADR-0036 D1 (C2a optional-late; deferral path to
      `disk-resilience-capacity`) satisfied; ADR-0028 (membership/ring
      determinism and convergence — the weighted ring must stay a pure
      function of the merged view) satisfied; ADR-0029 §D2/D5 (manifest as
      the weight source; routing on manifests) satisfied.
- [ ] **Perf:** frontmatter `perf: []`; prose constraints: weight
      derivation is a rare topology/gossip-change computation (no per-read
      cost); ring rebuild is bounded to topology events by hysteresis; the
      ring stays an immutable snapshot swapped on change (perf rules 2.4 /
      7.2 — reads never block on rebuild).
- [ ] **Integration:** integration test at the cluster boundary proves
      heterogeneous nodes converge on the same weighted ring and that a
      d5-style detach re-weights ownership without moving existing data
      (no C2b rebalance). **No load suite is run locally** (PIPELINE §6).

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **Weight semantics (sketch's open question).** Total capacity vs. free
  capacity vs. explicit pool weight. This doc's default: sum of healthy
  data-pool `weight` — stable, already on the wire
  (`PoolManifest.weight`), proportional to total capacity under
  auto-weight (`auto_weight`, `pool/mod.rs:672`). Free capacity is
  rejected for *ownership* (volatile, needs hysteresis to compensate);
  total bytes is semantically cleanest but needs a manifest/proto addition
  (`total_bytes` is not currently gossiped). Decide at feature start.
- **Hysteresis threshold semantics.** Re-weight only on topology events
  vs. also on gradual capacity drift; pick the threshold (absolute share
  delta vs. relative weight delta) that prevents churn without
  under-reacting to real shrink. Calibration belongs to the measurement
  step.
- **Interaction with stamped `storage_locations`.** Ring share ≠ current
  placement: after a re-weight, existing segments stay where their holder
  sets say until heal/write traffic rebalances. Rebalance-free acceptance
  (new writes/repairs reclaim freed capacity slowly) is the documented
  interim behavior — confirm it is acceptable to the operator model, or
  escalate C2b.
- **Ring rebuild on the write path.** The write-path replica set derives
  from the ring; a re-weight changes replica selection for *new* segments
  immediately. Confirm the coordinator/fetch path tolerates the
  membership-view skew window during convergence (the existing stale-cache
  failover path covers it — ADR-0029 §D5).

## Deferred (difficulty measurement, 2026-09-09)

The difficulty measurement — this feature's first deliverable — is complete,
and the stakeholder has decided to **DEFER** d6. Per ADR-0036 D1, deferral is
a **successful outcome** of the measurement step, not a failure: the
assessment of the ADR-0028 membership/ring path and its routing consumers
found disruption beyond a bounded change (findings below), so C2a moves to
the backlog epic `disk-resilience-capacity` (which already carries C2b).

### C2a-vs-C2b framing (decision deferred)

C2a and C2b are judged to be **the same redistribution functionality at two
different granularities**, so **only one of the two will ever be built**:
C2a re-weights the ring so future ownership lands by capacity; C2b
additionally moves existing healthy segments to chase the ideal. The
stakeholder chose to consider **C2b only, LATER** — and that C2b/C2a decision
is **gated on the pending fleet/load-test data**. Today's interim behavior
(placement and repair-target selection are already capacity-aware via
gossiped manifests, ADR-0033; attach → drain → detach steers future writes by
free-space) stands until that decision is made from test data. The scope
content above is retained intact as the record for when/if the C2b attempt is
made.

### Measurement findings (grounded at HEAD)

1. **The ring is the metadata-discovery mechanism for EVERY existing key —
   there is no second index.** Writes place object-metadata rows on the
   `ring.lookup(hash(bucket/key))` successors
   (`crates/oceanfs-server/src/write/coordinator.rs:474`); reads find rows on
   the same ring set (`read/coordinator.rs:738`, `read/coordinator.rs:1054`;
   "ring replicas hold the object's metadata by construction",
   `read/coordinator.rs:716-718`).
2. **Ring membership is deliberately quasi-static.** Nodes are added only at
   admission (empty) and removed only on graceful `Left` after drain; **DEAD
   nodes STAY in the ring** (`membership/manager.rs:1214-1222`, the
   churn-divergence comment). Ring changes are lossless only at empty
   boundaries.
3. **There is NO ownership/row-migration machinery.** Capacity re-weighting a
   live ring moves arcs; keys whose old RF holder set leaves the new
   successor window become undiscoverable — their metadata rows are on nodes
   the new ring no longer queries. Drain-to-zero + detach → weight 0 orphans
   the node's whole owned arc while its rows are still on it. Making ring
   share track capacity is therefore **ownership redistribution = C2b-class
   machinery** (metadata row migration/chase) or a data-plane redesign —
   precisely the ADR-0028 disruption the ADR-0036 D1 deferral clause
   anticipates.
4. **Placement and repair-target selection are ALREADY capacity-aware** via
   gossiped manifests (ADR-0033). Today's attach → drain → detach steers
   future writes by free-space; that interim behavior stands until a C2b
   decision is made from test data.

**Verdict: DEFER.** No C2a code in this epic. The feature doc above remains
the record for the future C2b attempt (backlog `disk-resilience-capacity`).

## Deviations (accepted)

The accepted outcome of the measurement step is the **deferral recorded in
[Deferred (difficulty measurement, 2026-09-09)](#deferred-difficulty-measurement-2026-09-09)
above** — the go/defer verdict of the difficulty measurement is resolved as
DEFER, with the difficulty findings documented there. **Deferral to the
backlog `disk-resilience-capacity` epic is an accepted outcome of this
feature's measurement step**, not a failure. No code deviations exist (no
code was written). Remaining expected-deviation candidates apply only to the
future C2b attempt, should it happen: the weight-source choice, the
hysteresis rule, and a manifest/proto `total_bytes` addition if that option
is chosen.
