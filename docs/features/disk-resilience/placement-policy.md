---
feature: "Storage Pools: Placement Policy"
epic: "disk-resilience"
status: done
priority: high
owner: ""
dependencies: ["pool-runtime"]
adr: [0029]
perf: [1.3, 7.1, 9.3]
created: 2026-08-22
updated: 2026-08-22
---

# Storage Pools: Placement Policy

## Summary

The placement decision for ADR-0029 §D1/D8: given the node's data pools,
pick the pool a new segment is written to. Policy: role-aware (only
`data`-role pools are eligible for segment placement), weight-aware (a
`weight = 2` pool attracts ~2× the segments of `weight = 1`), and
least-free-capacity within the weighted budget. Pure logic over a
`PoolRegistry` snapshot — no I/O, no wiring to the sealer yet (that is f5).

## Scope

### In Scope

- New `oceanfs-storage::pool::placement` module:
  - `pub struct PlacementPolicy` — stateless; `PlacementPolicy::new()`
    (no knobs in Phase A — see deviations).
  - `pub fn select_data_pool(&self, registry: &PoolRegistry) -> Option<Arc<StoragePool>>`:
    - collect eligible pools: `role == Data`, `status == Healthy`,
      `write_degraded == false`, `free_bytes > min_free_headroom`
      (`MIN_FREE_HEADROOM_BYTES: u64 = 64 * 1024 * 1024`, a module const);
    - if none → `None` (f5 decides the fallback);
    - **selection = weighted least-free**: pick the eligible pool with the
      maximum `free_bytes / weight` (weight as resolved by f2, min 1).
      Rationale: a pool with `weight = 2` needs only half the free space of
      a `weight = 1` pool to be preferred — capacity-aware AND
      weight-aware in one monotone score;
    - deterministic: ties (equal score) break by smaller pool id.
  - `pub fn select_pinned_pool(registry: &PoolRegistry, role: PoolRole) -> Option<Arc<StoragePool>>` —
    for `wal`/`metadata`/`hints`: returns the cardinality-1 pool of that role
    if Healthy, else `None`. (f4 uses this to resolve each pinned path.)
- Tests (pure unit, no I/O):
  - only Data pools eligible (wal/metadata/hints never returned by
    `select_data_pool`);
  - Degraded/`write_degraded` pools excluded (status is Healthy-only in
    Phase A, but the filter is exercised via `set_status` stub);
  - weighted least-free: pool A `weight 1 / free 10 GiB` vs pool B
    `weight 2 / free 10 GiB` → A wins (10 GiB/weight vs 5 GiB/weight);
    <!-- REVIEW: corrected by review 2026-08-22 — the doc originally said
    "B wins (5 GiB/weight vs 10 GiB/weight)". That example contradicted the
    Scope rule text, the Interface, and this same doc's capacity test below
    (after sealing 15 GiB into B, A must win — under the min rule B would
    win, so the capacity flip requires max). Resolution: max free/weight is
    authoritative (normative rule text + Interface + capacity example all
    require it; "weight = 2 attracts ~2×" holds under max water-filling and
    fails under min, which starves high-weight pools). Implemented as
    `select_data_pool` (placement.rs:112-145), pinned by
    `weighted_selection_prefers_pool_with_more_free_per_weight`
    (placement.rs:246-258). No code flip needed. -->
  - capacity: pool A `weight 1 / free 10 GiB` vs pool B
    `weight 1 / free 20 GiB` → B wins; after sealing 15 GiB into B
    (simulated via registry capacity), A wins;
  - headroom: pool below `MIN_FREE_HEADROOM_BYTES` excluded even with the
    best score;
  - empty registry / all-excluded → `None`;
  - determinism: identical registry state → identical selection across
    repeated calls (and the tie-break case: equal score → lower pool id).

### Out of Scope

- Wiring into the sealer/segment store (f5).
- Capacity-aware vnode weighting at the ring level (Phase C).
- Drain/rebalance, hot-add policy interplay (Phase C / f8).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New `pool::placement` module |

## Interface (Public API)

- `pub struct PlacementPolicy` — stateless; `new()`.
- `pub fn select_data_pool(&self, registry: &PoolRegistry) -> Option<Arc<StoragePool>>` —
  weighted least-free (`max free_bytes / weight`), tie-break by pool id.
- `pub fn select_pinned_pool(registry: &PoolRegistry, role: PoolRole) -> Option<Arc<StoragePool>>`.

## Data Flow

```
sealer/f5: reserve segment ──▶ PlacementPolicy::select_data_pool(registry)
                                └─ (eligible data pools, weighted score)
                                   └─ Arc<StoragePool> root for the new segment
f4: metadata/wal/hints dirs ──▶ select_pinned_pool(registry, role) ──▶ root
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-storage`
      (verified by review: `cargo build --all-targets -p oceanfs-storage`
      clean; `cargo fmt --all -- --check` clean; `cargo clippy -p
      oceanfs-storage --lib -- -D warnings` clean; `cargo clippy -p
      oceanfs-node --tests -- -D warnings` clean)
- [x] **Tests:** every case above green (incl. Degraded exclusion via the
      f2 stub, determinism, headroom)
      (verified by review: 11 `pool::placement::tests` + 346 storage lib +
      43 doctests + 10 storage integration binaries + node
      `placement_policy` + `pool_registry` + 32 node lib — all green,
      `--test-threads=1`)
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
      (verified by review: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
      -p oceanfs-storage` clean; `# Examples` on all 4 pub items in
      `placement.rs` and on `set_pool_capacity`)
- [x] **ADR:** ADR-0029 §D1 (placement at pool granularity) + §D8
      (weight-aware, capacity-aware) satisfied
      (verified by review: role/weight/capacity-aware selection over the
      node's own pools; weights resolved by f2; no Ceph-OSD ring-membership
      / node-granular-only / probe-blind rejected alternatives re-implemented)
- [x] **Perf:** 1.3 (pre-sized candidate vec), 7.1 (single snapshot read of
      the registry; no lock held across the scoring loop — snapshot taken
      once, cloned Arcs), 9.3 (pool id comparison, no string work in the
      hot path)
      (verified by review: `Vec::with_capacity` at placement.rs:119; one
      `data_pools()` snapshot at placement.rs:116, scoring lock-free on
      cloned Arcs at placement.rs:131-143; integer-only `free/weight` score
      at placement.rs:133, `id()` comparison at placement.rs:137)
- [x] **Integration:** an `oceanfs-node` test constructs a 2-data-pool
      registry, seals several small segments through the existing
      `SegmentSealer` with the policy injected, and asserts the distribution
      lands on both pools (this exercises f2+f3 together; f5 completes the
      multi-root store)
      (verified by review: `cargo test -p oceanfs-node --test
      placement_policy -- --test-threads=1` — 1 passed; real
      `SegmentSealer`/`WalWriter`/`SegmentLifecycleCoordinator`/`EventWal`,
      `.dat` files land in the policy-selected roots, 8 seals distribute
      4/4)

## Deviations (accepted)

- **No operator-facing placement knob in Phase A.** The brainstorm's
  `weight_bias` blend parameter is dropped: `max free/weight` is a single
  deterministic, capacity- and weight-aware rule that needs no tuning.
  If fleet measurements show pathological skew, a knob can be added in
  Phase C without changing the selection contract.
- **f2 API amendment: `PoolRegistry::set_pool_capacity(id, total, free)`.**
  Added in f3 (pool/mod.rs:965) because the DoD integration test drives
  capacity evolution on a single test filesystem — both tempdir pool roots
  share one filesystem, so real `statvfs` cannot show per-pool deltas and
  the least-free flip cannot be observed. Mirrors the f2
  `set_status`/`set_write_degraded` shape (same no-op-on-unknown-id
  semantics, updates the metric gauges); documented for future
  drain/rebalance accounting (Phase C) and runtime pool attach (f8).
  <!-- REVIEW: recorded by review 2026-08-22 — the implementer flagged this
  in the Implementation Report (deviation #2) but the feature doc's
  Deviations section was not updated. Verified justified: without it the
  DoD integration test cannot assert distribution on both pools (the
  equal-capacity tie-break would send every seal to the lower pool id). -->
- **Doc correction: original weighted example contradicted the max-rule
  contract.** The draft's Scope example ("pool A `weight 1` vs pool B
  `weight 2`, both `free 10 GiB` → B wins") asserted the min-rule outcome.
  The normative rule text, the Interface, and the doc's own capacity
  example all require the max rule (`max free_bytes / weight`, so A wins,
  10 GiB/weight vs 5 GiB/weight), and the max rule is what makes
  "weight = 2 attracts ~2× the segments" hold under water-filling. The
  example in the Scope test list was corrected (inline REVIEW comment
  there, 2026-08-22); no code flip was needed — `select_data_pool`
  (placement.rs:112-145) and the pinning test
  `weighted_selection_prefers_pool_with_more_free_per_weight`
  (placement.rs:246-258) were already max-rule compliant.
