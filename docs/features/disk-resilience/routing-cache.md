---
feature: "Storage Pools: Cached Routing State"
epic: "disk-resilience"
status: proposed
priority: medium
owner: ""
dependencies: ["data-pool-placement", "manifest-gossip"]
adr: [0029]
perf: [2.4, 7.2]
created: 2026-08-22
updated: 2026-08-22
---

# Storage Pools: Cached Routing State

## Summary

ADR-0029 §D5's peer-side view: a per-peer `NodeManifest` cache that routing
consumes as a *hint* — never a dependency. Populated from the gossip plane
(f6) and consulted by the read path (fetch strategy) and the write path
(replica target selection): avoid pools reported Degraded/Dead, avoid
`write_degraded` nodes, and fall through to the next replica on I/O error
regardless of what the cache said. Phase A: every manifest is Healthy, so
the cache is observationally neutral — but the structure, the error
fallback, and the metrics are in place for Phase B's status transitions.

## Scope

### In Scope

- `oceanfs-node` new `routing_cache.rs`:
  - `pub struct ManifestCache` — `ArcSwap`-backed map of node_id →
    `Arc<NodeManifest>` (perf 2.4: lock-free reads; the map is replaced
    wholesale on gossip-driven updates, never mutated in place);
    - `get(node_id) -> Option<Arc<NodeManifest>>` (missing = unknown peer;
      callers treat as "no pool info");
    - `update(node_id, Arc<NodeManifest>)` — called by the membership
      event handler on version-bumped entries (f6's `manifest_of` read);
    - `remove(node_id)` on Dead/evicted members.
  - Cache staleness policy: versioned by the entry's own version
    (ADR-0028); a stale-but-present manifest beats absent.
- Read-path hook (`fetch_strategy` LocalFirst, node.rs:361-362 config):
  - **Node-granular filter** (the manifest carries no range detail — ADR
    §D4): when selecting a replica node for a GET, exclude candidates whose
    manifest reports **zero Healthy data pools** (all `data` pools
    Degraded/Dead — the node cannot serve segment reads). Candidates with
    ≥1 Healthy data pool stay eligible; which pool serves the range is the
    node's local placement decision, not routing's.
  - Phase A: every manifest is Healthy, so the filter never excludes —
    observationally neutral. The filter function + the error-driven
    fallback loop structure are exercised by unit tests with synthetic
    manifests.
- Write-path hook (replica target selection for PUT):
  - exclude nodes whose manifest reports `write_degraded` or zero Healthy
    data pools (Phase A: never true; the filter runs and is unit-tested
    with synthetic manifests).
- Metrics: `oceanfs_routing_cache_misses_total` (get with no entry),
  `oceanfs_routing_failover_total` (error-driven fallback to next replica).
- Tests:
  - unit: get/update/remove; version-bumped update replaces wholesale;
    stale-but-present returns the last manifest;
  - unit (read): synthetic all-Dead-manifest candidate → excluded;
    ≥1-Healthy-pool candidate stays eligible; error-driven fallback picks
    the next replica; cache miss → no filter applied (fallback only);
  - unit (write): `write_degraded` node excluded; no-Healthy-pools node
    excluded;
  - integration: 3-node cluster — after convergence, each node's cache
    holds 3 manifests; a synthetic status flip on one node propagates and
    the read path route changes (via the f6 test harness's manifest
    injection).

### Out of Scope

- Health monitor driving real status transitions (Phase B).
- Loss announcements / reconciliation (Phase B).
- Capacity-aware vnode weighting (Phase C).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | New `routing_cache.rs`; read/write path hooks; metrics |

## Interface (Public API)

- `pub struct ManifestCache` — `get`, `update`, `remove`.
- `pub fn healthy_data_pools(manifest: &NodeManifest) -> usize` — helper
  used by both path hooks.
- `pub fn is_write_degraded(manifest: &NodeManifest) -> bool`.

## Data Flow

```
membership events (f6) ──▶ ManifestCache.update(node_id, manifest)
   read path ──▶ cache.get(candidate) ──▶ healthy_data_pools filter
   write path ──▶ cache.get(candidate) ──▶ is_write_degraded filter
   I/O error on a candidate ──▶ routing_failover_total++ ──▶ next replica
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-node`
- [ ] **Tests:** all listed green (cache ops, filters, failover, cluster
      propagation)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D5 (cached routing = hint, not dependency;
      failover on error) satisfied
- [ ] **Perf:** 2.4 (ArcSwap lock-free reads on the hot path), 7.2
      (read-only shared manifests; no lock in the read/write path)
- [ ] **Integration:** the 3-node cluster test asserts post-convergence
      caches match and that the synthetic status-flip changes routing (the
      epic DoD's "cached routing state" item)

## Deviations (accepted)

- **Read-path filter is range-agnostic in Phase A.** The manifest has no
  per-range detail (by design, ADR-0029 §D4); the filter works at
  node/pool granularity. Range-level refinement is Phase B's loss
  announcement consumer, not this feature.
