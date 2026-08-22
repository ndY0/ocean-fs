---
feature: "Storage Pools: Runtime Attach (Admin API)"
epic: "disk-resilience"
status: proposed
priority: high
owner: ""
dependencies: ["pool-runtime", "manifest-gossip"]
adr: [0029]
perf: [7.1]
created: 2026-08-22
updated: 2026-08-22
---

# Storage Pools: Runtime Attach (Admin API)

## Summary

ADR-0029 §D8's operator requirement: **adding a pool must not require a
restart.** A node admin endpoint (`POST /admin/pools`) accepts a new pool
definition, probes the root (write+read), registers it into the live
`PoolRegistry`, rebuilds + re-gossips the `NodeManifest` (f6), and lets
placement start filling it (f3/f5). No restart anywhere in the path. The
same endpoint serves hot-swapped devices (a re-inserted disk attaches as a
new pool).

## Scope

### In Scope

- `oceanfs-node` admin route `POST /admin/pools` (the existing admin HTTP
  surface — `/admin/health`, `/admin/ring` already exist on the node):
  - request body: `PoolConfig` (name, role, root, weight, tech, health);
  - steps: (1) `StorageConfig::validate` on the single pool (f1 rules:
    unique name/root, one-root, role cardinality vs the live registry);
    (2) probe the root (f2's write+read probe; `MissingRootPolicy` —
    the admin path uses the node's configured policy);
    (3) `PoolRegistry::attach(pool) -> u32` (assigns the next id,
    registers under the registry lock — a short `parking_lot::Mutex`
    critical section, perf 7.1);
    (4) rebuild `NodeManifest` + `Membership::set_self_manifest` (f6) so
    peers see the new capacity;
    (5) respond `201 { "pool_id": n }`.
  - response codes: `201` attached; `400` validation failure (with the
    specific rule violated); `409` duplicate name/root; `500` probe
    failure.
- `PoolRegistry::attach(&self, pool: PoolConfig) -> Result<u32, String>`
  — extends f2's registry (boot-time-only construction becomes
  attach-capable; the registry keeps the same RwLock).
- Placement integration: after attach, `PlacementPolicy::select_data_pool`
  sees the new pool immediately (it reads the registry snapshot — no
  caching of the pool list in the policy).
- Hot-swap path: the same endpoint with an existing root re-attaches a
  revived pool (Phase A: attach of a previously-dead pool is treated as a
  new pool with a fresh id; Phase B defines revival semantics).
- Tests:
  - unit (registry): attach assigns sequential ids; duplicate name/root
    rejected; cardinality enforced against live pools;
  - unit (policy): after attach, selection may target the new pool
    (weight/free dependent);
  - integration (local e2e): boot a 1-data-pool node → `POST /admin/pools`
    with a second data pool → manifest on peers gains a 5th pool (4→5
    in the 4-pool template) → new segments land on both roots → node never
    restarts.

### Out of Scope

- Detach/drain/rebalance (Phase C).
- Device hot-remove detection (Phase B — the health monitor notices a dead
  pool; the operator's attach endpoint is the *add* side only).
- Capacity-aware vnode reweighting on attach (Phase C).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | Admin route + attach wiring |
| `oceanfs-storage` | `PoolRegistry::attach` |

## Interface (Public API)

- `POST /admin/pools` — request `PoolConfig` JSON; response
  `201 {"pool_id": u32}`.
- `PoolRegistry::attach(&self, pool: PoolConfig) -> Result<u32, String>`.
- `PoolRegistry::pool_count(&self) -> usize` (admin/observability).

## Data Flow

```
operator ──▶ POST /admin/pools {PoolConfig}
   ├─ validate (f1) ──▶ 400/409
   ├─ probe root (f2) ──▶ 500
   ├─ attach → pool_id (registry lock)
   ├─ rebuild NodeManifest ──▶ set_self_manifest (f6) ──▶ gossip
   └─ 201 {pool_id}
placement (f3/f5) ──▶ next segment may target the new pool
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-node`,
      `oceanfs-storage`
- [ ] **Tests:** unit (attach semantics, conflicts, cardinality) +
      integration (attach under load, manifest propagation, placement
      spread, no restart)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D8 runtime pool attach (no restart) satisfied
- [ ] **Perf:** 7.1 (attach is a rare admin op; the registry lock is held
      only for registration, never during placement reads)
- [ ] **Integration:** the epic DoD's runtime-attach item — a node gains a
      pool mid-run and the cluster observes the manifest change

## Deviations (accepted)

- **Admin endpoint is per-node, not cluster-wide.** An operator attaches a
  pool to a specific node by addressing that node (consistent with the
  per-node topology config model, ADR-0029 §D8). A fleet-level orchestration
  surface is out of scope for this project.
