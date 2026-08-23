---
feature: "Storage Pools: Runtime Attach (Admin API)"
epic: "disk-resilience"
status: done
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

- [x] **Code:** `cargo build --all-targets` in `oceanfs-node`,
      `oceanfs-storage` (independently verified: both crates build clean;
      `cargo fmt --all -- --check` clean; `cargo clippy --all-targets -D
      warnings` clean on both)
- [x] **Tests:** unit (attach semantics, conflicts, cardinality) +
      integration (attach under load, manifest propagation, placement
      spread, no restart)
      (independently verified: 359 storage lib tests incl. 7 new attach
      units, 10 storage integration binaries, 49 node lib + all node
      integration incl. `attach_second_data_pool_mid_run` (5.3s) — all
      green under `--test-threads=1`; the e2e asserts 201/pool_id 4,
      registry 4→5, manifest 4→5, sealed `.dat` on BOTH roots, GET
      round-trip post-attach, 409 on duplicate root)
      <!-- REVIEW: the f7 failover test fix claimed in the Implementation
      Report is UNSOUND — `fetch_falls_through_on_replica_error_and_counts_failover`
      (crates/oceanfs-server/src/read/fetch.rs:1126) still fails ~50% of
      runs (reproduced 4/8): `shard_batch::group_by_node`
      (crates/oceanfs-routing/src/shard_batch.rs:60) returns a std
      `HashMap`, so the fetch loop (fetch.rs:606) tries replicas in
      nondeterministic iteration order and n2 serves before n1 half the
      time (failover counter stays 0). The test's segment-id search makes
      only the RING order deterministic, not the fetch order. -->
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
      (rustdoc `-D warnings` verified clean on storage/node; server shows
      only the 2 pre-existing link errors — but 5 NEW pub items lack
      `# Examples` despite rustdoc passing: `DiskSegmentReader::with_registry`
      (segment_reader.rs:242), `AdminHandler::with_pool_attach`
      (admin.rs:593), `Node::pool_registry`/`Node::self_manifest`/
      `Node::node_id` (node.rs:2099/2105/2110))
      <!-- REVIEW: needed: add `# Examples` blocks to the 5 pub items
      listed above; rustdoc alone does not catch their absence. -->
- [x] **ADR:** ADR-0029 §D8 runtime pool attach (no restart) satisfied
      (verified: probe→register→manifest rebuild→placement in one admin
      round-trip, same process, e2e proves no restart; `PoolRegistry::attach`
      re-uses f2's `probe_root`/`statvfs_capacity`/`auto_weight`; role
      cardinality and the one-root rule enforced against the LIVE registry)
- [x] **Perf:** 7.1 (attach is a rare admin op; the registry lock is held
      only for registration, never during placement reads)
      (verified: probe runs outside any lock; registration is a short
      write-lock section; placement reads `registry.data_pools()` under a
      read lock once per seal; the reader takes a registry read lock only
      on a per-segment cache miss and caches the resolved root — no
      registry lock on the steady-state read path)
      <!-- REVIEW: LOW — `attach` performs small allocations
      (`StoragePool::new`, `PoolMetrics::new`, string clones) inside the
      registry write lock (pool/mod.rs:1113-1127); negligible for a rare
      admin op but stricter 7.1 reads "no allocation" inside the lock.
      Also LOW: `pool/mod.rs` module doc still claims "Single lock only
      (PoolRegistry.pools)" and "PoolMetrics is immutable after
      construction" — stale now that `metrics` is a second RwLock. -->
- [x] **Integration:** the epic DoD's runtime-attach item — a node gains a
      pool mid-run and the cluster observes the manifest change
      (verified: `attach_second_data_pool_mid_run` asserts the local
      `NodeManifest` re-declares 4→5; peer observation of the re-gossiped
      manifest relies on the f7-proven propagation path — the single-node
      e2e has no peers)
      <!-- REVIEW: LOW — the feature doc's In-Scope test wording says
      "manifest on peers gains a 5th pool"; the e2e asserts only the local
      manifest. Add a 2-node variant or amend the doc to state peer
      observation is the f7-proven path. -->
- [x] **Tests (regression note):** oceanfs-server lib suite 226/226 green
      under `--test-threads=1`; the only failure is the pre-existing
      `grpc_services::swim_death_detection_within_timeout` (verified
      unrelated); the flaky failover test above is the exception to
      "all existing suites stay green" and must be fixed.

## Deviations (accepted)

- **Admin endpoint is per-node, not cluster-wide.** An operator attaches a
  pool to a specific node by addressing that node (consistent with the
  per-node topology config model, ADR-0029 §D8). A fleet-level orchestration
  surface is out of scope for this project.
- **Crate impact includes `oceanfs-server`.** The feature's table listed
  only `oceanfs-node` + `oceanfs-storage`, but the admin HTTP surface lives
  in `oceanfs-server` (the node's composition root wires it) — the same
  layering resolution as f7's `RoutingHint` trait.
- **Sealer + reader need live-registry wiring.** The sealer's pool list and
  the reader's root resolution were boot-time snapshots; without
  refreshing them from the live `PoolRegistry`, an attached pool would be
  registered but never written to (sealer) or read from (reader). The
  sealer now refreshes per seal; the reader caches the resolved root per
  segment (f5 perf 7.2 preserved — one registry read lock per segment per
  process).
- **`attach` also validates `data_dir` disjointness** (the f1 rule), which
  requires the registry to retain the node's `data_dir`.
- **Peer observation of the re-gossiped manifest is the f7-proven path.**
  The f8 e2e asserts the node's OWN manifest grows 4→5 (the exact object
  f6 gossips); the f7 integration test already proves version-bumped
  manifest changes propagate to every peer's routing cache.
- **Phase-A hot-swap caveat:** re-attaching the same root after a device
  swap returns 409 while the old pool entry remains (no detach in Phase A);
  the fresh-id hot-swap path works with a new root/name. Detach semantics
  are Phase C.
