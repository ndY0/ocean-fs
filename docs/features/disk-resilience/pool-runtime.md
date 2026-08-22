---
feature: "Storage Pools: Pool Runtime + Registry"
epic: "disk-resilience"
status: done
priority: high
owner: ""
dependencies: ["pool-config"]
adr: [0029]
perf: [2.3, 2.4, 7.2]
created: 2026-08-22
updated: 2026-08-22
---

# Storage Pools: Pool Runtime + Registry

## Summary

The runtime heart of ADR-0029's pool model: a `StoragePool` (root, role,
weight, tech, status, capacity) and a `PoolRegistry` that owns the node's
pool set, probes roots at startup (write+read), reports per-pool metrics,
and serves the lookup API the rest of the data plane consumes. Legacy mode
(no pools configured) yields a single implicit pool rooted at `data_dir`, so
nothing downstream changes behavior.

## Scope

### In Scope

- New `oceanfs-storage::pool` module:
  - `struct StoragePool` — `id: u32` (stable, config-order index), `name`,
    `role`, `root: PathBuf`, `weight: u32` (resolved: config value or
    auto-derived, see below), `tech` (resolved from `Auto`), `status:
    PoolStatus` (Healthy|Degraded|Dead — Phase A: always Healthy),
    `write_degraded: bool` (Phase A: always false), capacity snapshot
    (`total_bytes`, `free_bytes` from `statvfs`).
  - `struct PoolRegistry` — the node's pool set behind
    `parking_lot::RwLock` (reads dominate: placement + routing lookup each
    request; perf 2.3/7.2). API:
    - `from_config(storage: &StorageConfig, data_dir: &Path) -> Result<PoolRegistry, String>`
      (legacy: one implicit `data` pool at `data_dir` with weight 1; explicit:
      one `StoragePool` per `PoolConfig`);
    - `pools(&self) -> Vec<Arc<StoragePool>>` (snapshot copy);
    - `pool_by_id(u32) -> Option<Arc<StoragePool>>`; `pool_by_role(PoolRole) -> Option<Arc<StoragePool>>` (first match; wal/metadata/hints are cardinality-1);
    - `data_pools(&self) -> Vec<Arc<StoragePool>>`;
    - `refresh_capacity(&self)` — re-statvfs each pool (called by the node's
      periodic maintenance task, not per-request);
    - `set_status(id, PoolStatus)` / `set_write_degraded(id, bool)` — stubs
      that Phase B's health monitor drives; Phase A: never called.
  - Weight resolution: explicit `weight` wins; `None` → auto-detect from
    `statvfs` total bytes scaled to a unit (`total / 1 GiB`, min 1). Tech
    resolution: `Auto` → `Nvme` placeholder (documented; real detection in
    Phase B).
- Startup probing: `from_config` performs a write+read probe in each pool
  root (`create_dir_all` → write a `.probe` temp file → fsync → read back →
  remove). On failure:
  - `MissingRootPolicy::Fatal` (default) → `Err(...)` (node refuses to
    start, mirroring today's `create_dir_all` failure at node.rs:2015-2016);
  - `Degraded` → pool registered with `status = Degraded` + warn log
    (Phase A note: a Degraded pool at startup is treated as Healthy by
    placement since Phase B is not wired; the status field carries the
    signal).
- Metrics (registered once at `PoolRegistry::from_config` via the node's
  existing metrics registry — the same `metrics` pattern the durability
  counters use, e.g. `hinted_handoff_hints_stored_total`):
  - `oceanfs_pool_status{pool_id, role}` (0=Healthy 1=Degraded 2=Dead);
  - `oceanfs_pool_bytes_free{pool_id}` + `oceanfs_pool_bytes_total{pool_id}`;
  - `oceanfs_pool_io_errors_total{pool_id}` (Phase A: 0 counter exists,
    incremented by nothing yet — Phase B's `DiskIo` fault path feeds it).
- Tests (all in `oceanfs-storage`):
  - legacy mode: no pools → exactly one pool, root == data_dir, role Data;
  - explicit mode: 4 pools parsed, ids 0..3, cardinality enforced at
    construction (re-validate via `StorageConfig::validate`);
  - probe success/failure: tempdir probe OK; non-existent root with
    `Fatal` errors; with `Degraded` succeeds with status Degraded;
  - weight resolution: explicit weight kept; auto weight = max(1, total/GiB);
  - capacity refresh: write a file into a pool root, `refresh_capacity`
    reflects the change;
  - registry lookup: by id, by role, data_pools ordering stable.

### Out of Scope

- Placement selection logic (f3) — the registry only lists pools.
- Role-pinned path wiring in the node (f4).
- Health monitor, status transitions, `DiskIo` abstraction (Phase B).
- Runtime attach (f8) — registry is constructed once in Phase A.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New `pool` module: `StoragePool`, `PoolRegistry`, metrics registration |
| `oceanfs-core` | (already from f1) `PoolStatus` lives here? No — `PoolStatus` in `oceanfs-storage::pool` (data-plane concern; the gossip wire re-encodes it in f6). |

## Interface (Public API)

- `pub struct StoragePool` — see above; `Arc<StoragePool>` shared read-only.
- `pub struct PoolRegistry` — as above.
- `PoolRegistry::from_config(...)` — construction incl. startup probing.
- `PoolRegistry::refresh_capacity(&self)` — periodic statvfs refresh.
- `PoolRegistry::set_status(...)` / `set_write_degraded(...)` — Phase B hooks.

## Data Flow

```
NodeConfig.storage ──▶ PoolRegistry::from_config
                        ├─ probe each root (write+read, policy Fatal/Degraded)
                        ├─ resolve weight + tech
                        └─ Arc<StoragePool> set (RwLock-guarded)
   periodic task ──▶ refresh_capacity() ──▶ metrics + lookup snapshots
   f3 placement ──▶ data_pools() / pool_by_id()
   f6 manifest   ──▶ pools() snapshot → NodeManifest
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-storage`
      (verified: clean; also `cargo fmt --all -- --check` clean)
- [x] **Tests:** all listed tests green (probe policy, weight resolution,
      capacity refresh, lookups, legacy fallback)
      (verified: 13 `pool::tests` + 334 lib + 38 doctests + 10 integration
      binaries in oceanfs-storage, 32 lib in oceanfs-node — all green,
      `--test-threads=1`)
- [x] **Docs:** `# Examples` on every `pub` item; rustdoc clean
      (verified: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
      -p oceanfs-storage` clean; all 3 pub types + 16 pub methods have
      `# Examples`; `from_config` has `# Errors`)
- [x] **ADR:** ADR-0029 §D1 (pool runtime = unit of placement/routing),
      §D8 startup probing + weights + capacity auto-detect satisfied
      (verified: registry = lookup API only; probe write+read per root;
      Fatal/Degraded policy; weight auto = max(1, total/GiB); zero-config
      fallback; no Ceph-OSD / node-granular / probe-blind rejected
      alternatives re-implemented)
- [x] **Perf:** 2.3 (parking_lot RwLock), 7.2 (read-only Arc snapshots;
      lookups never hold the lock during I/O — `refresh_capacity` runs
      outside the lock by taking a snapshot first), 2.4 (Arc<StoragePool>
      shared immutably, no per-request clone)
      (verified: `parking_lot::RwLock` at pool.rs:626; `refresh_capacity`
      snapshot-then-statvfs at pool.rs:865-875; atomics for status/
      write_degraded/capacity — no lock on read paths; pre-sized Vec at
      pool.rs:671-672)
- [x] **Integration:** a test in `oceanfs-node` builds a `PoolRegistry`
      from a 4-pool tempdir config and asserts all four roots are probed +
      registered (exercises the public API end to end)
      (verified: `cargo test -p oceanfs-node --test pool_registry` — 1
      passed; asserts roots created, no `.probe-*` litter, ids 0..3, roles,
      lookups, capacity refresh)

## Deviations (accepted, Phase A)

- **Auto tech = Nvme placeholder.** Real auto-detection (rotational
  detection, sysfs reads) lands in Phase B with the health monitor where
  `tech` first matters. The `PoolTech::Auto` variant keeps the config shape
  stable.
- **Degraded at startup is a status flag only.** Until Phase B, a
  Degraded-registered pool is treated as Healthy by consumers; the status
  field exists so Phase B's routing hooks have a place to read.
- **Metrics construction vs registration.** Metric descriptors are
  constructed in `PoolRegistry::from_config`, but registration happens
  through a separate `register_metrics(&self, registrar)` method — the same
  durability-counters pattern this doc references (e.g.
  `hinted_handoff_hints_stored_total`). The node wires the registration call
  in f4 (role-pinned path wiring); `from_config` alone does not touch the
  global metrics registry, keeping `oceanfs-storage` side-effect-free.
- **Legacy-mode probe is always Fatal.** The implicit legacy pool's
  `data_dir` root probe uses `MissingRootPolicy::Fatal` semantics regardless
  of the configured `missing_root_policy`, mirroring today's node behavior
  (`create_dir_all` failure refuses startup, node.rs:2015-2016). The policy
  only applies to explicit pools.
- **Doctests use tempdirs, not real paths.** `# Examples` blocks build their
  fixtures in `tempfile::TempDir` rather than `/var/lib/oceanfs` (or any
  real filesystem path), so `cargo test --doc` never touches real paths on
  the host and is safe to run anywhere.
