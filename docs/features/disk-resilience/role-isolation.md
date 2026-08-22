---
feature: "Storage Pools: Role Isolation (WAL/Metadata/Hints Pinning)"
epic: "disk-resilience"
status: proposed
priority: high
owner: ""
dependencies: ["pool-runtime"]
adr: [0029]
perf: [3.4, 7.1]
created: 2026-08-22
updated: 2026-08-22
---

# Storage Pools: Role Isolation (WAL/Metadata/Hints Pinning)

## Summary

The durability-isolation headline of ADR-0029 §D8: when pools are
configured, the node's non-segment data paths move off `data_dir` onto their
role-pinned pool roots — metadata store → `metadata` pool, data WAL →
`wal` pool, event WAL → `wal` pool, hint WAL → `hints` pool. In legacy mode
(no pools) every path resolves exactly as today. This is wiring only: the
paths resolve through the `PoolRegistry` instead of `config.data_dir`
arithmetic.

## Scope

### In Scope

- A `path_for(registry, role, fallback)` helper in `oceanfs-node`
  (`src/pool_paths.rs`). **Precedence rule (pinned):** when the registry is
  in pool mode (any pool configured), each role resolves to its pinned pool
  root — `fallback` is used ONLY when the registry is in legacy mode (no
  pools) or the pinned pool is missing with `Degraded` startup policy.
  - `metadata` → `select_pinned_pool(registry, Metadata)?.root` else
    `data_dir.join("metadata")` (legacy);
  - `wal` → `select_pinned_pool(registry, Wal)?.root` else
    `data_dir.join("wal")`;
  - `event-wal` → same `wal` pool root joined `"event-wal"` (event log rides
    the journal device; ADR-0024 config keeps its own dir under it) else
    `data_dir.join("event-wal")`;
  - `hints` → `select_pinned_pool(registry, Hints)?.root` else
    `hint_wal_dir.clone().unwrap_or_else(|| data_dir.join("hints"))`
    (the legacy `hint_wal_dir` override is honored in legacy mode only;
    when pools are configured, the hints pool root wins — the pool
    topology is the authoritative layout).
- Node wiring replacements (observed code):
  - `node.rs:463-468` metadata store: `MetadataConfig { data_dir:
    path_for(metadata), .. }`;
  - `node.rs:558-564` data WAL: `WalConfig { data_dir: path_for(wal),
    .. }`;
  - `node.rs:696-707` event WAL: `event_wal_dir: path_for(event-wal)`;
  - `node.rs:997-1012` hint WAL: `wal_dir: path_for(hints)`.
- Startup behavior when a pinned pool is missing at boot: `Fatal` policy →
  node refuses to start with the pool's root in the error (consistent with
  `create_dir_all` today, node.rs:2015-2016); `Degraded` policy → the
  role falls back to its legacy `data_dir` path with a prominent WARN
  (documented: a Degraded-policy node may mix pool and legacy paths; Phase B
  turns this into real degraded semantics).
- The `segments` path is explicitly NOT touched by this feature (stays
  `data_dir.join("segments")` until f5).
- Tests:
  - unit (`pool_paths.rs`): legacy-mode resolution equals today's paths
    byte-for-byte; explicit-mode resolution hits pool roots; missing
    `wal` pool with `Degraded` policy falls back to `data_dir/wal`;
  - node-level: boot a node with a 4-pool tempdir topology
    (`PoolRegistry::from_config` + the wiring) and assert the metadata
    store opened at the metadata pool root, WAL at the wal pool root, event
    WAL at wal root/event-wal, hint WAL at the hints root (probe via
    filesystem presence + the components' own open handles);
  - regression: boot with no pools → identical path layout to pre-feature.

### Out of Scope

- Segment placement on data pools (f5).
- `write_degraded` semantics when a wal pool dies (Phase B).
- Runtime attach (f8).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | New `pool_paths.rs`; replace four `data_dir.join(...)` sites |

## Interface (Public API)

- `pub(crate) fn pool_paths(registry: &PoolRegistry, data_dir: &Path,
  hint_wal_dir: &Option<PathBuf>) -> PoolPaths` — resolves
  `{metadata, wal, event_wal, hints}` dirs (legacy vs pinned).
- `pub struct PoolPaths { pub metadata: PathBuf, pub wal: PathBuf, pub
  event_wal: PathBuf, pub hints: PathBuf }`.

## Data Flow

```
config ──▶ PoolRegistry ──▶ pool_paths(registry, data_dir, hint_wal_dir)
  ├─ legacy (no pools) ──▶ data_dir/{metadata,wal,event-wal,hints}  (unchanged)
  └─ pinned ──▶ metadataPool.root / walPool.root / walPool.root/event-wal / hintsPool.root
       └─ MetadataConfig / WalConfig / EventWalConfig / HintedHandoffConfig
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` in `oceanfs-node` and
      dependents
- [ ] **Tests:** unit resolution matrix + node boot test + regression
      (legacy layout identical)
- [ ] **Docs:** `# Examples` on pub items; rustdoc clean
- [ ] **ADR:** ADR-0029 §D8 role pinning (WAL/metadata/hints isolation)
      satisfied
- [ ] **Perf:** 3.4 (group-commit WAL unchanged — only the dir resolves
      once at boot, not per-write), 7.1 (path resolution is a boot-time
      operation; no lock in the write path)
- [ ] **Integration:** a 4-pool node boots and serves an S3 PUT+GET in a
      local e2e test; the report asserts files landed on the pinned roots

## Deviations (accepted)

- **`Degraded` startup policy falls back to legacy paths for the missing
  role** rather than failing the node. This is a Phase A bridge: real
  degraded semantics (route around, reject writes) arrive in Phase B; the
  fallback keeps a misconfigured node bootable and loudly WARNs.
