---
feature: "Storage Pools: Role Isolation (WAL/Metadata/Hints Pinning)"
epic: "disk-resilience"
status: done
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

- [x] **Code:** `cargo build --all-targets` in `oceanfs-node` and
      dependents
      (verified by review: `cargo build --all-targets -p oceanfs-node`
      and `-p oceanfs` clean; `cargo fmt --all -- --check` clean; `cargo
      clippy -p oceanfs-node --lib -- -D warnings` and `--tests` clean)
- [x] **Tests:** unit resolution matrix + node boot test + regression
      (legacy layout identical)
      (verified by review: 4 `pool_paths` unit tests — legacy
      byte-for-byte, explicit pinned incl. `wal_root/event-wal`,
      pool-mode-without-role fallback, Degraded fallback; 3
      `role_isolation` integration tests — 4-pool boot, legacy
      regression, S3 PUT+GET on pinned roots. All green,
      `--test-threads=1`. Full node suite as regression gate: 36 lib +
      79 integration (17 binaries) + 2 doctests = 117 passed, 0 failed.)
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
      (verified: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p
      oceanfs-node` clean; `# Examples` on `PoolPaths` (pool_paths.rs:19-31),
      module docs on `pool_paths.rs`; doctest runs green)
- [x] **ADR:** ADR-0029 §D8 role pinning (WAL/metadata/hints isolation)
      satisfied
      (verified: metadata → metadata pool root, WAL → wal pool root,
      event WAL → wal root/event-wal, hints → hints pool root; legacy
      zero-config fallback byte-for-byte; Fatal probe failure surfaces at
      `from_config` with the pool root in the error (pool/mod.rs:715-719);
      Degraded falls back with a WARN only when a pool of the role exists
      but is not Healthy (pool_paths.rs:90-98) — legacy mode never warns;
      no Ceph-OSD / node-granular / probe-blind rejected alternatives
      re-implemented)
- [x] **Perf:** 3.4 (group-commit WAL unchanged — only the dir resolves
      once at boot, not per-write), 7.1 (path resolution is a boot-time
      operation; no lock in the write path)
      (verified by review: `pool_paths()` called once at node.rs:475-476;
      the four `paths.*` values feed MetadataConfig/WalConfig/
      EventWalConfig/HintedHandoffConfig only; WAL write path code
      untouched; no locks added)
- [x] **Integration:** a 4-pool node boots and serves an S3 PUT+GET in a
      local e2e test; the report asserts files landed on the pinned roots
      (verified by review: `cargo test -p oceanfs-node --test
      role_isolation -- --test-threads=1` — 3 passed; RocksDB CURRENT at
      the metadata root, `wal_*.log` at the wal root, event-wal under the
      wal root, legacy `data_dir/{metadata,wal,event-wal,hints}` absent,
      segments stay at `data_dir/segments`; PUT+GET 200 with body
      round-trip on the 4-pool node)
      <!-- REVIEW: hints pinning is only weakly proven end-to-end:
      `hints_root.exists()` (tests/role_isolation.rs:88) holds because the
      pool probe creates the root — it would pass even if the hints wiring
      fell back to data_dir/hints. Hint WAL files are opened lazily per
      peer node on first hint (hinted_handoff/hint_wal.rs:77-89), so no
      hint file exists at boot to assert. Mitigated by the unit tests
      (explicit_mode_resolves_to_pool_roots asserts the hints root) and
      the static one-line binding node.rs:1015. A stronger e2e would force
      a hint (e.g., write a HintWal at the hints root and replay, or make
      a peer hand off). LOW severity — DoD integration item is met. -->
      <!-- REVIEW: the Degraded WARN is not asserted by any test (only the
      fallback path is, pool_paths.rs:232-244). A tracing-subscriber test
      would pin the "WARN only when a role pool exists but is Degraded"
      behavior. LOW severity. -->

## Deviations (accepted)

- **`Degraded` startup policy falls back to legacy paths for the missing
  role** rather than failing the node. This is a Phase A bridge: real
  degraded semantics (route around, reject writes) arrive in Phase B; the
  fallback keeps a misconfigured node bootable and loudly WARNs.

### Implementation Notes

- **f2 → f4 closure (`PoolRegistry::register_metrics`).** The f2 spec note
  (`pool-runtime.md`, "metrics" section) said the node wires
  `register_metrics` in f4; that wiring landed here as part of this feature:
  `pool_registry.register_metrics(&*metrics)` in `Node::start`'s metrics
  block (`node.rs:1347`), alongside the other per-component registrations.
- **Degraded fallback WARN is conditional.** The prominent WARN fires only
  when a pool of the role exists but is not Healthy; the legacy-mode (no
  pools) and pool-mode-without-role fallbacks are silent. Behavior pinned by
  tracing-subscriber unit tests (`degraded_pinned_pool_fallback_emits_warn`,
  `legacy_fallback_is_silent` in `pool_paths.rs`).
- **Tracked follow-up (reviewer LOW): hints pinning not end-to-end
  observable at boot.** `HintedHandoffManager` opens per-peer hint WAL files
  lazily on first hint, so no hint file exists at boot to assert pinning.
  The pinning is covered by the `explicit_mode_resolves_to_pool_roots` unit
  test and the static `node.rs` binding; closing the gap requires driving a
  real hint handoff (Phase B machinery). Consolidates the inline REVIEW
  comments under the DoD Integration item.
