---
feature: "Storage Pools: Config Schema + Validation"
epic: "disk-resilience"
status: proposed
priority: high
owner: ""
dependencies: []
adr: [0029]
perf: []
created: 2026-08-22
updated: 2026-08-22
---

# Storage Pools: Config Schema + Validation

## Summary

The configuration foundation for ADR-0029: introduce the pool config types
(`PoolRole`, `PoolTech`, `PoolHealthConfig`, `PoolConfig`, `StorageConfig`)
in `oceanfs-core`, wire `NodeConfig::storage` (serde-defaulted, so absence
means legacy single-`data_dir` behavior), and implement validation of the
one-root-per-pool rule and role cardinality. No runtime behavior changes —
the pools are not consumed until f2.

## Scope

### In Scope

- `oceanfs-core` new `config/storage.rs`:
  - `enum PoolRole { Data, Wal, Metadata, Hints }` — serde lowercase.
  - `enum PoolTech { Hdd, Ssd, Nvme, CloudEphemeral, Auto }` — `Auto` is the
    default; resolved later (Phase A: placeholder, see epic).
  - `enum MissingRootPolicy { Fatal, Degraded }` — startup behavior for a
    pool whose root is missing (default `Fatal`).
  - `struct PoolHealthConfig` — `error_rate_threshold: f64` (0.001),
    `min_errors: u64` (3), `latency_factor: f64` (5.0),
    `trend_window_secs: u64` (300), `detection_window_secs: u64` (30),
    `recovery_window_secs: u64` (300) — carried now, consumed by Phase B.
  - `struct PoolConfig` — `name: String`, `role: PoolRole`, `root: PathBuf`
    (single root — multi-root pools rejected by validation),
    `weight: Option<u32>` (None = auto from capacity, f2),
    `tech: PoolTech` (default Auto), `health: PoolHealthConfig`.
  - `struct StorageConfig` — `pools: Vec<PoolConfig>`, `missing_root_policy:
    MissingRootPolicy`; `fn validate(&self, data_dir: &Path) -> Result<(),
    String>`.
- `NodeConfig` gains `#[serde(default)] pub storage: StorageConfig`.
- Validation rules (ADR-0029 §D8):
  - pool names unique and non-empty; roots absolute and unique;
  - **one root per pool** (the config schema only has one `root` field —
    validation rejects any attempt to express multiple roots);
  - at most one `wal`, `metadata`, `hints` pool; ≥ 1 `data` pool when pools
    are configured;
  - `weight` > 0 when set; `tech` parses; health thresholds sane
    (`error_rate_threshold` in (0,1), windows > 0);
  - `StorageConfig::validate` treats the empty-pool list as the legacy
    fallback and validates nothing (compatible with every existing config).
- Tests: serde round-trip of `StorageConfig` (inline + file), each
  validation rule (duplicate name, duplicate root, non-absolute root,
  missing wal role, two wal pools, zero weight, multi-root rejection),
  legacy-fallback acceptance.
- The ADR §D8 example shape deserializes as:
  ```toml
  [storage]
  missing_root_policy = "fatal"   # or "degraded"

  [[storage.pools]]
  name = "fast-nvme-0"
  role = "data"
  root = "/mnt/nvme0"
  weight = 2
  tech = "nvme"
  health = { error_rate_threshold = 0.001, min_errors = 3,
             latency_factor = 5.0, trend_window_secs = 300,
             detection_window_secs = 30, recovery_window_secs = 300 }
  ```
  (`health` is an inline table on each pool — per-pool, not a global
  `[storage.pools.health]` block; validation must reject a malformed
  global-shape attempt with a clear message.)

### Out of Scope

- Pool runtime / registry (f2).
- Placement (f3), role-pinned wiring (f4), segment store (f5).
- Health monitoring (Phase B) — `PoolHealthConfig` is defined and validated
  but never read.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New `config/storage.rs`; `NodeConfig::storage` field |

## Interface (Public API)

- `pub enum PoolRole { Data, Wal, Metadata, Hints }` — pool purpose.
- `pub enum PoolTech { Hdd, Ssd, Nvme, CloudEphemeral, Auto }` — device
  class; `Auto` default.
- `pub enum MissingRootPolicy { Fatal, Degraded }` — startup policy.
- `pub struct PoolHealthConfig` — trend/threshold knobs (Phase B input).
- `pub struct PoolConfig` — one pool definition (single `root`).
- `pub struct StorageConfig` — `pools: Vec<PoolConfig>`; empty = legacy.
- `StorageConfig::validate(&self, data_dir: &Path) -> Result<(), String>` —
  config validation (ADR-0029 §D8 rules).

## Data Flow

```
oceanfs.toml
  └─ [storage] → NodeConfig.storage → StorageConfig::validate
       └─ (empty) = legacy single-data_dir mode (unchanged)
       └─ (non-empty) = pool config consumed by f2 PoolRegistry
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core`
- [ ] **Tests:** round-trip + one test per validation rule (incl. legacy
      fallback); existing `NodeConfig` tests unchanged and green
- [ ] **Docs:** every `pub` item has `# Examples`; rustdoc `-D warnings`
- [ ] **ADR:** ADR-0029 §D8 config schema (one root per pool, roles,
      weights, tech, health) and §D8 zero-config fallback satisfied
- [ ] **Perf:** none (config-only, cold path)
- [ ] **Integration:** a `oceanfs-core` unit test deserializes a full
      example `[storage]` block from the ADR §D8 and validates it

## Deviations (none)

(Intentionally empty — resolved in the brainstorm rev. 2: multi-root pools
forbidden; `tech` default Auto resolved in f2 placeholder.)
