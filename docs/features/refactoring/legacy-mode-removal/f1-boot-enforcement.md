---
feature: "f1: Boot Enforcement — Pools Required at Startup"
epic: "refactoring/legacy-mode-removal"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: c1 precedes (roadmap wave 2 ①); the empty-pool branches deleted here live in the region c1 extracts
  - feature: legacy-mode-removal/README
    reason: Coordination + landing order (f1 merges together with the f3 fixture-prep commit)
adr:
  - 0031-remove-single-datadir-legacy-mode
  - 0029-storage-pools-disk-resilience
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f1: Boot Enforcement — Pools Required at Startup

## Summary

Make storage pools mandatory at the config boundary (ADR-0031 D1): an empty
`[storage.pools]` is rejected with an explicit, role-listing error instead of
silently booting the implicit single-`data_dir` pool. Concretely:
`StorageConfig::validate` stops accepting the empty list and enforces the
role-pinning topology; `PoolRegistry::from_config` deletes the implicit-pool
fallback branch (`pool/mod.rs:787-806`); the storage builder (c1's
`modules/storage.rs`) stops branching on `config.storage.pools.is_empty()`
(`modules/storage.rs:256-257` `data_pools`, `:290` `SealConfig.registry` —
the branches moved there verbatim with the c1 pure-move and die here). After
this feature, every booting node has declared pools; the code still compiles
with the (now unreachable) legacy fallbacks in the data-access layer, which
f2 deletes.

## Scope

### In Scope

> **STAKEHOLDER CONFIRMED (2026-09-04):** the separate `hints` pool in the
> required role list (one `data`, one `wal`, one `metadata`, one `hints`)
> is the intended interpretation — do not relax it to share the `wal`
> pool.

- `oceanfs-core` `crates/oceanfs-core/src/config/storage.rs`:
  - `StorageConfig::validate` (`:361-366`): replace the "Legacy zero-config
    fallback: no pools = single data_dir" early-`Ok` with an error. Message
    names the required roles (ADR-0031 D1): at minimum one `data`, one `wal`,
    one `metadata`, and one `hints` pool (role pinning, ADR-0029 §D8).
  - Enforce the role-presence rule for **non-empty** lists too: at least one
    `data` pool; exactly one `wal`, exactly one `metadata`, exactly one
    `hints` pool (the existing "at most one" cardinality checks become
    presence checks — f2's pools-only resolution is only total because every
    pinned role exists).
  - Update the module doc (`:9-11`), `StorageConfig` doc (`:294-318`) and the
    `validate` doc (`:321-360`): the zero-config fallback sentence is gone;
    `pool_id` is untouched by this feature.
  - Update the unit tests that pin the legacy/partial topology:
    `default_storage_config_is_legacy_mode` (`:566`),
    `legacy_fallback_validate_accepts_any_data_dir` (`:573`),
    `validate_missing_wal_pool_is_allowed` (`:806`),
    `validate_ok_with_multiple_data_pools` (`:959`),
    `adr_d8_example_deserializes_and_validates` / `..._parses_inside_node_config`
    (`:716`, `:730` — add a `hints` pool to the fixture topology; ADR-0031 D1
    amends the ADR-0029 §D8 example list), plus the `StorageConfig` serde
    round-trip tests that build data-only configs.

- `oceanfs-storage` `crates/oceanfs-storage/src/pool/mod.rs`:
  - Delete the implicit-pool fallback branch in
    `PoolRegistry::from_config` (`:787-806`, guarded by the review marker at
    `:783-786`): no more synthetic `"legacy"` data pool at `data_dir`, no
    legacy `probe_root(data_dir)`.
  - `Vec::with_capacity(storage.pools.len().max(1))` → `.len()` (`:780-781`).
  - Update `PoolRegistry` / `from_config` docs (`:700-702`, `:746-750`) and the
    `data_dir` field doc (`:736-738`): the registry no longer creates an
    implicit pool; `data_dir` remains only for the attach-time disjointness
    check.
  - Delete `legacy_mode_creates_single_implicit_data_pool` (`:1535`).
  - Update every doc example and unit test that builds
    `PoolRegistry::from_config(&StorageConfig::default(), ...)` (~30 doc
    snippets across `pool/mod.rs`, `pool/health.rs`, `pool/placement.rs`) to a
    minimal four-role topology under the `tempdir` (add a small helper in the
    tests module, e.g. `pools_config(tmp)` returning `(StorageConfig,
    Vec<PathBuf>)`).
  - Extend the probe tests that use data-only configs —
    `missing_root_with_fatal_policy_fails_startup` (`:1608`) and
    `missing_root_with_degraded_policy_registers_degraded_pool` (`:1621`) —
    to a four-role topology (keeping the doomed/degraded root as the `data`
    pool), and the g6 availability tests (~`:1784`) likewise: the role
    presence check now runs before the probe.

- `oceanfs-node` — `crates/oceanfs-node/src/modules/storage.rs`
  (`StorageModule::build`, the c1 home of the moved §6/§f5 material;
  ADR-0031 D1):
  - `modules/storage.rs:256-257`: `let data_pools = if
    config.storage.pools.is_empty() { Vec::new() } else {
    registry.data_pools() };` — drop the empty-list conditional.
  - `modules/storage.rs:290`: `registry: if
    config.storage.pools.is_empty() { None } else {
    Some(registry.clone()) }` — drop the `None` arm; update the
    surrounding comment blocks (`:251-255` legacy empty-list paragraph and
    the f8-attach comment above `:290`) that document the legacy empty
    list.
  - The pools-mandatory error now surfaces through the existing
    `storage pool registry: {e}` map in `Node::start` (`node.rs:385-387`);
    keep that mapping.
  - Update the `// ---- 0.` section comment in `node.rs` (`:375-386`) that
    describes legacy mode and the Degraded→legacy bridge (the bridge
    itself dies in f2).

### Out of Scope

- `legacy_dir` removal from `DiskSegmentStore` / `DiskSegmentShardStore` and
  the `pool_paths.rs` fallback arms — feature f2 (this feature leaves them
  compiling; they are unreachable once validate rejects empty/partial pools).
- Event-WAL / checkpoint format removal — feature f3.
- Config-fixture updates in node/server/e2e tests and doc examples — f3's
  fixture prep commit, which must merge together with this feature (README
  landing order).
- `io/segment_reader.rs` and `segment/sealer.rs` internal legacy arms — theme-1
  unification (wave 2 ②), out of this epic.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | `config/storage.rs`: `StorageConfig::validate` rejects empty and role-incomplete pool lists; docs + unit tests updated |
| `oceanfs-storage` | `pool/mod.rs`: implicit-pool fallback deleted from `PoolRegistry::from_config`; docs + doc examples updated |
| `oceanfs-node` | `modules/storage.rs` (`StorageModule::build`): empty-pool branches removed (`data_pools`, `SealConfig.registry`) |

## Interface (Public API)

- `StorageConfig::validate(&self, data_dir: &Path) -> Result<(), String>` —
  behavior change only: now returns `Err` for an empty `pools` list (message
  lists the required roles) and for a list missing any pinned role. No
  signature change.
- `PoolRegistry::from_config(storage: &StorageConfig, data_dir: &Path) ->
  Result<PoolRegistry, String>` — behavior change only: never constructs an
  implicit `"legacy"` pool; an empty/invalid topology returns the validate
  error. No signature change.
- `Node::start` — behavior change only (boot refuses when pools are absent).

## Data Flow

```
oceanfs.toml (no [storage.pools])
  → Node::start (node.rs:342) → StorageModule::build (modules/storage.rs)
  → StorageConfig::validate → Err("at least one 'data', 'wal',
  → PoolRegistry::from_config      'metadata', and 'hints' pool is
        (no implicit pool branch)  required … storage pools are
                                   mandatory (ADR-0031)")
  → boot aborts, explicit message; node never serves

oceanfs.toml (declared pools)
  → validate (role presence) → from_config (probe each root) → data_pools =
    registry.data_pools()      (no is_empty() branch) → boot continues
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` passes.
- [ ] **Tests:** empty `[storage.pools]` fails validate and `from_config` with
      a message naming the `data`/`wal`/`metadata`/`hints` roles; a data-only
      or missing-role topology fails validate; the ADR-0029 §D8 example
      (amended with a `hints` pool) validates. Run
      `cargo test -p oceanfs-core`, `cargo test -p oceanfs-storage --lib --
      --test-threads=1`, `cargo test -p oceanfs-node --lib -- --test-threads=1`.
      (Requires the f3 fixture-prep commit to have landed — see README.)
- [ ] **Docs:** `#![deny(missing_docs)]` passes; no doc example or module doc
      still claims "empty pools = legacy mode"; every
      `StorageConfig::default()`-based doc example in `oceanfs-storage`
      rebuilt around a four-role config.
- [ ] **ADR:** ADR-0031 D1 satisfied — no implicit-pool fallback remains in
      `PoolRegistry::from_config`; `Node::start` has no
      `config.storage.pools.is_empty()` branch
      (`grep -rn "pools.is_empty()" crates/oceanfs-node crates/oceanfs-storage
      crates/oceanfs-core --include=*.rs` returns only test/refusal
      assertions).
- [ ] **Perf:** no hot-path change; boot-time-only code touched.
- [ ] **Integration:** a node booting with no `[storage.pools]` fails startup
      with the role-listing error; a node booting with the amended four-role
      topology reaches the `Starting OceanFS node` log.
