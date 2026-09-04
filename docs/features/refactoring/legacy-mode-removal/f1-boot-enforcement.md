---
feature: "f1: Boot Enforcement — Pools Required at Startup"
epic: "refactoring/legacy-mode-removal"
status: done
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

- [x] **Code:** `cargo build --all-targets` passes.
<!-- REVIEW: verified 2026-09-04 — cargo build --all-targets clean; cargo fmt --check clean. -->
- [x] **Tests:** empty `[storage.pools]` fails validate and `from_config` with
      a message naming the `data`/`wal`/`metadata`/`hints` roles; a data-only
      or missing-role topology fails validate; the ADR-0029 §D8 example
      (amended with a `hints` pool) validates. Run
      `cargo test -p oceanfs-core`, `cargo test -p oceanfs-storage --lib --
      --test-threads=1`, `cargo test -p oceanfs-node --lib -- --test-threads=1`.
      (Requires the f3 fixture-prep commit to have landed — see README.)
<!-- REVIEW: verified 2026-09-04 (iter 2, line refs refreshed) — core 232 lib + 60 doc green (empty_pools_rejected_with_role_listing_error storage.rs:654, data_only_topology_rejected_missing_pinned_roles :912, topology_missing_hints_pool_rejected :923, adr_d8_example fixture fn :607 + deserialize/parse tests :805/:820); storage 426 lib + 92 doc re-run green in iter 2 (empty_pools_refused_at_from_config pool/mod.rs:1653); node 66 lib + 38 doc green (node_start_without_pools_refuses_with_role_error node.rs:3181, doc 38/38 re-run in iter 2) + full tests/ suite green incl. node_without_pools_refuses_to_boot in role_isolation.rs:105 and data_pool_placement.rs:187 (both re-run green in iter 2). -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; no doc example or module doc
      still claims "empty pools = legacy mode"; every
      `StorageConfig::default()`-based doc example in `oceanfs-storage`
      rebuilt around a four-role config.
<!-- REVIEW: verified 2026-09-04 (iter 2) — RUSTDOCFLAGS="-D warnings" cargo doc --no-deps clean on core/storage/node/server/durability; `grep -rn "from_config(&StorageConfig::default(" crates` returns exactly ONE hit, the refusal assertion inside `empty_pools_refused_at_from_config` (pool/mod.rs:1655); the doc assertion at core storage.rs:409-412 uses `StorageConfig::default().validate(...)` to assert the empty-list refusal. Both iteration-1 nits are FIXED: pool/placement.rs:61 expect message now says "a data pool"; the node.rs §0 comment (node.rs:371-378) and the deleted `[review][cleanup][high]` legacy-registration marker were reworked to "Pools are mandatory (ADR-0031): an empty [storage.pools] fails startup here with the role-listing error" (verified in git diff). resolve_pool_root doc (pool/mod.rs:73-79) reworded: pools mandatory since f1, legacy_dir is only the unknown-id fallback "until f2 deletes it". -->
- [x] **ADR:** ADR-0031 D1 satisfied — no implicit-pool fallback remains in
      `PoolRegistry::from_config`; `Node::start` has no
      `config.storage.pools.is_empty()` branch (the only boot-path
      `pools.is_empty()` left is the `validate` refusal itself,
      `crates/oceanfs-core/src/config/storage.rs:417`).
<!-- REVIEW: verified 2026-09-04 (iter 2) — from_config (pool/mod.rs:837-901) has no implicit pool/probe of data_dir; modules/storage.rs:250 `let data_pools = registry.data_pools();` and :278 `registry: Some(registry.clone())` — branches gone. Literal grep caveats (all non-boot, verified semantically): crates/oceanfs-node/src/repair.rs:332 + crates/oceanfs-durability/src/reconcile.rs:510,571 are peer-manifest data-deadness predicates (NOT the config legacy branch — fine); oceanfs-durability/src/segment_store_impl.rs:56 + gc/garbage_collector.rs:651 + oceanfs-storage io/segment_reader.rs:342 + segment/sealer.rs:386 are the data-access-layer legacy arms f2/theme-1 deletes (explicitly out of f1 scope, left compiling unreachable — acknowledged); oceanfs-core storage.rs:417 is the validate refusal itself and :649 the `default_storage_config_has_no_pools` assertion. The item text's "returns only test/refusal assertions" over-claims under the literal pattern; the enumerated list above is the exact residual. -->
- [x] **Perf:** no hot-path change; boot-time-only code touched.
<!-- REVIEW: verified 2026-09-04 — validate/from_config/builder changes are boot-time; capacity Vec::with_capacity(storage.pools.len()) pool/mod.rs:840-841; no production delta outside validate/from_config/build docs+tests. -->
- [x] **Integration:** a node booting with no `[storage.pools]` fails startup
      with the role-listing error; a node booting with the amended four-role
      topology reaches the `Starting OceanFS node` log.
<!-- REVIEW: verified 2026-09-04 (iter 2) — both halves green on fresh runs. (1) Refusal: node_start_without_pools_refuses_with_role_error (node.rs:3181) asserts the 'data'/'wal'/'metadata'/'hints'/'mandatory' message; node-level integration node_without_pools_refuses_to_boot green in role_isolation.rs:105 and data_pool_placement.rs:187 (re-run: role_isolation 3/3 ok, data_pool_placement 2/2 ok). (2) Four-role boot: the CRITICAL iteration-1 gap is FIXED — e2e/src/harness.rs:461-494 spawn_inner treats the caller's data_dir as the node BASE; the node's data_dir is `{base}/data` and the auto-appended four-role pools sit on SIBLING roots `{base}/pool-{data,wal,meta,hints}` (validate's paths_overlap equal-or-nested rule satisfied); probe_root create_dir_all (pool/mod.rs:602) + node.rs:2711 create_dir_all make the layout self-creating; ports file + restart reuse semantics unchanged (same base). Configs declaring `[storage]` are untouched, and no e2e consumer config declares `[storage]` or embeds `data_dir` (grep over e2e/ → harness.rs only), so all harness consumers take the appended block. Re-run against the fresh debug binary: crash_restart 1/1, wal_recovery 1/1 (restart-reuse), cluster_lifecycle 4/4 (3-node, both spawn entry points) — all boot to "Starting OceanFS node" and serve. wal_retention boots but still fails its POST-boot prune-convergence assertion (wal_retention.rs:185: "pruning never converged after the load: 34 files (initial 1)") — retention code is untouched by f1 and the failure is byte-identical at HEAD (implementer stash-verified); NOT an f1 regression, tracked as a pre-existing e2e gap outside this feature's DoD. -->

## Implementation Notes (2026-09-04)

Facts verified in the working tree at close (reviewer PASS, iteration 2):

- **`StorageConfig::validate`** (crates/oceanfs-core/src/config/storage.rs ~:417-527)
  rejects an empty `pools` list with the role-listing error and enforces
  exactly-one `wal`/`metadata`/`hints` + ≥1 `data` (role pinning, ADR-0029
  §D8); module / `StorageConfig` / `validate` docs reworked.
- **`PoolRegistry::from_config`** (crates/oceanfs-storage/src/pool/mod.rs
  ~:837-901) no longer synthesizes an implicit `"legacy"` pool and no longer
  probes `data_dir`; `Vec::with_capacity(storage.pools.len())` (no `.max(1)`).
  The test `empty_pools_refused_at_from_config` (:1653) replaced the deleted
  `legacy_mode_creates_single_implicit_data_pool`.
- **`StorageModule::build`** (crates/oceanfs-node/src/modules/storage.rs)
  dropped both `config.storage.pools.is_empty()` branches (`data_pools` →
  `registry.data_pools()` :250; `registry: Some(registry.clone())` :278); the
  node.rs §0 comment (node.rs:371-378) was reworked to "Pools are mandatory
  (ADR-0031)". Boot-refusal tests added: unit
  `node_start_without_pools_refuses_with_role_error` (node.rs:3181) +
  integration `node_without_pools_refuses_to_boot` (tests/role_isolation.rs:105,
  tests/data_pool_placement.rs:187).
- **Fixture prep (f3 §D, merged per README landing order):** node unit
  `test_config` + 10 `Node::start` doc examples + module tests + node
  integration tests + oceanfs-server/durability fixtures declare the
  four-role topology (data first = pool id 0); legacy-node tests
  deleted/replaced. The e2e harness `spawn_inner` (e2e/src/harness.rs:461-494)
  appends a minimal `[storage.pools]` block when the config lacks `[storage]`
  (node base = caller `data_dir`; node data dir `{base}/data`; pool roots
  `{base}/pool-{data,wal,meta,hints}`) — crash_restart / wal_recovery /
  cluster_lifecycle e2e tests boot and serve.
- **Residual `pools.is_empty()` hits** are the f2/theme-1 legacy arms
  (io/segment_reader.rs, segment/sealer.rs, durability stores, node
  repair.rs predicate) — out of f1 scope, left compiling unreachable.
- **Pre-existing, non-f1 (at HEAD):** e2e `wal_retention` prune-convergence
  failure; `--all-targets` clippy lints (core `field_reassign` ×3 in an
  unchanged test, durability `dead_code`).
