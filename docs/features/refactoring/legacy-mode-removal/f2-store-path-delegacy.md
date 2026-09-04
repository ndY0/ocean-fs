---
feature: "f2: Store & Path Delegacy — Pools-Only Data Access"
epic: "refactoring/legacy-mode-removal"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: legacy-mode-removal/f1-boot-enforcement
    reason: f1 makes pools mandatory and guarantees every pinned role exists; f2 can then delete every "empty = legacy" branch and keep resolution total
  - epic: refactoring/composition-root-decomposition
    reason: c1's single-store consolidation consumes the pools-only constructors produced here
adr:
  - 0031-remove-single-datadir-legacy-mode
  - 0029-storage-pools-disk-resilience
perf:
  - "7.2: resolution stays a lock-free lookup over the pool snapshot (boot-time / per-segment-cached; never per-read I/O)"
  - "2.3: no new locking introduced while deleting the legacy branches"
created: 2026-09-04
updated: 2026-09-04
---

# f2: Store & Path Delegacy — Pools-Only Data Access

> **SEQUENCING CONSTRAINT (roadmap §4):** this feature edits
> `DiskSegmentStore` / `DiskSegmentShardStore` (the durability impls).
> Store-unification `f2-single-impl.md` **deletes those same impls**
> (ADR-0032). These two features MUST NOT run in parallel. Land this f2
> BEFORE store-unification f2 (delegacy first, then the unified impl is
> pools-only by construction), or — if store-unification f2 lands first —
> fold this feature's delegacy into it. The store-unification epic's
> README and the program roadmap §4 record the same rule.

## Summary

Delete the legacy half of the data-access layer (ADR-0031 D2):
`DiskSegmentStore` and `DiskSegmentShardStore` lose their `legacy_dir` field
and the "empty `data_pools` = legacy mode" resolution branch, and
`pool_paths.rs` loses its fallback arms. Role-pinned dirs (metadata, wal,
event-wal, hints) and segment `.dat` roots resolve **from pools only**; a
segment whose durable `pool_id` does not name a registered pool is surfaced as
an explicit data-integrity error, never silently routed to `data_dir`. Because
f1's role-presence validation guarantees every pinned role exists, the path
resolution functions stay total and the Degraded-pool → `data_dir` bridge and
the `hint_wal_dir` override are deleted.

## Scope

### In Scope

- `oceanfs-durability` `segment_store_impl.rs` (`DiskSegmentStore`):
  - Remove the review-marker comment (`:16-18`) and the `legacy_dir` field
    (`:31`); update the struct doc (`:19-26`) and constructor doc (`:37-42`).
  - New signature:
    `DiskSegmentStore::new(data_pools: Vec<Arc<oceanfs_storage::StoragePool>>,
    pool_id_for: oceanfs_storage::PoolIdResolver)` — no `legacy_dir`.
  - `resolve` (`:55-62`): drop the `data_pools.is_empty()` branch. When
    `pool_id_for(segment_id)` returns `None`, or no registered pool carries
    the returned id, return an explicit
    `Error::InvalidConfig("segment {id} references unknown pool {pid}")`
    (propagate through `read_segment_data` / `write_segment_data`, which are
    already `Result`-shaped) instead of falling back to `legacy_dir`.
  - Update tests: `legacy_store` helper (`:136`) and
    `write_then_read_roundtrip_is_header_valid` (`:140`) build a pools-backed
    store; `resolve_uses_pool_id_to_pick_root` (`:163`) drops the
    unknown-id→legacy and no-pools→legacy expectations (`:199-210`) in favor
    of unknown-id→`Err` assertions and a pool-0→first-root assertion.

- `oceanfs-durability` `gc/garbage_collector.rs` (`DiskSegmentShardStore`):
  - Remove the duplication/legacy review comment (`:613-619`), the
    `legacy_dir` field (`:630`), and the empty-branch in `resolve` (`:651-655`);
    new signature mirrors `DiskSegmentStore::new` (data_pools + pool_id_for).
  - `resolve_with_pool` (`:661-668`): pool id must name a registered pool;
    unknown id → explicit error.
  - `list_segment_files` (`:694-700`): drop the `(legacy_dir, 0)` seed — scan
    the data pool roots only; every listed `pool_id` is a real pool id.
  - Update the construction test at `:1279` and the orphan-reaper wiring in
    `orphan_reaper.rs:736` for the new signature.

- `oceanfs-storage` `pool/mod.rs` shared helper `resolve_pool_root`
  (`:73-106`):
  - Change to pools-only resolution: `resolve_pool_root(pools, pool_id) ->
    Option<PathBuf>` (or an equivalent that returns the root only when the id
    is registered); the `legacy_dir` parameter and the empty-pools doc example
    (`:94-98`) go away.
  - Mechanically adapt the remaining callers that still own a legacy field:
    `io/segment_reader.rs:358` and `segment/sealer.rs` fall back to their own
    `legacy_dir` via `unwrap_or_else` — their internal legacy branches are NOT
    removed here (theme-1 unification, wave 2 ②, removes them later).

- `oceanfs-node` `pool_paths.rs` (ADR-0031 D2):
  - `pool_paths` (`:64-85`) becomes `pub(crate) fn pool_paths(registry:
    &PoolRegistry) -> PoolPaths` — no `data_dir`, no `hint_wal_dir`
    parameters; every path resolves from the pinned pool of its role
    (metadata → metadata pool root; wal → wal pool root; event-wal → wal pool
    root + `event-wal`; hints → hints pool root).
  - Delete `pinned_root`'s `Healthy`-only filter + Degraded WARN (`:87-107`)
    and `resolve_pinned`'s fallback (`:109-113`): a pool of the role exists by
    construction (f1 role presence); resolution ignores health status — a
    Degraded pool still owns its root (Phase B semantics arrive later).
  - Rewrite module doc (`:1-10`) and the doc on the struct/function
    (`:44-64`); remove the `hint_wal_dir` override handling.
  - Test rework (ADR-0031 D4): delete
    `legacy_mode_resolves_exactly_as_before` (`:171`),
    `pool_mode_without_role_pool_falls_back_to_legacy` (`:219`),
    `degraded_pinned_pool_falls_back_to_legacy_path` (`:236`),
    `degraded_pinned_pool_fallback_emits_warn` (`:298`), and
    `legacy_fallback_is_silent` (`:323`). Replace with pool-only resolution
    tests (four-role registry → exact pool roots; Degraded pool → its own
    root, no WARN) and reference the f1 no-pools boot-refusal tests.

- `oceanfs-core` `config/node.rs`: remove the now-dead `hint_wal_dir` field
  (`:384-388`) and its default (`:703`) — the hints pool root replaces the
  override; update `node.rs:612` and any remaining references accordingly.
  (`NodeConfig` has no `deny_unknown_fields`; dropping the field silently
  ignores old configs that set it, which is acceptable for the unpublished
  system — noted so the config-hygiene pass (wave 4) can add an explicit
  rejection if desired.)

### Out of Scope

- The `io/segment_reader.rs` and `segment/sealer.rs` internal `legacy_dir`
  branches — they become unreachable after f1; deletion is theme-1 store
  unification (wave 2 ②), not this epic.
- `StorageConfig`/validate role-presence changes — f1.
- `NodeLeaveHandler` `read_segment_data`/single-`segment_dir` — superseded by
  the pool-aware replica model; fixed in c1.
- Event-WAL / checkpoint format removal — f3.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `segment_store_impl.rs`, `gc/garbage_collector.rs`: `legacy_dir` field + empty-pool branch deleted; constructors lose the `legacy_dir` argument; unknown-pool resolution errors explicitly |
| `oceanfs-node` | `pool_paths.rs`: pools-only resolution; `node.rs` store-construction call sites updated; `config.hint_wal_dir` no longer passed |
| `oceanfs-core` | `config/node.rs`: `hint_wal_dir` field removed |
| `oceanfs-storage` | `pool/mod.rs`: `resolve_pool_root` drops the legacy parameter (callers in reader/sealer adapted) |

## Interface (Public API)

- `DiskSegmentStore::new(data_pools: Vec<Arc<oceanfs_storage::StoragePool>>,
  pool_id_for: oceanfs_storage::PoolIdResolver)` — signature change
  (`oceanfs-durability` facade re-export).
- `DiskSegmentShardStore::new(...)` — same signature change
  (`oceanfs-durability` facade re-export).
- `oceanfs_storage::resolve_pool_root(pools, pool_id) -> Option<PathBuf>` —
  signature change (drop `legacy_dir`); callers updated.
- `oceanfs_node::pool_paths::pool_paths(registry) -> PoolPaths` —
  signature change (drop `data_dir`, `hint_wal_dir`); `pool_paths` is
  `pub(crate)`.
- `NodeConfig.hint_wal_dir` — field removed.
- Resolution errors: a segment referencing an unknown pool now yields
  `Error::InvalidConfig` (previously a silent `legacy_dir` fallback).

## Data Flow

```
Sealed segment read (GC/AE/heal/read-after-seal)
  → DiskSegmentStore::resolve(segment_id)
  → pool_id = pool_id_for(segment_id)          // lifecycle registry
  → find data pool by pool_id  —None→ Err("…unknown pool {id}…")  [no legacy_dir]
  → {pool root}/{segment_id}.dat

Node::start role dirs
  → pool_paths(&pool_registry)                  // no data_dir, no hint_wal_dir
  → metadata/wal/event-wal/hints := pinned pool roots (roles guaranteed by f1)
  → Degraded pool → still its own root (no legacy WARN bridge)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` passes; `grep -rn "legacy_dir" crates/oceanfs-durability crates/oceanfs-node/src/pool_paths.rs --include=*.rs` returns nothing outside `#[cfg(test)]`/removed comments.
- [ ] **Tests:** pools-only resolution tests cover every role and a Degraded
      pool; unknown-pool-id reads/GC-deletes return the explicit
      `InvalidConfig` error; pool-0 resolves to the first configured data
      pool. Run `cargo test -p oceanfs-durability --lib -- --test-threads=1`,
      `cargo test -p oceanfs-node --lib -- --test-threads=1`, `cargo test -p
      oceanfs-storage --lib -- --test-threads=1` (RocksDB caveat, PIPELINE.md
      §4.6), `cargo test -p oceanfs-core`.
- [ ] **Docs:** `#![deny(missing_docs)]` passes; constructor and `pool_paths`
      docs no longer mention a legacy fallback.
- [ ] **ADR:** ADR-0031 D2 satisfied — no `legacy_dir`/empty-pool fallback in
      `DiskSegmentStore`, `DiskSegmentShardStore`, or `pool_paths`; no
      Degraded→legacy bridge; `hint_wal_dir` override gone; `pool_id` still
      resolves against real pools only.
- [ ] **Perf:** resolution remains a lock-free snapshot lookup, no per-read
      I/O or locking added (perf 7.2/2.3); `list_segment_files` stops scanning
      a directory that cannot contain segments.
- [ ] **Integration:** a pool-mode node boots with all role dirs on pool roots
      (never `data_dir/{role}`); a sealed `.dat` written on pool 0 reads back
      after restart; GC unlinks from the owning pool root.
