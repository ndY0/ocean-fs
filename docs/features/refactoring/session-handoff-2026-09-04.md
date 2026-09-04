---
feature: "Session Handoff — 2026-09-04 (c1 + legacy f1 landed)"
epic: "refactoring"
status: active
priority: critical
owner: ""
created: 2026-09-04
updated: 2026-09-04
---

# Session Handoff — 2026-09-04

Continuation notes for the next implementer session on the 2026-09 review
program (`docs/features/refactoring/review-2026-09-orchestration.md`). This
file captures where the program stands, what landed, the decisions that
shaped the work, and exactly what to pick up next. Read it together with the
orchestration doc's status board and the per-epic READMEs.

## Program position

Wave 2 of the review program is in flight. Landed so far (all on `main`):

| Commit | What |
|---|---|
| `3f681eb` | composition-root **c1** (StorageModule builder) — review PASS |
| `70d81cd` | legacy-mode-removal **f1** (pools mandatory at boot, ADR-0031 D1) + f3-§D fixture prep — review PASS |
| `caea833` | wal_retention test rework (rotation-driven reclaim semantics) + storage-api/cache doctest fixes |

Status board (also updated in the orchestration doc):

| Epic | Status |
|---|---|
| review-wave-0-1 | done (B1 closed by c1) |
| composition-root-decomposition | **c1 done**; c2–c5 not started |
| legacy-mode-removal | **f1 done**; **f2, f3 not started** |
| store-unification, durability-scheduler, manifest-aware-repair, bounded-metadata-scans | docs written, not started |
| g7/g8 healing | blocked on wave 2 |

## What the code looks like now (key facts)

- `crates/oceanfs-node/src/modules/storage.rs`: `StorageModule` (pub(crate))
  with `build(config, paths, registry, metadata_store, accel, ring_cache,
  membership, pool)` + `run_startup_recovery(&self)`. Exactly TWO store
  construction sites in the whole crate (one `DiskSegmentStore`, one
  `DiskSegmentShardStore`). §0–§5 and durability/server material still
  inline in `Node::start()` (node.rs, 3474 lines).
- **Pools are mandatory** (f1): `StorageConfig::validate` rejects empty or
  role-incomplete `[storage.pools]`; `PoolRegistry::from_config` has no
  implicit-pool fallback; every booting fixture declares one data + one wal
  + one metadata + one hints pool, **data configured first = pool id 0**.
- e2e harness: node base dir = caller's `data_dir`; node data at
  `{base}/data`; pools at `{base}/pool-{data,wal,meta,hints}`; appended only
  when the config lacks `[storage]` (fleet/SUT configs untouched).
- WAL cleanup runs only at rotation (`WalWriter::rotate` →
  `cleanup_old_wal_files`); garbage = entry's segment Sealed with
  `data_wal_pos ≥ p` or Deleted; there is no idle sweep by design.
- `NodeLeaveHandler` deleted; `Node::shutdown()` step 1 =
  `membership.leave(None)`.

## Decisions recorded (do not re-litigate)

- c1 = **pure move**; ADR-0031 boot enforcement is f1's (same landing
  train). c1's two `pools.is_empty()` branches died in f1
  (`modules/storage.rs` now: `registry.data_pools()` /
  `registry: Some(registry.clone())`).
- No DI framework (plain builders). No renumbering of `// ----` sections
  before c5. No crate-boundary changes beyond the f1 config surface.
- c1 interface deviations (approved): 8-arg `build`, no `wal_reader` /
  `pool_registry_for_server` fields, added `paths`, write-path pools,
  `active_pools`, `startup_rebuild_gauge`. Recorded in the c1 doc.
- B1 (fixed-76-byte leave-handler slice, review #35) closed by c1's
  `NodeLeaveHandler` deletion; recorded in c1 doc + wave-0/1 f1 doc.
- wal_retention: **test-only fix** (no production change — user decision):
  pruning is rotation-triggered; a quiet node legitimately retains
  protected files; the test now asserts reclaim + no-rebound under a
  sustained write stream.

## NEXT STEP — legacy-mode-removal f2

`docs/features/refactoring/legacy-mode-removal/f2-store-path-delegacy.md`
(feature exists in the epic dir; read it first), per the README landing
order **f1 → f2** (f3 format removal can follow; f3's format work is
independent of f2 beyond the fixture prep already merged).

f2 deletes the store/path legacy surface that f1 made unreachable:
- `legacy_dir` field + empty-`data_pools` resolution branch in
  `oceanfs-durability` `DiskSegmentStore` (`segment_store_impl.rs`) and
  `DiskSegmentShardStore` (`gc/garbage_collector.rs`) — constructors lose
  the `legacy_dir` arg (consumers: `modules/storage.rs` builds them).
- `pool_paths.rs` legacy fallback arms (`resolve_pinned(...)
  .unwrap_or(data_dir.join(...))`, the Degraded→legacy bridge, the
  `hint_wal_dir` override when no hints pool), plus `resolve_pool_root`'s
  `legacy_dir` param (consumers: durability stores + node stores).
- An unknown/None `pool_id` becomes an explicit data-integrity error
  (`Error::InvalidConfig`), never a silent `data_dir` route.

NOT f2 (theme-1 / store-unification territory): the internal legacy arms in
`io/segment_reader.rs:342` and `segment/sealer.rs:386`, and the node
`repair.rs` peer-manifest predicate (`!data_pools.is_empty()` — a HEALTH
check, not a legacy branch; leave it). f2's doc records the hard
sequencing rule: f2 must land BEFORE store-unification f2 (same impls are
deleted there).

Sequencing notes: legacy f2 must land **before store-unification f2**
(ADR-0032), which itself sits behind c1. Composition-root **c2**
(DurabilityModule) can be done in parallel with legacy f2 if desired — it
only needs c1 (done).

## Environment / verification recipes

- RocksDB-touching suites need `-- --test-threads=1`
  (PIPELINE.md §4.6): `cargo test -p oceanfs-storage --lib -- --test-threads=1`,
  `cargo test -p oceanfs-node --lib -- --test-threads=1`, node integration
  suite the same.
- e2e tests spawn the `oceanfs` binary and refuse a STALE one (mtime vs
  newest source): always `cargo build -p oceanfs` before
  `cargo test -p e2e --test <name>`.
- wal_retention takes ~3.5 min locally (debug build); crash_restart /
  wal_recovery / cluster_lifecycle are the quick boot checks.
- Reviewer + spec-writer loop per feature (implementer workflow); feature
  docs get `status: done` + REVIEW annotations in place; re-index changed
  docs via `doc-graph_index_document`.

## Known pre-existing issues (not ours, verified at HEAD)

- `--all-targets` clippy: 3× `field_reassign_with_default` in an unchanged
  oceanfs-core test (`config/storage.rs` `validate_zero_health_windows_rejected`)
  and 1× dead_code in oceanfs-durability (`hint_wal.rs:848`). lib-target
  clippy is clean on all touched crates.
- No other open failures: core 232+60, storage 426+92, node 66 lib + 38
  doc + integration suite, server 244+12, durability 265+24, storage-api
  4/4 doc, cache 12/12 doc — all green as of `caea833`.

## Files most likely touched next (f2)

- `crates/oceanfs-durability/src/segment_store_impl.rs`
- `crates/oceanfs-durability/src/gc/garbage_collector.rs`
- `crates/oceanfs-node/src/pool_paths.rs` (+ its tests)
- `crates/oceanfs-node/src/modules/storage.rs` (store constructor calls)
- `crates/oceanfs-storage/src/pool/mod.rs` (`resolve_pool_root`)
- fixture helpers that pass `legacy_dir`/`segments` fallbacks in
  durability/node tests (sealer/reader/GC store tests)
