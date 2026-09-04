---
feature: "Legacy-Mode Removal — Program Coordination"
epic: "refactoring/legacy-mode-removal"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring
    reason: Part of the 2026-09 review triage program — roadmap wave 2 item ⑤ (ADR-0031 accepted; implement the cleanup)
  - epic: refactoring/composition-root-decomposition
    reason: Composition-root c1 must precede/co-land — the storage builder (c1) consolidates the store instances into ONE, and it must construct them with the pools-only constructors this epic produces
adr:
  - 0031-remove-single-datadir-legacy-mode
  - 0029-storage-pools-disk-resilience
created: 2026-09-04
updated: 2026-09-04
---

# Legacy-Mode Removal — Program Coordination

> **This is the coordination document for the ADR-0031 legacy-removal epic.**
> If you are implementing any feature under this epic, read this first: it
> tells you where your work sits in the whole (wave 2 ⑤), what must exist
> before you start, and what must not regress while you work. The per-feature
> docs are the authority for your feature; this document is the map.

## Summary

ADR-0031 (accepted 2026-09-04) makes **storage pools mandatory** and removes
the single-`data_dir` legacy mode that ADR-0029 §D8 shipped as a zero-config
fallback. In today's code that fallback is everywhere: `StorageConfig::validate`
accepts an empty `pools` list, `PoolRegistry::from_config` boots an implicit
"legacy" data pool at `data_dir`, every store and path resolver carries an
"empty = legacy" branch, and the event-WAL / checkpoint wire formats still
carry a `pool_id`-less record shape.

The reviewer's mandate is "we do not version, we refactor": there is no
production data, so the legacy branches are **deleted**, not deprecated. A
node whose data directory contains pre-pool event-WAL/checkpoint files must
**refuse to boot with an explicit "unsupported pre-pool data directory"
error** — never silently migrate, never silently start empty over it.

This epic is the concrete work behind roadmap wave 2 ⑤. It is a
**precondition/companion to the composition-root storage builder (c1)**:
`StorageModule::build` must consolidate today's eight `DiskSegmentStore` /
`DiskSegmentShardStore` constructions into ONE instance each, and that
consolidation only makes sense on pools-only constructors. Sequence the two
epics so c1 never moves a legacy branch it will have to delete.

## Critical nuance (do not break pool 0)

`pool_id` starts at 0, so **pool 0 is a real, first-configured pool**. "Legacy"
is *the absence of pools*, never `pool_id == 0`. After this epic:

- a pools-enabled node's Seal records always carry the pool id (value 0
  included) — `pool_id = 0` decodes to the first configured pool, exactly as
  it must;
- only the *no-pool-flag* record shape is gone, and a directory containing it
  is refused at boot with the explicit error above;
- `[storage.pools]` schema is unchanged; only the "no pools allowed"
  validation is added.

## Placement

| Context | Reference |
|---|---|
| Governing ADR | `docs/adr/0031-remove-single-datadir-legacy-mode.md` (Accepted, D1–D4) |
| Topology ADR it amends | `docs/adr/0029-storage-pools-disk-resilience.md` §D8 |
| Program roadmap | `docs/features/refactoring/review-2026-09-roadmap.md` wave 2 ⑤ |
| Companion epic | `docs/features/refactoring/composition-root-decomposition/README.md` c1 |
| Spec | pools replace the single `data_dir`; config surface unchanged |

## Feature DAG

```
composition-root c1 (pure-move storage builder — wave 2 ①)
        │   (coordination: builder consumes the pools-only constructors here)
        ▼
f1 boot-enforcement          pools required at boot; empty-list fallback deleted
   ├───────────────────────────────┐
   ▼                               ▼
f2 store-path-delegacy       f3 format-break-and-test-rework
```

Dependency edges:

- **c1 → f1/f2/f3** — roadmap wave 2 ① precedes ⑤; c1 extracts
  `StorageModule` from `Node::start`. Because c1's store consolidation must
  build on pools-only constructors, this epic's PRs land immediately after (or
  interleaved with) c1 so the builder never inherits legacy branches. f2's
  constructor deletions are the exact surface c1 consumes.
- **f1 → f2** — f2 makes role-pinned resolution pools-only, which presumes the
  pools-mandatory validation f1 adds.
- **f1 → f3** — f3's boot-refusal tests and fixture rework describe a
  pools-only world; the fixture additions must be merged with f1 (below).
- **f2 ⊥ f3** — independent; either order after f1.

**Landing order (keeps every PR green).** Pools-mode is fully supported today,
so fixture work can land *before* the enforcement:

1. c1 (composition-root) — pure move, legacy behavior preserved.
2. **f3 fixture prep commit** — dev/test/e2e configs + doc examples gain
   minimal pool blocks while legacy is still accepted (harmless, green).
3. **f1** — boot enforcement + the core/registry unit tests it invalidates
   (green: every booting fixture now declares pools).
4. **f2** — store/path legacy deletion (green: fixtures are pools-only).
5. **f3 format removal** — event-WAL/checkpoint legacy shape removal +
   boot-refusal tests + legacy-test deletion (green last).

## What must not regress

- `pool_id = 0` remains valid and round-trips for pools-enabled nodes (ADR-0031
  D3). The roadmap wave 0 note at `segment/event_wal.rs:1579` ("keep the live
  pool-0 wire format") is about the *pool-id-carrying* decode; the no-flag
  decode is what ADR-0031 D4 deletes.
- `[storage.pools]` schema and the `PoolConfig`/`PoolRole` types are unchanged.
- Role-pinned dirs resolve **from pools only** after f2 — no fallback to
  `data_dir`, no `hint_wal_dir` override, no Degraded-pool bridge.
- RocksDB crates must always be tested with `--test-threads=1` (PIPELINE.md
  §4.6).

## Out of scope (deliberately not here)

- **NodeLeaveHandler** `read_segment_data` fixed-76-byte slice and its
  single-`segment_dir` listing (review #34/#35) — superseded by the
  pool-aware replica model and fixed inside c1.
- **io reader + sealer internal legacy arms** (`io/segment_reader.rs`,
  `segment/sealer.rs`) — their own "empty = legacy" branches become
  unreachable once f1 stops passing empty lists; removing them belongs to the
  Theme-1 store unification (wave 2 ②).
- **Membership-state relocation** off `data_dir` (review #42), **pool health
  persistence** (review #80), and any change to the `pool_id` numbering — all
  separate items per ADR-0031.
- Wave-4 config plumbing (Theme 3) beyond the minimal `hint_wal_dir` removal
  that f2 needs.

## Epic Definition of Done

- [ ] `cargo build --all-targets` passes across the workspace.
- [ ] No `config.storage.pools.is_empty()` branch remains in production code
      (`grep -rn "pools.is_empty()" crates --include=*.rs` returns nothing
      outside `#[cfg(test)]` fixture builders that must assert refusal).
- [ ] An empty `[storage.pools]` fails `StorageConfig::validate`,
      `PoolRegistry::from_config`, and `Node::start` with an explicit error
      naming the required roles.
- [ ] `DiskSegmentStore`, `DiskSegmentShardStore`, and `pool_paths` carry no
      `legacy_dir` / empty-pool fallback; role-pinned dirs resolve from pools
      only.
- [ ] The event-WAL no-flag Seal shape and the v2 checkpoint decode are gone;
      a node booting onto a directory with pre-pool event-WAL/checkpoint files
      fails startup with "unsupported pre-pool data directory", and
      `pool_id = 0` records from pools-enabled nodes round-trip byte-exact.
- [ ] Legacy-pinning tests deleted; replacement tests (no-pools refusal,
      pool-only resolution, pool-0 round-trip, pre-pool boot refusal) exist.
- [ ] Dev/test/e2e configs all declare pools; deploy scripts unchanged.
- [ ] Regression gate: `cargo test -p oceanfs-core`, and
      `cargo test -p oceanfs-storage --lib -- --test-threads=1`,
      `cargo test -p oceanfs-node --lib -- --test-threads=1`,
      `cargo test -p oceanfs-durability --lib -- --test-threads=1` pass;
      e2e single-node write/read green.
- [ ] c1 coordination: the composition-root storage builder constructs stores
      with pools-only signatures (no legacy branch moved into
      `modules/storage.rs`).

## References

- ADR-0031 (the decision), ADR-0029 §D8 (the topology this amends)
- Review anchors: `node.rs:830`, `pool/mod.rs:783`, `segment_store_impl.rs:16`,
  `pool_paths.rs:44`, `gc/garbage_collector.rs:613`
- Triage program: `features/refactoring/review-2026-09-roadmap.md` wave 2 ⑤
- Companion: `features/refactoring/composition-root-decomposition/README.md`
