---
feature: "f2: Dead-Code & Test-Only Purge"
epic: "refactoring/review-wave-4"
status: proposed
priority: medium
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f2: Dead-Code & Test-Only Purge

## Summary

Remove production-binary dead code and gate test-only helpers with
`#[cfg(test)]` (reviews `garbage_collector.rs:599`, `segment/pool.rs:700,
762`, `read/coordinator.rs:1539`, `metadata/cf.rs:9`, `route_write.rs:15,
51`, `reconcile.rs:241`, `engine.rs:663,807,951`,
`write/coordinator.rs:119`). The project's rule: **no dead code in the
repository, period**; test-only helpers must not bloat the production
binary.

## Scope

### In Scope
- **Remove dead items** (verified unused via grep/code-graph):
  - `route_write.rs` (`InlineWriter`, `route_write` module + in-file tests)
    — `#[allow(dead_code)]` with no production callers.
  - `ReadCoordinator::verify_blake3` (`read/coordinator.rs:1544`) — no
    callers anywhere.
  - `WriteCoordinator::shard_small`/`shard_standard` fields + constructor
    params (`write/coordinator.rs:146-152`) — superseded by
    `segment_pool_small`/`segment_pool_standard`; also remove their
    construction in `node.rs:764-775` (keep metric registration on the
    pools).
  - `ALL_COLUMN_FAMILIES` + `encode_deletion_key`
    (`metadata/cf.rs:16,39`) — no callers.
  - `cf.rs:9` comment and any other confirmed-dead `#[allow(dead_code)]`
    production items.
- **Gate test-only helpers** with `#[cfg(test)]` (do NOT export into the
  binary):
  - `InMemorySegmentShardStore` (`garbage_collector.rs:608`) — re-exported
    today through `gc/mod.rs`/`lib.rs`; make test-only.
  - `InMemorySegmentStore` (`anti_entropy/merkle_tree.rs:65`) — same.
  - `HolderIndex::total_segments` (`reconcile.rs:246`) — test-only.
  - `AntiEntropy::ec_repair_segment` + `merkle_repair_diverged_leaves`
    (`engine.rs:663-857`) — retained for testing; `#[cfg(test)]` (and
    delete the "backward compatibility" claims — review #27).
  - `MerkleExchangeProtocol` (`engine.rs:951+`) — test-only.
  - `SegmentPool::append`/`slot_count` (`pool.rs:700,762`) — test-only.
- After cleanup: remove now-unneeded `#[allow(dead_code)]` annotations and
  add a CI lint that denies `dead_code` on production code (non-test).

### Out of Scope
- Anything referenced by the wave-2 store-unification epic (that epic
  deletes `DiskSegmentShardStore`/durability impls wholesale) — avoid
  double work; coordinate.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | cfg-gate test helpers; delete MerkleExchangeProtocol if truly unused in prod |
| `oceanfs-server` | delete `verify_blake3`, shard fields |
| `oceanfs-storage` | delete `route_write`, cf dead items, cfg-gate pool test fns |
| CI | add dead-code denial |

## Definition of Done

- [ ] Grep: removed items have zero references outside `#[cfg(test)]`.
- [ ] Production binary size / symbol check: test-only types no longer
      exported.
- [ ] `cargo build --all-targets` + workspace tests green (RocksDB crates
      `--test-threads=1`).
- [ ] No `#[allow(dead_code)]` on production (non-test) items without a
      written justification.
