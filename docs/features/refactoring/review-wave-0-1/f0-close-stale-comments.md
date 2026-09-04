---
feature: "f0: Close Stale Review Comments"
epic: "refactoring/review-wave-0-1"
status: proposed
priority: high
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f0: Close Stale Review Comments

## Summary

Delete the `// [review][...] ... // [end]` blocks that are factually wrong
against today's code or were resolved by commits after the review pass was
written. These were triaged on 2026-09-04 by reading each site against the
code; each deletion below records *why* it is stale so the deletion is
reviewable.

## Scope

### In Scope — delete these review blocks

| # | Location | Why stale |
|---|---|---|
| 1 | `crates/oceanfs-durability/src/repair.rs:447` | Resolved — proto `RequestReReplicationRequest` carries `tier/ec_k/ec_m`; dispatcher fills them; a `[resolved] 2026-09-03` note already marks it |
| 2 | `crates/oceanfs-durability/src/anti_entropy/engine.rs:632` | `local_merkle_verify` is the active no-peer fallback, not pointless code |
| 3 | `crates/oceanfs-durability/src/anti_entropy/merkle_tree.rs:22` | Premise false: `SegmentDataStore` IS used by heal/repair/GC/AE; only the module-placement critique is fair, and that moves with ADR-0032 anyway |
| 4 | `crates/oceanfs-server/src/grpc/segment_service.rs:300` | Remap alias is a shared `Arc<SegmentRemapAlias>` updated live by the healing service — not stale |
| 5 | `crates/oceanfs-server/src/grpc/segment_service.rs:467` | `fetch_shard` serves real data (only the hard-coded `total_shards = 6` residue survives — that is a wave-0/1 bug, see f1) |
| 6 | `crates/oceanfs-server/src/write/coordinator.rs:472` | `replicate_write` + `forward_write` exist |
| 7 | `crates/oceanfs-server/src/read/assembly.rs:76` | "64 MB buffer" is actually a 64 KiB capacity hint |
| 8 | `crates/oceanfs-node/src/node.rs:1069` | The `repair_sink` clone is required (3 owners) |
| 9 | `crates/oceanfs-storage/src/segment/event_wal.rs:1579` | Test pins the live `pool_id = 0` wire format — keep the test, drop the comment |
| 10 | `crates/oceanfs-storage/src/segment/lifecycle.rs:425` | Hash-uniformity question; no defect found |
| 11 | `crates/oceanfs-node/src/node.rs:8` header block (broader remarks) | Its actionable items (adaptive strategy, DI/composition, durability layout, in-memory data) are now tracked in the roadmap/orchestration — replace with a pointer to `review-2026-09-roadmap.md` or delete |

> Note: comments at `route_write.rs:15,51`, `event_checkpoint.rs:453`,
> `garbage_collector.rs:599`, `pool.rs:700,762`, `read/coordinator.rs:1539`,
> `metadata/cf.rs:9`, `write/coordinator.rs:119` are VALID cleanup
> comments — they are NOT deleted here; they are dead-code/tests items
> handled in wave 4. Only the ~11 above are stale.

### Out of Scope
- Valid cleanup/dead-code comments (wave 4).
- Any behavioral code change.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability`, `oceanfs-server`, `oceanfs-node`, `oceanfs-storage` | Remove stale comment blocks only |

## Definition of Done

- [ ] Grep for the deleted anchors: none of the 11 remain.
- [ ] `cargo build --all-targets` passes.
- [ ] No code behavior change (only comments removed; if a comment removal
      requires touching code, that code change belongs to f1/wave 4).
