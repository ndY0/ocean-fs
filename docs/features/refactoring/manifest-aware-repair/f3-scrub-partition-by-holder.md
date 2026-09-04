---
feature: "f3: Holder-Aware Scrub Partitioning + Real assign_partition Execution"
epic: "refactoring/manifest-aware-repair"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: f1-peer-selector-trait
    epic: refactoring/manifest-aware-repair
    reason: ScrubCoordinator consumes the injected PartitionPlanner trait and the node-layer ManifestPartitionPlanner impl
  - epic: refactoring/composition-root-decomposition
    reason: ScrubCoordinator::new and ScrubGrpcService::new signatures change at exactly two call sites — c2 (durability builder, node.rs:1266) and c3 (server builder, node.rs:2168) respectively
  - epic: refactoring/store-unification
    reason: The executor (ScrubGrpcService → ScrubWorker) must read the receiving node's sealed segments and .dat through the single shared store (ADR-0032) so a partition assignment and its execution observe the same data
adr:
  - 0033-manifest-aware-peer-selection
  - 0015-anti-entropy-merkle-protocol
  - 0029-storage-pools-disk-resilience
perf:
  - "2.7 bounded concurrency"
  - "2.6 no unbounded fan-out"
created: 2026-09-04
updated: 2026-09-04
---

# f3: Holder-Aware Scrub Partitioning + Real assign_partition Execution

## Summary

Fixes the two scrub defects ADR-0033 D1 names. **(1)** The distributed
partition machinery
(`ScrubCoordinator::partition_for_current_nodes`/`partition_segments`,
`scrub.rs:612-671`, plus the `with_distributed`/`alive_peers` scaffolding,
`scrub.rs:577-600`) partitions the node's whole local sealed-segment list
across **all alive peers** — review `scrub.rs:601`: it "assumes that each
peer holds this node's segments." It is replaced by a partition planner
that assigns each segment to one of its **eligible `storage_locations`
holders** (injected `PartitionPlanner`, f1), so a peer is never assigned a
segment it does not hold and local-only segments stay in the self
partition. **(2)** `ScrubGrpcService::assign_partition`
(`scrub_service.rs:43-57`) currently acks without doing anything. It is
wired to **execute a real partition scrub** through the receiving node's
`ScrubWorker` and returns a truthful response. The ADR's considered
alternatives reject deleting the distributed half — wiring it via
per-segment holders is the resolution. Full-scan semantics (spec §7.5) and
the local `ScrubCoordinator::run_cycle` are unchanged.

## Scope

### In Scope
- **Holder-aware partition planner in the coordinator:** replace the
  distributed scaffolding — `partition_for_current_nodes`
  (`scrub.rs:612-623`, `#[allow(dead_code)]`), which is the only caller of
  `partition_segments` (`scrub.rs:638`) and `alive_peers`
  (`scrub.rs:590`), and `with_distributed` (`scrub.rs:577`, never called
  in production — `node.rs:1266` uses plain `ScrubCoordinator::new`) —
  with a single planner-backed method,
  `plan_cycle_partitions(&self, segments: &[SegmentMetadata]) ->
  Vec<SegmentPartition>`, which delegates to the injected
  `Arc<dyn PartitionPlanner>`. `ScrubCoordinator` gains the planner +
  `self_id` (constructor, `scrub.rs:548-570`); the membership/pool fields
  and `with_distributed` are deleted.
- **`run_cycle` local partition is the self partition** (`scrub.rs:708`):
  the existing local full scan keeps its behavior but routes its batching
  through `plan_cycle_partitions` so the self partition is computed by the
  same planner (a registry's Sealed set is by construction segments self
  holds — behavior-neutral, but the local-only path becomes explicit and
  tested). `ScrubWorker`/`SegmentPartition` stay the execution vehicle.
- **Wire `assign_partition` to execute** (`scrub_service.rs:43-57`):
  `ScrubGrpcService` gains a `ScrubWorker` handle (constructor change);
  `assign_partition` runs the assigned segment list through
  `ScrubWorker::scrub_partition` on `spawn_blocking`, and returns
  `accepted: true` only after the partition actually ran. An execution
  failure returns an error `Status` (no accept-and-ignore). A small
  in-memory last-result buffer on the service makes the executed summary
  observable to the coordinator-side `report_partition_result` handler
  and to tests.
- **Truthful `report_partition_result`** (`scrub_service.rs:60-79`):
  keep the handler as the coordinator-side aggregator entry; log the
  executed summary and forward it to the coordinator's pending-cycle
  aggregator when one exists (additive — today it only logs, which is not
  a silent ack and stays acceptable while dispatch is unbuilt).
- **Node wiring updates:** `ScrubCoordinator::new` call site
  (`node.rs:1266`) and `ScrubGrpcService::new` call site (`node.rs:2168`)
  gain the injected planner / worker (c2/c3 builders after the
  composition-root decomposition).
- Remove the resolved review block at `scrub.rs:601-605`.
- Unit tests for the planner (never assigns a non-holder; local-only →
  self), for the local-partition path in `run_cycle`, and for
  `assign_partition` executing a real scrub over a corrupt segment.

### Out of Scope
- **Coordinator dispatch scheduling** (electing one coordinator per cycle
  and having `run_cycle` push partitions to peers over gRPC): not built
  today (the composition root never calls the distributed path); this
  feature makes the *mechanism* truthful — planner + executor — and leaves
  dispatch scheduling as a follow-up on the same machinery.
- The Merkle-protocol/heal path, scrub full-scan verification internals
  (`ScrubWorker::scrub_segment`, ADR-0015), scrub cadence/config — all
  unchanged.
- `report_partition_result` aggregation *state machine* for a full
  multi-node cycle (follow-up with dispatch).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `scrub.rs`: `ScrubCoordinator` fields/ctor gain `Arc<dyn PartitionPlanner>` + `self_id`; `partition_for_current_nodes`/`partition_segments`/`alive_peers`/`with_distributed` deleted; `plan_cycle_partitions` added; `run_cycle` batches via the self partition; review block :601-605 removed. `scrub_service.rs`: `ScrubGrpcService` gains a worker handle; `assign_partition` executes; last-result buffer added. `lib.rs` exports unchanged (SegmentPartition export landed in f1). |
| `oceanfs-node` | `node.rs:1266` `ScrubCoordinator::new(...)` gains planner + self id; `node.rs:2168` `ScrubGrpcService::new(...)` gains the worker (constructed from the shared registry + data store). |

## Interface (Public API)

`ScrubCoordinator` (`scrub.rs:538`):
- `pub fn new(config, planner: Arc<dyn PartitionPlanner>, self_id: NodeId) -> Self`
  — replaces `ScrubCoordinator::new(config)` + `with_distributed(...)`.
- `pub(crate) fn plan_cycle_partitions(&self, segments: &[SegmentMetadata])
  -> Vec<SegmentPartition>` — planner-backed; no peer is ever assigned a
  segment it does not hold; local-only segments are in the self partition.
- Removed pub items: `with_distributed` (scrub.rs:577), `alive_peers`
  (scrub.rs:590).

`ScrubGrpcService` (`scrub_service.rs:21`):
- `pub fn new(worker: Arc<ScrubWorker>, ...)` — replaces
  `ScrubGrpcService::new(metadata_store, data_store)`; the service holds
  the worker that owns the registry + data store. (`ScrubWorker` and
  `SegmentPartition` are `pub(crate)`-visible within
  `oceanfs-durability`; no new pub type needed.)
- `assign_partition` (unchanged signature) now **executes** and returns
  `accepted: true` only on completion.

## Data Flow

```
ScrubCoordinator (every node, per cycle)
  registry.for_each(Sealed) → inventory: Vec<SegmentMetadata>     // self-held set
  plan_cycle_partitions(inventory)
    per segment: holders = metadata.storage_locations
                 eligible = planner.filter to alive + Healthy-data-pool holders (f1)
                 assign segment to one eligible holder; none → self partition
  self partition → ScrubWorker::scrub_partition → ScrubReport (local scan, unchanged)

worker side (future dispatch path — mechanism is now truthful)
  coordinator → AssignPartition { segment_ids ⊆ receiver's storage_locations }
  ScrubGrpcService::assign_partition
    spawn_blocking(ScrubWorker::scrub_partition(partition))     // REAL scan
    accepted = true  ⇔  partition executed (heals enqueued on mismatch)
  worker → ReportPartitionResult(summary)                        // aggregator entry
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds for
      `oceanfs-durability` and `oceanfs-node`.
- [ ] **No holder-blind partition:** a workspace grep finds no remaining
      use of `partition_for_current_nodes`, `partition_segments`,
      `with_distributed`, or `alive_peers` in `scrub.rs`; partitions come
      only from `plan_cycle_partitions` / the injected `PartitionPlanner`.
- [ ] **Planner correctness test:** a registry inventory whose segments
      list `storage_locations` spanning {self, A, B} plus one local-only
      segment yields partitions where every segment in peer A's partition
      lists A, peer B's partition lists B, and the local-only segment is
      in the self partition; no segment appears in more than one partition
      and no partition contains a non-holder.
- [ ] **run_cycle regression:** the local full scan produces the same
      healthy/corrupt/healed counts as before (self partition == sealed
      inventory) with `ScrubWorker::scrub_partition`.
- [ ] **assign_partition executes:** a service-level test drives
      `assign_partition` with a segment list that includes a corrupt
      segment in the receiving worker's registry; asserts the corrupt
      segment was actually scanned (heal enqueued / result recorded in the
      last-result buffer) and `accepted == true`; a worker with an
      unavailable data store returns an error Status, never a silent ack.
- [ ] **Review marker:** the `scrub.rs:601-605` block is removed.
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `cargo test -p oceanfs-node --lib -- --test-threads=1` pass
      (RocksDB caveat, PIPELINE.md §4.6).
- [ ] **Docs:** changed `pub` items keep `# Examples`;
      `#![deny(missing_docs)]` passes.
- [ ] **ADR:** ADR-0033 D1 scrub half satisfied (partitions per-segment
      over `storage_locations`; peers scrub only segments they hold;
      `assign_partition` is wired, no silent acks); ADR-0015 full-scan
      semantics unchanged.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and
> `ignore`-tagged doc examples are non-blocking (see
> `guidelines/coding.md` §9.2).
