---
feature: "f3: Holder-Aware Scrub Partitioning + Real assign_partition Execution"
epic: "refactoring/manifest-aware-repair"
status: done
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
updated: 2026-09-06
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
- **`run_cycle` plans the inventory and executes it locally**
  (`scrub.rs:708`): `run_cycle` gathers the registry's Sealed set, calls
  `plan_cycle_partitions` over it, and — while coordinator dispatch over
  gRPC is not wired — executes the UNION of all planned partitions on the
  local `ScrubWorker` (== the sealed inventory; each segment exactly once),
  preserving the spec §7.5 full-scan guarantee (D3-A). When dispatch
  scheduling lands, only the self partition stays local and the rest go
  over the wire.
- **Wire `assign_partition` to execute** (`scrub_service.rs:43-57`):
  `ScrubGrpcService` builds a `ScrubWorker` handle internally from the
  registry + data store (constructor change — D4-A); `assign_partition`
  runs the assigned segment list through `ScrubWorker::scrub_partition`
  (spawned via `tokio::task::spawn` over the async store reads — NOT
  `spawn_blocking`; see Deviations) and returns `accepted: true` only after
  the partition actually ran. An execution failure returns an error
  `Status` (no accept-and-ignore). A small in-memory last-result buffer on
  the service makes the executed summary observable to the coordinator-side
  `report_partition_result` handler and to tests.
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
| `oceanfs-durability` | `scrub.rs`: `ScrubCoordinator` fields/ctor gain `Arc<dyn PartitionPlanner>` + `self_id`; `partition_for_current_nodes`/`partition_segments`/`alive_peers`/`with_distributed` deleted; `plan_cycle_partitions` added (`#[doc(hidden)] pub`); `run_cycle` plans + executes the union of all planned partitions locally (== the sealed inventory); review block :601-605 removed. `scrub_service.rs`: `ScrubGrpcService` builds its `ScrubWorker` internally from registry + data store; `assign_partition` executes; last-result buffer added. `lib.rs` exports unchanged (SegmentPartition export landed in f1). |
| `oceanfs-node` | c2 durability builder (`modules/durability.rs`) `ScrubCoordinator::new(...)` gains planner + self id; c3 server builder (`modules/server.rs`) `ScrubGrpcService::new(registry, data_store)`. |

## Interface (Public API)

`ScrubCoordinator` (`scrub.rs:538`):
- `pub fn new(config, planner: Arc<dyn PartitionPlanner>, self_id: NodeId) -> Self`
  — replaces `ScrubCoordinator::new(config)` + `with_distributed(...)`;
  there is no planner-less coordinator state (D2-A).
- `#[doc(hidden)] pub fn plan_cycle_partitions(&self, segments:
  &[SegmentMetadata]) -> Vec<SegmentPartition>` — planner-backed; no peer is
  ever assigned a segment it does not hold; local-only segments are in the
  self partition. `#[doc(hidden)] pub` (NOT `pub(crate)`) so the node-crate
  wiring test can assert the planner output (test-observability seam,
  `ca0f7cb`).
- Removed pub items: `with_distributed` (scrub.rs:577), `alive_peers`
  (scrub.rs:590).

`ScrubGrpcService` (`scrub_service.rs:21`):
- `pub fn new(registry, data_store)` — builds its own `Arc<ScrubWorker>`
  internally from the receiving node's registry + unified data store
  (D4-A). Replaces `ScrubGrpcService::new(metadata_store, data_store)`; the
  ctor does NOT take an `Arc<ScrubWorker>` — that earlier shape was
  inconsistent with `ScrubWorker` staying `pub(crate)`.
  (`ScrubWorker` remains `pub(crate)` within `oceanfs-durability`;
  `SegmentPartition` is `pub` + `#[doc(hidden)]` via f1. No new pub type
  needed.)
- `assign_partition` (unchanged signature) now **executes** and returns
  `accepted: true` only on completion.

## Data Flow

```
ScrubCoordinator (every node, per cycle)  // dispatch NOT wired (D3-A)
  registry.for_each(Sealed) → inventory: Vec<SegmentMetadata>     // self-held set
  plan_cycle_partitions(inventory)
    per segment: holders = metadata.storage_locations
                 eligible = planner.filter to alive + Healthy-data-pool holders (f1)
                 assign segment to one eligible holder; none → self partition
  // until dispatch lands, the UNION of all planned partitions runs locally:
  for batch in union(all_partitions)        // == the sealed inventory, exactly once
    ScrubWorker::scrub_partition(batch) → ScrubReport (full-scan guarantee, spec §7.5)

worker side (future dispatch path — mechanism is now truthful)
  coordinator → AssignPartition { segment_ids ⊆ receiver's storage_locations }
  ScrubGrpcService::assign_partition
    tokio::task::spawn(ScrubWorker::scrub_partition(partition))  // REAL scan (async store)
    accepted = true  ⇔  partition executed (heals enqueued on mismatch)
  worker → ReportPartitionResult(summary)                        // aggregator entry
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds for
      `oceanfs-durability` and `oceanfs-node`.
- [x] **No holder-blind partition:** a workspace grep finds no remaining
      use of `partition_for_current_nodes`, `partition_segments`,
      `with_distributed`, or `alive_peers` in `scrub.rs`; partitions come
      only from `plan_cycle_partitions` / the injected `PartitionPlanner`.
- [x] **Planner correctness test:** a registry inventory whose segments
      list `storage_locations` spanning {self, A, B} plus one local-only
      segment yields partitions where every segment in peer A's partition
      lists A, peer B's partition lists B, and the local-only segment is
      in the self partition; no segment appears in more than one partition
      and no partition contains a non-holder.
- [x] **run_cycle regression:** the local full scan produces the same
      healthy/corrupt/healed counts as before. (Precise semantics under
      D3-A: `run_cycle` executes the UNION of all planned partitions
      locally — which equals the sealed inventory, each segment exactly
      once — NOT "the self partition" alone, since coordinator dispatch is
      not yet wired.) Runs via `ScrubWorker::scrub_partition`.
- [x] **assign_partition executes:** a service-level test drives
      `assign_partition` with a segment list that includes a corrupt
      segment in the receiving worker's registry; asserts the corrupt
      segment was actually scanned (heal enqueued / result recorded in the
      last-result buffer) and `accepted == true`; a worker with an
      unavailable data store returns an error Status, never a silent ack.
- [x] **Review marker:** the `scrub.rs:601-605` block is removed.
- [x] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      and `cargo test -p oceanfs-node --lib -- --test-threads=1` pass
      (RocksDB caveat, PIPELINE.md §4.6).
- [x] **Docs:** changed `pub` items keep `# Examples`;
      `#![deny(missing_docs)]` passes.
- [x] **ADR:** ADR-0033 D1 scrub half satisfied (partitions per-segment
      over `storage_locations`; peers scrub only segments they hold;
      `assign_partition` is wired, no silent acks); ADR-0015 full-scan
      semantics unchanged.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and
> `ignore`-tagged doc examples are non-blocking (see
> `guidelines/coding.md` §9.2).

## Deviations

Landed (`4bc0914`) with the following accepted deviations against the prose
above (implementer + reviewer agreed; user validated D2-A / D3-A / D4-A):

- **D2-A — coordinator ctor is planner-mandatory.** `ScrubCoordinator::new`
  takes `(config, planner: Arc<dyn PartitionPlanner>, self_id: NodeId)` and
  there is no planner-less coordinator state; the old `with_distributed`
  scaffolding was deleted, not kept dormant.
- **D3-A — run_cycle executes the union of all planned partitions locally.**
  While coordinator dispatch over gRPC is not wired,
  `ScrubCoordinator::run_cycle` plans the sealed inventory via
  `plan_cycle_partitions` and executes the UNION of all planned partitions
  on the local worker (== the inventory; each segment exactly once),
  preserving the spec §7.5 full-scan guarantee. The DoD bullet
  "run_cycle regression" originally read "self partition == sealed
  inventory"; under this semantics the executed set is the union of *all*
  planned partitions (not only the self partition), so the DoD item above
  is clarified accordingly.
- **D4-A — ScrubGrpcService builds its ScrubWorker internally.**
  `ScrubGrpcService::new(registry, data_store)` constructs its
  `Arc<ScrubWorker>` internally from the receiving node's registry + data
  store, so `ScrubWorker` stays `pub(crate)`. The earlier doc text implying
  the service ctor takes `Arc<ScrubWorker>` was inconsistent with that
  visibility — the Interface (Public API) section is corrected.
- **Observability seam.** `ScrubCoordinator::plan_cycle_partitions` is
  `#[doc(hidden)] pub`, not `pub(crate)` (added in the review-gap fix
  `ca0f7cb`) so the node-crate wiring test can assert the planner output;
  the Interface section is corrected.
- **Optional LOW (noted, non-blocking):** this doc's Scope / Data Flow
  originally specified `spawn_blocking` for `assign_partition`, while the
  code uses `tokio::task::spawn` over the async store reads (consistent
  with `run_cycle`). Prose above is corrected to match; no behavior impact.

Epic-level process deviation only (no further f3 technical deviation):
f3 was implemented as part of the one-pass epic (per-feature reviewer gates
intentionally skipped) and was covered by the SINGLE independent review at
the end (PASS, iteration 2).
