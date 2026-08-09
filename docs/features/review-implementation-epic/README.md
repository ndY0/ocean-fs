---
feature: "Review Implementation Epic"
epic: "review-implementation-epic"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: gap-closure-addendum
    reason: Config fields, trait-object conversions, and buffer pool wiring must be
      complete before new features build on them
  - epic: config-system-fix
    reason: Config merge/pass-through must work before HintedHandoff, MerkleWal,
      fetch strategies, and shard auto-detect can read their config values
adr:
  - 0015-anti-entropy-merkle-protocol
  - 0016-pluggable-cache-eviction
  - 0017-durability-task-abstraction
  - 0009-storage-crate-split
  - 0005-trait-in-consuming-crate
created: 2026-08-09
updated: 2026-08-09
---

# Review Implementation Epic

## Epic Summary

This epic covers seven architecturally-new features designed during the
2026-08-09 code review discussion. These are NOT gap-closure items (those are
in `docs/features/gap-closure/review-gaps-addendum/feature.md`). Each feature
here introduces new traits, structs, protocols, and crate boundaries that did
not exist before the review. They represent substantive implementation work —
not fixing missing config fields or replacing hardcoded literals.

The seven features are ordered so that foundational abstractions are built
first (WalWriter pattern established by HintWal, DurabilityTask trait
established by the scheduler) and dependent features follow (MerkleWal
reuses WalWriter; existing GC/orphan-reaper/scrub refactored into
DurabilityTask implementors).

## Dependency Graph

```
 ┌─────────────────────────┐       ┌──────────────────────────────┐
 │ Feature 1                │       │ Feature 4                     │
 │ Hinted Handoff Durability│       │ Durability Task Scheduler     │
 │ (HintWal establishes      │       │ (DurabilityTask trait,        │
 │  WalWriter pattern)       │       │  DurabilityScheduler,         │
 └───────────┬──────────────┘       │  keyspace sharding)            │
             │                       └──────────────────────────────┘
             │ depends-on (pattern)                  │
             ▼                                       │ refactors-into
 ┌─────────────────────────┐                         ▼
 │ Feature 2                │       ┌──────────────────────────────┐
 │ Incremental Merkle Tree  │       │ Existing durability tasks     │
 │ Protocol                 │       │ (GC, orphan reaper, compactor, │
 │ (MerkleWal reuses         │       │  AE, scrub) become            │
 │  WalWriter trait)         │       │  DurabilityTask impls         │
 └──────────────────────────┘       └──────────────────────────────┘

 ┌─────────────────────────┐
 │ Feature 3                │       ← independent
 │ Pluggable Cache Eviction │
 └──────────────────────────┘

 ┌─────────────────────────┐
 │ Feature 5                │       ← independent
 │ Shard Count Auto-Detect  │
 └──────────────────────────┘

 ┌─────────────────────────┐
 │ Feature 6                │       ← independent
 │ Fetch Shard Batching     │
 └──────────────────────────┘

 ┌─────────────────────────┐
 │ Feature 7                │       ← independent
 │ Per-Bucket Fetch Strategy │
 └──────────────────────────┘
```

**Ordering rationale:**

| Order | Feature | Depends On | Can Run In Parallel With |
|--------|---------|------------|--------------------------|
| 1st | Hinted Handoff Durability | Nothing | 3, 4, 5, 6, 7 |
| 2nd | Incremental Merkle Tree Protocol | 1 (WalWriter pattern) | 4, 5, 6, 7 |
| 3rd | Durability Task Scheduler | Nothing | 1, 3, 5, 6, 7 |
| 4th | Pluggable Cache Eviction | Nothing | 1, 2, 4, 5, 6, 7 |
| 5th | Shard Count Auto-Detect | Nothing | 1, 2, 3, 4, 6, 7 |
| 6th | Fetch Shard Batching | Nothing | 1, 2, 3, 4, 5, 7 |
| 7th | Per-Bucket Fetch Strategy | Nothing | 1, 2, 3, 4, 5, 6 |

No feature depends on the Durability Task Scheduler — existing durability
tasks are refactored into it after the trait is established, but other
features (MerkleTree, HintedHandoff) are new implementations that can
implement the trait from the start.

## Features

| # | Feature | Review Finding | ADR | Summary |
|---|---------|----------------|-----|---------|
| 1 | [Hinted Handoff Durability](./hinted-handoff-durability/feature.md) | #25 | 0009 | Persistent `HintWal` with inline and segment-ref record types; batched gRPC delivery; truncation on success |
| 2 | [Incremental Merkle Tree Protocol](./incremental-merkle-tree-protocol/feature.md) | #15-18, #27 | 0015 | Incremental Merkle tree updated on segment seal; `MerkleWal` for crash recovery; continuous + sampling AE modes; pre-built tree gRPC exchange; EC path unified through heal pool |
| 3 | [Pluggable Cache Eviction](./pluggable-cache-eviction/feature.md) | #13 | 0016 | `EvictionPolicy` trait; GDSF for L1 object cache; LRU+TTL for L2 metadata cache; O(log n) eviction replacing linear scan |
| 4 | [Durability Task Scheduler](./durability-task-scheduler/feature.md) | #20, #21, #19 | 0017 | `DurabilityTask` trait; `DurabilityScheduler` with concurrency semaphore; keyspace sharding for GC/orphan-reaper; unified metrics |
| 5 | [Shard Count Auto-Detect](./shard-count-auto-detect/feature.md) | #5 | — | `segment_shard_count = 0` → auto-detect from CPU count; startup memory validation; cache pool scaling with derived shard count |
| 6 | [Fetch Shard Batching](./fetch-shard-batching/feature.md) | #26, #30 | — | `group_by_node()` utility; one batched gRPC per node instead of one per shard; applied in read and heal paths |
| 7 | [Per-Bucket Fetch Strategy](./per-bucket-fetch-strategy/feature.md) | #29 | — | `FetchStrategy` enum (LocalFirst, FastestK, BandwidthOptimized, CpuOptimized); per-bucket config; read coordinator dispatch |
