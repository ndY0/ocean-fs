# ADR-0017: Durability Task Abstraction — `DurabilityTask` Trait and Scheduler

**Status:** Proposed
**Date:** 2026-08-09
**Deciders:** OceanFS design team

---

## Context

A manual code review on 2026-08-09 identified that durability background
processes — garbage collection, orphan reaper, segment compactor,
anti-entropy, scrub — all follow the same pattern:

> "look up a column family + act" (finding #21)

Despite this shared structure, each is implemented independently with
duplicated scheduling logic, hardcoded intervals, no backpressure between
tasks, and no unified observability. Two specific observations:

| # | Finding |
|---|---|
| **#20** | GC runs on all deletion keyspace at once. Could optimise by running on a subset, more frequently. |
| **#21** | Every durability subprocess could get a strategy/family config dictating behaviour, since they all operate on the same "lookup a column family + act" principle. |

The spec §10 (GC), §6.5 (healing), §7.4 (anti-entropy), and §7.5 (scrub)
each define their own interval and parallelism configuration. The architecture
(ADR-0009) places these tasks in `oceanfs-durability`.

## Decision

### 1. `DurabilityTask` Trait in `oceanfs-durability`

```rust
/// A background maintenance task that operates on the storage engine.
///
/// Implementations include garbage collection, orphan reaping, segment
/// compaction, anti-entropy, and scrubbing. Each task defines its own
/// interval, keyspace fraction, and execution logic.
///
/// The scheduler runs tasks on their configured intervals, enforces
/// concurrency limits, and collects metrics.
pub trait DurabilityTask: Send + Sync {
    /// Human-readable name for logging and metrics labels.
    fn name(&self) -> &'static str;

    /// Run one cycle of this task.
    ///
    /// Returns the number of items processed (segments checked, tombstones
    /// collected, etc.) or an error if the cycle failed. Errors are logged
    /// and counted but do not stop the scheduler.
    async fn run_cycle(
        &self,
        metadata: &dyn MetadataStore,
        segments: &dyn SegmentStore,
    ) -> Result<u64>;

    /// Interval between consecutive cycles.
    fn interval(&self) -> Duration;

    /// Fraction of the keyspace to process per cycle, in range (0.0, 1.0].
    /// Default 1.0 means process everything.
    ///
    /// Used by the scheduler to partition work across cycles. For example,
    /// a task with `keyspace_fraction = 0.25` processes one quarter of the
    /// total keyspace each cycle, taking four cycles to complete a full pass.
    fn keyspace_fraction(&self) -> f64 { 1.0 }

    /// Whether this task can run concurrently with itself.
    /// Default `false` — only one cycle runs at a time.
    fn concurrent_cycles(&self) -> bool { false }
}
```

### 2. `DurabilityScheduler`

A single scheduler manages all durability tasks:

```rust
pub struct DurabilityScheduler {
    tasks: Vec<Arc<dyn DurabilityTask>>,
    concurrency_limit: Arc<Semaphore>,   // global I/O limit
    metadata: Arc<dyn MetadataStore>,
    segments: Arc<dyn SegmentStore>,
}

impl DurabilityScheduler {
    pub fn new(config: DurabilityConfig) -> Self;
    pub fn register(&mut self, task: Arc<dyn DurabilityTask>);
    pub async fn run(self) -> Result<()>;
}
```

**Scheduler behaviour:**
- Each task runs on its own `interval` — the scheduler spawns a `tokio::task`
  per task with a `tokio::time::interval` loop
- Before running a cycle, the task acquires a permit from the global
  `concurrency_limit` semaphore — this bounds total durability I/O across all
  tasks
- If `keyspace_fraction < 1.0`, the scheduler tracks the last-processed
  position and advances it each cycle (round-robin partitioning)
- If `concurrent_cycles = false`, the scheduler skips a cycle if the previous
  one is still running (prevents backlog buildup under load)
- Cycles that return `Err` are logged and counted in metrics; the scheduler
  continues with the next interval

**Configuration:**

```toml
[durability]
max_concurrent_tasks = 2     # semaphore permits — limits total I/O
task_timeout_sec = 3600      # maximum duration for a single cycle
```

Individual task intervals and parameters remain in their respective config
sections (`[gc]`, `[heal]`, `[anti_entropy]`, etc.) — this ADR does not
relocate them.

### 3. Keyspace Sharding for GC and Orphan Reaper

The `keyspace_fraction` mechanism directly enables the optimisation from
finding #20:

- **GC:** Instead of scanning the full `deletions` CF every `gc_interval_sec`
  (default 3600s), set `keyspace_fraction = 0.1` to process 10% each cycle.
  A full pass takes 10 cycles but each cycle is 10× faster and more frequent.
  This smooths I/O and avoids the periodic GC spike.

- **Orphan reaper:** Same pattern — scan a fraction of the `segments` CF
  each cycle, checking for unreferenced segments.

The scheduler tracks partition boundaries internally: for a task with
`keyspace_fraction = 0.25`, it divides the key range `[min_key, max_key]`
into four shards and rotates through them.

### 4. Metrics and Observability

Each `DurabilityTask` is instrumented automatically by the scheduler —
individual tasks do not need to implement their own metrics:

| Metric | Type | Labels |
|---|---|---|
| `durability_cycle_total` | Counter | `task`, `status` (ok/error) |
| `durability_cycle_duration_seconds` | Histogram | `task` |
| `durability_items_processed_total` | Counter | `task` |
| `durability_cycle_skipped_total` | Counter | `task`, `reason` (concurrent/timeout) |
| `durability_scheduler_backlog` | Gauge | — |

This replaces the per-task metrics that currently pollute constructors
(review finding #19).

### Scope

**In scope:**
- `DurabilityTask` trait with 5 methods
- `DurabilityScheduler` with registration, concurrency limiting, and
  keyspace partitioning
- Keyspace sharding for GC and orphan reaper
- Unified metrics for all durability tasks
- Integration with existing config sections (no config relocation)

**Out of scope:**
- Fetch ordering strategy (review finding #29 — separate concern,
  read-path, not durability)
- Hinted handoff delivery (separate concern, already designed with
  `HintWal`)
- Changing existing task implementations beyond implementing the trait
- Adaptive interval tuning (future)

## Consequences

### Positive

- **Unified scheduling.** One scheduler, one concurrency limit, one set of
  metrics. Currently each task implements its own interval loop and logging.
- **Backpressure.** The `concurrency_limit` semaphore prevents durability
  tasks from collectively saturating disk I/O. Without this, a simultaneous
  GC cycle + scrub cycle + heal burst could degrade read/write latency.
- **Smoother GC.** `keyspace_fraction = 0.1` means GC runs 10× more
  frequently at 1/10th the cost per cycle. No more periodic I/O spikes when
  GC kicks in.
- **Extensible.** Adding a new durability task (e.g., tiered storage
  migration in the future) requires implementing the trait and calling
  `scheduler.register()` — no new scheduling infrastructure.
- **Cleaner constructors.** Metrics initialisation moves from per-task
  constructors to the scheduler, addressing finding #19.

### Negative

- **Abstraction overhead for small N.** There are currently ~5 durability
  tasks. A trait + scheduler for 5 implementations is arguably overengineered.
  The value is in the unified behaviour (backpressure, metrics, keyspace
  sharding), not code deduplication.
- **Trait in durability crate.** The trait lives in `oceanfs-durability`,
  which is the consuming crate. This follows ADR-0005. However, if a task
  in another crate (e.g., future `oceanfs-tiered-storage`) needs to plug
  into the scheduler, it would need to depend on `oceanfs-durability` — a
  dependency inversion question that can be resolved later (the trait could
  graduate to `oceanfs-core` or a new `oceanfs-durability-api` if needed).
- **Keyspace partitioning accuracy.** The scheduler divides the key range
  into equal-sized shards, but data distribution may be skewed (some
  shards contain more segments than others). This is a minor unfairness,
  not a correctness issue — the next cycle picks up where the last one
  left off.

### Neutral

- **Task intervals remain in existing config sections.** No migration of
  `gc_interval_sec` from `[gc]` to `[durability]`. Operators see no config
  change.
- **Existing task implementations must be adapted.** Each task needs a
  wrapper that implements `DurabilityTask` and delegates to the existing
  `run_cycle` logic. This is mechanical — not a redesign of the task
  internals.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **A. No abstraction — keep per-task scheduling** | No new trait; no refactoring | Duplicated interval loops; no unified backpressure; no keyspace sharding without per-task implementation; metrics remain polluting constructors | Rejected: the review correctly identified that the pattern is shared across all durability tasks; centralising it enables features (backpressure, sharding) that are impractical per-task |
| **B. Trait in `oceanfs-core`** | Available to all crates without depending on `oceanfs-durability` | `core` becomes a dumping ground for traits that aren't cross-cutting; ADR-0005 favours trait-in-consuming-crate | Rejected: durability tasks are specific to `oceanfs-durability`; if a future crate needs to register a task, the trait can be graduated at that point |
| **C. `tokio::sync::Notify` or channel-based scheduling** | More flexible than interval-based; tasks can be triggered on events (e.g., "segment sealed" → trigger compactor) | More complex; event-driven scheduling interacts poorly with backpressure (what if events arrive faster than tasks can process?) | Rejected: interval-based scheduling is simpler and sufficient; the incremental Merkle tree (ADR-0015) handles event-driven AE separately |
| **D. Feature-gate each task** | Operators can compile out tasks they don't need | Adds feature-flag complexity across the workspace; tasks are lightweight when idle; compile-time gating doesn't help runtime resource management | Rejected: runtime scheduling with a semaphore is the right tool for resource management; compile-time gating is orthogonal |

## References

- [Spec §10: Garbage Collection & Compaction](../../docs/spec.md#10-garbage-collection-compaction)
- [Spec §6.5: Healing](../../docs/spec.md#65-healing)
- [Spec §7.4: Anti-Entropy](../../docs/spec.md#74-anti-entropy-background)
- [Spec §7.5: Distributed Scrubbing](../../docs/spec.md#75-distributed-scrubbing)
- [Review 2026-08-09, findings #20, #21, #19](../../review/08-09-2026.md)
- [ADR-0005: Trait-in-Consuming-Crate Pattern](./0005-trait-in-consuming-crate.md)
- [ADR-0009: Storage Crate Split](./0009-storage-crate-split.md)
- [ADR-0015: Anti-Entropy Merkle Protocol](./0015-anti-entropy-merkle-protocol.md)
