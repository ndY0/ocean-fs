---
feature: "f1: DurabilityTask Trait + Task Adaptors"
epic: "refactoring/durability-scheduler"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: c2-split-durability-builder
    epic: refactoring/composition-root-decomposition
    reason: The adaptors are constructed inside DurabilityModule (c2); this feature's module is crate-level and may proceed once c2 exists, but f4 wiring assumes it
  - feature: f1-unify-trait
    epic: refactoring/store-unification
    reason: The ScrubTask adaptor captures the unified store; after ADR-0032 f1 the trait is oceanfs_storage_api::SegmentDataStore — write the adaptor against the post-unification type path
  - feature: f2-single-impl
    epic: refactoring/store-unification
    reason: The unified DiskSegmentStore impl lands before scheduler wiring so task adaptors see one store (ADR-0032 D2)
  - feature: f3-single-instance-wiring
    epic: refactoring/store-unification
    reason: One Arc<dyn SegmentDataStore> instance is injected into the tasks the adaptors wrap (ADR-0032 D4)
adr:
  - 0017-durability-task-abstraction
  - 0032-unify-segment-data-access
  - 0025-segment-lifecycle-state-machine
  - 0005-trait-in-consuming-crate
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f1: DurabilityTask Trait + Task Adaptors

## Summary

Define the `DurabilityTask` trait in `crates/oceanfs-durability/src/scheduler/`
and four thin adaptor structs — one per interval task the scheduler manages —
that implement the trait by delegating to today's workers
(`GarbageCollector`, `OrphanReaper`, `ScrubCoordinator`, `AntiEntropy`) with
the deps each actually needs captured in the adaptor. This reconciles
ADR-0017's `run_cycle(metadata, segments)` signature with today's reality:
GC runs against `(Arc<dyn MetadataStore>, SegmentLifecycleRegistry)`, scrub
against `(SegmentLifecycleRegistry, SegmentDataStore)`, AE against its own
internal state (incremental tree + registry), and the orphan reaper against
the lifecycle coordinator + shard store it already holds. ADR-0005 places the
trait in `oceanfs-durability` (the consuming crate). Heal, reconciliation,
and hint delivery are deliberately NOT wrapped (see Scope).

## Scope

### In Scope
- `pub trait DurabilityTask` (in `oceanfs-durability`, new `scheduler`
  module): `name()`, `interval()`, `keyspace_fraction()`,
  `concurrent_cycles()`, `run_cycle(&self, window: KeyspaceWindow)`.
- `pub enum KeyspaceWindow` — `Full` (fraction == 1.0) or
  `Shard { index, total }` (round-robin, consumed only by shard-aware tasks;
  see f3).
- Adaptor structs (each `pub` in the `scheduler` module, re-exported from
  `lib.rs`):
  - `GcTask { gc: Arc<GarbageCollector>, metadata: Arc<dyn MetadataStore>,
    registry: Arc<SegmentLifecycleRegistry>, interval: Duration }`
  - `OrphanTask { reaper: Arc<OrphanReaper>, interval: Duration }`
  - `ScrubTask { scrub: Arc<ScrubCoordinator>,
    registry: Arc<SegmentLifecycleRegistry>,
    data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore>, interval: Duration }`
  - `AeTask { ae: Arc<AntiEntropy>, interval: Duration }`
- Unit tests proving each adaptor maps the worker's stats to the trait's
  "items processed" count and propagates worker errors.

### Out of Scope (for this feature)
- The `DurabilityScheduler` engine (f2).
- Keyspace-fraction rotation and GC/orphan sharding decisions (f3).
- Node wiring / config plumbing (f4).
- Wrapping heal (`HealWorker`, queue-driven), reconciliation
  (`ReconciliationLoop`, event+wake), or hint delivery — these are not
  interval tasks; do NOT force them into the trait (see README scope table).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | New `src/scheduler/mod.rs` (+ `scheduler/task.rs`, `scheduler/adaptors.rs`); re-exports `DurabilityTask`, `KeyspaceWindow`, `GcTask`, `OrphanTask`, `ScrubTask`, `AeTask` from `lib.rs` |
| `oceanfs-node` | None in this feature (wiring is f4) |
| `oceanfs-storage-api` | None (consumes the unified `SegmentDataStore` trait from ADR-0032 f1) |

## Interface (Public API)

```rust
// crates/oceanfs-durability/src/scheduler/task.rs

/// The window of a task's keyspace a single cycle should process.
pub enum KeyspaceWindow {
    /// Process everything (used when `keyspace_fraction() == 1.0`).
    Full,
    /// Process shard `index` of `total` (round-robin rotation; only
    /// shard-aware tasks receive this — see f3).
    Shard { index: u64, total: u64 },
}

/// A background maintenance task scheduled by the [`DurabilityScheduler`](f2).
pub trait DurabilityTask: Send + Sync {
    /// Human-readable name for logging and metrics labels (`"gc"`,
    /// `"orphan_reaper"`, `"scrub"`, `"anti_entropy"`).
    fn name(&self) -> &'static str;

    /// Interval between consecutive cycles. Read from the same `NodeConfig`
    /// fields the node's spawn loops use today (`gc_interval_sec`, etc.) and
    /// captured at adaptor construction — intervals do NOT move.
    fn interval(&self) -> Duration;

    /// Fraction of the keyspace to process per cycle (0.0, 1.0].
    /// Default 1.0 = full pass. Tasks that cannot shard return 1.0.
    fn keyspace_fraction(&self) -> f64 { 1.0 }

    /// Whether a new cycle may start while a previous one is still running.
    /// Default `false` (serial per task).
    fn concurrent_cycles(&self) -> bool { false }

    /// Run one cycle over `window`. Returns the number of items processed
    /// (segments scanned / compared) or an error. Errors are logged and
    /// counted by the scheduler but do not stop it.
    async fn run_cycle(&self, window: KeyspaceWindow)
        -> oceanfs_durability::Result<u64>;
}
```

Adaptor delegations (each implements `run_cycle(Full, …)` only and asserts
`window == Full`, because GC/orphan/scrub/AE all have `keyspace_fraction() ==
1.0` in this epic):

| Adaptor | Delegates to (today's code) | Items processed = |
|---|---|---|
| `GcTask::run_cycle` | `self.gc.run_cycle(self.metadata.clone(), &self.registry).await` (garbage_collector.rs:232) | `stats.segments_scanned` |
| `OrphanTask::run_cycle` | `self.reaper.run_cycle().await` (orphan_reaper.rs:120) | `stats.segments_scanned` |
| `ScrubTask::run_cycle` | `self.scrub.run_cycle(self.registry.clone(), self.data_store.clone()).await` (scrub.rs:708) | `report.segments_total` |
| `AeTask::run_cycle` | Preserves node.rs:3352 dispatch: `if self.ae.config().core().continuous_enabled { ae.run_continuous_cycle().await } else { ae.run_cycle().await }` (engine.rs:307 / :178) | `stats.segments_compared` |

Intervals are captured at construction (not re-read from worker configs)
because today the *authoritative* cadence comes from `NodeConfig` fields in
`node.rs::spawn_background_tasks` — e.g. the reaper's loop runs at
`orphan_reaper_interval_sec` while its `GcConfig` carries `gc_interval_sec`
(node.rs:3410 vs :1018-1023). Preserving the exact current cadence is the
goal; reconciling the two config sources is config-plumbing (roadmap wave 4,
theme 3), out of scope.

## Data Flow

```
DurabilityModule::build (c2, f4)
  → GcTask::new(gc, metadata, registry, interval)          // GC
  → OrphanTask::new(reaper, interval)                      // orphan reaper
  → ScrubTask::new(scrub, registry, data_store, interval)  // scrub
  → AeTask::new(ae, interval)                              // anti-entropy
  → scheduler.register(Arc<dyn DurabilityTask>)            // f2 API
        │
        ▼  each cycle tick (f2)
   task.run_cycle(KeyspaceWindow::Full)
        ▼
   worker.run_cycle(...) → stats → items_processed (u64)
        ▼
   scheduler metrics (durability_items_processed_total{task})
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`;
      new `scheduler` module re-exported from `lib.rs`.
- [ ] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      (PIPELINE.md §4.6) passes; new tests cover:
      - each adaptor returns the worker's mapped count on a real RocksDB
        store (reuse the existing `gc_compaction`, `orphan_reaper`,
        `scrub_cycle` test scaffolding in the durability crate);
      - each adaptor surfaces a worker error as `Err`;
      - each adaptor asserts `KeyspaceWindow::Full` and rejects
        `KeyspaceWindow::Shard` with a clear `Error::Internal` (guard for
        f3).
- [ ] **Docs:** every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes in `oceanfs-durability`.
- [ ] **ADR:** trait lives in `oceanfs-durability` (ADR-0005); adaptors hold
      only the deps each task actually uses (ADR-0017 reconciliation §1);
      the unified store type comes from `oceanfs_storage_api` (ADR-0032).
- [ ] **Integration:** `cargo test -p oceanfs-node --test durability_wiring
      -- --test-threads=1` still passes (the node's durability components
      remain wireable); adaptors are exercised end-to-end once f4 lands.
- [ ] **Not wrapped:** no `DurabilityTask` impl exists for `HealWorker`,
      `ReconciliationLoop`, or hint delivery; the `scheduler` module doc
      comment states why.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
