---
feature: "f1: DurabilityTask Trait + Task Adaptors"
epic: "refactoring/durability-scheduler"
status: done
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
  - feature: f2-accounting-liveness
    epic: refactoring/bounded-metadata-scans
    reason: The accounting substrate (ADR-0034) defines the scan shape the GC/orphan adaptors preserve full-space
adr:
  - 0017-durability-task-abstraction
  - 0032-unify-segment-data-access
  - 0025-segment-lifecycle-state-machine
  - 0005-trait-in-consuming-crate
perf: []
created: 2026-09-04
updated: 2026-09-06
---

# f1: DurabilityTask Trait + Task Adaptors

> **FINAL STATE (2026-09-06):** `done`. Independent review verdict **PASS**
> (iteration 2). Code green: fmt, `cargo build --all-targets`, clippy `-D
> warnings`, rustdoc `-D warnings`, lib suite 276 tests incl. 19 scheduler
> tests (`--test-threads=1`). Accepted deviations vs the original exact-DoD
> wording are recorded inline under Definition of Done below (items 5–6 of the
> epic's recorded deviations): (5) no dedicated per-adaptor "worker error
> propagates as `Err`" unit test — error propagation is structural `?`
> forwarding + engine-level error tolerance; (6) the `AeTask` Full-window
> behavior pin was not added (needs the full AE scaffold) — GC/orphan/scrub
> behavior pins exist.

## Summary

Define the `DurabilityTask` trait in `crates/oceanfs-durability/src/scheduler/`
and four thin adaptor structs — one per **Tier-1 (housekeeping) interval
task** the scheduler manages — that implement the trait by delegating to
today's workers (`GarbageCollector`, `OrphanReaper`, `ScrubCoordinator`,
`AntiEntropy`) with the deps each actually needs captured in the adaptor. This
reconciles ADR-0017's `run_cycle(metadata, segments)` signature with today's
reality: GC runs against `(Arc<dyn MetadataStore>, SegmentLifecycleRegistry)`,
scrub against `(SegmentLifecycleRegistry, SegmentDataStore)`, AE against its
own internal state (incremental tree + registry), and the orphan reaper
against the lifecycle coordinator + store it already holds. ADR-0005 places
the trait in `oceanfs-durability` (the consuming crate).

Per the ADR-0017 2026-09-06 amendment the four scheduled tasks are Tier-1
**budget clients**: each cycle the scheduler acquires a Tier-1 permit from the
shared `DurabilityBudget` (f2) before running `run_cycle`. Heal, re-rep, and
inbound hint apply are Tier-0 budget clients and are deliberately NOT
`DurabilityTask`s (they are not interval-scheduled; see README membership).

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
- The `DurabilityBudget` + `DurabilityScheduler` engine (f2).
- Keyspace-fraction rotation and GC/orphan sharding decisions (f3).
- Node wiring / config plumbing (f4).
- Wrapping heal (`HealWorker`, queue-driven), re-rep (`ReRepWorker`,
  queue-driven), reconciliation (`ReconciliationLoop`, event+wake), or hint
  delivery — they are not interval tasks; heal/re-rep participate in the
  two-tier budget as Tier-0 clients in f2's wake (see README scope table), but
  do NOT implement `DurabilityTask`.

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
| `GcTask::run_cycle` | `self.gc.run_cycle(self.metadata.clone(), &self.registry).await` (garbage_collector.rs:244) | `stats.segments_scanned` |
| `OrphanTask::run_cycle` | `self.reaper.run_cycle().await` (orphan_reaper.rs:145) | `stats.segments_scanned` |
| `ScrubTask::run_cycle` | `self.scrub.run_cycle(self.registry.clone(), self.data_store.clone()).await` (scrub.rs:702) | `report.segments_total` |
| `AeTask::run_cycle` | Preserves the dispatch today's spawn loop makes (modules/durability.rs:564-569): `if self.ae.config().core().continuous_enabled { ae.run_continuous_cycle().await } else { ae.run_cycle().await }` (engine.rs:314 / :179) | `stats.segments_compared` |

Intervals are captured at construction (not re-read from worker configs)
because today the *authoritative* cadence comes from `NodeConfig` fields in
`DurabilityModule::spawn_loops`
(`crates/oceanfs-node/src/modules/durability.rs`) — e.g. the reaper's loop
runs at `orphan_reaper_interval_sec` while its `GcConfig` carries
`gc_interval_sec`. Preserving the exact current cadence is the goal;
reconciling the two config sources is config-plumbing (roadmap wave 4,
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
        ▼  each cycle tick (f2): acquire Tier-1 permit (housekeeping_max_active)
   task.run_cycle(KeyspaceWindow::Full)
        ▼
   worker.run_cycle(...) → stats → items_processed (u64)
        ▼
   scheduler metrics (durability_items_processed_total{task})
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-durability`;
      new `scheduler` module re-exported from `lib.rs`.
- [x] **Tests:** `cargo test -p oceanfs-durability --lib -- --test-threads=1`
      (PIPELINE.md §4.6) passes; new tests cover:
      - each adaptor returns the worker's mapped count on a real RocksDB
        store (reuse the existing `gc_compaction`, `orphan_reaper`,
        `scrub_cycle` test scaffolding in the durability crate). GC/orphan/
        scrub behavior pins exist; the `AeTask` Full-window behavior pin was
        **not** added (recorded deviation 6 — see REVIEW note below);
      - each adaptor surfaces a worker error as `Err` — **not** delivered as a
        per-adaptor injection test (recorded deviation 5; propagation is
        structural `?` + engine error tolerance — see REVIEW note below);
      - each adaptor asserts `KeyspaceWindow::Full` and rejects
        `KeyspaceWindow::Shard` with a clear `Error::Internal` (guard for
        f3).
<!-- REVIEW: verified 2026-09-06 (iter 2, verdict PASS). Full lib suite passes (276 tests incl. 19 scheduler tests, --test-threads=1). Sub-bullet coverage in crates/oceanfs-durability/src/scheduler/adaptors.rs:
- Full => mapped count == worker stat on a seeded real RocksDB store: gc_behavior_pin_full_matches_direct (adaptors.rs:481-502, asserts == direct.segments_scanned == 2), orphan_behavior_pin_full_matches_direct (adaptors.rs:459-476), and scrub_behavior_pin_full_matches_direct (adaptors.rs:580) all run the adaptor and the worker's direct run_cycle over the same seeded RocksDB fixture.
- Shard => Error::Internal: shard_window_is_rejected_before_delegation (GcTask, adaptors.rs:239), orphan_rejects_shard_window (adaptors.rs:506), scrub_rejects_shard_window (adaptors.rs:525), ae_rejects_shard_window (adaptors.rs:543). Fraction==1.0 for gc/orphan/scrub asserted in adaptors_report_full_keyspace_fraction (adaptors.rs:274); full_window_is_accepted (adaptors.rs:257).
- "each adaptor surfaces a worker error as Err": ACCEPTED DOCUMENTED LIMITATION — RECORDED DEVIATION 5 (per-adaptor error injection is infeasible with the current doubles — GC/orphan dead-chunk feeds return Vec<io::Result<_>> and per-record errors are skipped by design; scrub/AE failure paths need network/store fault doubles). Error propagation is structurally `?`-forwarding in all four run_cycle bodies (adaptors.rs:87,126,175,219) and the scheduler-boundary Err contract is covered by engine::error_tolerance_keeps_loop_alive (engine.rs:494).
Residual CLOSED for scrub: scrub Full-window behavior pin added (scrub_behavior_pin_full_matches_direct, adaptors.rs:580). RECORDED DEVIATION 6: only the AeTask Full-window behavior pin remains un-added (requires the full AntiEntropy scaffold already used in ae_rejects_shard_window). Node-level scrub_cycle/orphan_reaper/gc_compaction integration suites pass with the scheduler path in the tree.
-->
- [x] **Docs:** every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes in `oceanfs-durability`.
- [x] **ADR:** trait lives in `oceanfs-durability` (ADR-0005); adaptors hold
      only the deps each task actually uses (ADR-0017 reconciliation §1);
      the unified store type comes from `oceanfs_storage_api` (ADR-0032);
      adaptors are the Tier-1 budget clients of the amended ADR (each cycle
      runs under a `housekeeping_max_active` permit).
- [x] **Integration:** `cargo test -p oceanfs-node --test durability_wiring
      -- --test-threads=1` still passes (the node's durability components
      remain wireable); adaptors are exercised end-to-end once f4 lands.
- [x] **Not wrapped:** no `DurabilityTask` impl exists for `HealWorker`,
      `ReRepWorker`, `ReconciliationLoop`, or hint delivery; the `scheduler`
      module doc comment states why (queue/event-driven — Tier-0 budget
      clients are not interval tasks).

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> on production code. Test-code clippy warnings and `ignore`-tagged doc
> examples are non-blocking (see `guidelines/coding.md` §9.2).
