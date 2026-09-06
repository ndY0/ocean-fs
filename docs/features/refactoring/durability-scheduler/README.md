---
feature: "Durability Scheduler (ADR-0017) — Implementation Coordination"
epic: "refactoring/durability-scheduler"
status: in_progress
priority: high
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: c2-split-durability-builder must exist first — the ADR-0017 scheduler wrapper, task adaptors, and the shared DurabilityBudget are constructed and registered inside DurabilityModule (roadmap wave 2 ① before ③)
  - epic: refactoring/store-unification
    reason: ADR-0032 (roadmap wave 2 ② before ③) delivers the unified SegmentDataStore the scheduler-registered tasks operate against; adaptors are written against oceanfs_storage_api::SegmentDataStore (its f1/f2/f3), so store unification lands before scheduler wiring
  - epic: refactoring/bounded-metadata-scans
    reason: ADR-0034 (roadmap wave 2 ⑥) replaced the whole-store O(objects) scans in GC/orphan with accounting-based liveness — the f3 keyspace-fraction mechanism ships against that substrate
adr:
  - 0017-durability-task-abstraction
  - 0032-unify-segment-data-access
  - 0025-segment-lifecycle-state-machine
  - 0034-bounded-metadata-accounting
created: 2026-09-04
updated: 2026-09-06
---

# Durability Scheduler — Program Coordination

> **FINAL STATE (2026-09-06):** implementation COMPLETE — the two-tier
> `DurabilityBudget` + `DurabilityScheduler` epic (ADR-0017 + the 2026-09-06
> amendment) is code-green and passed independent review (verdict **PASS**,
> iteration 2): fmt, `cargo build --all-targets`, clippy `-D warnings`
> (core/storage/durability/node libs), `cargo doc -D warnings`, lib tests
> (core 231, storage 458, durability 276 incl. 19 scheduler tests, node 66),
> and the node integration suite `durability_wiring` / `scrub_cycle` /
> `orphan_reaper` / `gc_compaction` / `read_write_roundtrip` /
> `node_lifecycle` (all `--test-threads=1`). f1–f3 are `done`; f4 and this
> README stay **`in_progress`** for one reason only — the single remaining
> open item is an external cloud-harness gate that has NOT been executed
> (DoD #10 below / f4 Boot-e2e; PIPELINE.md §6). It is the **sole remaining
> gate** for the epic; flip f4 + this README to `done` once the harness e2e
> run is green. Recorded accepted deviations (from reviewer-verified state)
> are consolidated in the "Recorded accepted deviations" section below.

> **This is the coordination document for the ADR-0017 scheduler epic.** If
> you are implementing any feature under `refactoring/durability-scheduler/`,
> read this first — it tells you where your work sits in the whole, what must
> exist before you start, what is in/out of the two-tier budget and why, and
> what must not regress while you work. The per-feature docs are the authority
> for your feature; this document is the map.

## Summary

ADR-0017 (2026-08-09) proposed a `DurabilityTask` trait + `DurabilityScheduler`
to unify the durability background tasks (GC, orphan reaper, scrub, AE):
per-task interval loops, one concurrency limit, keyspace-fraction round-robin
partitioning, skip-if-still-running, error tolerance, and unified
`durability_*` metrics. The ADR sat unimplemented through the composition-root
and store-unification waves and was implemented by this epic (2026-09-06).

**ADR-0017 was amended 2026-09-06 (Amendment — Two-Tier Durability I/O
Budget).** A single flat semaphore that only bounds the scheduler's interval
cycles does not close the review's bounded-concurrency concern: queue/event-
driven durability workers (heal, re-rep, inbound hint apply) perform the same
`.dat`/metadata I/O yet were outside the bound, each with its own private gate,
and the review anchor (`healing_service.rs` per-RPC `Semaphore(16)`) bounded
nothing across calls. The amendment replaces the flat semaphore with a
**two-tier admission budget** — Tier-0 (data-layer/repair) work is **never
gated behind Tier-1 (housekeeping)** work; within a tier admission is FIFO-
fair — and puts *every* heavy local durability I/O producer on a tier. Tier
separation is admission-level only: the io/cpu niceness helpers are removed
(they were process-wide / thread-contaminating and never classified the real
syscall threads), and there is no device-level preemption.

Today's reality (verified 2026-09-06): after composition-root c2/c5 and store
unification, every durability-owned loop is spawned by
`DurabilityModule::spawn_loops` in
`crates/oceanfs-node/src/modules/durability.rs` (no longer inline in
`node.rs`): GC, AE, scrub, orphan reaper run `tokio::time::interval` loops
there; the EC heal worker drains `HealQueue`; the re-replication worker +
dispatcher (g5, ADR-0030) and the reconciliation loop are queue/event-driven;
the healing service gRPC handlers serve inbound hints/re-rep/merkle requests.
Each worker historically owned its own semaphore / concurrency control
(`HealWorker::max_concurrent_heals`, `ReRepWorker::max_concurrent_repairs`,
the healing service's per-RPC `Semaphore(16)` at healing_service.rs:1032).

This epic implements ADR-0017 + the 2026-09-06 amendment against that
reality:

- `DurabilityTask` trait whose interval-task implementations register with
  the state/deps they actually need (adaptors capture the worker + its
  `Arc` deps); trait + scheduler live in `crates/oceanfs-durability`
  (ADR-0005).
- `DurabilityBudget` — the two-tier admission budget shared by the scheduler
  (Tier-1, one permit per scheduled cycle) and the Tier-0 workers
  (heal/re-rep/hint apply, one permit per operation). The Tier-0 budget is
  the single gate: the workers' private semaphores are deleted.
- `DurabilityScheduler` engine — owns the Tier-1 interval loops, skip/
  overrun, timeout, error tolerance, `CancellationToken` shutdown, and the
  per-cycle metrics; each cycle acquires a Tier-1 permit from the shared
  budget before running.
- Composition-root wiring in `DurabilityModule` (c2) + the healing gRPC
  service (c3/c5 server module), config `[durability]`, and deletion of the
  old per-task spawn loops and per-worker gates (f4).

## Scope decision — two-tier budget membership (amends the old "in/out" table)

| Producer | Local durability I/O | Tier | Why |
|---|---|---|---|
| GC (`GarbageCollector`, `gc/garbage_collector.rs`) | cycle: accounting-based liveness + compaction `.dat` writes + deletes | **1 — housekeeping** | Clock-driven maintenance; delay is invisible to the contract. Registered as a `DurabilityTask`; 1 Tier-1 permit per cycle |
| Orphan reaper (`OrphanReaper`, `gc/orphan_reaper.rs`) | cycle: accounting liveness + `.dat` unlink + delete requests | **1 — housekeeping** | Clock-driven; 1 Tier-1 permit per cycle |
| Scrub (`ScrubCoordinator`, `scrub.rs`) | cycle: `.dat` reads + merkle verify | **1 — housekeeping** | Verification (its *findings* feed heal, but scrub itself is clock-driven) |
| Anti-entropy (`AntiEntropy`, `anti_entropy/engine.rs`) | cycle: root exchange / full-cycle reads + possible divergence repair | **1 — housekeeping** | Clock cadence; keeps ADR-0015 continuous/sampling internals intact |
| EC heal worker (`HealWorker`, `heal/worker.rs`) | heal op: fetch + EC decode + `write_segment_data` + metadata refresh | **0 — data-layer** | Placement restoration; the durability contract is in arrears until it finishes. NOT a `DurabilityTask` (not interval-scheduled); acquires 1 Tier-0 permit per heal op from the shared budget |
| Re-replication worker (`ReRepWorker`, `repair.rs`) | pull + `write_segment_data` + stamp | **0 — data-layer** | Same restoration contract; 1 Tier-0 permit per pull/write |
| Inbound hint apply (healing service gRPC hint batch) | applies peer writes locally: segment append + metadata | **0 — data-layer** | A peer's delayed write landing locally; 1 Tier-0 permit per hint batch. The per-RPC `Semaphore(16)` at healing_service.rs:1032 is **deleted** — the shared budget is the cross-RPC bound (review anchor closed) |
| Reconciliation (`ReconciliationLoop`, `reconcile.rs`) | dispatch-only locally (enqueues repair); drift scan reads the in-memory registry | **exempt** | No heavy local `.dat`/metadata-CF I/O on this node; ADR-0017 §Considered-C rejects event-driven scheduling |
| Hinted handoff prune / delivery | prune: WAL lifecycle on the hints pool; delivery: outbound network | **exempt** | Hint-WAL lifecycle is internal to `HintedHandoffManager`; outbound delivery is not local durable I/O |
| Segment compactor (`gc/segment_compactor.rs`) | runs **inside** `GarbageCollector::run_cycle` | part of GC | Not separately scheduled; inside GC's Tier-1 permit |
| Client read/write path | `.dat` + metadata | outside this budget | Data plane. The budget meters durability/background producers *relative to each other*; the tier design ensures housekeeping cannot gate Tier-0, and Tier-0 behaves like client traffic |

Metering rule: the budget meters work that performs `.dat` reads/writes or
metadata-CF batch writes on this node, plus whole-store scans. Everything
else is un-metered. One permit = one top-level operation; within-operation
parallelism (heal shard fetch, scrub batch concurrency, GC
`max_concurrent_compactions`) is unchanged and lives inside the permit.

## Feature DAG

```
[epic gates — roadmap wave 2 ① then ② then ⑥ before ③]
  refactoring/composition-root-decomposition/c2-split-durability-builder
  refactoring/store-unification/ f1-unify-trait → f2-single-impl → f3-single-instance-wiring
  refactoring/bounded-metadata-scans/ (ADR-0034 — accounting liveness substrate)
                                    │
                                    ▼
docs/features/refactoring/durability-scheduler/
  README.md                       ← this document (map)
  ├── f1-durability-task-trait    [critical]  DurabilityTask trait + KeyspaceWindow +
  │        │                                  Tier-1 adaptors (GC/orphan/scrub/AE)
  │        ▼
  ├── f2-scheduler                [critical]  DurabilityBudget (two-tier admission) +
  │        │                                  DurabilityScheduler engine: Tier-1 interval
  │        │                                  loops, skip/overrun, timeout, error tolerance,
  │        │                                  shutdown, durability_* + budget metrics
  │        ▼
  ├── f3-keyspace-sharding        [medium]    keyspace_fraction round-robin mechanism;
  │                                           GC/orphan keep keyspace_fraction=1.0
  │                                           (full-space; accounting-based scans stay
  │                                           full-pass — sharding needs a range-scan API)
  │        │
  │        ▼
  └── f4-scheduler-wiring         [high]      DurabilityModule builds the budget + scheduler +
                                              adaptors; heal/re-rep/healing-service acquire
                                              Tier-0; delete per-worker gates + per-task loops
                                              from spawn_loops; [durability] config; drop
                                              io/cpu niceness helpers + config fields
```

Ordering rules:

1. **c1 → c2 → store-unification f1→f2→f3 → bounded-metadata-scans, then this
   epic.** The roadmap (§4) sequences wave 2 ① → ②/⑤ → ⑥ → ③. f4 assumes
   `DurabilityModule` exists, the unified `oceanfs_storage_api::SegmentDataStore`
   is injected by `StorageModule`, and the ADR-0034 accounting substrate is in
   place (f3's full-pass guarantee rests on it).
2. **f1 → f2 → f3 → f4 within this epic.** f2 needs the trait; f3 needs the
   trait's `keyspace_fraction` + the scheduler's rotation cursor; f4 wires the
   completed adaptors + budget + scheduler into the node. f1–f3 are crate-level
   work in `oceanfs-durability` (no node dependency); f4 is the only feature
   that touches `oceanfs-node` (plus `oceanfs-core` for `[durability]` config).
3. **The Tier-0 budget client work (heal/re-rep/hint apply acquiring the
   shared budget, deletion of their private semaphores) is crate-level work in
   `oceanfs-durability`** and lands in f2's wake; the *wiring* (constructing
   the budget and passing it into the workers + the healing gRPC service) is
   f4.
4. **f3 may be re-scoped** by whoever implements it — see f3's scope section.
   It exists to *not* make the full-space scans worse while shipping the
   partition mechanism.

## Reconciliation with ADR-0017 (decisions from 2026-09-04 triage + 2026-09-06 amendment)

1. **No `(metadata, segments)` on the trait.** The scheduler does not know or
   care what a task scans. Each adaptor owns the real deps
   (`Arc<dyn MetadataStore>` + `Arc<SegmentLifecycleRegistry>` for GC,
   `Arc<SegmentLifecycleRegistry>` + `Arc<dyn SegmentDataStore>` for scrub,
   etc.) and the trait is `async fn run_cycle(&self, window) -> Result<u64>`.
2. **GC and orphan reaper keep `keyspace_fraction = 1.0`.** ADR-0034 replaced
   their whole-store object-list passes with accounting-based liveness, but
   both still sweep the full segment/registry space per cycle and the
   `MetadataStore` API has no range-scan method, so a per-cycle fraction
   cannot yet bound a pass. Naive sharding would multiply whole passes per
   unit time — the feature MUST NOT do that. A segment-scoped/range-scan API
   is the enabler (future feature).
3. **AE is not keyspace-sharded.** ADR-0015's incremental tree + sampling is a
   different model; AE registers with `concurrent_cycles=false`,
   `keyspace_fraction=1.0`, and its existing continuous/full dispatch is
   preserved verbatim inside the adaptor.
4. **Metrics keep domain counters.** Existing per-task counters
   (`gc_cycles_total`, `ae_segments_compared_total`,
   `scrub_segments_checked_total`, `orphan_segments_reaped_total`, …) stay.
   The scheduler adds the generic `durability_*` cycle metrics (ADR-0017 §4)
   and the budget adds per-tier active/waiters/wait-duration metrics.
5. **The global flat semaphore is superseded by the two-tier budget**
   (ADR-0017 Amendment). Tier-0 members are **not** `DurabilityTask`s — they
   are budget clients. Event-driven *scheduling* stays rejected
   (ADR-0017 §Considered-C).
6. **io/cpu niceness helpers are removed** (ADR-0017 Amendment): the
   `apply_background_io_class`/`apply_background_cpu_sched` calls and the
   `background_io_class_idle`/`background_cpu_sched_idle` config fields are
   deleted. They classified the wrong threads and one was process-wide.

## Recorded accepted deviations (2026-09-06, from reviewer-verified state)

The following were accepted during implementation/review and are recorded so
no later reader treats them as unaddressed spec drift. Where a deviation
changes an exact-DoD statement, the owning feature doc records it inline.

1. **Engine file location.** The engine lives at
   `scheduler/engine.rs` (not `scheduler/scheduler.rs`); the public path
   `oceanfs_durability::scheduler::DurabilityScheduler` is unchanged (f2).
2. **Metric names.** Duration/wait metrics are named `*_millis` and are u64
   histograms — e.g. `durability_cycle_duration_millis`,
   `durability_repair_wait_duration_millis`,
   `durability_housekeeping_wait_duration_millis` (f2).
3. **Healing-service fetch cap retained.** The healing-service intra-batch
   fetch cap (`FETCH_CONCURRENCY`, healing_service.rs:1069) is retained
   inside the Tier-0 permit as within-operation parallelism, not deleted as a
   gate (f4).
4. **Tier-0 unbounded only when no budget is wired.** Tier-0 clients
   (`HealWorker`/`ReRepWorker`/healing service) are unbounded only when the
   budget is `Option None` (test-only); the composition root always wires the
   shared budget (f4).
5. **No per-adaptor worker-error test.** A dedicated per-adaptor "worker
   error propagates as `Err`" unit test was not added: GC/orphan dead-chunk
   feeds tolerate per-record errors by design; adaptors forward errors via
   `?`; scheduler engine error-tolerance covers `Err` cycles (f1).
6. **`AeTask` Full-window behavior pin not added.** f1's exact-DoD note: the
   `AeTask` Full-window behavior pin was not added (needs the full AE
   scaffold); GC/orphan/scrub behavior pins exist (f1).

## Epic-level DoD (ADR-0017 + amendment acceptance)

- [x] `crates/oceanfs-durability` exposes `DurabilityTask`, `KeyspaceWindow`,
      `DurabilityBudget`, and `DurabilityScheduler`; Tier-1 adaptors exist for
      GC, orphan reaper, scrub, and AE; heal/reconciliation/hint-delivery are
      NOT wrapped as tasks (documented in this README + f1).
- [x] `DurabilityBudget` implements the two-tier invariant: Tier-0
      acquisitions are never gated behind Tier-1 activity (separate permits);
      within a tier admission is FIFO-fair. Unit-tested with mock
      rendezvous (see f2 DoD).
- [x] The Tier-0 budget is the single gate: `HealWorker`'s
      `max_concurrent_heals` semaphore, `ReRepWorker`'s
      `max_concurrent_repairs` semaphore, and the healing service's per-RPC
      `Semaphore(16)` are all deleted and replaced by Tier-0 budget
      acquisitions (`[durability].repair_max_active`, default 16).
- [x] The scheduler owns the Tier-1 interval loops:
      `grep -n "tokio::time::interval"` in
      `crates/oceanfs-node/src/modules/durability.rs` no longer matches
      gc/ae/scrub/orphan-reaper blocks (hint-prune loop remains — internal WAL
      lifecycle; heal/reconcile/re-rep are queue/event-driven).
- [x] The io/cpu niceness helpers and the
      `background_io_class_idle` / `background_cpu_sched_idle` config fields
      are removed; no `apply_background_io_class` /
      `apply_background_cpu_sched` call remains in the node.
- [x] Unified metrics registered once: `durability_cycle_total{task,status}`,
      `durability_cycle_duration_millis{task}`,
      `durability_items_processed_total{task}`,
      `durability_cycle_skipped_total{task,reason}`,
      `durability_repair_active`, `durability_housekeeping_active`,
      `durability_repair_waiters`, `durability_housekeeping_waiters`, and
      per-tier wait-duration histograms.
- [x] GC + orphan reaper scan behavior is byte-for-byte unchanged versus today
      (full-space passes; same `segments_scanned` counts); no task runs more
      than one full pass per cycle (documented in f3).
- [x] Config: `[durability]` `repair_max_active` / `housekeeping_max_active` /
      `task_timeout_sec` in `NodeConfig`; `heal_parallel_segments` removed
      (its role is `repair_max_active`); individual task intervals continue to
      come from their existing `NodeConfig` fields (`gc_interval_sec`,
      `ae_interval_sec`, `scrub_interval_sec`, `orphan_reaper_interval_sec`) —
      no config relocation (ADR-0017 Neutral).
- [x] Green: `cargo build --all-targets`; `cargo test -p oceanfs-durability
      --lib -- --test-threads=1`, `-p oceanfs-node --lib -- --test-threads=1`,
      and the node integration tests `durability_wiring`, `scrub_cycle`,
      `orphan_reaper`, `gc_compaction`, `read_write_roundtrip`,
      `node_lifecycle` (all `--test-threads=1` per PIPELINE.md §4.6) pass.
- [ ] **SOLE REMAINING EXTERNAL GATE (do not mark done until executed):**
      Node boots and the e2e write/read scenario is green with the scheduler
      running GC/orphan/scrub/AE on their configured intervals and heal/re-rep
      acquiring Tier-0 permits.
<!-- REVIEW: Epic DoD #10 (node boots + e2e write/read green, scheduler driving all four cycles, heal/re-rep acquiring Tier-0) remains the SOLE OPEN item after the iter-2 re-verification — it is NOT verifiable locally (PIPELINE.md §6, cloud e2e harness only). Locally verified proxies (all green, iter 2, verdict PASS): cargo build --all-targets; oceanfs-durability --lib 276 tests incl. 19 scheduler tests; oceanfs-node --lib 66 tests incl. background_tasks_spawns_all_handles (node boots with the scheduler handle live, shuts down cleanly) and register_metrics_covers_all_workers (budget + scheduler metrics registered once); node integration durability_wiring / scrub_cycle / orphan_reaper / gc_compaction / read_write_roundtrip / node_lifecycle (--test-threads=1). Condition to pass: green cluster write/read e2e on the harness with durability_cycle_total moving per task and durability_repair_active observed. DoD #10 must stay [ ] until that harness e2e executes (sole remaining external gate, shared with f4 Boot/e2e). -->

## References

- ADR-0017 (this epic's decision, incl. the 2026-09-06 two-tier amendment),
  ADR-0032 (unified store — substrate), ADR-0034 (bounded metadata accounting
  — scan substrate), ADR-0025 (lifecycle registry/coordinator — the machine
  tasks read), ADR-0015 (AE incremental tree — why AE is not sharded),
  ADR-0005 (trait in consuming crate)
- Review triage roadmap: `docs/features/refactoring/review-2026-09-roadmap.md`
  §2 Theme 2, §3 wave 2 ③ (anchors `healing_service.rs:1030`,
  `garbage_collector.rs:160,192`, `health.rs:83`, `node.rs:1932,2620`,
  `write/coordinator.rs:382`)
- Composition root: `docs/features/refactoring/composition-root-decomposition/`
  (c2 = `modules/durability.rs`, "ADR-0017 scheduler lands here as the
  wrapper"; c5 moved all remaining spawns into module-owned `spawn_*` methods,
  bundled by `modules/background.rs`)
- Store unification: `docs/features/refactoring/store-unification/README.md`
- Bounded metadata scans: `docs/features/refactoring/bounded-metadata-scans/README.md`
