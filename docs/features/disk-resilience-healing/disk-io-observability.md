---
feature: "Disk IO Observability + Fault Injection"
epic: "disk-resilience-healing"
status: proposed
priority: high
owner: ""
dependencies: []
adr: [0029]
perf: [1.3, 7.1, 11.1]
created: 2026-08-22
updated: 2026-08-23
---

# Disk IO Observability + Fault Injection

## Summary

The signal source for ADR-0029 §D3: a `DiskIo` abstraction over the storage
crate's file ops that (a) records per-pool I/O error counters and latency
samples (the health monitor's input) and (b) enables unit-level fault
injection (`FaultyIo`). Trend-based, tech-aware degradation detection lives
here as pure logic. No status transitions yet — this feature produces
signals; g2 consumes them.

## Scope

### In Scope

- `oceanfs-storage::io` extension:
  - `trait DiskIo: Send + Sync` — the file-op surface used by the segment
    read/write path (the `io` module already has `IoReadMode`/
    `SegmentWriteMode`, node.rs:648-651; the trait wraps the ops the health
    monitor must observe: read/write/fsync/open). Methods return
    `io::Result` and record on a shared `IoObserver` when called with one.
  - `struct IoObserver` — per-pool signal accumulation:
    - `record_error(pool_id, err_kind)` — increments
      `oceanfs_pool_io_errors_total{pool_id}` and appends the error kind to
      a time-bucketed ring buffer (bounded, pre-sized, perf 1.3);
    - `record_latency(pool_id, op, duration)` — per-op latency histogram
      (p50/p99/p999) per pool, bounded;
    - `snapshot(pool_id) -> PoolSignal` — the last window's error rate +
      latency percentiles + SMART counters (where available) for the trend
      detector.
  - `struct FaultyIo` — test wrapper around any `DiskIo`:
    `fail_next(n, err)`, `fail_after(trigger)`, `delay(dur)`,
    `die_on_read/write` (asymmetric) — the test-framework Level-1 injector.
  - Zero-cost in release: `IoObserver` is an `Arc` with atomic counters; a
    `NoopIoObserver` (const, no-op) is the default so the hot path adds one
    branch-free atomic increment per op (perf 3.2/7.1 — no lock, no
    allocation in the record path).
- Trend detector (`oceanfs-storage::pool::health`):
  - `fn evaluate_trend(signal_history: &[PoolSignal], tech: PoolTech) -> TrendVerdict`
    — pure function over the last `N` windows (N from
    `PoolHealthConfig::trend_window_secs / detection_window_secs`, min 4):
    compute the error-rate series `e[i]` and p99-latency series `l[i]`
    over windows; **`Degrading` iff any series shows a monotonic-worsening
    slope: `x[i] >= 2 * x[i-1]` for the LAST TWO consecutive window pairs
    (i.e. both `x[n-1] >= 2*x[n-2]` and `x[n-2] >= 2*x[n-3]`), even when
    every `x[i]` is below the absolute threshold**. `Stable` otherwise.
    Erratic single-window spikes do not trip the slope (they accumulate
    into the next window's baseline), and are handled by the absolute
    fast-path threshold in g2 instead.
  - Tech baselines: `hdd` additionally treats SMART reallocated+pending
    sector growth > 0 across windows as `Degrading`; `ssd`/`nvme`
    additionally treats uncorrectable-ECC/wear growth; `cloud-ephemeral`
    uses I/O signals only (defaults from the brainstorm §2.3).
  - `PoolSignal` — the per-window aggregate (error rate, p50/p99/p999
    latency, smart counters: Option<u64> per tech).
- Tests (all unit):
  - `FaultyIo`: fail_next/fail_after/delay/die_on_read behavior;
  - observer: counters increment, ring buffer bounded, snapshot correct;
  - trend: exponential-below-threshold error growth triggers `Degrading`;
    flat low errors → `Stable`; erratic/intermittent errors accumulate but
    do not flip state alone; per-tech baselines differ (hdd SMART tell vs
    nvme ECC tell vs cloud-ephemeral I/O-only).

### Out of Scope

- Status transitions / state machine (g2).
- Wiring the observer into every storage path (g2 does the plumbing;
  here the trait + observer exist and the segment io module implements
  `DiskIo` for its own ops).
- Loss announcements, reconciliation, re-replication (g3-g5).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | New `io::disk_io` module (`DiskIo`, `IoObserver`, `FaultyIo`), `pool::health` trend detector |

## Interface (Public API)

- `pub trait DiskIo` — observed file ops (read/write/fsync/open).
- `pub struct IoObserver` — `record_error`, `record_latency`, `snapshot`.
- `pub struct FaultyIo` — test fault injector.
- `pub enum TrendVerdict { Stable, Degrading }` + `evaluate_trend(...)`.
- `pub struct PoolSignal` — per-window aggregate.

## Data Flow

```
segment io ops ──▶ DiskIo ──▶ IoObserver.record_error/latency
   └─ per-pool ring buffers (bounded)
health monitor task (g2) ──▶ observer.snapshot(pool_id) ──▶ evaluate_trend
   └─ TrendVerdict ──▶ state machine input
unit tests ──▶ FaultyIo ──▶ DiskIo under injection
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-storage`
      (verified: storage + node + server all build clean)
- [x] **Tests:** all listed green (FaultyIo, observer, trend per-tech)
      (verified: storage 479 tests, node 49 lib + 20 integration bins incl.
      the 2 new io_observer tests, server 226 lib — all pass under
      `--test-threads=1`)
<!-- REVIEW: `cargo clippy -p oceanfs-node --all-targets -- -D warnings` FAILS on
     the new integration test crates/oceanfs-node/tests/io_observer_faulty.rs:9-10
     (`doc_lazy_continuation` — "doc list item without indentation"). The lib
     target is clean; only the new file's `//!` doc list continuation trips the
     lint. Fix: indent the continuation lines under the list item. Would pass
     once `cargo clippy -p oceanfs-node --all-targets -- -D warnings` is clean.
     Also note: the pre-existing `grpc_services::swim_death_detection_within_timeout`
     failure (crates/oceanfs-server/tests/grpc_services.rs:631) reproduces at HEAD
     b53d6aa and is unrelated to this feature. -->
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
      (verified: `RUSTDOCFLAGS="-D warnings" cargo doc` clean on
      oceanfs-storage + oceanfs-node; 78 doctests pass; server's 2 link errors
      at admin.rs:325 + write/coordinator.rs:1826 reproduce at HEAD and are
      pre-existing)
- [x] **ADR:** ADR-0029 §D3 (trend-based detection, tech-aware signal sets)
      satisfied at the signal level
      (verified: `doubling` slope rule matches "x[i] >= 2*x[i-1] for the last
      two consecutive pairs"; tech baselines hdd/ssd/nvme/cloud-ephemeral
      match §D3; no status transitions added)
- [x] **Perf:** 11.1 (record path is atomic increments — no lock, no
      alloc), 7.1 (observer snapshots take a bounded lock only on the
      periodic path, never on the record path), 1.3 (pre-sized ring
      buffers)
      (verified: record path is bounds-checked `OnceLock` slot lookup +
      `AtomicU64`/`Ordering::Relaxed` increments only; the only lock is
      `parking_lot::Mutex` on the snapshot rotation path; window ring +
      error-kind + histogram arrays are all fixed pre-sized)
- [x] **Integration:** a `oceanfs-node` test runs a small write cycle
      through a `FaultyIo`-wrapped store and asserts the observer counted
      the injected errors per pool
      (verified: tests/io_observer_faulty.rs counts 2 injected errors on pool
      0 and 0 on pool 1; tests/io_observer_wiring.rs additionally proves a
      real PUT → seal → observer feeds write+fsync latency end-to-end)

## Deviations (accepted)

- **Smart counters are `Option<u64>` placeholders in Phase B v1.** Real
  SMART reads (sysfs) land later; the signal shape exists so the trend
  detector and tech profiles are correct, and the observer can be fed
  synthetic SMART values in tests.
- **Scope extension (owner-agreed): the seal pipeline's writes + the
  flush coordinator's fsync route through the `DiskIo` trait in g1.**
  The D3 Dead-confirming signals (EIO on write, EIO on fsync) are
  observable per pool immediately — not deferred to g2's plumbing. This
  adds `oceanfs-node` wiring (`IoObserver` + `observe_into` +
  `SealConfig.observer`/`io_backend` + `Node::io_observer()`) and
  server/bench rename fallout beyond the crate-impact table, which
  listed only `oceanfs-storage`. g2's remaining plumbing is then
  genuinely incremental: the read path (`DiskSegmentReader`) via a
  pool-aware `ObservedIo`, the WAL sync path, and the health-monitor
  task that consumes snapshots.
- **`IoObserving` surface trait.** `DiskIo::observer()` returns
  `&dyn IoObserving` — a small trait shared by the real `IoObserver`
  and the const `NoopIoObserver` (needed so a zero-cost no-op observer
  implements the same record surface).
- **The error-kind "time-bucketed ring buffer" is a per-window
  per-kind atomic counter array over the fixed window ring.** The
  lock-free record path (perf 11.1) precludes an event-level ring;
  per-event detail is deferred. Kind counts ride each window bucket and
  are reset on snapshot rotation.
- **The trait's `open` op is not exercised on the seal path in g1.**
  Temp-file creation (O_DIRECT / `create_temp`) stays a raw `std::fs`
  op; write + fsync are the observed ops. g2 wires `open` where the
  read path needs it.
- **`FaultyIo` injection is exercised at the storage/io layer** (the
  injector's home): `io_observer_faulty.rs` wraps a pool-aware
  `ObservedIo` and asserts per-pool error counts for the exact
  write+fsync cycle the seal performs; `io_observer_wiring.rs` proves
  the real node's seal pipeline feeds the observer end-to-end (a
  `FaultyIo` cannot wrap the node's internal sealer from a test).
- **`PoolTech::Auto` is treated as I/O-signals-only** in
  `evaluate_trend` (it is resolved to a concrete tech by the pool
  runtime before this layer).
