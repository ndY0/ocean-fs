---
feature: "Pool Failure State Machine + Role Consequences"
epic: "disk-resilience-healing"
status: done
priority: high
owner: ""
dependencies: ["disk-io-observability"]
adr: [0029]
perf: [2.3, 7.1]
created: 2026-08-22
updated: 2026-08-23
---

# Pool Failure State Machine + Role Consequences

## Summary

The decision layer of ADR-0029 §D3: a per-node monitor task consumes
`PoolSignal`s (g1) and drives each pool through
Healthy → Degraded → Dead (confirmed-loss rules), applies the
**role-consequence matrix** (wal Dead → write_degraded; metadata Dead →
full unavailability; data Dead → segment loss accounting; hints Dead →
hint-rejection), and sets the manifest flags (`write_degraded`) peers read.
Dead transitions require *confirmation* (ENOENT/EIO/unplug), never latency
alone.

## Scope

### In Scope

- `oceanfs-storage::pool::health` extension:
  - `struct HealthMonitor` — per-node task:
    - tick every `detection_window_secs` (config, f1): snapshot each pool
      via `IoObserver`, run `evaluate_trend` (g1);
    - **transition rules (pinned)**:
      - Healthy → Degraded: `TrendVerdict::Degrading` OR absolute threshold
        spike (error rate > `error_rate_threshold` / `min_errors` in one
        window, OR p99 > `latency_factor` × baseline);
      - Degraded → Dead: *confirmed loss only* — ENOENT on an owned
        segment file, EIO on fsync of a segment/WAL write, or device-unplug
        detection. Latency/trend alone NEVER confirms Dead;
      - Degraded → Healthy: `recovery_window_secs` of zero errors
        (hysteresis);
    - drives `PoolRegistry::set_status` + `set_write_degraded` (f2 stubs
      become live).
- Role-consequence wiring (node layer), **per the ADR-0029 §D3 matrix —
  Degraded never sets `write_degraded`** (Degraded = route around, not
  reject):
  - `write_degraded` set ONLY when the **wal** pool is **Dead** (journal
    gone → cannot durably accept writes); cleared when it returns to
    Healthy (replacement + catch-up, g7);
  - **metadata** pool Dead → node sets a `node_unavailable` flag (g3's
    announcement payload; the S3 API + read path reject/503, see g6);
  - **data** pool Dead → the affected segment set is derived from the
    lifecycle registry (`pool_id == dead_pool`, Phase A f5) — handed to g3
    for the loss announcement;
  - **hints** pool Dead → hint enqueue returns an error (the hint receiver
    path, `HintedHandoffManager` in node.rs:1017-1029) — reconciliation is
    the safety net for lost debt.
- Startup interaction (Phase A f2 policy): a pool that boots Degraded
  (missing-root policy) participates in the same state machine — it can
  confirm to Dead or recover.
- Metrics: `oceanfs_pool_status{pool_id, role}` transitions are now live;
  `oceanfs_pool_write_degraded{pool_id}` gauge.
- Tests:
  - unit: each transition rule (incl. "latency alone never confirms
    Dead"; hysteresis window; recovery clears write_degraded);
  - unit: role matrix — wal Degraded does NOT set write_degraded,
      wal Dead sets it; metadata Dead
    sets node_unavailable; hints Dead rejects enqueue;
  - unit: confirmed-loss inputs (ENOENT/EIO from the observer's error
    kinds) transition to Dead; unplug (write-verify failure) does too.

### Out of Scope

- Announcement/reconciliation/healing (g3-g5) — this feature sets state and
  derives the affected segment set; it does not ship it anywhere.
- Write-path routing on `write_degraded` (g6).
- Catch-up/recovery after wal/metadata replacement (g7/g8).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `HealthMonitor`, transition rules, live status metrics |
| `oceanfs-node` | monitor task spawn, role-consequence wiring, hint-enqueue rejection |

## Interface (Public API)

- `pub struct HealthMonitor` — `new(registry, observer, config)`,
  `run(shutdown_token)`.
- `pub enum ConfirmedLoss { SegmentNotFound, FsyncIo, DeviceUnplug }` —
  the only inputs that may transition Degraded → Dead.
- `pub fn derive_affected_segments(registry: &SegmentLifecycleRegistry, pool_id: u32)
  -> Vec<SegmentId>`
  (node-side helper, Phase A f5 mapping).

## Data Flow

```
IoObserver.snapshot ──▶ HealthMonitor tick ──▶ evaluate_trend (g1)
   └─ Healthy ─▶ Degraded (trend/spike) ─▶ Dead (confirmed loss only)
       └─ PoolRegistry.set_status / set_write_degraded
           ├─ wal Dead ─▶ write_degraded = true
           ├─ metadata Dead ─▶ node_unavailable = true
           ├─ data Dead ─▶ derive_affected_segments (→ g3)
           └─ hints Dead ─▶ hint enqueue rejected
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` in `oceanfs-storage`,
      `oceanfs-node`
      (verified: workspace build clean at HEAD 1aec8cd)
- [x] **Tests:** all listed green (transitions, hysteresis, role matrix,
      confirmed-loss rules)
      (verified: storage 419 lib + 87 doc + 81 integration (10 bins),
      node 49 lib + 24 integration bins incl. failure_state_machine,
      server 227 lib incl. hints_pool_guard — all pass under
      `--test-threads=1`; 36 pool::health unit tests incl. "latency alone
      never confirms Dead", hysteresis, wal Degraded/Dead write_degraded
      matrix)
- [x] **Docs:** `# Examples` on pub items; rustdoc clean
      (verified: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p
      oceanfs-storage -p oceanfs-node` clean; server's 2 link errors are
      pre-existing and unrelated — RING_PROBE_HASHES admin.rs:325,
      HintObjectApplier coordinator.rs:1879)
- [x] **ADR:** ADR-0029 §D3 (typed failure semantics, role-aware
      consequences) satisfied
      (verified: Dead requires confirmed loss only — decide_transition
      crates/oceanfs-storage/src/pool/health.rs:746; Degraded never sets
      write_degraded — health.rs:688-694; role matrix wired — node/health.rs
      metadata→node_unavailable, data→derive_affected_segments,
      coordinator.rs:1152/1841 hints rejection)
- [x] **Perf:** 2.3/7.1 (state transitions are rare; the monitor's
      per-tick snapshot is off the hot path; `set_status` takes the
      registry write lock briefly)
      (verified: parking_lot only in affected files; decision computed
      under per-pool state lock with registry writes outside —
      health.rs:641-679; no std::sync::Mutex/RwLock, bounded event channel)
- [x] **Integration:** a 4-pool node boots; a FaultyIo-injected pool
      degrades and recovers under the monitor, and the manifest's
      `write_degraded` flag flips accordingly (verified via the f6
      membership API)
      (verified: failure_state_machine e2e drives wal Degraded → manifest
      degraded → recover (hysteresis) → Dead → manifest dead +
      write_degraded, and metadata Dead → node_unavailable. Accepted
      deviation: injection is via synthetic observer signals — the
      FaultyIo→observer path is covered by g1's io_observer_faulty.rs; the
      monitor consumes the observer by design)

## Deviations (accepted)

- **`ConfirmedLoss` is detected from observer error kinds, not from raw
  syscall errno at the call site.** The `DiskIo` wrapper (g1) classifies
  ENOENT/EIO at the op boundary — the monitor never interprets errno
  itself. This keeps the storage layer's error semantics in one place.

- **DoD "FaultyIo-injection" is exercised via synthetic observer signals,
  not the FaultyIo injector.** The monitor consumes the g1 `IoObserver`
  trait by design; the FaultyIo injector lives at the storage/io layer
  (covered by g1's `io_observer_faulty.rs`). The e2e test drives the
  monitor's observer input directly (see the Integration DoD note).

- **`HealthMonitorConfig.tick_interval` is overridable for tests.**
  The per-pool f1 knobs (`detection_window_secs`, `recovery_window_secs`,
  thresholds) remain the default cadence; the tick override exists only to
  make unit/e2e tests deterministic without waiting real windows.

- **`write_degraded` is driven by the monitor (storage), not the node
  applier.** This matches the In-Scope bullet "drives
  `PoolRegistry::set_status` + `set_write_degraded`" — the monitor owns the
  wal Dead → write_degraded decision. The node applier handles only the
  metadata/data/hints consequences (`node_unavailable`,
  `derive_affected_segments`, hint-enqueue rejection).

- **`HealthMonitor::new` returns `(Arc<HealthMonitor>, mpsc::Receiver<HealthEvent>)`.**
  The bounded event channel is the seam the consequence applier consumes;
  `run(shutdown_token)` consumes the registry side. This replaces the
  simple `new(registry, observer, config)` signature sketched in the
  Interface section.

- **`reset_pool(pool_id, status)` is the g7 handoff seam.** Dead recovery
  (replacement + catch-up) is out of g2 scope; without `reset_pool`, the
  monitor's Dead mirror would remain absorbed forever after an external
  `registry.set_status(id, Healthy)` — a future confirmed loss would be a
  no-op. g7 calls `reset_pool` after replacement + catch-up; covered by
  the unit test `monitor_reconfirms_dead_after_registry_reset`.
