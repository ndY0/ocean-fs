---
feature: "c5: Background-Spawn Extraction + start() Slimming"
epic: "refactoring/composition-root-decomposition"
status: done
priority: medium
owner: ""
dependencies:
  - feature: c4-split-network-builder
    epic: refactoring/composition-root-decomposition
    reason: "c4 LANDED 2026-09-05 (membership + data-plane modules, one pass) — all c1–c4 module builders exist; c5 is the final extraction — start() slimming + module-owned spawn functions + the one-time // ---- renumber"
adr:
  - 0031-remove-single-datadir-legacy-mode
perf: []
created: 2026-09-04
updated: 2026-09-05
---

# c5: Background-Spawn Extraction + start() Slimming

## Summary

The final feature of the epic. Move every inline background loop out of
`Node::start()` — GC/AE/scrub/reaper/prefetch/FD/heal/hint-prune
(`spawn_background_tasks`, node.rs:3265-3540), the health monitor +
consequence applier + loss announcer (16b), segment replicator (16c),
reconciliation (16d), re-rep worker + dispatcher (16e), hinted-handoff
delivery watcher (17), the process/metric poller (node.rs:1937-1952), and
the ready-gate task — into module-owned `spawn` functions. Then slim
`Node::start()` to the builder calls + shutdown wiring. Re-number the
`// ----` sections once, at the end.

## Scope

### In Scope
- Each module (storage/durability/server/membership/data_plane) gains its
  own `spawn(&self, cancel: &CancellationToken) -> Vec<JoinHandle<()>>`
  (or a `BackgroundModule` aggregator). The membership module's c4 spawn
  entry is `start_plane_and_join`; the data-plane module's is `serve`. `c5`
  completes the picture by adding module-owned `spawn` functions for the
  remaining background loops (see Summary).
- `BackgroundTasks` struct + shutdown sequence (`node.rs:3107-3213`)
  simplified to aggregate module handles.
- Configurable shutdown grace period (review #71) lands here (replaces
  hard-coded `10s`/`5s` timeouts).
- Cancellable metric poller (review #68) lands here.
- Guideline update: architecture §4.1 — no `tokio::spawn` in `start()`.

### Out of Scope
- ADR-0017 scheduler (still a later epic) — the spawn extraction makes
  the later swap *easier*, it does not perform it.

## Definition of Done

- [x] `Node::start()` under ~300 lines; all background loops module-owned.
      Verified: `start()` = node.rs:327-448 (122 lines); 0 `tokio::spawn`
      in `start()` (the only 2 spawns in node.rs are in `mod tests`);
      loops live in `DurabilityModule::spawn_loops` (GC/AE/scrub/reaper/
      heal/hint-prune/delivery-watcher/reconciliation/re-rep worker+
      dispatcher), `StorageModule::spawn_loops` (health monitor +
      segment replicator), `server::spawn_prefetch_loop`,
      `MembershipModule::spawn_ready_gate`; bundler `modules/background.rs`
      holds only glue + the cancellable metric poller.
- [x] `node.rs` under ~2,000 lines.
      Verified: 1,744 lines (from 2,606).
- [x] Shutdown drains all module handles with configurable grace.
      Verified: node.rs:990-1092 — leave → gRPC cancel → HTTP cancel →
      cancel all 15 tokens (incl. previously-undrained reconciliation,
      health_monitor, health_consequences, metric_poller) → await 13
      Option-handles under `shutdown_grace_secs` (10) → best-effort
      prefetch/delivery/gRPC under `shutdown_fast_grace_secs` (5) → WAL
      sync → metadata close → `membership.shutdown()`; `await_handle`
      helper at node.rs:975.
- [x] Review #68/#71 closed.
      Verified: metric poller cancellable via own `metric_poller_cancel`
      token (modules/background.rs:188-218; review-#68 note at :174);
      grace configurable via `NodeConfig.shutdown_grace_secs`=10 /
      `shutdown_fast_grace_secs`=5 (crates/oceanfs-core/src/config/
      node.rs:455-475, defaults preserve old 10s/5s).
- [x] Guideline §4.1 updated; rustdoc/clippy clean.
      Verified: guidelines/architecture.md §4.1 (lines ~265-283) — no
      `tokio::spawn` in `Node::start()` + module-owned spawn entries +
      bundler; `RUSTDOCFLAGS="-D warnings" cargo doc` clean; `cargo
      clippy -p oceanfs-node -- -D warnings` clean (the `--all-targets`
      test-scope `expect_used` hits in modules/storage.rs `test_support`
      pre-date c5 — reproduced at HEAD; c5 adds none).
- [x] Node tests + e2e write/read + multi-node tests green.
      Verified: oceanfs-node lib 66 passed; doc 38 passed; 29/29
      oceanfs-node integration suites passed (+ oceanfs-core 1 suite =
      30); e2e allowlist green: crash_restart, segment_lifecycle,
      wal_recovery, cluster_lifecycle (4), cluster_write_path (6),
      cluster_read_path (5), garbage_collection, rewrite_leak_test —
      against a fresh `cargo build --release -p oceanfs` binary. No
      load suites run.

## Implementation Notes / Accepted Deviations

- **Module-owned spawns + bundler-only `background.rs` (approved
  design).** Each background loop is spawned by its *owning* module,
  never by `Node::start()`: `DurabilityModule::spawn_loops` (GC/AE/
  scrub/reaper/heal/hint-prune/delivery-watcher/reconciliation/re-rep
  worker + dispatcher — durability.rs:479), `StorageModule::spawn_loops`
  (pool health monitor + segment replicator — storage.rs:716),
  `modules/server.rs::spawn_prefetch_loop` (server.rs:708),
  `MembershipModule::spawn_ready_gate` (membership.rs:500), and the
  pre-existing `DataPlaneModule::serve`. `modules/background.rs::spawn_all`
  (background.rs:48) is a **bundler only**: it glues the module spawn
  entries, holds the health-consequence applier + loss-announcer
  composition glue, and owns the cancellable metric poller — it spawns
  no loops of its own. Result: `Node::start()` contains zero
  `tokio::spawn` (the only 2 spawns left in node.rs are in `mod tests`).
- **Hinted-handoff machinery re-seated into `DurabilityModule::build`.**
  The WAL-hint machinery (replay + hint-prune loop) and its delivery
  watcher live with durability, which already owns the recovery and the
  re-rep path that consume hints. The scope split is by subsystem
  ownership, not by original node.rs section number.
- **Gossip/FD standbys dropped.** The pre-c5 background block's
  standalone gossip/failure-detector standby spawns are dropped — they
  had no remaining work of their own once the membership-plane
  construction moved into `MembershipModule` (c4 re-seat): gossip +
  probe liveness run inside the membership module's plane join, so a
  node.rs-level standby added nothing.
- **`BackgroundTasks` Option-handle rework with `new()`.** The struct
  (node.rs:43) now carries 16 `Option<JoinHandle<()>>` fields and
  starts empty via `BackgroundTasks::new()` (node.rs:130); each module
  spawn entry fills in the handles it owns. `shutdown()` awaits only
  the handles that exist — no dummy/never handles, no unconditional
  joins.
- **Configurable shutdown grace (review #71).** The hard-coded `10s`/
  `5s` timeouts moved to config: `NodeConfig.shutdown_grace_secs` = 10
  and `shutdown_fast_grace_secs` = 5
  (crates/oceanfs-core/src/config/node.rs:465-472, defaults :661-664) —
  the defaults preserve the old behavior exactly, so no deployment or
  test changes were needed.
- **Shutdown now also drains health/reconciliation/poller — a
  deliberate DoD improvement.** Pre-c5, the reconciliation,
  health_monitor, health_consequences, and metric_poller handles were
  never drained (best-effort spawns). The c5 shutdown sequence
  (node.rs:990-1092) cancels all 15 tokens — including those four — and
  awaits all 16 Option-handles — 13 under `shutdown_grace_secs` (one
  `timeout(grace)` around a `try_join!`), then the best-effort
  prefetch/delivery/gRPC handles under `shutdown_fast_grace_secs`, then
  WAL sync + metadata close + `membership.shutdown()` (`await_handle`
  helper at node.rs:975). The metric poller is cancellable via its own
  `metric_poller_cancel` token
  (modules/background.rs:188-218; review #68 — closed).
- **Reviewer INFO items (2026-09-05, record only — non-blocking, no
  action taken):**
  - **WAL hint replay ordering.** Hint replay now runs inside
    `DurabilityModule::build`, i.e. before startup recovery. There is
    no data dependency between the two paths, so the earlier inline
    ordering (recovery first) and the landed ordering are equivalent.
  - **Ready-gate deadline epoch.** The cluster-ready-gate deadline
    epoch now starts when the bundler (`spawn_all`) invokes
    `MembershipModule::spawn_ready_gate` — after the module builders
    — rather than at the old inline §11 spawn site. Quorum-open
    behavior is unaffected (single-node gates are already open; the
    multi-node gate opens on ring count or the configured bound).

### Landing record (2026-09-05) — status flipped to `done`

c5 was implemented and the independent review returned **PASS on
iteration 1 — 0 blocking gaps**. All six DoD items verified above;
`Node::start()` slimmed ~720 → 122 lines (node.rs:327-448) with zero
`tokio::spawn`; node.rs 2,606 → 1,744 lines; shutdown drains every
module handle under configurable grace (reviews #68/#71 closed);
guideline architecture §4.1 updated. Verification: node lib 66, doc 38,
30 integration suites, e2e allowlist green; clippy (`-D warnings`) /
rustdoc / fmt clean; no load suites (PIPELINE.md §6). c5 closes the
epic — c1, c2, c3a, c3, c4, and c5 are all LANDED with independent
review PASS (epic README STATUS banner updated to EPIC COMPLETE).
