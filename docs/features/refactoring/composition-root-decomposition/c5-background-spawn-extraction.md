---
feature: "c5: Background-Spawn Extraction + start() Slimming"
epic: "refactoring/composition-root-decomposition"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: c4-split-network-builder
    epic: refactoring/composition-root-decomposition
    reason: All four builders must exist before the final start() slimming and spawn extraction
adr:
  - 0031-remove-single-datadir-legacy-mode
perf: []
created: 2026-09-04
updated: 2026-09-04
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
- Each module (storage/durability/server/network) gains its own
  `spawn(&self, cancel: &CancellationToken) -> Vec<JoinHandle<()>>`
  (or a `BackgroundModule` aggregator).
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

- [ ] `Node::start()` under ~300 lines; all background loops module-owned.
- [ ] `node.rs` under ~2,000 lines.
- [ ] Shutdown drains all module handles with configurable grace.
- [ ] Review #68/#71 closed.
- [ ] Guideline §4.1 updated; rustdoc/clippy clean.
- [ ] Node tests + e2e write/read + multi-node tests green.
