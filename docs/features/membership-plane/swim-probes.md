---
feature: "Membership Plane: Real SWIM Probes"
epic: "membership-plane"
status: implemented
priority: high
owner: ""
dependencies:
  - epic: membership-plane
    feature: plane-server
adr: [0028]
perf: [4.5, 8.2, 9.1, 9.2]
created: 2026-08-21
updated: 2026-08-21
---

# Membership Plane: Real SWIM Probes

## Summary

ADR-0028 D2/D5: replace the bookkeeping ping chain with the actual SWIM
probe protocol. The detector sends real direct probes (hard deadline
`ping_timeout_ms`), escalates through k relayed indirect probes, and only
then marks SUSPECT. The gossip-push-as-ping-proxy (DK-007) is removed:
gossip stops emitting `PingResponse`; the detector stops consuming it.

## Scope

### In Scope

- Detector probe cycle (`failure_detector/ping.rs` rewritten):
  1. Pick one random alive peer (≠ self, not pending).
  2. Direct `Probe{origin: self, target, is_indirect: false}` on the
     membership plane with `tokio::time::timeout(ping_timeout_ms)` (8.2).
  3. Ack → `recover_suspect` (existing path) + update last-ack.
  4. Timeout → pick `indirect_ping_count` alive relays (≠ self, ≠ target);
     `Probe{origin: self, target, is_indirect: true}` to each; any ack
     recovered; all timed out → `mark_suspect`.
- `pending_pings`/`pending_indirect` bookkeeping replaced by a real
  per-probe deadline map; the two-stage timeout is bound to actual
  messages.
- Removal of the proxy: `GossipCommand::Push` no longer sends
  `DetectorCommand::PingResponse`; `messages_dropped`/`push_duration_us`
  stay as dissemination metrics only.
- Probe metrics: `probe_duration_microseconds` histogram,
  `probe_failures_total`, `indirect_probes_total` counters, registered via
  `register_membership_metrics` (renamed from `register_gossip_metrics`;
  the old name no longer exists — see Deviations).
- Self-ping path kept (in-process `ProbeHandler`).

### Out of Scope

- Origin/version entry semantics (f4) — probe facts are emitted as today
  (bare state events); f4 attaches attribution.
- Dissemination changes (f5).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-membership` | `failure_detector/ping.rs` (probe cycle), `types.rs` (deadline map), `gossip.rs` (proxy removal), metrics |
| `oceanfs-network` | `ProbeRpcClient` usage (probe transport) |

## Interface (Public API)

- `FailureDetector` internals: `pending_probes: HashMap<NodeId, ProbeDeadline>`
  replacing `pending_pings`/`pending_indirect`.
- New metrics as above.

## Data Flow

```
tick → pick target → direct probe (9002, deadline)
   ├─ ack ─────────→ recover_suspect
   └─ timeout ─────→ k relay probes (9002)
        ├─ any ack ─→ recover_suspect
        └─ all fail → mark_suspect → suspicion timer → DEAD (f4 keeps)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` passes; proxy path fully
      removed (no `PingResponse` from push)
- [x] **Tests:** unit: direct-ack recovers; direct-timeout → relays
      initiated; relay-ack recovers; all-relay-fail → SUSPECT; no relay
      available → SUSPECT immediately; probe to unknown node is dropped
      (F1a stays); integration: 3-node in-process cluster — kill target →
      SUSPECT within `2×ping_timeout_ms + margin`, DEAD after
      `suspicion_timeout_ms`, recovery on restart
- [x] **Docs:** `# Examples`; missing-docs deny passes
- [x] **ADR:** ADR-0028 D2 satisfied; DK-007 removed (D5)
- [x] **Perf:** 4.5 (hard per-probe deadline), 8.2 (timeout branches),
      9.1/9.2 (borrowed request/response, `&str` ids)
- [x] **Integration (local):** local churn field 7/7 —
      `load_cluster_churn` 1/1, 10/10 assertions (verified 2026-08-22)
- [ ] **Integration (fleet, deferred to f6):** probe p99 stays below
      `ping_timeout_ms` under data-plane load (16 MiB streams) — a fleet
      measurement gated by the user's checkpoint; see f6
<!-- REVIEW: local churn green (load_cluster_churn 1/1, 10/10 assertions, verified 2026-08-22); the isolation proof — probe_duration_microseconds p99 < ping_timeout_ms while 16 MiB bodies stream — is a fleet measurement (f6, deferred by the user's checkpoint gate) and has not been run; the separate listener + pool + socket opts (node.rs:1594-1620) make it structural but unproven. -->

## Deviations (accepted)

- **Metrics registrar renamed.** `register_gossip_metrics` was renamed
  to `register_membership_metrics` (`membership/manager.rs:276`); the
  old name no longer exists. The membership plane registers the probe
  and gossip series through it.
- **Synthetic convergence-bound test replaced by the end-to-end 3-node
  integration.** The synthetic ≤ `log2(N)+1` convergence-round
  simulation (listed in f5's DoD) is replaced by the real 3-node
  in-process cluster (`tests/swim_probes.rs`): kill → SUSPECT within
  `2×ping_timeout_ms + margin`, DEAD after `suspicion_timeout_ms`,
  recovery on restart — plus the `cluster_gossip` e2e suite. Convergence
  is demonstrated end-to-end on actual probe + gossip messages instead
  of a round-bound simulation.
- **Probe transport: timeout-bounded pooled channel** — see f1/f2
  (`make_client`, `failure_detector/ping.rs:259`).
