---
feature: "Membership Plane: Real SWIM Probes"
epic: "membership-plane"
status: proposed
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
  `register_gossip_metrics` (rename to `register_membership_metrics`).
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

- [ ] **Code:** `cargo build --all-targets` passes; proxy path fully
      removed (no `PingResponse` from push)
- [ ] **Tests:** unit: direct-ack recovers; direct-timeout → relays
      initiated; relay-ack recovers; all-relay-fail → SUSPECT; no relay
      available → SUSPECT immediately; probe to unknown node is dropped
      (F1a stays); integration: 3-node in-process cluster — kill target →
      SUSPECT within `2×ping_timeout_ms + margin`, DEAD after
      `suspicion_timeout_ms`, recovery on restart
- [ ] **Docs:** `# Examples`; missing-docs deny passes
- [ ] **ADR:** ADR-0028 D2 satisfied; DK-007 removed (D5)
- [ ] **Perf:** 4.5 (hard per-probe deadline), 8.2 (timeout branches),
      9.1/9.2 (borrowed request/response, `&str` ids)
- [ ] **Integration:** local churn field 7/7; probe p99 stays below
      `ping_timeout_ms` under data-plane load (16 MiB streams)
