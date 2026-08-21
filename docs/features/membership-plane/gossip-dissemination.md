---
feature: "Membership Plane: Version Vectors + Push-Pull Dissemination"
epic: "membership-plane"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: membership-plane
    feature: plane-server
adr: [0028]
perf: [1.3, 2.6, 4.1, 9.2]
created: 2026-08-21
updated: 2026-08-21
---

# Membership Plane: Version Vectors + Push-Pull Dissemination

## Summary

ADR-0028 D4: replace full-state fanout-all pushes with bounded
push-pull gossip. Each node keeps a per-node version vector; deltas are
computed against the peer's watermark; the push response carries the
peer's pull (one round trip per peer per round); fanout is
`k = min(fanout_k, alive-1)` random peers (default 3). Pull remains for
join and explicit re-sync.

## Scope

### In Scope

- `GossipState` gains the local version vector
  `HashMap<NodeId, u64>` (max applied per-node version) and per-peer
  watermarks `HashMap<NodeId, HashMap<NodeId, u64>>`.
- `GossipProtocol::build_delta(peer)` returns entries with
  `version > watermark[peer][node]` (from f4's entry versions).
- Round: pick k random alive peers → `Push(delta, my vector)` →
  `GossipAck{accepted, delta: peer's pull, version_vector}` → merge (f4
  rules) → advance watermarks.
- `Pull` path: join pulls with an empty vector (full list); re-sync pull
  when a peer's vector lacks an entry I hold (divergence heal).
- `GossipMessage.ring_version`/`hlc` removal consumed (f1).
- Metrics: `gossip_delta_entries` histogram, per-round push/pull counts.

### Out of Scope

- Probe mechanics (f3) and merge rules (f4) — used as-is.
- Cross-plane concerns (f2).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-membership` | `gossip.rs` (round + watermarks), `membership/state.rs` (vector), `grpc/gossip_service.rs` (ack payload + vector pull) |
| `oceanfs-network` | generated code (f1) consumed |

## Interface (Public API)

- `GossipProtocol::build_delta_for(peer: &NodeId) -> GossipDelta` (was
  `build_delta`).
- `GossipAck { accepted, updated_entries, delta, version_vector }`.

## Data Flow

```
round → pick k peers → Push(delta vs watermark, my vector)
     ← Ack(delta vs my vector, peer vector) → merge → advance watermarks
join → Pull(empty vector) → full list → announce self
re-sync → Pull(my vector) → missing entries
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` passes
- [ ] **Tests:** watermark advance after ack; delta excludes
      already-acked entries; ack pull excludes already-held entries;
      fanout respects k (metrics or spy); convergence in ≤ `log2(N)+1`
      rounds on a synthetic state spread; re-sync heals a deliberately
      diverged vector
- [ ] **Docs:** `# Examples`; missing-docs deny passes
- [ ] **ADR:** ADR-0028 D4 satisfied
- [ ] **Perf:** 1.3 (vector pre-sizing), 2.6 (bounded round channels),
      4.1 (plane pool), 9.2 (`&str` keys in delta computation)
- [ ] **Integration:** local churn field 7/7; delta size metric shows
      bounded deltas (≪ full list after warmup); the fleet gossip traffic
      per round drops to O(k)
