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

- [x] **Code:** `cargo build --all-targets` passes
- [ ] **Tests:** watermark advance after ack; delta excludes
      already-acked entries; ack pull excludes already-held entries;
      fanout respects k (metrics or spy); convergence in ≤ `log2(N)+1`
      rounds on a synthetic state spread; re-sync heals a deliberately
      diverged vector
<!-- REVIEW: watermark advance, delta exclusion, ack-pull exclusion, and fanout-k are tested (gossip.rs tests); the ≤ log2(N)+1 convergence-round test is missing (acknowledged by implementer), and the divergence-heal re-sync pull has NO test because the feature is not implemented (no re-sync trigger exists in gossip.rs — pull is join-only). Would pass when a re-sync pull path exists and both tests land. -->
- [x] **Docs:** `# Examples`; missing-docs deny passes
- [ ] **ADR:** ADR-0028 D4 satisfied
<!-- REVIEW: D4's "explicit re-sync when a peer's vector is missing an entry the local node has (healing divergence)" is not implemented — GossipCommand has no re-sync path and nothing triggers a vector-comparison pull outside join. Push-pull + watermarks + ack-carried pull are complete. Would pass when the divergence-heal pull exists. -->
- [x] **Perf:** 1.3 (vector pre-sizing), 2.6 (bounded round channels),
      4.1 (plane pool), 9.2 (`&str` keys in delta computation)
- [ ] **Integration:** local churn field 7/7; delta size metric shows
      bounded deltas (≪ full list after warmup); the fleet gossip traffic
      per round drops to O(k)
<!-- REVIEW: local churn is green, but push deltas are the only measured series (gossip_delta_entries observes the push side only) and the ack-carried pull never converges to empty: recover_suspect (failure_detector/mod.rs:127) emits a Suspect→Alive event on EVERY successful probe even when the target is not Suspect, bumping the per-(node, origin) version in the manager state each interval; the ack pull (gossip_service.rs:165-206, nodes_full) therefore re-sends the prober-attributed entry every round, and the receiver's self-liveness rule (authority_class = 0) rejects it, so the sender's watermark can never cover it. Fix: gate the recovery event on the target actually being Suspect in alive_nodes. Fleet O(k) traffic measurement is part of f6 (not run). -->
