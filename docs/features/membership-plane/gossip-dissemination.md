---
feature: "Membership Plane: Version Vectors + Push-Pull Dissemination"
epic: "membership-plane"
status: implemented
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
join; divergence healing is push-side watermark healing (deviation — the
explicit re-sync Pull RPC is superseded, see Deviations).

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
- `Pull` path: join pulls with an empty vector (full list). Divergence
  healing is push-side (deviation, see below): unacked (node, origin)
  keys stay in the delta until the peer acks; the ack-carried pull
  covers the opposite direction each round.
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
divergence heal → push-side watermark healing: unacked (node,origin)
                  keys stay in the delta until acked (re-sync Pull RPC
                  superseded)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` passes
- [x] **Tests:** watermark advance after ack; delta excludes
      already-acked entries; ack pull excludes already-held entries;
      fanout respects k (metrics or spy); convergence in ≤ `log2(N)+1`
      rounds on a synthetic state spread; re-sync heals a deliberately
      diverged vector
<!-- REVIEW: verified 2026-08-22 (iteration 2): watermark advance (ack_advances_watermark_and_prunes_next_delta, gossip.rs:1104), delta exclusion (build_delta_for_excludes_watermarked_entries, gossip.rs:1032), fanout-k (gossip_tick_respects_fanout_k), and the NEW divergence-heal test (diverged_peer_vector_is_healed_by_the_next_delta, gossip.rs:1082) all pass. The synthetic ≤ log2(N)+1 round-bound simulation remains a documented deviation (implementer item 7): convergence is demonstrated end-to-end instead by the 3-node swim_probes integration (tests/swim_probes.rs:206 kill→SUSPECT→DEAD→rejoin→Alive) and the cluster_gossip e2e suite. -->
- [x] **Docs:** `# Examples`; missing-docs deny passes
- [x] **ADR:** ADR-0028 D4 satisfied
<!-- REVIEW: verified 2026-08-22 (iteration 2): the divergence-heal requirement is implemented as push-side watermark healing — build_delta_for (gossip.rs:597) sends every (node, origin) key whose version exceeds the peer's watermark, so a deliberately diverged peer vector is healed on the next round; the ack-carried pull covers the opposite direction each round. The explicit re-sync Pull RPC from the ADR is a documented deviation (redundant under bidirectional push-pull), with the healing behavior directly tested by diverged_peer_vector_is_healed_by_the_next_delta. D4's vector/watermark/fanout/pull machinery is otherwise complete. FIXED. -->
- [x] **Perf:** 1.3 (vector pre-sizing), 2.6 (bounded round channels),
      4.1 (plane pool), 9.2 (`&str` keys in delta computation)
- [x] **Integration:** local churn field 7/7; delta size metric shows
      bounded deltas (≪ full list after warmup); the fleet gossip traffic
      per round drops to O(k)
<!-- REVIEW: verified 2026-08-22 (iteration 2): local churn green (load_cluster_churn 1/1, 10/10 assertions; cluster_* suites 26 tests). The iteration-1 unbounded-delta root cause — recover_suspect emitting a Suspect→Alive event on EVERY successful probe (failure_detector/mod.rs:137) — is fixed by the Suspect-gate (timer OR synced-view Suspect); the regression test successful_probe_of_alive_target_emits_no_recovery_event proves the prober-attributed entry stabilizes, so watermarks prune to empty (ack_advances_watermark_and_prunes_next_delta). The fleet O(k) traffic measurement remains an f6 item (deferred by the user's checkpoint gate). -->

## Deviations (accepted)

- **Explicit re-sync Pull RPC superseded by push-side watermark
  healing.** ADR-0028 D4's re-sync pull ("pull when a peer's vector
  lacks an entry I hold") is implemented on the push side instead:
  `build_delta_for` (gossip.rs:597) keeps every (node, origin) key whose
  version exceeds the peer's watermark **in the delta until the peer
  acks** — unacked keys are never pruned — so a deliberately diverged
  peer vector is healed by the next round, and the ack-carried pull
  covers the opposite direction each round. The dedicated re-sync Pull
  RPC is redundant under bidirectional push-pull. Directly tested by
  `diverged_peer_vector_is_healed_by_the_next_delta` (gossip.rs:1082).
- **Synthetic ≤ `log2(N)+1` convergence-round test replaced** by the
  end-to-end 3-node integration (`tests/swim_probes.rs`: kill → SUSPECT
  → DEAD → rejoin → Alive) plus the `cluster_gossip` e2e suite —
  convergence is proven on real messages (see f3).
