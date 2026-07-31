---
feature: "SWIM Gossip Membership"
epic: "phase-2-distributed-connectivity"
status: proposed
priority: critical
owner: ""
dependencies:
  - feature: dht-ring-consistent-hashing
    reason: Membership changes trigger ring topology updates
  - feature: connection-pool-grpc
    reason: Gossip messages are sent over gRPC connections
adr: []
perf:
  - "2.4: ArcSwap for read-mostly shared data"
  - "2.6: Bounded channels for inter-task communication"
  - "4.3: TCP_NODELAY on all sockets"
  - "4.5: Adaptive per-operation timeouts"
created: 2026-07-30
updated: 2026-07-30
---

# SWIM Gossip Membership

## Summary

Implement the SWIM-based failure detector and push-pull gossip membership
protocol in `oceanfs-membership`. Each node maintains a partial view of the
cluster. SWIM provides failure detection via direct and indirect pings with
configurable suspicion/failure timeouts. Gossip disseminates membership state
and ring topology changes every `gossip_interval_ms`.

## Scope

### In Scope
- `Membership` struct: manages local membership view, node state machine
- SWIM failure detection: direct ping → indirect ping (k peers) → SUSPECT → DEAD
- `FailureDetector`: ping scheduler, suspicion timer, failure confirmation
- `GossipProtocol`: push-pull gossip every `gossip_interval_ms` with random peers
- Node states: `ALIVE`, `SUSPECT`, `DEAD`, `LEAVING`, `LEFT`
- Gossip message types: `GossipPush` (full or delta state), `GossipPull` (request), `GossipAck`
- Membership state: incarnation number, generation, node metadata (address, vnodes)
- Join protocol: contact seed node → receive full membership + ring → announce self
- Leave protocol: announce `LEAVING` → drain → announce `LEFT`
- Bounded channels for gossip message queues (backpressure)
- Unit tests for state transitions, suspicion timeout, ping failure/recovery, join/leave

### Out of Scope
- Hinted handoff delivery (Phase 4)
- Ring rebalancing orchestration (ring update is triggered, migration is Phase 4)
- Cross-region or multi-DC gossip (single cluster)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `NodeState`, `GossipConfig`, `MembershipEvent`, `Incarnation` |
| `oceanfs-membership` | New crate; modules: `membership.rs`, `failure_detector.rs`, `gossip.rs`, `state.rs` |
| `oceanfs-membership` | Facade exports: `pub use membership::Membership`, `pub use failure_detector::FailureDetector` |

## Interface (Public API)

- `pub enum NodeState` — `Alive`, `Suspect`, `Dead`, `Leaving`, `Left`
- `pub struct GossipConfig` — `interval_ms: u64`, `suspicion_timeout_ms: u64`, `failure_timeout_ms: u64`, `indirect_ping_count: u8`, `seed_nodes: Vec<SocketAddr>`
- `pub struct Membership` — `pub fn new(config: GossipConfig, ring: Arc<RingCache>) -> Self`, `pub async fn join(&self) -> Result<()>`, `pub async fn leave(&self) -> Result<()>`, `pub fn nodes(&self) -> Vec<(NodeId, NodeState)>`, `pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<MembershipEvent>`
- `pub struct MembershipEvent` — `node_id: NodeId`, `old_state: NodeState`, `new_state: NodeState`, `timestamp: Instant`
- `pub(crate) struct FailureDetector` — internal: schedules pings, manages suspicion timers
- `pub(crate) struct GossipProtocol` — internal: push-pull gossip task

## Data Flow

```
Node join:
  1. Contact seed node → receive full gossip state + ring
  2. Announce self (ALIVE, Incarnation=1) via gossip push to all known peers
  3. Peers update membership, gossip to their peers
  4. Ring recomputed with new node's vnodes

Failure detection:
  [background task every gossip_interval_ms]
  Pick random peer P:
    └─ Direct ping P
         ├─ Ack received → P remains ALIVE
         └─ No ack within ping_timeout
              └─ Indirect ping: ask k random peers to ping P
                   ├─ Any ack → P remains ALIVE
                   └─ No ack → P marked SUSPECT
                        └─ After suspicion_timeout_ms → P marked DEAD
                             └─ MembershipEvent emitted → ring.remove_node(P)

Gossip dissemination:
  Every gossip_interval_ms:
    └─ Select random peer
         └─ Push membership delta (changed nodes since last exchange)
              └─ Peer merges delta, responds with its own delta
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core` and `oceanfs-membership`
- [x] **Tests:** Unit tests (24): all state transitions (Alive→Suspect→Dead chain), suspicion timeout, indirect ping delegation, gossip merge (newer incarnation wins), join/leave lifecycle, start-twice error, subscribe. Integration (7): membership creation, subscribe events, state transitions, nodes listing, state_of, join, leave. All 31 pass.
- [x] **Coverage:** `cargo tarpaulin -p oceanfs-membership` aggregate 32.69% (transitive deps). Core membership logic covered by 24 unit + 7 integration tests.
<!-- REVIEW ITER-3: Tarpaulin aggregate still 32.69% due to transitively linked crates. Per-file analysis shows membership.rs, failure_detector.rs, gossip.rs have good coverage. A per-crate isolation approach or --exclude-files would give a more representative figure. -->
- [ ] **Lint:** `cargo clippy --all-targets -- -D warnings` FAILS — 1 unused import (MembershipEvent in membership_lifecycle.rs:11) + 15 unwrap_used/expect_used in test code (12 in membership_lifecycle.rs, 3 in membership.rs test module). `cargo clippy --lib` passes clean.
<!-- REVIEW ITER-3: FIXED from iter-2 (broadcast import, cmd_tx). Remaining issues: only in test files — remove unused import from membership_lifecycle.rs:11 and add #[allow(clippy::unwrap_used, clippy::expect_used)] to test modules. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `Membership` documented with join/leave lifecycle. `RUSTDOCFLAGS="-D warnings" cargo doc` passes.
- [x] **ADR:** N/A (ADR-0002 forthcoming; SWIM vs Raft rationale in spec §2.2)
- [x] **Perf:** Rule 2.6 (bounded gossip channels: mpsc::channel(64) at membership.rs:112-113), 4.3 (TCP_NODELAY — handled by network crate), 4.5 (timeout per ping/indirect/failure — DetectorConfig at failure_detector.rs:23-33). All verified.
- [x] **Integration:** `tests/membership_lifecycle.rs`: 7 tests — creation, subscribe, state transitions, nodes listing, state_of, join, leave. All pass.
<!-- REVIEW ITER-3: FIXED — integration tests exist and pass (7/7). -->
- [ ] **Manual:** Doc example is `ignore`-tagged (membership.rs:55-60), so it does not compile in doctests.
<!-- REVIEW ITER-3: Still UNCERTAIN — doc example is `ignore`-tagged. Either make it live (remove `ignore`) with compilable setup, or keep as-is with justification that example requires a running cluster. -->
