---
feature: "Membership Plane: Dedicated Server + Deploy"
epic: "membership-plane"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: membership-plane
    feature: plane-proto-config
adr: [0028]
perf: [4.1, 4.3, 7.1]
created: 2026-08-21
updated: 2026-08-21
---

# Membership Plane: Dedicated Server + Deploy

## Summary

ADR-0028 D1: a second tonic server on `membership_listen_addr` (9002)
hosting **only** the membership services — `GossipRpc` and the new
`ProbeRpc`. The data-plane server (9001) drops Gossip and keeps
Segment/Healing/Cache/Scrub. The membership `ConnectionPool` from f1
becomes the pool the gossip protocol and join path use. Deploy scripts
open the port and derive the announced membership address.

## Scope

### In Scope

- `oceanfs-node::node`: bind the membership listener with reuseport +
  socket opts (quickack/busy-poll, 4.3); register `GossipRpcServer` +
  `ProbeRpcServer` on it; remove `GossipRpcServer` from the data-plane
  router; wire the membership pool into `Membership::set_pool`.
- Membership announce address = derived membership address (f1 helper);
  `join` pull + self-announce run over the membership pool.
- `ProbeRpc` service shell: direct-probe handler (target == self → ack
  with incarnation) and **relay forwarding** (is_indirect, target ≠ self →
  forward a direct probe to the target over the membership pool, relay the
  response back). The detector's client-side cycle is f3.
- Startup order invariant (existing): membership server bound BEFORE
  `membership.start()` + join.
- `scripts/sut-deploy.sh` + fleet templates: open 9002 (ufw), advertise
  membership address in the node config; `scripts/observe.sh` unchanged
  (metrics port untouched); harness deployment unchanged.

### Out of Scope

- Detector probe cycle / timeouts (f3).
- Merge/dissemination changes (f4, f5).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-membership` | `grpc/probe_service.rs` gains the tonic `ProbeRpc` impl (direct + relay forward); `gossip_service` unchanged |
| `oceanfs-node` | Second listener + router split; membership pool wiring |
| `scripts/sut-deploy.sh` | Port 9002 firewall + config |
| `.hetzner/*` templates | Firewall/security group for 9002 |

## Interface (Public API)

- `oceanfs_membership::grpc::probe_service::ProbeGrpcService::new(probe_handler, pool, self_id)`
  — the tonic service; direct probes ack locally, relay probes forward via
  the membership pool.
- `Membership::set_pool` unchanged (now receives the plane pool).

## Data Flow

```
node start → bind 9001 (data) + 9002 (membership)
           → membership.start() → gossip pushes/join over 9002 pool
peer A → Probe{direct} → peer B (9002) → ack{incarnation}
peer A → Probe{indirect, target=B} → relay C (9002) → direct probe B → ack → C → A
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` passes; both listeners bind;
      data-plane server serves Segment/Healing/Cache/Scrub only
- [x] **Tests:** gRPC integration: push/pull over 9002; direct probe
      round-trip; relay probe round-trip through a real 3-node in-process
      cluster (probe from A through C to B, ack returns B's incarnation)
- [x] **Docs:** `# Examples` on new `pub` items; missing-docs deny passes
- [x] **ADR:** ADR-0028 D1 satisfied; D2 service present
- [x] **Perf:** 4.1 (plane pool), 4.3 (socket opts), 7.1 (no lock held
      across the forward call)
<!-- REVIEW: verified 2026-08-22 (iteration 2): the membership listener at crates/oceanfs-node/src/node.rs:1594 now binds via create_reuseport_listener, and accepted connections get apply_opts_to_fd(quickack, busy_poll) in the TcpListenerStream map (node.rs:1613-1620) — the same socket-opts treatment as the data-plane listener (node.rs:1560-1574). FIXED. -->
- [x] **Integration:** local 3-node churn field still spotless (7/7) with
      gossip now on 9002; deploy script provisions a 3-node fleet with
      9002 open and gossip converges
