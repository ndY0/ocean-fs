---
feature: "c4: Extract NetworkModule Builder"
epic: "refactoring/composition-root-decomposition"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: c3-split-server-builder
    epic: refactoring/composition-root-decomposition
    reason: gRPC services built by c3 are bound here; membership bootstrap needs the router-ready services
adr:
  - 0028-membership-plane-full-swim-gossip
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# c4: Extract NetworkModule Builder

## Summary

Extract membership/connection-pool construction, HTTP + gRPC listener
binding, membership-plane binding, join/bootstrap, manifest declaration,
and the routing-cache event subscriber (node.rs sections 4, 4a, 4b, 5,
14, 15b–15e) into `modules/network.rs`. Returns the bound handles + the
membership/pool/routing-cache Arcs `Node` holds for shutdown and
observability.

```rust
pub struct NetworkModule {
    pub membership: Arc<Membership>,
    pub pool: Arc<ConnectionPool>,
    pub manifest_cache: Arc<crate::routing_cache::ManifestCache>,
    pub membership_state_store: MembershipStateStore,
    pub http_shutdown: CancellationToken,
    pub grpc_shutdown: CancellationToken,
    pub http_handle: JoinHandle<()>,
    pub grpc_handle: JoinHandle<()>,
    pub membership_handle: JoinHandle<()>,
    // cache-event subscriber + rejoin task join handles
}
```

## Scope

### In Scope
- Move membership (incl. rejoin state + incarnation bump, ADR-0022) and
  connection-pool construction.
- Move HTTP/gRPC/membership-plane listener binding and `serve` spawns.
- Move join bootstrap + background rejoin, manifest declaration
  (ADR-0029 D2), and routing-cache event subscriber.
- Apply review #64 in this module: network addresses that cannot be
  defaulted must fail startup (no silent `127.0.0.1` fallback).

### Out of Scope
- gRPC service *construction* (c3) — only binding moves here.
- Background-task spawns that are not network-plane (c5).

## Definition of Done

- [ ] Listeners bound and servers spawned from `modules/network.rs`.
- [ ] Membership join/rejoin/fallback-seed persistence behavior identical.
- [ ] No silent default network addresses (review #64 closed).
- [ ] Node tests green; multi-node membership tests green.
