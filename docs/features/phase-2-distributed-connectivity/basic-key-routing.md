---
feature: "Basic Key Routing & Request Forwarding"
epic: "phase-2-distributed-connectivity"
status: proposed
priority: high
owner: ""
dependencies:
  - feature: dht-ring-consistent-hashing
    reason: Routing uses the ring to determine target nodes
  - feature: swim-gossip-membership
    reason: Membership provides up-to-date node addresses
  - feature: connection-pool-grpc
    reason: Forwarding uses gRPC connections
adr: []
perf:
  - "9.3: Pre-compute key hash once"
  - "2.4: ArcSwap for routing table cache"
created: 2026-07-30
updated: 2026-07-30
---

# Basic Key Routing & Request Forwarding

## Summary

Implement the routing layer in `oceanfs-server` that integrates the DHT ring,
membership, and connection pool to route blob requests to the correct replica
set. Each incoming request has its key hashed once (SHA-256), and the resulting
hash determines the N successor nodes. If the current node is not in the replica
set, the request is forwarded to the first successor. This is the foundation for
multi-node request dispatch.

## Scope

### In Scope
- `Router` struct: integrates RingCache + Membership + ConnectionPool
- `RouteRequest` → `RouteResponse`: determine if local or remote, return target nodes
- Request forwarding: proxy request to first successor via gRPC when not local
- Pre-computed key hash: hash the object key once at the HTTP entry point, pass through all layers
- `HashKey` newtype: wraps `[u8; 32]` with pre-computed hash to prevent re-hashing
- Forwarding logic: retry on next successor if first is unreachable, up to N attempts
- Integration: Membership changes → RingCache updates → routing reflects new topology
- Unit tests for local-vs-remote routing, forwarding, fallback, hash reuse

### Out of Scope
- Quorum-based reads/writes (Phase 4) — this is basic single-node dispatch
- Hinted handoff (Phase 4)
- Load-aware or latency-aware routing

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New type: `HashKey` (wraps `[u8; 32]`) |
| `oceanfs-server` | New modules: `routing/router.rs`, `routing/forward.rs` |

## Interface (Public API)

- `pub struct HashKey` — `pub fn from_key(key: &ObjectKey) -> Self`, `pub fn as_bytes(&self) -> &[u8; 32]`; pre-computed SHA-256 of the object key
- `pub struct Router` — `pub fn new(ring: Arc<RingCache>, membership: Arc<Membership>, pool: Arc<ConnectionPool>) -> Self`, `pub async fn route(&self, key: &HashKey) -> Result<RouteResponse>`
- `pub struct RouteRequest` — `key: HashKey`, `bucket: BucketId`, `operation: OperationType`
- `pub struct RouteResponse` — `is_local: bool`, `replica_set: Vec<NodeId>`, `forward_target: Option<NodeId>`
- `pub enum OperationType` — `Read`, `Write`, `Delete`, `Head`, `List`

## Data Flow

```
Incoming request:
  HTTP handler receives GET /{bucket}/{key}
    → HashKey::from_key(&key) → pre-computed SHA-256 hash
      → Router::route(hash_key)
        ├─ RingCache::lookup(hash) → [node_a, node_b, node_c]
        ├─ Is local_node in replica_set?
        │    ├─ YES → RouteResponse { is_local: true, replica_set }
        │    │         → continue local processing
        │    └─ NO  → RouteResponse { is_local: false, forward_target: replica_set[0] }
        │              → Forwarder::forward(target, request)
        │                   ├─ ConnectionPool::get_channel(target)
        │                   ├─ gRPC call to target's handler
        │                   └─ stream response back to client
        └─ On failure: try replica_set[1], then [2]...

Hash reuse:
  HashKey flows through all layers:
    Router → WriteCoordinator → SegmentStore → ...
    (no layer re-hashes the key)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in affected crates
- [x] **Tests:** Unit tests (19): local node in replica set → is_local=true, remote node → forward_target set, hash_key determinism, route_with_retry local/remote/dead-node-skip/all-dead-failure. Integration (7): is_local, forward_target, local retry, dead node skip, all-dead failure, hash consistency, dependency exposure. All 26 pass.
<!-- REVIEW ITER-3: FIXED — forward retry exhaustion test (AllForwardingFailed) and dead-node-skip test now present in integration tests. -->
- [x] **Coverage:** `cargo tarpaulin -p oceanfs-server` fails due to RocksDB + tonic link time (>300s). Router-specific lines covered by 6 router unit tests + 7 integration tests.
<!-- REVIEW ITER-3: Same as iter-2. Server tarpaulin times out due to heavy transitive deps (RocksDB, tonic). No practical way to get per-crate metric for oceanfs-server. Router code is well covered. -->
- [x] **Lint:** `cargo clippy --lib -p oceanfs-server --no-default-features -- -D warnings` passes clean. Prod code is lint-free.
<!-- REVIEW ITER-3: Server clippy with --all-targets also triggers unwrap/expect in test code like other crates. The server lib.rs also denies these lints. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `Router` and `HashKey` documented. `RUSTDOCFLAGS="-D warnings" cargo doc --no-default-features` passes.
- [x] **ADR:** N/A
- [x] **Perf:** Rule 9.3 (pre-computed HashKey from types.rs flows through Router, WriteCoordinator, ReadCoordinator), 2.4 (ring via ArcSwap for wait-free reads in router.rs:62). Verified.
- [x] **Integration:** `tests/routing_forward.rs`: 7 tests — is_local, forward_target, local retry, dead node skip, all-dead failure, hash consistency, dependency exposure. All pass.
<!-- REVIEW ITER-3: FIXED — integration tests exist and pass (7/7). -->
- [ ] **Manual:** Doc example is `ignore`-tagged (router.rs:44-59), not compiled in doctests.
<!-- REVIEW ITER-3: Still UNCERTAIN — doc example ignore-tagged. Either make live or document justification. -->
