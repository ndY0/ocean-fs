---
feature: "Connection Pool & gRPC Transport"
epic: "phase-2-distributed-connectivity"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: phase-0-project-scaffold
    reason: Requires protobuf service definitions from oceanfs-core
adr: []
perf:
  - "4.1: Persistent gRPC connection pool per peer"
  - "4.3: TCP_NODELAY on all sockets"
  - "4.4: Streaming gRPC for large data transfers"
  - "4.5: Adaptive per-operation timeouts"
created: 2026-07-30
updated: 2026-07-30
---

# Connection Pool & gRPC Transport

## Summary

Implement the persistent gRPC connection pool in `oceanfs-network`. Each peer
node gets a pool of N reusable gRPC channels with HTTP/2 multiplexing.
This eliminates per-request TLS handshake and connection setup latency.
Channels are acquired from the pool for each RPC call and returned on
completion. Configuration includes keepalive, idle timeout, and pool sizing.

## Scope

### In Scope
- `ConnectionPool`: pool of `N` gRPC channels per peer, with `acquire()`/`release()` API
- Per-peer pool management: lazy channel creation, health checking, idle eviction
- gRPC client stubs for internal RPC services (AppendSegment, FetchShard, GossipPush/Pull, Probe, etc.)
- Configurable pool parameters: `pool_size_per_peer`, `keepalive_sec`, `max_idle_connections`, `connect_timeout_ms`, `request_timeout_ms`
- `TCP_NODELAY` on all sockets
- TLS configuration for node-to-node communication (mTLS placeholder)
- `RpcClient` trait: abstraction over gRPC for testability
- Unit tests for pool acquire/release, connection reuse, idle eviction, timeout handling

### Out of Scope
- Actual RPC service implementations (each crate provides its own)
- HTTP API (Phase 5) — this is internal node-to-node only
- Authentication/authorization on gRPC (Phase 5)

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `RpcConfig`, `PeerAddress` |
| `oceanfs-network` | New crate; modules: `pool.rs`, `client.rs`, `tls.rs` |
| `oceanfs-network` | Facade exports: `pub use pool::ConnectionPool`, `pub use client::RpcClient` |

## Interface (Public API)

- `pub struct RpcConfig` — `pool_size_per_peer: usize`, `keepalive_sec: u64`, `max_idle_connections: usize`, `connect_timeout_ms: u64`, `request_timeout_ms: u64`, `tls_cert_path: Option<PathBuf>`
- `pub struct ConnectionPool` — `pub fn new(config: RpcConfig) -> Self`, `pub async fn get_channel(&self, peer: SocketAddr) -> Result<PooledChannel>`, `pub fn release(&self, channel: PooledChannel)`, `pub async fn health_check(&self)`
- `pub struct PooledChannel` — wraps a gRPC `Channel` with pool metadata; `Deref` to `Channel`
- `pub trait RpcClient: Send + Sync` — marker trait for service-specific client stubs
- `pub(crate) mod tls` — `pub(crate) fn client_tls_config(cert_path: &Path) -> Result<ClientTlsConfig>`

## Data Flow

```
Node-to-node RPC call:
  ConnectionPool::get_channel(peer_addr)
    ├─ Pool has idle channel for peer? → return pooled channel
    └─ No idle channel (or pool not full)?
         └─ Create new gRPC channel:
              ├─ Configure HTTP/2 with keepalive
              ├─ Set TCP_NODELAY on underlying socket
              ├─ Apply TLS config if mTLS enabled
              └─ Add to pool, return
    → Use channel for RPC call (AppendSegment, FetchShard, etc.)
      └─ On completion: ConnectionPool::release(channel)
           └─ Channel returned to idle pool for reuse

Per-peer pool state:
  peer_a: [channel_0 (idle), channel_1 (in_use), channel_2 (idle), channel_3 (idle)]
  peer_b: [channel_0 (in_use), channel_1 (idle)]
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-core` and `oceanfs-network`
- [ ] **Tests:** Unit tests: acquire/release cycle (same channel returned), pool exhaustion (waits for release), idle eviction, connect timeout, channel health check, concurrent acquire from multiple tasks
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-network`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `ConnectionPool` documented with usage example
- [ ] **ADR:** N/A
- [ ] **Perf:** Rule 4.1 (persistent pool, no per-RPC connect), 4.3 (TCP_NODELAY), 4.4 (streaming gRPC support), 4.5 (per-operation timeouts)
- [ ] **Integration:** `tests/connection_pool.rs`: create pool, acquire channels for 2 peers, make concurrent acquire/release calls, verify channel reuse, verify idle channels evicted after keepalive timeout
- [ ] **Manual:** Example in `ConnectionPool` docs compiles and runs
