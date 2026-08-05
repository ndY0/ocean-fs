---
feature: "Network Socket Tunings"
epic: "performance-optimization"
status: proposed
priority: medium
owner: ""
dependencies:
  - epic: gap-closure-epic-2
    reason: "Multi-node connectivity (DHT ring, gossip, connection pooling) must be working before socket-level optimizations on the gRPC data path can be tested and benchmarked."
adr:
  - 0006-hardware-acceleration-tier-model
perf:
  - "4.1 Persistent gRPC connection pool per peer"
  - "4.3 TCP_NODELAY on all sockets"
  - "4.4 Streaming gRPC for large data transfers"
  - "10.6 Conditional platform-specific code paths"
  - "11.4 Criterion benchmarks for hot-path functions"
created: 2026-08-05
updated: 2026-08-05
---

# Network Socket Tunings

## Summary

Three Linux socket-level optimizations applied to the gRPC server and
client sockets that reduce inter-node RPC latency and improve connection
distribution. `SO_BUSY_POLL` enables low-latency network polling via
busy-waiting, eliminating the kernel interrupt wakeup latency for
short RPCs — ideal for quorum write/read patterns where median RPCs are
small. `TCP_QUICKACK` disables delayed ACKs on gRPC connections,
eliminating up to 500ms of unnecessary ack delay for independent
request-response RPCs. `SO_REUSEPORT` enables multiple sockets on the
same port, each bound to its own tokio runtime thread, eliminating
contention on the single accept queue. All are Linux-specific
(`#[cfg(target_os = "linux")]`-gated), applied via `setsockopt`, and
integrated into the gRPC server/client setup in `oceanfs-network` and
`oceanfs-server`. Code touches `oceanfs-network/src/pool.rs` (client
socket options) and `oceanfs-server/src/grpc_server.rs` (server socket
options).

## Scope

### In Scope

- **`SO_BUSY_POLL` on gRPC server sockets.** Enable low-latency network
  polling via `setsockopt(libc::SOL_SOCKET, libc::SO_BUSY_POLL,
  &time_us)` on the gRPC server's listening socket. When enabled, the
  kernel busy-waits (spins in a tight loop polling the NIC's receive
  ring) for up to `time_us` microseconds instead of sleeping and
  waiting for a hardware interrupt. Useful for workloads where the
  median RPC payload is small (< 4 KB) and latency matters more than
  CPU efficiency — exactly the inter-node quorum write ack pattern
  (`AppendSegment` ack is a small protobuf, ~100 bytes) and
  `ProbeRequest` (SWIM ping, ~50 bytes). Default poll time: 50 µs
  (configurable via `grpc_busy_poll_us`). Note: `SO_BUSY_POLL` burns
  CPU — for a storage node where the primary bottleneck is I/O (not
  CPU), the tradeoff is favorable. `#[cfg(target_os = "linux")]` gated.
  Requires Linux 3.11+.

- **`TCP_QUICKACK` on gRPC sockets.** Disable delayed ACKs on gRPC
  client and server sockets via `setsockopt(libc::IPPROTO_TCP,
  libc::TCP_QUICKACK, &1)`. Normally, TCP delays ACKs up to 500ms
  (or 40ms minimum with `TCP_ATO_MIN`) to coalesce them with response
  data — useful for bidirectional streaming where the next send from
  the ACK-receiver can piggyback. For OceanFS inter-node RPCs, each
  message from the same connection is an independent request-response
  pair: `AppendSegment` → ack, `FetchShard` → shard data,
  `GossipPush` → ack. There is no bidirectional streaming where ACKs
  can piggyback. Delaying ACKs adds unnecessary latency — up to 40ms
  per RPC round-trip. Apply `TCP_QUICKACK` on every gRPC socket (both
  server accept and client connect). This is a one-time `setsockopt`
  at socket creation — the kernel honors it until a subsequent
  `TCP_QUICKACK` with value 0, which OceanFS never sends. `#[cfg
  (target_os = "linux")]` gated.

- **`SO_REUSEPORT` for gRPC server.** Bind multiple sockets to the same
  gRPC port, each in its own tokio runtime worker thread. The kernel
  distributes incoming TCP connections across the socket set using a
  hash of the 4-tuple (src_ip, src_port, dst_ip, dst_port), eliminating
  contention on the single accept queue that occurs with a single
  listening socket. Implementation:
  1. Create N sockets (where N = number of tokio worker threads, or
     configurable via `grpc_reuseport_sockets`).
  2. Set `SO_REUSEPORT` on each via `setsockopt(libc::SOL_SOCKET,
     libc::SO_REUSEPORT, &1)` before binding.
  3. Bind each to the same address:port.
  4. Pass one socket to each of N tonic server instances, each running
     on its own tokio `current_thread` runtime or spawned on separate
     worker threads.
  5. Each server instance handles connections independently — no
     cross-thread coordination needed.
  This eliminates the single-listener bottleneck where one thread
  handles all `accept()` calls. On a 32-core machine, 32 sockets ×
  32 accept queues = zero accept contention. Requires Linux 3.9+.
  `#[cfg(target_os = "linux")]` gated. Falls back to single-socket
  listener on non-Linux or when `SO_REUSEPORT` is unavailable.

### Out of Scope (for this feature)

- **gRPC connection pooling.** Already covered by perf guideline §4.1
  and the connection pool implementation in `oceanfs-network`.
- **HTTP/2 multiplexing for client API.** Already covered by perf
  guideline §4.2. This feature only addresses socket-level options, not
  protocol-level optimizations.
- **Streaming gRPC for large data transfers.** Already covered by perf
  guideline §4.4. This feature complements streaming by reducing
  per-message latency, but the streaming architecture is separate.
- **TCP congestion control algorithm selection** (BBR vs cubic). While
  BBR can improve throughput on lossy networks, it is a kernel-level
  configuration (`sysctl net.ipv4.tcp_congestion_control`) rather than
  a per-socket option. Documented as an operational recommendation, not
  a code change.
- **Kernel bypass networking** (DPDK, XDP, AF_XDP). These are
  architectural changes that replace the kernel TCP stack — out of
  scope for this feature. RDMA is covered separately (Feature 9:
  hardware-offload).
- **TLS session resumption for gRPC.** TLS handshake optimization is
  relevant but orthogonal to socket options. Covered by gRPC channel
  configuration in the connection pool.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-network` | New module `src/socket_opts.rs` with functions `set_busy_poll(fd, us)`, `set_quickack(fd)`, `set_reuseport(fd)`. Modify `src/pool.rs` to apply `SO_BUSY_POLL` and `TCP_QUICKACK` on client sockets after `connect`. |
| `oceanfs-server` | Modify `src/grpc_server.rs` to: (a) apply `SO_REUSEPORT` with N listening sockets, (b) apply `SO_BUSY_POLL` and `TCP_QUICKACK` on accepted sockets. |
| `oceanfs-core` | New config fields in `GrpcConfig`: `busy_poll_us: u32` (default 50), `quickack: bool` (default true), `reuseport_sockets: usize` (0 = auto/num_cpus). |

## Interface (Public API)

- `pub(crate) fn set_busy_poll(fd: RawFd, poll_us: u32) -> io::Result<()>`
  in `oceanfs-network::socket_opts` — applies `SO_BUSY_POLL`. No-op on
  non-Linux.
- `pub(crate) fn set_quickack(fd: RawFd) -> io::Result<()>` in
  `oceanfs-network::socket_opts` — applies `TCP_QUICKACK`. No-op on
  non-Linux.
- `pub(crate) fn set_reuseport(fd: RawFd) -> io::Result<()>` in
  `oceanfs-network::socket_opts` — applies `SO_REUSEPORT`. No-op on
  non-Linux or if the socket is already bound.
- No new public types exposed outside `oceanfs-network`. All
  optimizations are internal socket configuration applied during
  connection setup.

## Data Flow

**gRPC server startup with `SO_REUSEPORT`:**
```
Node::start()
  ├─ create N sockets with SO_REUSEPORT set
  │     for i in 0..N:
  │       fd = socket(AF_INET, SOCK_STREAM)
  │       setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &1)
  │       bind(fd, listen_addr)
  │       listen(fd, 1024)
  ├─ for each socket, spawn tokio task:
  │     tonic::transport::Server::builder()
  │       .add_service(NodeRpcServer::new(service))
  │       .serve_with_incoming(tokio::net::TcpListener::from_std(socket))
  ├─ kernel distributes incoming connections across N sockets
  │     (hash of 4-tuple → socket index)
  └─ each tonic instance handles its connections independently
```

**gRPC client connection with socket opts:**
```
ConnectionPool::acquire(peer) → existing channel or new connection
  ├─ [new connection]
  ├─ connect to peer_addr
  ├─ set_quickack(socket_fd)      // disable delayed ACKs
  ├─ set_busy_poll(socket_fd, 50) // busy-poll for 50µs
  └─ build gRPC channel from socket
```

**RPC latency profile (before vs after):**
```
Before (default TCP):
  Client → [write] → 40ms ack delay → [read response] → total: ~40.5ms
  (for small RPCs where the ack delay dominates)

After (TCP_QUICKACK + SO_BUSY_POLL):
  Client → [write] → immediate ACK → [busy-poll 50µs] → [read response]
  → total: ~0.1ms + RTT
```

## Definition of Done

- [ ] **`SO_BUSY_POLL`:** `set_busy_poll()` function implemented in
  `oceanfs-network/src/socket_opts.rs`. Applied to gRPC server listening
  sockets and accepted client sockets. Configurable via
  `grpc_busy_poll_us` (default 50, 0 = disable). `#[cfg(target_os =
  "linux")]` gated. Verified via `getsockopt` or `ss -o` showing busy
  poll timeout on the socket.

- [ ] **`TCP_QUICKACK`:** `set_quickack()` function implemented.
  Applied to all gRPC client sockets after connect and all server
  accepted sockets. Configurable via `grpc_quickack` (default true).
  Verified via `/proc/net/tcp` quickack bit.

- [ ] **`SO_REUSEPORT`:** `set_reuseport()` function implemented.
  Server creates N sockets where N = `num_cpus::get()` or
  `grpc_reuseport_sockets` if configured. Each socket passed to a
  separate tonic instance. Falls back to single-socket listener when
  `SO_REUSEPORT` unavailable or on non-Linux. Verified via `ss -lnp`
  showing N sockets on the same port.

- [ ] **Config:** `GrpcConfig` gains `busy_poll_us`, `quickack`,
  `reuseport_sockets`. All have sensible defaults. Cross-compilation
  succeeds (non-Linux targets ignore the Linux-specific fields).

- [ ] **Code:** `cargo build --all-targets` succeeds on Linux.
  Cross-compilation to macOS succeeds (all `setsockopt` calls
  `#[cfg]`-gated).

- [ ] **Tests:** New tests: `socket_opts` unit tests verify that
  `set_quickack`, `set_busy_poll`, `set_reuseport` return `Ok` on
  valid sockets (Linux) or `Ok(())`/no-op (non-Linux). Integration
  test: gRPC server starts with N reuseport sockets; N concurrent
  clients connect; verify connections are distributed across sockets
  (inspect `ss` output or per-socket connection counts). Small-RPC
  latency benchmark: with `TCP_QUICKACK`, 100-byte RPC round-trip
  latency is reduced vs baseline.

- [ ] **Docs:** Module-level doc in `src/socket_opts.rs` explains each
  socket option, its tradeoff, and the kernel version requirement.
  Deployment docs recommend `tcp_congestion_control = bbr` for
  inter-node traffic on high-bandwidth links.

- [ ] **ADR:** ADR-0006 constraints satisfied — socket optimizations
  do not affect acceleration tier probing or backend selection.

- [ ] **Perf:** Criterion benchmarks added to `benches/network_benchmark.rs`:
  - RPC latency (small payload, 100B) with vs without `TCP_QUICKACK`
  - RPC latency under load with vs without `SO_BUSY_POLL`
  - Connection throughput with vs without `SO_REUSEPORT` under
    N concurrent clients
  Expected: `TCP_QUICKACK` reduces median latency by >=30% for small
  RPCs under 1KB. `SO_BUSY_POLL` reduces tail latency (p99) by >=20%.
  `SO_REUSEPORT` eliminates connection setup contention at high
  connection rates.

- [ ] **Integration:** Multi-node test: 3-node cluster, all nodes with
  socket tunings enabled. PUT/GET/Probe RPCs succeed. Latency
  measured via tracing spans shows improvement vs baseline.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings and `ignore`-tagged
> doc examples are non-blocking (see `guidelines/coding.md` §9.2.1).
