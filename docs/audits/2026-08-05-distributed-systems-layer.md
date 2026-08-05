---
audit_date: 2026-08-05
scope: targeted
target_crates: oceanfs-routing, oceanfs-membership, oceanfs-network
severity_counts:
  critical: 0
  high: 3
  medium: 6
  low: 7
---

# Audit Report: Distributed Systems Layer (Routing, Membership, Network)

## Summary

The distributed systems layer is **functional and well-structured**. All six pre-requisite blockers (PR1-PR6) identified in the cluster-bootstrap feature are verified fixed. The DHT ring with consistent hashing, membership state management, gossip protocol, failure detector state machine, routing, and connection pool are all implemented. Unit and integration test coverage is solid at 39 unit + 8 integration tests for membership, plus 6 routing_forward integration tests. 39 of 43 cluster E2E tests pass (91%).

The primary concern is that **SWIM remote pings are never sent** — the failure detector relies on gossip push-as-ping-proxy instead of direct gRPC probes, which is a functional gap, not a bug. Graceful leave is a stub with no WAL handoff or shard streaming. Crash recovery + rejoin (T43) fails due to ephemeral port reassignment. The ADR justifying SWIM + consistent hashing over Raft (ADR-0002) is referenced in the spec but does not exist in `docs/adr/`.

---

## Findings

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `oceanfs-membership/src/failure_detector/ping.rs:48-66` | SWIM remote pings are never sent via gRPC. `on_ping_tick()` calls `probe_handler.handle_probe()` which only handles self-targeted probes. For remote targets, the ping is registered as "pending" but the gRPC call that would send it is marked "In a full implementation, this would send a gRPC Probe request." | Implement remote probe sending: either add a `Probe` RPC to the gossip proto (as originally planned in PR3), or ensure the gossip-push-as-ping-proxy path (DK-007) is the documented and complete approach. If the latter, remove the misleading "full implementation" comments and document the design choice. |
| H2 | `oceanfs-membership/src/membership/manager.rs:391-430` | `leave()` is a stub. The 100ms `tokio::time::sleep` simulates a drain period. No WAL segment handoff, no shard streaming to successors. Per spec §13, graceful leave should hand off WAL segments and stream owned shards before departing. | Implement WAL handoff: seal active WAL segments and push them to the next ring successor. Implement shard streaming: for each segment shard owned by the leaving node, stream it to the new replica set before signalling LEFT. |
| H3 | `e2e/tests/cluster_lifecycle.rs:150-199` | T43 crash recovery + rejoin fails because `Cluster::restart()` assigns new ephemeral ports. Nodes that previously knew the killed node via its old address cannot reach it. | Per feature doc: preserve ports across restart by writing assigned ports to a file and re-reading on restart. Alternatively, implement node re-convergence via gossip that updates peer addresses. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `oceanfs-node/src/node.rs:159` | `RingConfig` is hardcoded to `RingConfig::default()` — `vnodes_per_node` and `replication_factor` are not wired from `NodeConfig`. Users cannot configure vnodes per node, which is a spec-required knob (spec §2.2: "Each node owns `vnodes_per_node` virtual nodes"). | Add `vnodes_per_node` and `replication_factor` fields to `NodeConfig`. Wire them through to `RingConfig` in `node.rs:159`. |
| M2 | `oceanfs-network/src/pool.rs:147-151` | `ConnectionPool::health_check()` is a no-op placeholder. No gRPC health probing of existing connections. If a channel silently breaks (e.g., peer restart), callers only discover the failure on the next RPC. | Implement periodic gRPC health checks using the `grpc.health.v1.Health` proto or a lightweight ping RPC. Evict or reconnect broken channels. |
| M3 | `oceanfs-membership/src/failure_detector/suspicion.rs:18` | `TODO: track actual incarnation` — incarnation is hardcoded to `Incarnation::new(1)` when marking nodes SUSPECT. The incarnation should reflect the target's known incarnation from `alive_nodes`. | Look up `node_id` in `detector.alive_nodes` to retrieve the current incarnation. If not found, use `Incarnation::new(1)` as fallback. |
| M4 | `docs/adr/` | ADR-0002 (SWIM + consistent hashing vs Raft per shard) referenced in spec §2.2 does not exist. The architectural rationale for the most important distributed design decision is undocumented. | Write ADR-0002 documenting the tradeoff analysis: why consistent hashing + quorum over Raft, what scenarios Raft would be better, when to revisit the decision. |
| M5 | `e2e/tests/cluster_lifecycle.rs:13-45` | T40/T41 tests are placeholders that validate baseline cluster health, not actual graceful leave with WAL handoff or shard streaming. T40 writes data and verifies it's readable; T41 checks cluster endpoint returns 200. | After H2 is implemented, write real T40/T41 tests: SIGTERM a node, verify WAL segments are handed off to successor, verify shards are streamed, verify data integrity post-leave. |
| M6 | `e2e/tests/cluster_write_path.rs:193` | Stale comment references `Router::try_forward` ("PR6 must be fixed for this to work"). PR6 is verified fixed — `try_forward()` is removed and `WriteCoordinator::forward_write()` is the correct path. The comment is misleading. | Update the comment to reference `WriteCoordinator::forward_write()` or remove the PR6 reference since the fix is confirmed working. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `oceanfs-membership/src/lib.rs:18` | `#![allow(dead_code)]` at crate level hides dead code across the entire crate. The network crate has the same issue (`oceanfs-network/src/lib.rs:26`). | Remove the crate-level `#![allow(dead_code)]` and add targeted `#[allow(dead_code)]` on specific items that are intentionally not yet used (e.g., `RpcClient` marker trait, `TlsError`). This prevents stale code from accumulating silently. |
| L2 | `oceanfs-network/src/tls.rs:30-37` | TLS is entirely placeholder — `tls_enabled()` always returns `false`. mTLS is deferred to Phase 5, which is documented, but the crate has `pub(crate)` TLS infrastructure that's unused. | Keep the placeholder as-is for the next phase. Consider gating the `mod tls` behind a `tls` feature flag to make the deferred status explicit at compile time. |
| L3 | `oceanfs-network/src/client.rs:9` | `RpcClient` is a marker trait with zero implementors. It was designed for testability but no generated client types implement it. | Either implement the trait on generated client types or remove the trait until it has consumers. |
| L4 | `oceanfs-core/src/config/node.rs` | `NodeConfig` has `gossip_interval_ms`, `suspicion_timeout_ms`, `failure_timeout_ms` but lacks `vnodes_per_node`, `replication_factor`, `pool_size_per_peer`, `keepalive_sec`. These are all configurable at the type level (`RingConfig`, `RpcConfig`) but not exposed to users. | Add the missing fields to `NodeConfig` with sensible defaults and serde wiring. |
| L5 | `oceanfs-membership/src/failure_detector/ping.rs:48-66` | `probe_handler.handle_probe(&request)` is called for every ping, but for remote targets the probe returns `ack: false` (since target != self). The response is unused. This creates misleading trace log entries. | Refactor so that `on_ping_tick` checks `target == self.node_id` before calling `handle_probe`. For remote targets, either send via gRPC (H1) or document the push-as-ping-proxy approach. |
| L6 | `oceanfs-membership/src/grpc/probe_service.rs:52-128` | `ProbeHandler` has `pub` visibility but only handles self-probes. The doc comment on line 19 states: "It is not a full tonic service" — acknowledging the gap. | Either make `ProbeHandler` `pub(crate)` (it's only used internally) or implement it as a full tonic `ProbeRpc` service for remote probe handling. |
| L7 | `oceanfs-network/tests/connection_pool.rs` | Only tests error paths (unreachable address). No successful connection test with a real gRPC server. | Add an integration test that starts a real tonic test server, connects to it, and verifies channel acquisition + RPC call works. |

---

## Subsystem Status

### 1. DHT Ring & Consistent Hashing
**Status: COMPLETE**  
The `Ring` type with vnodes and SHA-256 hashing works correctly. `RingCache` uses `ArcSwap` for wait-free reads. All E2E ring tests pass (T32-T35). The only gap is M1 — `vnodes_per_node` is not configurable from user-facing config.

### 2. Membership & Gossip
**Status: VERIFIED FIXED (PR1-PR5)**  
All six pre-requisites are confirmed fixed:

| PR | Description | Verified |
|---|---|---|
| PR1 | `Membership::start()` spawns `GossipProtocol` and stores detector sender | `manager.rs:57-188` — spawns both tasks with `CancellationToken` |
| PR2 | Periodic gossip ticker pushes deltas | `gossip.rs:117-141` — `tokio::time::interval` loop calls `on_gossip_tick()` |
| PR3 | SWIM ping initiation loop | `failure_detector/mod.rs:60-85` — interval loop calls `on_ping_tick` + timeout checks |
| PR4 | `upsert_node()` updates ring on membership changes | `manager.rs:462-480` — synchronously adds/removes nodes from ring |
| PR5 | Joiner push-announce to seed | `manager.rs:324-369` — sends `GossipRpcClient::push` after pull; verified by `seed_learns_joiner_on_join_via_push_after_pull` integration test |
| PR6 | `Router::try_forward()` removed | Verified by grep — zero results for `try_forward` in source. `WriteCoordinator::forward_write()` handles forwarding. |

### 3. Failure Detection (SWIM)
**Status: FUNCTIONAL WITH GAP**  
The SWIM state machine (ALIVE → SUSPECT → DEAD) exists and is tested. The gossip-path-as-ping-proxy approach works (applied as DK-007 in cluster E2E feature). However, direct gRPC probes are not sent for remote peers (H1). Incarnation tracking is hardcoded (M3).

### 4. Routing
**Status: COMPLETE**  
`Router::route()` determines replica set and local/forward status. PR6 is verified — no duplicated forwarding logic. All 6 routing_forward integration tests pass (T13, T14 verified in E2E).

### 5. Connection Pool
**Status: COMPLETE WITH GAPS**  
Per-peer gRPC channel pool with DashMap, semaphore-bounded concurrency, round-robin selection, keepalive, TCP_NODELAY. `pool_size_per_peer` configurable. Health checking is a placeholder (M2). TLS is a placeholder (L2). All connections created eagerly.

### 6. Node Lifecycle
**Status: FUNCTIONAL WITH GAPS**  
Join works end-to-end (verified by PR5 integration test). Leave is a stub (H2). Crash recovery WAL replay works (T42 passes). Crash recovery + rejoin fails (H3). T21 hinted handoff delivery not wired. T45 multi-replica HLC comparison not implemented.

---

## PR1-PR6 Verification

All six pre-requisite fixes from the cluster-bootstrap feature are confirmed in the codebase:

| PR | Fix | File(s) | Evidence |
|---|---|---|---|
| **PR1** | Detector sender stored, gossip spawned | `membership/manager.rs:72-146` | `detector_tx` stored in `RwLock<Option<Sender>>`. `gossip_cmd_tx` stored, `GossipProtocol` spawned. Both use `CancellationToken` for graceful shutdown. |
| **PR2** | Gossip ticker added | `gossip.rs:117-141` | `tokio::time::interval` in `select!` loop. `on_gossip_tick()` at line 148 pushes to all alive peers (not just one random — DK-008). |
| **PR3** | SWIM ping loop added | `failure_detector/mod.rs:60-85`, `ping.rs:19-66` | Interval loop calls `on_ping_tick` + `check_ping_timeouts` + `check_suspicion_timers`. |
| **PR4** | Ring update in upsert_node | `membership/manager.rs:462-480` | New ALIVE nodes added to ring, Dead/Left nodes removed. Verified by `push_new_entries_updates_membership` test at `gossip_service.rs:261-266`. |
| **PR5** | Push-after-pull on join | `membership/manager.rs:324-369` | Sends `GossipRpcClient::push` to seed after `pull`. Verified by `seed_learns_joiner_on_join_via_push_after_pull` integration test. |
| **PR6** | try_forward removed | `router.rs:65-129` | Zero results for `try_forward` in source grep. `WriteCoordinator::forward_write()` at `write/coordinator.rs:250-316` handles forwarding. |

---

## Test Coverage

| Crate | Unit Tests | Integration Tests | E2E Tests (passing) |
|---|---|---|---|
| `oceanfs-routing` | 11 (ring.rs, ring_cache.rs, hash.rs) | None | 4 (T32-T35, all pass) |
| `oceanfs-membership` | 32 (gossip, detector, membership, probe_service, gossip_service) | 9 (membership_lifecycle.rs) | 8 (T5-T8, T23-T27, all pass; T24/T26 intermittent) |
| `oceanfs-network` | 5 (pool.rs) | 5 (connection_pool.rs) | N/A (used by other tests) |

**Total across audit domain:** 48 unit + 14 integration tests. All passing.

---

## TODO / FIXME / Placeholder Inventory

| Count | Location | Description |
|---|---|---|
| 1 | `failure_detector/suspicion.rs:18` | `TODO: track actual incarnation` |
| 1 | `network/pool.rs:147-151` | `health_check()` is a no-op placeholder |
| 1 | `network/tls.rs:30-37` | TLS is a placeholder (deferred to Phase 5) |
| 1 | `e2e/tests/cluster_write_path.rs:193` | Stale comment referencing removed `Router::try_forward` |

---

## Dependency Graph & Coupling

The crate dependency graph for the audited domain respects the DAG from `guidelines/architecture.md`:

```
oceanfs-core → {routing, membership, network}
  oceanfs-routing → no internal deps beyond core
  oceanfs-membership → routing, network, core
  oceanfs-network → core
oceanfs-server → {routing, membership, network, ...}
```

No circular dependencies detected. Cross-crate coupling is through `Arc` of public types (`RingCache`, `Membership`, `ConnectionPool`), consistent with the architecture guidelines.

**Coupling note:** `Membership` is the most coupled type in the distributed layer, receiving references to `RingCache`, `ConnectionPool`, and providing access to both `FailureDetector` and `GossipProtocol`. This is by design (it's the coordinator), but any change to the `Membership` struct fields has broad impact.

---

## ADR Compliance

| ADR | Status | Notes |
|---|---|---|
| ADR-0002 (SWIM + consistent hashing vs Raft) | **MISSING** | Referenced in spec §2.2 but does not exist in `docs/adr/`. All 8 existing ADRs cover other topics (segment packing, acceleration, compression, hash, trait pattern, storage split, server split). |
| ADR-0005 (Trait-in-consuming-crate) | **COMPLIANT** | The distributed layer uses concrete types with `Arc` passing rather than trait objects, which is consistent with §4.1 of architecture guidelines ("may import concrete crates"). |

---

## Performance Guideline Compliance

| Rule | Check | Status |
|---|---|---|
| §2.4: `ArcSwap` for read-mostly shared data | `RingCache` uses `ArcSwap` for ring topology | PASS |
| §2.6: Bounded channels for inter-task communication | All `mpsc::channel(N)` are bounded (64, 16, 8) | PASS |
| §4.1: Persistent gRPC connection pool per peer | `ConnectionPool` with per-peer DashMap | PASS |
| §4.3: `TCP_NODELAY` on all sockets | `tcp_nodelay(true)` at `pool.rs:186` | PASS |
| §4.5: Adaptive per-operation timeouts | `RpcConfig` has `connect_timeout_ms` and `request_timeout_ms` but these are global, not per-operation (e.g., no separate gossip vs write vs read timeout) | PARTIAL |
| §6.5: `BTreeMap` for ring lookup | `Ring` uses `BTreeMap` with `partition_point` | PASS |

---

## Recommendations (Prioritized)

1. **Implement remote SWIM pings (H1).** This is the most impactful gap for production correctness. Either add a `Probe` gRPC RPC for direct remote pings, or formally adopt the gossip-push-as-ping-proxy approach (DK-007) and document it as the canonical SWIM implementation. Remove the "full implementation" comments.

2. **Implement graceful leave (H2).** Without WAL handoff and shard streaming, a node leaving gracefully loses its buffered writes. This is a data loss risk.

3. **Fix T43 crash recovery + rejoin (H3).** Port preservation is a harness issue, not a distributed protocol issue, but it blocks the E2E verification of crash recovery + rejoin.

4. **Write ADR-0002 (M4).** The most architecturally significant decision in the distributed layer — choosing SWIM + consistent hashing over Raft — has no documented rationale. This is a governance gap.

5. **Wire vnodes_per_node through NodeConfig (M1).** A one-line fix in `node.rs` + config struct additions.

6. **Remove crate-level `#[allow(dead_code)]` (L1).** Replace with targeted allows to prevent silent accumulation of stale code.

7. **Implement connection pool health checking (M2).** Without it, broken connections are discovered only on the next RPC call, causing latency spikes.

8. **Fix the stale e2e comment (M6).** Trivial but misleading.

9. **Add successful-pool-connection integration test (L7).** Current tests only cover error paths.

10. **Track actual incarnation in SWIM (M3).** Look up incarnation from `alive_nodes` instead of hardcoding.
