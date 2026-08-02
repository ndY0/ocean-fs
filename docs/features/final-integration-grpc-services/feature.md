---
feature: "gRPC Service Implementations"
epic: "final-integration"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: final-integration
    feature: final-integration-composition-root
    reason: Needs Node struct with gRPC server port bound
  - epic: final-integration
    feature: final-integration-proto-grpc-stubs
    reason: Needs generated client/server stubs and proto message types
adr: []
perf:
  - "4.1: Persistent gRPC connection pool per peer"
  - "4.4: Streaming gRPC for large data transfers"
  - "4.5: Adaptive per-operation timeouts"
  - "2.6: Bounded channels for inter-task communication"
created: 2026-08-01
updated: 2026-08-01
---

# gRPC Service Implementations

## Summary

Implement the gRPC service handlers for all node-to-node RPCs defined in spec
§12.3, register them with the tonic gRPC server started in the composition root,
and replace every existing stub/placeholder/no-op in the inter-node
communication paths with real gRPC calls. This transforms OceanFS from a
single-node system (where all placeholders returned hardcoded data) into a
genuinely distributed system where nodes exchange segment data, gossip state,
and coordination messages over the wire.

## Scope

### In Scope

1. **Segment RPC service** (`oceanfs-server/src/grpc/segment_service.rs`):
   - Implement `AppendSegment(stream SegmentAppendRequest) →
     SegmentAppendResponse`:
     - Accept streaming segment data from a remote writer coordinator
     - Append each chunk to the local active segment buffer for the given
       segment ID and shard index
     - WAL-fsync after last chunk received
     - Return `wal_position` and `AckStatus::Ok`
   - Implement `FetchShard(ShardRequest) → stream ShardResponse`:
     - Read the requested shard from the local segment store
     - Stream shard data in chunked responses (64 KB chunks per perf §4.4)
     - Include per-chunk checksum in each response
     - Stream ends with a final empty-data chunk as EOF sentinel
2. **Gossip RPC service** (`oceanfs-membership/src/grpc/gossip_service.rs`):
   - Implement `GossipPush(stream GossipMessage) → GossipAck`:
     - Merge received membership delta into local Membership state
     - If the received ring version is newer, update the local RingCache
     - Return `accepted: true` and count of updated entries
   - Implement `GossipPull(GossipPullRequest) → stream GossipMessage`:
     - Compute delta: all membership entries with version >
       `req.last_known_version`
     - Stream back as GossipMessage chunks
3. **Healing RPC service** (`oceanfs-server/src/grpc/healing_service.rs`):
   - Implement `HintedHandoff(HintRequest) → HintResponse`:
     - Store the hint (segment data intended for a different node) in local
       hinted-handoff buffer
     - Return `accepted: true` and the local segment ID
   - Implement `MerkleExchange(MerkleRequest) → MerkleResponse`:
     - For each requested segment ID, compute the local Merkle root and leaf
       hashes at the requested depth
     - Return the comparison data
4. **SWIM Probe RPC** (`oceanfs-membership/src/grpc/probe_service.rs`):
   - Implement `Probe(ProbeRequest) → ProbeResponse`:
     - If target matches local node: respond with `ack: true` and current
       incarnation
     - If indirect ping: forward to the target node and relay response
5. **Cache Invalidation RPC** (`oceanfs-server/src/grpc/cache_service.rs`):
   - Implement `CacheInvalidate(CacheInvalidateRequest) →
     CacheInvalidateResponse`:
     - Invalidate the specified cache entries (object data, metadata, or both)
       in the local L1/L2 caches
     - Return `acknowledged: true`

6. **Replace placeholders in `write/replication.rs`:**
   - Replace simulated replica writes (returns `Ok` without any I/O) with actual
     gRPC `AppendSegment` streaming calls via the `SegmentRpcClient`
   - Fan out to all N successors simultaneously via `FuturesUnordered`
   - Collect responses until W acks received (or timeout)
   - Report per-node ack status in `WriteResult`

7. **Replace placeholders in `read/fetch.rs`:**
   - Replace zero-bytes return with actual gRPC `FetchShard` streaming calls via
     the `SegmentRpcClient`
   - Fetch k of k+m shards in parallel via `FuturesUnordered`
   - Stream shard data back as `BytesMut` chunks
   - Verify per-chunk checksums as data arrives
   - Implement "use fastest k" semantics: return as soon as k full shards are
     received

8. **Replace placeholder in `router.rs`:**
   - `Router::try_forward()` currently validates the target but never calls
     gRPC. Replace with: open a streaming `AppendSegment` RPC to the target
     node, forward the write request data, await the ack.
   - Implement single-hop forwarding (non-recursive: the receiver becomes the
     coordinator)

9. **Replace placeholder in `hinted_handoff.rs`:**
   - `deliver_single()` currently returns `Ok` immediately. Replace with:
     gRPC `HintedHandoff` call to the returning node.
   - On success, remove the hint from the local buffer.
   - On failure, re-queue with exponential backoff.

10. **Replace placeholder in `gossip.rs`:**
    - `gossip.rs:85` processes deltas locally only. Add: select random peers
      from the membership list, push deltas to them via `GossipRpcClient::Push`.
    - Pull deltas from peers the node hasn't heard from recently.
    - Merge received deltas per standard SWIM gossip protocol.

11. **Replace placeholder in `membership.rs`:**
    - `membership.rs:188` join protocol is simulated. Replace with: contact a
      seed node via `GossipRpcClient::Pull`, receive the full membership list,
      set local Membership state, announce self via `GossipRpcClient::Push`.
    - Implement SWIM probe loop: periodically ping a random peer; on timeout,
      request indirect pings from k peers; on all timeouts, mark SUSPECT.

12. **Replace placeholder in `anti_entropy.rs`:**
    - `anti_entropy.rs:513` returns empty stats. Replace with: for each partner
      node, exchange Merkle roots for segments in the shared partition via
      `HealingRpcClient::MerkleExchange`.
    - On mismatch, descend the tree (request deeper leaf hashes) to identify
      diverged shards.
    - Enqueue identified segments for repair.

13. **gRPC server wiring:**
    - In `oceanfs-node/src/node.rs`, register all service implementations with
      the tonic `Server`:
      - `SegmentRpcServer::new(segment_service)`
      - `GossipRpcServer::new(gossip_service)`
      - `HealingRpcServer::new(healing_service)`
      - The probe and cache invalidation RPCs are registered as additional
        services or folded into the existing services
    - Bind the tonic server to `config.grpc_listen_addr`

14. **Connection pool integration:**
    - `ConnectionPool` must dispatch to the correct peer's `SegmentRpcClient`,
      `GossipRpcClient`, `HealingRpcClient` based on `NodeId`
    - Generated clients are constructed from the tonic `Channel` maintained by
      the pool

### Out of Scope

- TLS/mTLS for gRPC connections (always plaintext for now; TLS placeholder
  already flagged as "always disabled")
- gRPC load balancing beyond the per-peer connection pool
- OpenTelemetry distributed tracing on gRPC spans
- Adaptive compression on gRPC streams
- gRPC health-checking protocol (`grpc.health.v1.Health`)
- Protobuf message schema evolution / backward compatibility handling

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | NEW: `src/grpc/segment_service.rs` — `AppendSegment` and `FetchShard` handlers |
| `oceanfs-server` | NEW: `src/grpc/healing_service.rs` — `HintedHandoff` and `MerkleExchange` handlers |
| `oceanfs-server` | NEW: `src/grpc/cache_service.rs` — `CacheInvalidate` handler |
| `oceanfs-server` | MODIFIED: `src/write/replication.rs` — replace no-op with real `AppendSegment` gRPC |
| `oceanfs-server` | MODIFIED: `src/read/fetch.rs` — replace zero-bytes with real `FetchShard` gRPC |
| `oceanfs-server` | MODIFIED: `src/router.rs` — implement actual forwarding via gRPC |
| `oceanfs-server` | MODIFIED: `src/hinted_handoff.rs` — implement actual delivery via gRPC |
| `oceanfs-membership` | NEW: `src/grpc/gossip_service.rs` — `GossipPush` and `GossipPull` handlers |
| `oceanfs-membership` | NEW: `src/grpc/probe_service.rs` — SWIM `Probe` handler |
| `oceanfs-membership` | MODIFIED: `src/gossip.rs` — replace local-only processing with actual gRPC Push/Pull |
| `oceanfs-membership` | MODIFIED: `src/membership.rs` — replace simulated join with actual gRPC seed node contact |
| `oceanfs-storage` | MODIFIED: `src/anti_entropy.rs` — replace empty cycle with actual MerkleExchange gRPC |
| `oceanfs-node` | MODIFIED: `src/node.rs` — register gRPC service implementations with tonic Server |

## Interface (Public API)

- `pub struct SegmentGrpcService` — tonic service implementing `SegmentRpc`
  - `pub fn new(store: Arc<dyn SegmentStore>, wal: Arc<WalWriter>, ...) -> Self`
- `pub struct GossipGrpcService` — tonic service implementing `GossipRpc`
  - `pub fn new(membership: Arc<Membership>, ring: Arc<RingCache>) -> Self`
- `pub struct HealingGrpcService` — tonic service implementing `HealingRpc`
  - `pub fn new(handoff: Arc<HintedHandoff>, segment_store: Arc<dyn SegmentStore>) -> Self`
- `pub struct CacheInvalidateGrpcService` — tonic service for cache
  invalidation
  - `pub fn new(object_cache: Arc<ObjectCache>, metadata_cache: Arc<MetadataCache>) -> Self`

## Data Flow

```
gRPC Write Replication (remote node):
  WriteCoordinator::replicate(segment_data, successors):
    for each successor in replica_set:
      pool.acquire_channel(successor.node_id) → tonic Channel
      client = SegmentRpcClient::new(channel)
      stream = client.append_segment(Request::new(
        futures::stream::iter(segment_data_chunks)
      ))
      futures.push(stream)
    FuturesUnordered::collect(futures):
      wait for W acks (or timeout)
        → all acked → WriteResult { ... }
        → timeout  → WriteError::QuorumNotMet

gRPC Read Fetch (remote shard):
  ReadCoordinator::fetch_shards(segment_id, shard_indices):
    for each shard_index in shard_indices:
      node = ring.locate_shard_node(segment_id, shard_index)
      pool.acquire_channel(node) → tonic Channel
      client = SegmentRpcClient::new(channel)
      stream = client.fetch_shard(ShardRequest { segment_id, shard_index, ... })
      futures.push(stream)
    FuturesUnordered::collect(futures):
      collect until k complete shards received
      for each response chunk: verify checksum, accumulate bytes
      → return Vec<ShardData>

gRPC Gossip Exchange:
  Membership::gossip_round():
    peers = membership.random_peers(fanout)
    for peer in peers:
      delta = membership.compute_delta(peer.last_known_version)
      if delta is non-empty:
        client = GossipRpcClient::new(pool.channel(peer))
        client.push(stream_of(delta)).await

gRPC Join Protocol:
  Membership::join_cluster(seed_nodes):
    for seed in seed_nodes:
      client = GossipRpcClient::new(pool.channel(seed))
      response = client.pull(GossipPullRequest { ... }).await
      merge_membership_list(response.messages)
      announce_self_via_push()
```

## Key Decisions

### DK-001: gRPC Service Placement

**Decision:** Segment and healing service handlers live in `oceanfs-server`;
gossip and probe service handlers live in `oceanfs-membership`.

**Rationale:** The service implementation needs access to the crate's internal
state (`SegmentStore`, `Membership`, etc.). Per architecture.md §2.1, services
live in the crate that implements them. The generated server traits from
`oceanfs-network` are implemented in the owning crate. This follows the pattern
of `SegmentRpc` service definition in `oceanfs-network/proto/storage.proto` but
the `impl` in `oceanfs-server`.

### DK-002: Streaming Chunk Size

**Decision:** Chunk gRPC streams at 64 KB boundaries for both `AppendSegment`
and `FetchShard`.

**Rationale:** Per perf guideline §4.4, streaming overlaps data transfer with
processing. 64 KB aligns with the buffer pool chunk size (spec §11.2) and the EC
strip size. Smaller chunks increase framing overhead; larger chunks reduce the
overlap benefit. 64 KB is the sweet spot for gRPC over HTTP/2 on a typical
datacenter network (10-100 Gbps, <1ms RTT).

### DK-003: Failure Detector Probe RPC

**Decision:** The SWIM `Probe` RPC is a unary call (not streaming). The indirect
ping path (where node A asks node C to ping node B on A's behalf) is
implemented as: C receives a `ProbeRequest` with `is_indirect = true`, C pings B
via its own `Probe` RPC, and returns the result to A.

**Rationale:** SWIM probes are small (a few bytes of node ID and incarnation).
The overhead of streaming setup would dominate. Unary RPC is appropriate here.
The indirect ping flow matches the standard SWIM protocol as described in the
original paper.

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds; all gRPC service impls
  compile
<!-- REVIEW (Iteration 3): ✅ Build passes. Four service implementation files exist (segment_service.rs, gossip_service.rs, healing_service.rs, cache_service.rs). gRPC services NOW REGISTERED with tonic Server at node.rs:254-270 — significant progress from iteration 2. probe_service.rs still does not exist. All service handlers remain SKELETONS: segment append counts bytes but doesn't write (segment_service.rs:48-51), fetch returns zero-filled data (segment_service.rs:96), gossip push doesn't merge state, gossip pull returns empty delta, healing returns zero-hash Merkle roots (healing_service.rs:63) -->
- [ ] **Tests:** Unit tests per service handler:
  - `SegmentGrpcService`: append empty stream → error, append valid stream →
    segment persisted, fetch existing shard → stream returned, fetch
    nonexistent → NOT_FOUND
  - `GossipGrpcService`: push new entries → membership updated, push stale
    entries → no-op, pull with known version → delta returned, pull with
    current version → empty
  - `HealingGrpcService`: handoff valid hint → accepted, merkle exchange →
    correct root returned
  - `Probe`: direct ping to self → ack, ping to other → forwarded correctly
<!-- REVIEW: Iteration 2 ❌ No service handler unit tests exist. Service impls are skeleton (count bytes, return placeholders). No #[cfg(test)] modules in any grpc/ service file. -->
- [ ] **Tests:** Integration tests:
  - Two-node cluster: append segment via gRPC, read back via gRPC, data
    matches
  - Three-node cluster: write with W=2, verify both replicas via gRPC
  - Gossip exchange: start node 2, seed from node 1, verify membership list
    converges within 5 gossip rounds
  - SWIM: kill node 2, node 1 detects DEAD within `failure_timeout_ms`
<!-- REVIEW: No gRPC service implementations → no integration tests possible -->
- [ ] **Tests:** `FuturesUnordered` concurrency: 3 replicas, 1 slow (delayed
  response), verify "fastest k" returns before slow replica completes
<!-- REVIEW: replication.rs (crates/oceanfs-server/src/write/replication.rs) does use FuturesUnordered but only sends simulated ACKs (line 78: `replica write (simulated)`). No real gRPC integration -->
  placeholder paths (replication, fetch, router, handoff, gossip, membership
  join) all tested
<!-- REVIEW: No service modules to cover. Placeholders remain: replication.rs uses simulated ACKs, router.rs's try_forward() is not implemented with actual gRPC, hinted_handoff.rs has no gRPC delivery, gossip/membership have no gRPC exchange -->
<!-- REVIEW: Iteration 2 ✅ clippy passes. Service handlers compile without unwrap/expect. -->
- [x] **Docs:** Every `pub` service struct has module docs with wire protocol
  description; gRPC handler methods documented with request/response semantics
<!-- REVIEW: Iteration 2 ✅ All 4 service files have module-level docs. Handler methods have doc comments describing behavior. segment_service.rs:1-5, gossip_service.rs:1-6, healing_service.rs:1-5, cache_service.rs:1-6 all have //! module docs. -->
- [ ] **ADR:** N/A (wire protocol driven by spec §12.3)
<!-- REVIEW: N/A — no implementation to verify -->
- [x] **Perf:** Rule 4.1 (connection pool reused — no per-RPC connect), Rule
  4.4 (streaming for data transfer — `stream` keyword on append/fetch), Rule
  4.5 (per-operation timeouts: WAL write 500ms, metadata 50ms, shard fetch
  30s), Rule 2.6 (bounded channels for gossip message queues — capacity
  configurable via `gossip_channel_capacity`)
<!-- REVIEW (Iteration 3): 4.4 ✅ streaming declared in proto. 4.1: ConnectionPool exists and is wired at node.rs:129,178-180 — gRPC services now registered with tonic Server (node.rs:254-270) but no actual gRPC calls flow through yet. 4.5: OperationTimeouts exist but not wired. 2.6: segment_service fetch uses bounded mpsc::channel(16) (line 86) ✅, gossip_service pull uses bounded mpsc::channel(16) (line 80) ✅. No unbounded channels found anywhere ✅ -->
- [ ] **Integration:** End-to-end test: 3-node mini-cluster with full gRPC
  wire-up — node 1 PUT blob, node 2 GET returns same blob, kill node 3, node 1
  PUT another blob, node 2 GET still works (via node 1's replica)
<!-- REVIEW: No gRPC service implementations → end-to-end test impossible -->
  node returns correct data
<!-- REVIEW: Manual verification not possible without gRPC service impls -->
