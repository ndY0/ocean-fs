---
feature: "Cluster Mode E2E Tests"
epic: "e2e-testing"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: cluster-bootstrap
    reason: PR1-PR6 must be fixed before any cluster test can pass
  - epic: e2e-testing
    feature: broad-smoke-tests
    reason: Shared e2e/ crate and NodeProcess harness
  - epic: final-integration
    feature: final-integration-grpc-services
    reason: All gRPC service handlers must be implemented and registered
  - epic: phase-2-distributed-connectivity
    reason: Ring, Membership, Gossip, ConnectionPool must exist
  - epic: phase-4-distributed-read-write
    reason: WriteCoordinator, ReadCoordinator, HintedHandoff, Router
adr: []
perf:
  - "4.1: Persistent gRPC connection pool per peer"
  - "4.4: Streaming gRPC for large data transfers"
  - "4.5: Adaptive per-operation timeouts"
created: 2026-08-03
updated: 2026-08-03
---

# Cluster Mode E2E Tests

## Summary

Validate every distributed code path in OceanFS by running 2-node and 3-node
mini-clusters in a single process group. Each test spawns N `oceanfs` binary
instances (via the `NodeProcess` harness from the `e2e/` crate), wires them
together through seed node configuration, exercises the distributed operations
(gossip, quorum writes, parallel reads, hinted handoff, anti-entropy, failure
detection), and asserts correctness through HTTP admin endpoints and S3 API
calls.

**These tests protect against regressions in the most complex, hardest-to-reason-about
parts of the system.** They are the air-tight correctness gate before any
iteration on cluster-mode features.

## Pre-Requisites — Must Fix Before Testing

A survey of the current cluster-mode code (commit `a78a430`) identified six
blockers that prevent two nodes from forming a cluster. These must be fixed
before any cluster test can pass.

| # | Blocker | Impact | Where |
|---|---|---|---|
| PR1 | `Membership::start()` spawns an unreachable detector task and drops the gossip receiver | No background tasks run. No failure detection. No gossip. | `membership.rs:138-169` |
| PR2 | No periodic gossip ticker | `GossipProtocol` logic is real but nothing triggers it. No delta exchange. | `gossip.rs` — needs `tokio::time::interval` loop |
| PR3 | No SWIM ping initiation loop | `FailureDetector` state machine is real but `select_random_peer` is never called. No pings sent. | `failure_detector.rs` — needs ping loop in `run()` |
| PR4 | `upsert_node()` does not update the `RingCache` | Nodes learned via gossip push never appear in the ring. Routing can't reach them. | `membership.rs:389-409` |
| PR5 | Seed node does not add joiner to its ring on join | Only the joiner adds itself to its own ring. The seed's ring stays single-node. | `membership.rs:join()` — seed side needs ring update |
| PR6 | `Router::try_forward()` validates reachability but drops the channel without forwarding data | Forwarding to the correct coordinator node doesn't actually happen. | `router.rs:180-215` |

**Without PR1-PR5, two nodes can start but never discover each other.**
The ring stays single-node on both sides; every distributed operation
fails with "ring returned empty replica set" or routes to self only.

PR6 is a correctness gap (forwarding doesn't forward) but only manifests
when a request arrives at a non-coordinator node.

## Scope

### In Scope

All tests use the `e2e/` crate harness (`NodeProcess`) and run against
the release binary.

#### Topology Tests

| # | Test | What It Validates |
|---|---|---|
| T1 | **2-node join** | Node B starts with seed=Node A. Both rings contain both nodes. `/admin/cluster` on each returns 2 nodes. |
| T2 | **3-node join** | Node B and C join via seed=A. All three rings converged. |
| T3 | **Graceful leave** | Node C sends SIGTERM. Nodes A and B remove C from membership and ring within `failure_timeout_ms`. |
| T4 | **Rejoin after leave** | Node C restarts with same data dir. Rejoins cluster. Ring converges to 3 again. |

#### Gossip & Membership Tests

| # | Test | What It Validates |
|---|---|---|
| T5 | **Gossip convergence** | Node B joins. Within N gossip rounds (N ≤ 10), Node A's membership list includes B in ALIVE state. Assert gossip interval-driven propagation works. |
| T6 | **Gossip delta propagation** | Node A adds a node (or changes state). Within 5 rounds, Node B and C see the change. |
| T7 | **Ring version propagation** | After a join/leave changes the ring, all nodes converge to the same ring generation within 10 gossip rounds. |
| T8 | **Incarnation monotonicity** | A node's incarnation number never decreases across gossip rounds. On rejoin, incarnation increments. |

#### Write Path Tests (Quorum)

| # | Test | What It Validates |
|---|---|---|
| T9 | **Single-replica write (W=1, N=3)** | PUT object. Write succeeds (200). Object readable from the node that accepted the write. |
| T10 | **Quorum write (W=2, N=3)** | PUT object with write_quorum=2. Write succeeds only if ≥2 nodes ack. Assert /admin/health on all 3 nodes; object readable from any. |
| T11 | **Full write (W=3, N=3)** | PUT object with write_quorum=3. All 3 nodes ack. Object readable from every node. |
| T12 | **Quorum not met (W=3, N=2)** | Request W=3 with only 2 replicas. Write fails with 503. |
| T13 | **Write forwarding** | PUT to a node that is NOT in the replica set. Request gets forwarded to the correct coordinator. Write succeeds. |
| T14 | **Write to dead node's successor** | Kill the coordinator node. PUT to a live node. Request routes to the next alive successor. Write succeeds. |

#### Read Path Tests (Quorum + Consistency)

| # | Test | What It Validates |
|---|---|---|
| T15 | **Single-replica read (R=1, N=3)** | Read from any node. Returns correct data. |
| T16 | **Quorum read (R=2, N=3)** | Read with read_quorum=2. Two replicas agree. Returns correct data. |
| T17 | **Stale replica detection** | Write to node A (W=1). Read from node B (R=2) before gossip propagates the write. Read-repair pushes correct data to B or returns the stale version depending on HLC comparison. Assert eventual consistency. |
| T18 | **Read from non-replica** | GET from a node not in the replica set. Request forwarded or routed to a replica. Correct data returned. |
| T19 | **Read from node where data was deleted** | DELETE on node A. GET on node B returns 404 after tombstone propagation (or 200 if still stale — assert eventual consistency). |

#### Hinted Handoff Tests

| # | Test | What It Validates |
|---|---|---|
| T20 | **Hint storage on unreachable successor** | Write with W=2, N=3. Kill 1 successor. Write succeeds with hinted handoff to fallback node. Hint stored. |
| T21 | **Hint delivery on node return** | Restart the killed successor. Hinted handoff delivers buffered data. Object readable from the returned node. |
| T22 | **Hint expiry** | If a node stays dead past hint TTL, hints are discarded. No delivery attempted. |

#### Failure Detection Tests (SWIM)

| # | Test | What It Validates |
|---|---|---|
| T23 | **Direct ping success** | All nodes ALIVE. Failure detector pings succeed. No state changes. |
| T24 | **SUSPECT on direct ping timeout** | Kill node C. Within `suspicion_timeout_ms`, nodes A and B mark C as SUSPECT. Assert via `/admin/cluster`. |
| T25 | **DEAD on suspicion timeout** | After `suspicion_timeout_ms` + `failure_timeout_ms`, SUSPECT transitions to DEAD. Assert via `/admin/cluster`. |
| T26 | **Indirect ping path** | Kill node C. Node A's direct ping to C fails. Node A requests indirect pings from B. B's ping to C also fails. A marks C SUSPECT. |
| T27 | **False positive resistance** | Brief network hiccup (simulated by pausing a node briefly). Node is NOT marked DEAD if it responds to indirect pings. |

#### Anti-Entropy & Healing Tests

| # | Test | What It Validates |
|---|---|---|
| T28 | **Cross-node Merkle exchange** | PUT objects on node A. On node B, anti-entropy cycle exchanges Merkle roots with A. No mismatches detected. |
| T29 | **Merkle mismatch detection** | Corrupt a shard on node B (overwrite segment file bytes). Anti-entropy detects root mismatch. Descends tree to find diverged leaves. Enqueues heal. |
| T30 | **Heal after corruption** | After T29, heal worker reconstructs corrupt shard from surviving replicas. Re-run anti-entropy: no mismatches. Object readable from B. |
| T31 | **Heal after node failure** | Kill node C (lost 1 parity shard). Heal scheduler reconstructs missing shard on a new node. Data integrity maintained. |

#### Ring & Routing Tests

| # | Test | What It Validates |
|---|---|---|
| T32 | **Consistent hashing determinism** | Same key → same replica set on all nodes. Assert `ring.lookup(key_hash)` returns identical successors on A, B, C. |
| T33 | **Replica set distinctness** | No node appears twice in a replica set. All `replication_factor` successors are distinct nodes. |
| T34 | **Ring rebalance on node add** | 2-node cluster. Add node C. Only O(N/M) keys change assignment. Assert most keys retain original replica set. |
| T35 | **Ring rebalance on node remove** | 3-node cluster. Remove node C. Keys that had C in their replica set now have a different successor. Assert lookup still returns `replication_factor` distinct nodes. |

#### Cache Invalidation Tests

| # | Test | What It Validates |
|---|---|---|
| T36 | **Remote cache invalidation on write** | Node A has object in L1 cache. Node B PUTs a new version. Node A's L1 cache is invalidated via gRPC `CacheInvalidate`. Node A's next GET returns the new version, not the stale cache. |
| T37 | **Remote cache invalidation on delete** | Node A has object in L1 cache. Node B DELETEs it. Node A's cache invalidated. Node A's next GET returns 404. |

#### Scrub Tests

| # | Test | What It Validates |
|---|---|---|
| T38 | **Distributed scrub partition assignment** | Trigger scrub via `POST /admin/scrub`. Each node receives a partition of segment IDs. All nodes report healthy. |
| T39 | **Scrub detects corruption** | Corrupt a shard on node B. Trigger scrub. Scrub worker on B detects Merkle root mismatch. Reports segment as corrupt. Enqueues heal. |

#### Node Lifecycle Tests

| # | Test | What It Validates |
|---|---|---|
| T40 | **Graceful leave — WAL handoff** | Node C leaves gracefully. Active WAL segments handed off to successor. Data not lost. |
| T41 | **Graceful leave — shard streaming** | Node C leaves. Owned segment shards streamed to successors before departure. Ring recomputed without C. |
| T42 | **Crash recovery — WAL replay** | Kill -9 node A mid-write. Restart. WAL replays unsealed data. Objects from before crash are readable. |
| T43 | **Crash recovery — rejoin** | Kill -9 node A. Restart with same data dir and seed config. Rejoins cluster. Ring converges. A's pre-crash data is still readable. |

#### Concurrency & Stress Tests

| # | Test | What It Validates |
|---|---|---|
| T44 | **Concurrent writes to different keys** | 10 concurrent PUTs to different keys from different nodes. All succeed. All readable from all nodes. No data corruption. |
| T45 | **Concurrent writes to same key** | 2 concurrent PUTs to the same key from different nodes. HLC resolves to a single winner. Both nodes eventually agree on the winning version. |
| T46 | **Write during node failure** | Start a PUT. Kill one successor mid-replication. Write completes with remaining W acks (or fails gracefully if quorum lost). |

### Out of Scope

- Multi-region / WAN replication (spec §16, future work)
- EC encoding pipeline integration (codec real, pipeline stub)
- Shard distribution to remote nodes (placeholder — EC is decoded locally)
- "Fastest k" parallel shard fetch from multiple replicas (code does sequential fallback)
- Bucket creation/deletion API (returns 404 — not yet implemented)
- S3 auth in multi-node context (SigV4 tested in single-node; key distribution is future work)
- gRPC TLS/mTLS (always plaintext for now)
- Performance benchmarks (throughput, latency percentiles)
- Config hot-reload (SIGHUP)
- OpenTelemetry distributed tracing

## Test Harness Extensions

The existing `NodeProcess` harness in `e2e/` needs cluster-aware extensions:

```rust
// e2e/src/harness.rs — additions

/// A managed cluster of N OceanFS nodes.
pub struct Cluster {
    nodes: Vec<NodeProcess>,
}

impl Cluster {
    /// Spawns `count` nodes. The first node has no seed; subsequent
    /// nodes use `nodes[0]` as seed. All share a common temp directory
    /// root (each node gets a subdirectory).
    pub async fn spawn(count: usize, base_config: &str) -> Result<Self>;

    /// Returns a reference to node `i`.
    pub fn node(&self, i: usize) -> &NodeProcess;

    /// HTTP GET from node `i`.
    pub async fn get(&self, i: usize, path: &str) -> reqwest::Result<Response>;

    /// HTTP PUT to node `i`.
    pub async fn put(&self, i: usize, path: &str, body: &[u8]) -> reqwest::Result<Response>;

    /// HTTP DELETE from node `i`.
    pub async fn delete(&self, i: usize, path: &str) -> reqwest::Result<Response>;

    /// Kill node `i` (SIGKILL).
    pub fn kill(&mut self, i: usize) -> Result<()>;

    /// Restart a previously killed node `i` with its original data dir.
    pub async fn restart(&mut self, i: usize) -> Result<()>;

    /// Wait until all nodes agree on cluster size `expected_nodes`
    /// (polls `/admin/cluster` on each node every 500ms, timeout 30s).
    pub async fn wait_for_convergence(&self, expected_nodes: usize) -> Result<()>;

    /// Shut down all nodes gracefully.
    pub async fn shutdown(self) -> Result<()>;
}
```

### Test Config Templates

Each test scenario needs a purpose-built config. The harness provides helpers:

```rust
/// Standard 3-node cluster config: W=2, R=2, N=3, replication_factor=3
fn config_3node_w2_r2() -> String { ... }

/// Shortened gossip interval (1s) for fast convergence tests
fn config_fast_gossip() -> String { ... }

/// Shortened failure detection (suspicion=2s, failure=5s) for SWIM tests
fn config_fast_swim() -> String { ... }

/// Shortened anti-entropy (10s) for Merkle exchange tests
fn config_fast_ae() -> String { ... }
```

## Test Ordering & Dependencies

Tests are organized in dependency order. Each test can run independently
(spawns its own cluster), but the logical flow is:

```
Topology (T1-T4)
  └─ Gossip & Membership (T5-T8)
       └─ Write Path (T9-T14)
            └─ Read Path (T15-T19)
                 └─ Hinted Handoff (T20-T22)
       └─ Failure Detection (T23-T27)
  └─ Anti-Entropy & Heal (T28-T31)
  └─ Ring & Routing (T32-T35)
  └─ Cache Invalidation (T36-T37)
  └─ Scrub (T38-T39)
  └─ Node Lifecycle (T40-T43)
  └─ Concurrency & Stress (T44-T46)
```

## Key Decisions

### DK-001: All Nodes in One Process Group

**Decision:** All nodes in a test run as child processes of the test binary.
No Docker, no VMs, no separate machines.

**Rationale:** The test validates distributed protocol correctness, not
network infrastructure. Spawning multiple processes on localhost exercises
the same gRPC code paths, gossip protocol, and quorum logic as a
geographically distributed deployment. Process isolation (separate address
spaces, separate RocksDB instances, separate ports) provides sufficient
isolation for protocol testing. The overhead of Docker/VMs would slow CI
without adding signal.

### DK-002: Unique Ports per Node

**Decision:** Each node gets unique HTTP and gRPC ports assigned from the
ephemeral range (OS-assigned via binding to port 0). The harness records
the assigned ports and uses them for HTTP requests and seed configuration.

**Rationale:** Hardcoded ports cause flakiness when tests run in parallel.
OS-assigned ports eliminate conflicts. The harness resolves actual ports
after bind and propagates them to dependent nodes via config file
templating.

### DK-003: Temp Directories per Test, per Node

**Decision:** Each test creates a fresh `TempDir`. Within it, each node
gets a subdirectory (`{tempdir}/node-0/`, `{tempdir}/node-1/`, etc.).
Directories are cleaned up when the test drops the `Cluster` handle.

**Rationale:** Test isolation. No state leaks between tests. Parallel
test execution requires disjoint data directories. TempDir's RAII cleanup
handles both success and panic paths.

### DK-004: Polling for Convergence, Not Sleeps

**Decision:** Tests that wait for gossip propagation, failure detection,
or ring convergence use active polling (check `/admin/cluster` every
500ms with a 30s timeout) rather than fixed `sleep()` calls.

**Rationale:** Fixed sleeps make tests slow and flaky (too short = race,
too long = waste). Polling with timeout adapts to machine speed. The
30s cap prevents infinite hangs in CI while being generous enough for
slow CI runners.

### DK-005: Node Failure via SIGKILL, Not SIGTERM

**Decision:** Failure detection tests use `kill -9` (SIGKILL) to simulate
crash failures, not SIGTERM (graceful shutdown).

**Rationale:** SIGTERM triggers the graceful leave path (handoff WAL,
stream shards, announce LEFT), which is tested separately in T40-T41.
SIGKILL simulates the crash-failure scenario that SWIM failure detection
is designed to handle — the node disappears without warning. This is
the hardest case and the one we need to protect against.

### DK-006: Pre-Requisite Fixes Are Not in This Feature

**Decision:** The six pre-requisite blockers (PR1-PR6) are documented
here but implemented as part of a separate "cluster-bootstrap" feature
or as direct fixes to the affected crates. This feature's DoD gates on
all E2E tests passing, which implies the pre-requisites are fixed.

**Rationale:** Separation of concerns. The pre-requisite fixes are
implementation work on `oceanfs-membership`, `oceanfs-routing`, and
`oceanfs-server`. The E2E tests are test infrastructure in `e2e/`.
Mixing them in one feature doc would blur the line between "make it
work" and "prove it works."

## Crate Impact

| Crate | Change |
|---|---|
| `e2e/` | MODIFIED — add `Cluster` harness, config helpers, all 46 test files |
| `Cargo.toml` | No change (e2e already in workspace) |
| `oceanfs-membership` | Pre-requisite fixes (separate feature) |
| `oceanfs-server` | Pre-requisite fixes (separate feature) |
| `oceanfs-routing` | Pre-requisite fixes (separate feature) |

## Definition of Done

### Pre-Requisites (must be done before any cluster test can pass)

- [ ] **PR1:** `Membership::start()` actually spawns detector + gossip tasks
- [ ] **PR2:** Periodic gossip ticker selects random peers and pushes deltas
- [ ] **PR3:** SWIM ping loop initiates direct and indirect pings
- [ ] **PR4:** `upsert_node()` and gossip push handler update the `RingCache`
- [ ] **PR5:** Seed node adds joiner to its ring on join
- [ ] **PR6:** `Router::try_forward()` actually forwards data, or is removed
  in favor of `WriteCoordinator::forward_write()` which already works

### Harness

- [ ] `Cluster::spawn(count, config)` works for 2 and 3 nodes
- [ ] `Cluster::kill(i)` and `Cluster::restart(i)` work correctly
- [ ] `Cluster::wait_for_convergence(n)` polls and returns within timeout
- [ ] Config helpers produce valid TOML for each test scenario
- [ ] All nodes bind to unique, OS-assigned ports

### Topology Tests (T1-T4)

- [ ] **T1:** 2-node join — both rings contain both nodes
- [ ] **T2:** 3-node join — all three rings converged
- [ ] **T3:** Graceful leave — departed node removed from rings
- [ ] **T4:** Rejoin after leave — ring converges to 3 again

### Gossip & Membership Tests (T5-T8)

- [ ] **T5:** Gossip convergence — B appears in A's membership within 10 rounds
- [ ] **T6:** Delta propagation — state change visible on all nodes within 5 rounds
- [ ] **T7:** Ring version propagation — ring generation converges within 10 rounds
- [ ] **T8:** Incarnation monotonicity — incarnation never decreases

### Write Path Tests (T9-T14)

- [ ] **T9:** W=1 write succeeds, object readable
- [ ] **T10:** W=2 quorum write succeeds, object readable from any node
- [ ] **T11:** W=3 full write succeeds, all nodes readable
- [ ] **T12:** W=3 with N=2 fails with 503
- [ ] **T13:** Write forwarding to non-replica succeeds
- [ ] **T14:** Write to dead successor's replacement succeeds

### Read Path Tests (T15-T19)

- [ ] **T15:** R=1 read returns correct data
- [ ] **T16:** R=2 quorum read returns correct data
- [ ] **T17:** Stale replica detected and repaired (or eventual consistency asserted)
- [ ] **T18:** Read from non-replica forwarded correctly
- [ ] **T19:** Post-delete read returns 404 after propagation

### Hinted Handoff Tests (T20-T22)

- [ ] **T20:** Hint stored when successor unreachable
- [ ] **T21:** Hint delivered when successor returns
- [ ] **T22:** Expired hints discarded

### Failure Detection Tests (T23-T27)

- [ ] **T23:** Direct ping succeeds, no false state changes
- [ ] **T24:** SUSPECT on ping timeout
- [ ] **T25:** DEAD on suspicion timeout
- [ ] **T26:** Indirect ping path works
- [ ] **T27:** Temporary hiccup does not cause false DEAD

### Anti-Entropy & Healing Tests (T28-T31)

- [ ] **T28:** Cross-node Merkle exchange finds no mismatches
- [ ] **T29:** Merkle mismatch detected, diverged leaves identified
- [ ] **T30:** Heal reconstructs corrupt shard, re-verification clean
- [ ] **T31:** Heal after node failure reconstructs lost shard

### Ring & Routing Tests (T32-T35)

- [ ] **T32:** Consistent hashing yields same replica set on all nodes
- [ ] **T33:** Replica set contains no duplicates
- [ ] **T34:** Ring rebalance on node add affects O(N/M) keys
- [ ] **T35:** Ring rebalance on node remove maintains distinct replicas

### Cache Invalidation Tests (T36-T37)

- [ ] **T36:** Remote cache invalidation on write
- [ ] **T37:** Remote cache invalidation on delete

### Scrub Tests (T38-T39)

- [ ] **T38:** Distributed scrub partition assignment works
- [ ] **T39:** Scrub detects corruption and enqueues heal

### Node Lifecycle Tests (T40-T43)

- [ ] **T40:** Graceful leave hands off WAL
- [ ] **T41:** Graceful leave streams shards to successors
- [ ] **T42:** Crash recovery replays WAL
- [ ] **T43:** Crash recovery + rejoin converges

### Concurrency & Stress Tests (T44-T46)

- [ ] **T44:** 10 concurrent writes to different keys — all succeed
- [ ] **T45:** Concurrent writes to same key — HLC resolves to single winner
- [ ] **T46:** Write during node failure — graceful degradation
