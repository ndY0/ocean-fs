# ADR-0002: SWIM + Consistent Hashing vs Raft per Shard

**Status:** Accepted
**Date:** 2026-08-07
**Deciders:** OceanFS architecture team

---

## Context

OceanFS needs a distributed coordination mechanism for blob placement,
membership management, and failure detection. The two fundamental choices
are:

1. **SWIM + Consistent Hashing**: Each node independently computes blob
   placement via a deterministic hash function on a shared ring topology.
   Membership is disseminated via gossip. Failure detection uses the SWIM
   protocol (Scalable Weakly-consistent Infection-style process group
   Membership).

2. **Raft per Shard**: A Raft consensus group (typically 3-5 nodes) is
   elected for each hash range (shard). The leader serializes all writes
   to that shard. Membership and failure detection are built into Raft.

Both approaches have been battle-tested in production distributed systems
(SWIM: HashiCorp Serf, Cassandra; Raft: etcd, TiKV, CockroachDB).

The question: given OceanFS's target workload (high-throughput blob store,
eventually-consistent metadata, S3-compatible API), which coordination
model should the system adopt?

### Forces

- OceanFS targets **throughput over strong consistency**. Blob writes are
  idempotent (PUT with content-hash). The S3 API already provides
  eventually-consistent semantics for list-after-put.

- The system must **scale horizontally** with minimal coordination
  overhead. Adding a node should rebalance ~O(N/M) data (where N = total
  data, M = current node count), not halt the cluster for a leader
  election.

- **Single-shard reads should be fast** (R=1). Consistent hashing maps
  each blob to a single preferred node per replica. Raft requires
  contacting the leader (or a read lease), adding latency.

- **Operational simplicity** matters for a team building v0.2. Raft requires
  careful tuning of heartbeat intervals, election timeouts, and log
  compaction. SWIM + consistent hashing requires tuning gossip interval
  and suspicion timeout.

- **Failure detector** must detect unresponsive nodes in O(seconds) for
  hinted handoff and healing to activate.

## Decision

**Adopt SWIM + Consistent Hashing as the canonical coordination model
for OceanFS.**

### SWIM Failure Detection

The SWIM protocol is implemented in `oceanfs-membership`:

- **Direct ping**: Each node randomly selects an alive peer on every
  gossip interval and sends a direct ping.
- **Indirect ping**: If no ack arrives within `ping_timeout_ms`, the
  node requests k random peers to ping the target on its behalf.
- **Suspicion**: If indirect pings also fail, the target is marked
  SUSPECT. After `suspicion_timeout_ms` without recovery, it is declared
  DEAD.

Remote probes (cross-node pings) are handled via the **gossip-push-as-ping-proxy**
approach (DK-007): the gossip protocol's push/pull delta carries probe
information, allowing nodes to detect failures without a dedicated gRPC
Probe RPC. Self-pings are handled in-process.

### Consistent Hashing Ring

Blob placement uses consistent hashing via `oceanfs-routing`:

- Each node owns a range of the hash ring.
- A blob's key is hashed to a ring position; the first N distinct
  successor nodes (N = replication factor) store replicas.
- Adding/removing a node remaps only the keys in that node's range
  (O(N/M) rebalance), not the entire ring.
- R=1 warm reads are possible because the routing directly maps a key
  to the responsible node.

### Rationale

| Criterion | SWIM + Consistent Hashing | Raft per Shard |
|---|---|---|
| Write throughput | O(1) nodes contacted per write (coordinator → replica) | O(1) leader contacted, but leader serializes all shard writes |
| Read latency | R=1 read from any replica | Leader read or read-lease required |
| Rebalance cost | O(N/M) keys remapped | Full state transfer to new Raft member |
| Failure detection | SWIM: O(log N) gossip rounds to converge | Raft: leader heartbeat timeout (typically 150-300ms) |
| Consistency | Eventually consistent | Strongly consistent (linearizable) |
| Operational complexity | Tune gossip interval + suspicion timeout | Tune heartbeat + election timeout + snapshot interval |
| Correctness model | Relies on anti-entropy (Merkle tree exchange) + hinted handoff | Raft guarantees safety via log replication |

OceanFS's workload (large blobs, idempotent writes, S3 semantics) does
not benefit from strong consistency at the coordination layer. The
eventual-consistency model of consistent hashing is sufficient.

### SWIM Remote Probe Design (DK-007)

Instead of adding a dedicated `Probe` gRPC RPC to the membership proto,
OceanFS uses the existing gossip push/pull mechanism as a ping proxy:

1. When node A wants to probe node B, A's failure detector registers a
   pending ping for B.
2. A's gossip protocol includes B in its next push delta with the
   pending ping flag.
3. When node C (a relay) receives A's gossip push, C forwards the probe
   to B via its next gossip push or by checking its own alive set.
4. B's response flows back through the gossip merge, eventually reaching
   A via pull or push from C.

This design avoids an additional RPC endpoint and leverages the existing
gossip mesh. The tradeoff is higher probe latency (2-3 gossip rounds
instead of 1 direct RPC), which is acceptable for OceanFS's target
failure detection window of 5-60 seconds.

## Consequences

### Positive

- **No leader bottleneck**: Write throughput scales linearly with node
  count. No serialization point for concurrent writes.
- **Fast rebalance**: Adding a node remaps only a fraction of keys.
  Removing a node triggers hinted handoff for in-flight writes.
- **Simple operational model**: No leader election tuning, no log
  compaction, no snapshot scheduling.
- **R=1 warm reads**: Hot blobs can be served from a single replica
  with no leader round-trip.

### Negative

- **No strong consistency**: Clients may observe stale reads (list
  results missing recent writes, GET returning old data during
  rebalance). Anti-entropy (Merkle tree exchange) eventually converges
  but does not guarantee linearizability.
- **Gossip convergence latency**: Membership changes propagate in
  O(log N) gossip rounds. During convergence, nodes may route writes
  to a departed node (fixed by hinted handoff) or read from a stale
  replica (fixed by HLC version comparison).
- **Metadata writes need conflict resolution**: If two coordinators
  write to the same object key concurrently, HLC timestamps provide
  last-writer-wins resolution. This is acceptable for S3 semantics
  (which already specifies last-writer-wins for same-key PUTs).

### Neutral

- **Operational tuning**: Both models require tuning. SWIM needs
  `interval_ms` (gossip frequency), `suspicion_timeout_ms`, and
  `indirect_ping_count`. These are documented in `GossipConfig`.
- **Anti-entropy is mandatory**: Without Raft's log replication,
  OceanFS relies on anti-entropy (Merkle tree exchange) to detect
  and repair divergent replicas. This is implemented in
  `oceanfs-durability`.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| Raft per shard | Strong consistency, built-in leader election, log replication as anti-entropy | Leader bottleneck on writes, full state transfer on membership change, complex snapshot/compaction tuning | Overkill for eventually-consistent blob store. Throughput penalty of serialized writes per shard. |
| Multi-Paxos | Similar to Raft with more mature academic foundation | Fewer production implementations, harder to tune | Raft has broader ecosystem support. |
| CRDT-based metadata | No coordination needed for metadata writes, automatic conflict resolution | Complex data model, no production blob store uses this | Too novel for v0.2; S3 API doesn't need CRDT semantics. |
| Central coordinator (monolithic) | Simple, no distributed protocol | Single point of failure, cannot scale writes | Violates OceanFS's distributed-by-design architecture. |

## When to Revisit

1. **If strong consistency becomes a requirement**: If a customer
   workload requires linearizable reads (e.g., financial metadata),
   investigate Raft-per-shard for metadata operations while keeping
   consistent hashing for blob data.

2. **If gossip convergence is too slow**: If membership changes cause
   >10-second periods of incorrect routing under high churn, consider
   augmenting SWIM with a dedicated gossip-accelerator channel or
   switching to a Raft-managed membership.

3. **If anti-entropy cost dominates**: If Merkle tree exchange consumes
   significant bandwidth in large clusters (>100 nodes), consider
   Raft's log-based state machine replication for metadata while
   keeping consistent hashing for blob placement.

## References

- [SWIM: Scalable Weakly-consistent Infection-style process group Membership protocol](http://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
- [Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) — consistent hashing in production
- [HashiCorp Serf](https://www.serf.io/docs/internals/gossip.html) — SWIM + gossip in Go
- [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)
- OceanFS ADR-0001: Segment Packing vs Per-Object EC
- OceanFS ADR-0005: Trait-in-Consuming-Crate Pattern
