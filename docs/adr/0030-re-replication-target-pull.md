# ADR-0030: Re-Replication Placement — Target-Pull with a Dedicated Request RPC

**Status:** Accepted
**Date:** 2026-08-23
**Deciders:** User (architecture owner) + Implementer

**Related:** [ADR-0029 disk resilience](../adr/0029-storage-pools-disk-resilience.md)
(§D4 pull safety net, §D6 repair pacing), [ADR-0028 membership plane](../adr/0028-membership-plane-full-swim-gossip.md),
[ADR-0025 segment lifecycle](../adr/0025-segment-lifecycle-state-machine.md),
[ADR-0024 segment event log](../adr/0024-segment-event-log.md)

---

## Context

ADR-0029 §D4's two detectors — g3's loss announcement (push, fast path)
and g4's periodic reconciliation (pull, safety net) — both enqueue
re-replication requests when a segment's live replica count falls below
RF. The question this ADR settles is **who executes the repair and how
the new copy lands on a target node**.

The initial g5 feature spec assumed a **holder-push** model: the node
that detects the under-replication (a holder of the segment, since only
holders know the segment exists in their registry) fetches the segment
data from a live holder and **pushes** it to a selected target via the
existing `PushSealedSegment` RPC (the sealed-segment-replication
backbone's full-segment mover).

The architecture owner challenged that premise: enqueueing the repair
on the **holder** shifts the responsibility for reconstructing a lost
copy onto a node that is not the one that will own it. The natural
owner of the reconstruction is the **acquiring node** — the one whose
store will hold the new copy. "Queue locally and fetch" — the target
pulls the data from a holder through its own segment pipeline, rather
than a holder pushing bytes at it.

Two forces shaped the decision:

1. **Registry-locality of detection.** A node only knows the segments
   in its own lifecycle registry. The holder is the only node that (a)
   knows a segment exists, (b) can verify the loss (its registry says
   the dead node was a legitimate holder), and (c) holds the data. A
   non-holder target cannot self-detect — the request must be routed to
   it. So *detection stays at the holder*, but *execution moves to the
   acquiring node*.
2. **Responsibility alignment.** The node whose pool will hold the new
   copy should materialize it through its own pool-aware
   `SegmentDataStore` (which selects the pool via `PlacementPolicy`,
   ADR-0029 f3). A holder pushing bytes at a target bypasses that: the
   bytes land in a pool the target's own placement logic never chose.

## Decisions

### Decision 1: Re-replication is target-pull, not holder-push

The holder that detects under-replication acts only as **dispatcher**:
it selects a target node (via a `RepairTargetSelector` — manifest
health + free capacity, ADR-0029 §D5/D6) and sends the target a
**dedicated `RequestReReplication` RPC**. The target enqueues the
request into its own local re-replication worker queue; the worker
**fetches** the full segment data from a live holder (via
`HealingRpcClient::fetch_shard` in full-segment mode), writes it
through its own pool-aware `SegmentDataStore`, registers the segment in
its lifecycle registry, and stamps `storage_locations`.

- Detection (g3/g4) → dispatcher (holder side) → `RequestReReplication`
  RPC → target's `ReRepWorker` queue → target fetches + writes +
  registers + stamps.
- The dispatcher filters the holder set to **live** holders before
  sending the request, so the target never attempts a dead holder.
- The target's write goes through the pool-aware store, so
  `PlacementPolicy` picks the pool on the node whose pool it actually
  is (f3), not a pool id borrowed from the holder's metadata.

### Decision 2: A dedicated `RequestReReplication` RPC

A dedicated RPC — rather than overloading an existing one or pushing
via `PushSealedSegment` — because the request is **routing intent**,
not data movement:

```
rpc RequestReReplication(RequestReReplicationRequest)
    returns (RequestReReplicationResponse);

message RequestReReplicationRequest {
  SegmentId segment_id = 1;
  repeated NodeId holders = 2;   // live holders the target may fetch from
  RepairReason reason = 3;       // Announcement | Reconciliation (pacing/metrics)
}

message RequestReReplicationResponse {
  bool accepted = 1;
}
```

- The request carries only metadata (segment id + live holder set +
  reason) — the data moves over the existing `FetchShard` stream, so
  the dedicated RPC stays small and cheap.
- `RepairReason` (Announcement | Reconciliation) rides the request so
  the worker can report which detector drove each repair
  (`oceanfs_repair_queue_depth{priority}`), preserving ADR-0029 §D6's
  urgency signal end-to-end.

### Decision 3: `storage_locations` update rides the durable refresh path

The target stamps its own `storage_locations` through the lifecycle
coordinator's existing durable write path — `request_refresh_metadata`
is extended to carry the new location set — so the **event-WAL remains
the only durable writer** (ADR-0024/25). No new event type; the
`MetadataRefreshEvent` payload grows a backward-compatible locations
section.

- This is a durability-critical format change (kind=3 payload gains an
  optional location list); decode-length checks, `MAX_PAYLOAD_SIZE`,
  and byte-length tests in `oceanfs-storage` are updated together.
- The holder dispatcher converges its own registry entry after a
  successful ack so its reconciliation loop stops re-dispatching the
  same segment (the g4 "post-repair holder set must be stamped"
  handoff).

### Decision 4 (consequence, not yet implemented): migration-plane isolation

The same isolation argument that created the membership plane in
ADR-0028 applies one level down: a re-replication storm (routine at
fleet scale — a disk dies every few days at 500 nodes × 10 disks) must
not starve client reads/writes on the data plane. This ADR records the
direction but **deliberately does not implement it**: it is an
ADR-scale topology change (new `NodeConfig` field, a `migration_address`
in membership state, a third `ConnectionPool`, node wiring, and every
integration test's port allocation), orthogonal to g5's correctness.

To keep the option open, the worker receives its `ConnectionPool` and
`membership` injected from the node (the HealWorker precedent) so a
later plane swap is a wiring change, not a `process()` rework.

## Alternatives considered

- **Holder-push via the existing `PushSealedSegment`** (the initial
  spec): zero new RPCs and one replication code path, but the holder
  moves the bytes and the target's pool placement is bypassed — the
  responsibility misalignment this ADR rejects.
- **New RPC + full-segment fetch mode as separate additions**: accepted
  — the fetch already exists as `FetchShard`; the new RPC is only the
  routing intent. A "fetch full segment" mode on `fetch_shard`
  (shard_index 0 + length 0 → whole data section) reuses the existing
  stream without a second data RPC.

## Consequences

- The node that will own the copy materializes it; the holder only
  routes. Detection stays where the knowledge is; execution stays where
  the responsibility is.
- One new RPC (`RequestReReplication`) + one backward-compatible
  `MetadataRefreshEvent` payload extension. The existing
  `PushSealedSegment` path is untouched (seal-time replication still
  pushes — that is the owner pushing its *own* new segment, a different
  responsibility than repairing a lost copy).
- The g4 reconciliation loop and g3 announcement handler keep enqueueing
  into the same `RepairSink`; the sink's node-side implementation
  becomes the dispatcher (target selection + RPC). Their public contract
  (`RepairSink::enqueue`) is unchanged.
- Fleet-scale storm isolation is tracked as a forward-looking
  consequence (Decision 4), implementable later without touching g5's
  worker logic.
