# ADR-0035: Replicated Segment Lifecycle State for Loss Recovery

**Status:** Proposed
**Date:** 2026-09-06
**Deciders:** OceanFS architecture team (pending review — draft for g7/g8)

---

## Context

Review #30 (`segment_replicator.rs:355-360`) flagged that replication
state is memory-resident and asked for "a replicated state" driven by the
event WAL. Wave-3 recovery features need a concrete answer:

- **g7 (wal-pool loss):** the data WAL, the segment **event WAL** and the
  in-memory lifecycle registry all ride the `wal` pool
  (`modules/storage.rs:155-160, 338-349`; `pool_paths.rs:77-83`). When that
  device is replaced, the node loses the authoritative record of what it
  holds and every seal-time metadata field (`merkle_root`, EC geometry,
  tier, `storage_locations`, `contained_objects`, `total_bytes`).
- **g8 (metadata-pool loss):** only the objects/deletions CFs die; the
  registry survives. g8 does not need this ADR's state, but shares its
  peer-fetch machinery and must NOT re-replicate segment data.
- A `.dat`-scan registry rebuild (the original g7 idea) cannot reproduce
  seal-time metadata and collides with the destructive once-per-boot
  residue sweep (audit C1/C3, `modules/storage.rs:658-722`).

Question: what is the node's durable, recoverable source of segment
lifecycle state after the wal pool dies — and how is it restored?

## Decision

### D1. The replicated lifecycle state is each holder's registry entry — no new store

For every segment with a live holder, the holder's lifecycle registry
entry **already is a replica of the segment's seal-time metadata**. The
sealed-segment push carries `tier`, `ec_k`, `ec_m`, `merkle_root`,
`storage_locations`, the full data section, and the `contained_objects`
membership (`PushSealedSegmentRequest`,
`proto/oceanfs/segment.proto:150`, generated
`crates/oceanfs-core/src/generated/oceanfs.segment.rs:196-217`); the
receiver registers that same shape (healing-service reserve/seal paths,
and the repair target mirrors it — `repair.rs:418-513`). We do **not**
create a replicated event-WAL, a second metadata store, or a new column
family. The reference copy for recovery is the registry entry on any
**alive holder** of the segment.

### D2. Recovery consumes the replicated state through a holder fetch RPC

Loss recovery is a pull from a live holder, not a broadcast:

- g7 re-derives its registry after a wal-pool replacement by (a)
  enumerating candidate segment ids from its intact data-pool `.dat`
  files, (b) asking a live holder for each segment's lifecycle metadata,
  (c) fetching full data + metadata for any segment that is missing
  locally, reusing the re-replication fetch/write/stamp path
  (`ReRepWorker`, ADR-0030). The exact RPC shape (a metadata fetch and/or
  a "list the segments you hold" enumeration) is specified by the g7
  feature doc on this decision.
- g8 rebuilds object rows (and, per the 2026-09-06 decision, deletions
  rows) from a live peer over ring ranges (`ListObjectsInRange`,
  specified by the g8 feature doc). Segment lifecycle state is *not*
  touched by g8.

### D3. Local-only segments are the documented gap

A segment whose eligible holder set is empty (`storage_locations ==
{self}` or no live remote holder) has **no remote copy of its lifecycle
metadata**. On wal-pool loss the node rebuilds it from its own `.dat`
(recompute the Merkle root; tier/geometry inferred from the segment
header and size). `contained_objects` / exact `total_bytes` accounting for
those segments is lost — an accepted accounting degradation (ADR-0034
GC liveness may over-retain), never a data-loss event. This mirrors the
AE/scrub local-only coverage shift (ADR-0033).

### D4. Scope boundary with the existing startup recovery and review #30

- The in-memory replication "needs set" (`segment_replicator.rs:355-360`)
  is already reconstructible at boot: the startup replication pass
  re-enqueues `Sealed` entries whose `storage_locations` is empty
  (`modules/storage.rs:739-760`). No needs-set replication is required.
- Incomplete-compaction recovery already folds from `repacked_from`
  markers with point reads (`modules/storage.rs:605-656`,
  `gc/compaction_recovery.rs`). This ADR does not merge the compaction
  state machine into the lifecycle machine; that remains a separate
  brainstorm (the second half of the review #30 comment).
- A **wal-pool replacement marker** must exist so boot can distinguish
  "wal pool was replaced → suppress the residue sweep, rebuild from
  holders" from "normal restart → fold the existing event WAL". The exact
  marker (explicit file vs empty-roots heuristic) is decided by the g7
  feature doc; the ADR requires that the residue sweep
  (`modules/storage.rs:658-722`) never runs against a rebuild-from-holders
  registry.

## Consequences

### Positive

- g7 recovery restores full seal-time metadata (merkle, EC geometry,
  tier, `storage_locations`, `contained_objects`) with **no new on-disk
  format**: the replicated state already exists because the sealed-segment
  push carried it.
- No second source of truth to keep consistent; a holder's entry is
  already kept identical to the owner's by the reserve/seal/refresh
  protocol.
- Reuses the ADR-0030 target-pull fetch/write/stamp machinery rather than
  inventing a new replication path.
- Unblocks g7 (wal-loss) and provides the peer-fetch substrate g8 needs.

### Negative

- Local-only segments cannot restore their `contained_objects` accounting
  after wal-pool loss (accepted, D3).
- Recovery correctness now depends on at least one live holder per
  replicated segment; the residual RF=1 window is explicit (same class as
  g8's documented residual window).
- Requires a small new RPC surface on the healing service (shape in the
  g7/g8 feature docs).

### Neutral

- Node boot order gains an extra recovery stage between the event-WAL fold
  path and the residue sweep; the two paths must be mutually exclusive.
- Operators replace a wal device and the node heals without a restart
  (live remount, g7 decision D2), reusing `HealthMonitor::reset_pool`
  (`pool/health.rs:641-685`), which currently has no production caller.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Stream/replicate the event WAL to peers** | Every node replays another's exact event log | WAL volume doubles; cross-node ordering/durability complexity; holders already hold the *folded* result | Rejected: redundant — the folded registry entry is the state, not the event log |
| **New per-peer RocksDB CF ("replicated lifecycle")** | Explicit, queryable | New store + new write path for every seal/delete/stamp; duplication of what the push already writes into each holder's registry | Rejected: D1 shows the state is already replicated per holder |
| **`.dat`-scan registry rebuild (original g7)** | No new RPC for presence | Loses merkle/contained/storage_locations; collides with the destructive residue sweep (audit C1/C3) | Rejected: cannot restore seal-time metadata; adoption hazard |
| **Full-mesh state pull from all alive nodes** | Simple | O(N) metadata transfers per recovery; holder-set pull is precise | Rejected: holder-targeted pull (ADR-0030 pattern) is cheaper and correct |

## References

- Review #30 comment: `crates/oceanfs-node/src/segment_replicator.rs:355-360`
- Sealed-segment push payload: `proto/oceanfs/segment.proto:150`,
  `PushSealedSegmentRequest` fields (tier/ec/merkle/storage_locations/
  data/contained_objects)
- Recovery + residue sweep: `crates/oceanfs-node/src/modules/storage.rs:562-762`
- WAL + event-WAL paths: `crates/oceanfs-node/src/pool_paths.rs:77-83`
- Re-replication fetch/write/stamp: `crates/oceanfs-durability/src/repair.rs:395-516`
- Reset hook (no caller today): `crates/oceanfs-storage/src/pool/health.rs:641-685`
- Audit: `docs/audits/2026-09-06-g7-g8-spec-code-verification.md`
- Related: ADR-0029 §D7 (WAL/metadata loss recovery), ADR-0025 (lifecycle
  registry = folded events), ADR-0030 (target-pull), ADR-0033 (holder-set
  entry point), ADR-0034 (accounting; `contained_objects`)
