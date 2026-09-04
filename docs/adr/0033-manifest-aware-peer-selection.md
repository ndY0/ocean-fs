# ADR-0033: Manifest-Aware Peer Selection for Anti-Entropy and Scrub

**Status:** Accepted
**Date:** 2026-09-04
**Deciders:** OceanFS architecture team

---

## Context

The 2026-08-25/09-03 review (triage Theme 5) flagged that anti-entropy and
scrub assume full replication between arbitrary peers. Verified in today's
code:

- **Anti-entropy peers are a random sample of ALL alive members.**
  `AntiEntropy::select_alive_peers` (`anti_entropy/engine.rs:863-878`)
  picks random alive nodes from the membership with no replica/manifest
  awareness, then the exchange sends this node's full sealed-segment list
  to whatever peer was chosen (`engine.rs:538-547`). Review
  `anti_entropy/engine.rs:226`: "nodes don't necessarily replicate each
  other's data, this is especially true since the data-pool re-replication
  mechanism. We need to rely on the manifest to determine which peer to
  compare against. Actually, this should be the entry point of the
  algorithm, not the segments."
- **Scrub's distributed partition assumes each peer holds every segment.**
  `partition_for_current_nodes` (`scrub.rs:612-623`) partitions this
  node's local sealed-segment list across alive peers, and
  `ScrubGrpcService::assign_partition` (`scrub_service.rs:43-57`) merely
  acks without doing anything — distribution is scaffolding, not wired.
  Review `scrub.rs:601`: "this implementation assumes that each peer holds
  this node's segments, which will not be true with the replication
  introduced with the data-pools evolution. We need to brainstorm about
  that, maybe leverage the manifest?"
- **A manifest + replica-aware selection machinery already exists** from
  the healing epic: `ManifestRepairTargetSelector`
  (`oceanfs-node/src/repair.rs`) implements
  `RepairTargetSelector` for g5's target-pull (ADR-0030) using membership
  + NodeManifest, and the lifecycle registry's `storage_locations`
  carries the authoritative replica set per segment. The read path's g6
  routing already filters on `storage_locations` + manifest pool health.
- Since the Phase-A all-healthy manifest was observationally neutral, AE's
  random sampling and scrub's full-replication assumption have not yet
  caused observable divergence — the debt is latent but structural, and it
  will bite exactly when partial replication (data-pool death /
  re-replication, the in-flight healing epic) becomes common.

## Decision

### D1. Peer selection and scrub partitioning are derived from `storage_locations` + manifest, never from "all alive nodes"

- **Anti-entropy:** the unit of comparison becomes the *segment's replica
  set*. For a segment this node holds, candidate peers =
  `lifecycle.storage_locations(segment) − self`, filtered to Alive +
  Healthy-data-pool nodes via the routing/manifest cache. AE exchanges
  roots only with peers that actually hold the segment. If a segment has
  no peer holder (single local copy), it is excluded from remote
  exchange and covered by local scrub instead.
- **Scrub:** partitions are computed per-segment over its
  `storage_locations`, not over the node's whole local set broadcast to
  every alive peer. A peer scrubs only the segments it holds (its own
  partition), and `assign_partition` is either wired to actually execute a
  scrub of the assigned partition or removed until it can be (no silent
  acks).

### D2. The manifest is the entry point of the AE algorithm

`run_cycle` starts from "which segments do I hold, and who else holds
each?" (the registry + `storage_locations`), not "which peer do I pick,
then what do I send it?" The segment → holder mapping (registry) drives
peer selection, matching the reconciliation loop's existing holder-index
pattern (g4).

### D3. Selection is injected, not hard-coded

AE and scrub accept a `PeerSelector`/`PartitionPlanner` trait injected
from the node layer (which holds the manifest cache + membership), the
same shape as g5's `RepairTargetSelector`. `oceanfs-durability` does not
gain a new dependency on `oceanfs-routing`/membership internals; the node
wires the concrete selector at construction (ADR-0009/ADR-0005 pattern).

### Out of scope

- The Merkle protocol itself (ADR-0015), incremental tree, sampling, and
  gRPC `MerkleExchange` — unchanged.
- The scrub full-scan semantics (spec §7.5) — only the *distribution*
  mechanism changes.
- Choosing replacement replicas (that is g5's job) — this ADR only
  *selects comparison/scrub partners* from existing holders.

## Consequences

### Positive

- AE and scrub compare/scrub against the correct peer set under partial
  replication; no more meaningless exchanges against non-holders.
- Reuses the holder-index/manifest machinery the healing epic already
  built; no new protocol.
- Removes the "silent ack" scrub distribution scaffolding (either wired or
  deleted).

### Negative

- AE can no longer exchange against a random peer when it holds no shared
  segments with anyone; local-only segments rely on scrub. Detection
  coverage shifts from "random global sample" to "replica-set sample" —
  which is the correct coverage for a replication system, but changes the
  corruption-detection probability model (must be reflected in scrub
  cadence).
- Manifest staleness: selection is a hint; I/O errors must remain the
  truth (same discipline as ADR-0029 §D5).

### Neutral

- `oceanfs-durability` gains one injected trait; node wiring changes in
  c2 (durability builder).

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Status quo: random alive peers** | Simple; works under full replication | Wrong under partial replication; the review identified the latent failure; scrub distribution is unwired scaffolding | Rejected: the in-flight healing epic makes partial replication the common case |
| **Ring-based peer selection** (pick ring successors) | Ring is available | Ring maps keys→nodes, not segments→holders; AE compares segments, whose holders are in `storage_locations` — the ring would pick non-holders | Rejected: segment-granular holder set is the correct unit (matches g3/g4/g6) |
| **Delete distributed scrub until wired** | No dead scaffolding | Distributed scrub is spec §7.5 scope; deleting it defers a required feature | Rejected for the scrub-partition half; wiring it via per-segment holders is the resolution |
| **Full mesh comparison per segment (exchange with all holders always)** | Max coverage | Bandwidth grows with RF; defeats the AE cost model | Rejected: sampling + continuous modes (ADR-0015) stay, applied to holder peers |

## References

- Review comments: `anti_entropy/engine.rs:226` (+ :184, :199), `scrub.rs:601`
- ADR-0015 (AE protocol), ADR-0029 §D2/D5 (manifest, routing cache),
  ADR-0030 (re-replication target-pull; the selector precedent)
- Roadmap: `docs/features/refactoring/review-2026-09-roadmap.md` (Theme 5,
  wave 2 ④)
- In-flight: g5 `ManifestRepairTargetSelector` (`oceanfs-node/src/repair.rs`),
  g4 holder index (`oceanfs-durability/src/reconcile.rs`), g6 routing
  (`oceanfs-node/src/routing_cache.rs`)
