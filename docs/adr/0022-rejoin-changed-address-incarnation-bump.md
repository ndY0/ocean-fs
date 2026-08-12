# ADR-0022: Rejoin with Changed Address — Incarnation Bump on Restart

**Status:** Proposed
**Date:** 2026-08-12
**Deciders:** OceanFS architecture team

---

## Context

Membership state maps `NodeId → (NodeState, Incarnation, SocketAddr)` and
disseminates it via gossip (spec §13, ADR-0002). The spec's join sequence
(§13.1) is: contact a seed → receive membership → announce self with
`Incarnation=1`. Nothing in the spec or the current implementation covers
what happens when a node **restarts with a different network address**: the
gossip merge does not update the address of a member that already exists
locally, and the restarting node re-announces with `Incarnation=1`, which is
rejected as stale by peers that hold a higher incarnation for it.

The 2026-08-12 e2e debug session proved this is not hypothetical:

- **T21 (hinted handoff delivery) — deterministic failure.** The restarted
  node-2's gRPC port changed (`32987 → 33547`) because the harness's
  port-preservation fallback assigned a new ephemeral port. Node-0 accepted
  the self-announcement but kept routing to the stale address; hint delivery
  failed with `forward failed to 127.0.0.1:32987: Connection refused` and the
  object stayed unreachable (404).
- **T43 (crash recovery rejoin) — deterministic failure.** Restarted node-0's
  gRPC port changed (`42455 → 42409`); node-0 has no seed nodes (it is the
  bootstrap node), so it started an empty cluster while nodes 1–2 kept
  dialing the dead address. Convergence never happened (`reports 1 nodes`).

The real-world analogues are DHCP address churn, container rescheduling, and
NAT re-mapping. Dynamic addressing is the norm for the environments OceanFS
targets (ADR-0019 two-VM test topology, cloud VMs), so address stability
cannot be an operational assumption.

### Forces

- **Node identity must survive restarts.** Segments are placed by ring
  position derived from `NodeId`; a new id per restart would orphan data and
  trigger full re-replication.
- **Incarnation monotonicity is already enforced.** T8 asserts that a node's
  incarnation never decreases; SWIM's standard recovery rule is that a node
  may resurrect itself by announcing a *higher* incarnation (Serf,
  Cassandra gossip do exactly this).
- **Restart is currently indistinguishable from a first join.** The
  implementation has no durable notion of "I have been here before".
- **The trust model is open gossip.** There is no authentication; any peer
  can fabricate announcements. A decision must not make identity hijacking
  any easier than it already is.
- **The bootstrap node is special.** It has no seed nodes, so after a
  restart it cannot re-contact the cluster by itself unless it remembers
  peers it has seen.

## Decision

**A node that has seen the cluster before must rejoin as the same identity
with a bumped incarnation and its current address; peers must accept a
strictly-higher incarnation as an authoritative update to both state and
address.**

Concretely:

1. **Persist the incarnation.** Each node persists its last-used incarnation
   in local durable state (RocksDB, alongside WAL metadata). On every start
   it announces with `incarnation = persisted + 1`; a first boot (no
   persisted value) announces with `Incarnation=1` per spec §13.1.

2. **Merge rule.** The gossip merge accepts an `AddNode` carrying
   `incarnation > local_incarnation` as an update to **both** state and
   address, from any source. Equal or lower incarnation is rejected (today's
   behavior, kept for T8). Because a third party cannot legitimately know
   another node's next incarnation, this makes address updates and
   DEAD→ALIVE resurrection effectively self-serve — the node itself is the
   only party that normally produces a higher incarnation for its own id.

3. **Rejoin path for seedless restarts.** Nodes persist their last-known
   membership addresses and re-contact them as fallback seeds on startup
   when the configured `seed_nodes` are unreachable or empty. This covers
   the bootstrap-node case (T43) without changing the bootstrap flow itself.

4. **No address caching at call sites.** Hinted handoff, delete forwarding,
   and write replication already resolve `membership.address_of()` at send
   time (`accessors.rs:39`). They keep doing so — once the membership entry
   is updated, every path converges automatically. No per-call-site changes.

Out of scope: authentication of announcements (pre-existing open-gossip
trust model), graceful-leave address handling (a LEAVING node that comes
back is the same rejoin case), and any change to ring placement.

## Consequences

### Positive

- Fixes the deterministic T21 and T43 failures and makes membership robust
  to address churn in general (DHCP, containers, VM reprovisioning).
- Reuses existing machinery (incarnation monotonicity, `address_of`), so
  the blast radius is small: one merge rule + one persisted counter + a
  fallback-seed list.
- Enables restart loops without operator intervention — a node that
  crashes repeatedly keeps re-joining with ever-higher incarnations.

### Negative

- A malicious peer can fabricate a high incarnation for another node and
  hijack its identity + address. Accepted risk — identical to the existing
  unauthenticated gossip trust model, now documented explicitly.
- Every start now performs one small durable write (incarnation bump).
- Restart storms increment incarnations rapidly; harmless numerically but
  adds log/state churn.

### Neutral

- T8 (incarnation monotonicity) semantics are unchanged.
- Operators may occasionally see a node's incarnation jump by more than 1
  after an unclean shutdown window — expected, not an error.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Static addressing as an operational requirement** — reject address updates; ops must guarantee stable addresses | No merge changes; simplest membership logic | Breaks the e2e harness (ephemeral ports), the ADR-0019 VM topology, and every dynamic environment; the debug session shows it fails today | Pushes a distributed-systems problem onto operators; contradicts the target environments |
| **Address change ⇒ new node id** — a restart on a new address joins as a fresh member | No incarnation/address-merge semantics needed | Orphans all segments of the old id; triggers full re-replication and orphan-reaper churn; diverges from spec §13.1 identity expectations | Node identity must survive restarts; too costly |
| **Explicit `generation` field** — separate restart generation from SWIM incarnation, peers keep `(id, generation, address)` history | Cleaner semantics; can distinguish "restart" from "liveness flap" | New state dimension across gossip, merge, and every membership query; redundant with incarnation for v0.2 | Deferred; revisit at v0.3 if incarnation alone proves insufficient |

## References

- Spec §13.1 (Join), §13.3 (Failure Detection)
- ADR-0002 (SWIM + consistent hashing), ADR-0019 (test harness topology)
- `oceanfs-membership/src/membership/accessors.rs:39` (`address_of`)
- `oceanfs-server/src/write/coordinator.rs:681` (`WriteCoordinator::delete`)
- `oceanfs-server/src/s3_handler/handlers.rs:456` (`delete_object`)
- E2E evidence (2026-08-12 debug session): `e2e/target/e2e-logs/.tmpN7VHKA-*`
  (T21, stale-address hint delivery), `e2e/target/e2e-logs/.tmpsDHLXJ-*`
  (T43, seedless rejoin), `e2e/tests/cluster_lifecycle.rs:176`
- Prior art: HashiCorp Serf (self-resurrection via incarnation), Cassandra
  gossip (higher-incarnation supersedes state)
