# ADR-0028: Dedicated Membership Plane — Full SWIM Probe + Gossip Dissemination

**Status:** Proposed
**Date:** 2026-08-21
**Deciders:** OceanFS architecture team

---

## Context

### The convergence saga

The phase-3 fleet churn campaign (3-node Hetzner fleet, ADR-0026) produced
seven consecutive fixes to the membership subsystem, all of them *guards on
the merge path* papering over protocol incompleteness:

| Fix | Guard added |
|---|---|
| `4d8c172` | stale SWIM suspicion reverted a fresh rejoin |
| `d3239d0` | a node must never accept Suspect/Dead for itself from gossip |
| `4afcf36` | successful pings recover gossip-applied Suspects |
| `766b260` | reject stale equal-incarnation Suspects over ping-verified Alive |
| `2ff5c77`/`47d103c`/`84cb999` | gossip metrics wiring + units (observability) |
| `f28c844` | torn-tail WAL replay (adjacent, not membership) |

The timing evidence (the decisive run) proved the suspicion itself is
legitimate: gossip pushes — the only liveness signal — are 28 ms p50 /
**195 ms p99** under churn, and they fail genuinely during kills. The
recovery path was the fragile 60% of the saga because the protocol has no
notion of *who observed what*.

### The protocol is incomplete (2026-08-21 audit of the code)

Reading `oceanfs-membership` end to end:

- **The SWIM "direct ping" never sends a message.** `on_ping_tick` selects
  a random alive peer and registers `pending_pings[target] = now`; the
  `ProbeHandler` only answers self-targeted in-process probes. The actual
  liveness signal is the *gossip push to that peer* — which happens to
  **all** peers every round with the **full** membership list.
- **The indirect ping is bookkeeping fiction.** `initiate_indirect_pings`
  selects relays and registers `pending_indirect[target] = (relay, now)` —
  no relay message exists anywhere in the codebase. The "indirect stage" is
  a second `ping_timeout_ms` delay before SUSPECT.
- **Suspicion has no origin.** A Suspect entry in a gossip delta is
  anonymous. Every guard in the saga (`self-downgrade`, `stale-suspect`,
  `terminality`) exists to compensate for missing attribution.
- **Dissemination is full-state fanout-all.** `build_delta()` returns the
  entire membership list every round; deltas are never pruned (no per-peer
  watermark); `Pull` is join-only; `last_known_version` is a fake
  incarnation filter; `GossipMessage.ring_version`/`hlc` are dead fields.
- The spec (§12.3) already defines `rpc Probe(ProbeRequest)` — it was never
  implemented. The 2025-08-05 distributed-systems audit flagged this as
  H1 ("SWIM remote pings are never sent"); DK-007 (gossip-push-as-ping-
  proxy) was adopted instead, but its described relay mechanism was never
  built either.

### Data-plane pollution

One tonic server on `grpc_listen_addr` (9001) hosts Segment (64 MiB decode
limit), Healing (64 MiB), Cache, Scrub, **and** Gossip; one
`ConnectionPool` is shared by everything. `get_channel` waits on a
semaphore behind whatever holds the per-peer channels — including 16 MiB
segment streams. Probe latency therefore inherits the data plane's tail
(the 195 ms p99 push). The user has decided: **isolate the protocol on its
own set of ports.**

### Forces

- **The data plane is busy and adversarial to latency.** 16 MiB replica
  bodies, hinted-handoff batches, healing transfers, scrub scans. Liveness
  probing needs a tiny, fast, bounded message that must not queue behind
  any of it.
- **SWIM's guarantees are only real if the ping is real.** Direct probe →
  k-relay indirect probe → SUSPECT → DEAD is the protocol; a timeout chain
  bound to actual messages is what makes detection time bounded.
- **The state machine needs attribution, not heuristics.** The seven guards
  are correct behavior expressed as exceptions. Origin-attributed facts
  express the same behavior as rules.
- **Incarnation semantics are correct and must be kept.** T8 monotonicity,
  ADR-0022 rejoin (persisted + 1, address update), F1d re-admission gating,
  Dead-retention in the ring (stable N-set topology) — all principled.
- **Open trust model.** No authentication between nodes; a decision must
  not make identity spoofing easier than it already is.
- **The fleet is small (3–10 nodes).** Protocol completeness matters more
  than asymptotic fanout; we still choose bounded fanout because it is part
  of the protocol, not because O(N) is a problem at N=3.

## Decision

**OceanFS splits the membership plane onto its own port and implements the
full SWIM + gossip protocol with origin-attributed state.**

### D1. Dedicated membership plane

- New `NodeConfig::membership_listen_addr` (default `0.0.0.0:9002`): a
  second tonic server hosting **only** `GossipRpc` and the new `ProbeRpc`.
  The data-plane server (9001) keeps Segment / Healing / Cache / Scrub and
  drops Gossip.
- A **separate `ConnectionPool`** for the membership plane: small per-peer
  pool (2), connect timeout = `ping_timeout_ms`. Probes additionally use a
  fresh channel with a hard per-call deadline rather than waiting on the
  pool semaphore.
- The announced membership address derives from `membership_listen_addr`
  with the same `0.0.0.0 → advertise-IP` substitution the deploy scripts
  already apply to the gRPC address.
- Deployment (ADR-0026 fleet templates, `sut-deploy.sh`, firewall) opens
  the new port; `scripts/observe.sh` and the harness resolve it.

Rationale: a second port gives independent connections, independent
backpressure, independent TLS and message-size limits. gRPC/HTTP2
multiplexing would share one TCP connection; a 16 MiB stream's flow-control
state and the pool semaphore couple ping latency to data-plane behavior.
A separate port is the minimal isolation that makes the protocol's timeouts
mean what they say.

### D2. Real SWIM probes

Wire (spec §12.3 shape, `ProbeRequest`/`ProbeResponse` already exist in
`membership.proto`):

```protobuf
rpc Probe(ProbeRequest) returns (ProbeResponse); // SWIM direct/indirect ping
```

Probe cycle, one target per interval:

1. Pick a random alive peer (not self, not already pending).
2. **Direct probe**: `Probe{origin: self, target, is_indirect: false}` with
   a hard deadline of `ping_timeout_ms`. Ack carries the target's
   incarnation.
3. On timeout: pick `k = indirect_ping_count` alive relays (≠ self, ≠
   target); send `Probe{origin: self, target, is_indirect: true}` to each.
   The relay **forwards a direct probe to the target and relays the ack
   back to the origin** (target == self at the relay → ack directly).
4. Any ack → alive (recover). All relays failed or timed out →
   SUSPECT (origin = self, suspicion timer started).
5. Suspicion timer expiry without recovery → DEAD (origin = self), unless
   the target re-announced at a higher incarnation in the meantime
   (stale-suspicion cancellation stays, `4d8c172`).

The gossip-push-as-ping-proxy (DK-007) is **removed**: the detector stops
consuming `PingResponse` from push results; gossip stops emitting them.

### D3. Origin-attributed state — the merge rules

Membership entries become attributed facts:

```
MemberEntry = (node_id, state, incarnation, version, address, origin)
```

- `incarnation` — node-authoritative, bumped **only by the node itself**
  at rejoin (`persisted + 1`, ADR-0022). Governs re-admission (F1d).
- `version` — per-`(node_id, origin)` monotonic counter; every state change
  by that observer bumps it.
- `origin` — the node that last observed/changed the state (self for
  announcements; the detector node for Suspect/Dead; the leave handler for
  Leaving/Left).

**Merge of incoming entry E vs local entry L about node X:**

1. `E.incarnation > L.incarnation` → accept (authoritative rejoin;
   state + address update, ADR-0022).
2. `E.incarnation < L.incarnation` → reject (stale, T8).
3. Equal incarnation → compare **authority class of `E.origin` for X**:

   | Class | Origin | Wins against |
   |---|---|---|
   | 4 | the target itself, for Left/Leaving (the leaver's terminal claim) | everything at equal incarnation |
   | 3 | my own detector / my own local observations | class 2, 1 |
   | 2 | another member's detector facts (Suspect/Dead/recovery) | class 1 |
   | 1 | the target's own Alive announcement (replayable history) | nothing (idempotent within same origin+version) |
   | 0 | entries about SELF from another origin | rejected outright (self-liveness authority) |

   Within the same class, higher `version` wins; equal version is
   idempotent (dropped); the same class from a different origin keeps
   the local entry (no cross-origin churn — my own detector is the
   authority to move the state forward).

   Note (implementation clarification, 2026-08): the target's own Alive
   announcement is class **1**, not the top — a node's announcement
   records that it WAS alive at announce time; liveness is a present-
   time fact only my own probes establish. Class 4 (the leaver's own
   Left/Leaving) is what an earlier draft's "self-announcement wins
   over everything" was really about: terminal leave claims must beat
   stale detector facts. This ordering preserves every documented
   outcome — remote suspicion applies over announcements (class 2 >
   1), my ping-verified Alive beats remote Suspect (class 3 > 2), and
   rejoin authority is governed by the incarnation gate, not the class
   table.

**Rules that fall out of the table:**

- **Self-liveness authority** (replaces the `d3239d0` guard): a
  self-announcement (class 3) always beats any peer's Suspect/Dead at the
  same incarnation. A node never needs a special-case guard — the class
  ordering is the rule.
- **Ping-verified Alive beats remote Suspect** (replaces the `766b260`
  guard): my detector's Alive (class 2) beats another detector's Suspect
  (class 1) at the same incarnation.
- **Remote suspicion is first-class** (replaces the terminality ordering):
  a peer's Suspect about X *is* applied when I have no newer fact of a
  higher class — with its origin recorded. Suspicion dissemination works
  as designed instead of being fought by guards.
- **DEAD is detector-local**: a peer's Dead at the same incarnation is
  applied (class 1 vs my class 2 Alive → my Alive wins; vs nothing → the
  peer's Dead applies). The rejoin case is governed by incarnation (rule 1
  and the stale-suspicion cancellation). The Dead↔Alive oscillation loop
  (t24) is closed by construction: an echo of my own fact is class 0 and
  idempotent.
- The F1d re-admission gate, Dead-retention in the ring, and the
  `merge_delta`/`upsert_node` structural guards remain, now expressed
  against attributed entries instead of bare state.

### D4. Complete gossip dissemination

- **Version vectors**: every node keeps `Vector = map<NodeId, version>`
  of the highest per-node version it has applied. Deltas are computed as
  `entries with version > watermark[node]`.
- **Push-pull in one round trip**: the round selects
  `k = min(fanout_k, alive-1)` random alive peers (default `fanout_k = 3`);
  sends `Push(delta, my vector)`; the response `GossipAck` is extended to
  carry the peer's delta (entries newer than my vector) and the peer's
  vector. Both sides merge and advance watermarks. O(k) messages per
  round, O(delta) bytes.
- **Pull** remains for join (empty vector → full list) and for an explicit
  re-sync when a peer's vector is missing an entry the local node has
  (healing divergence). `GossipPullRequest.last_known_version` is replaced
  by a version vector.
- `GossipMessage.ring_version`/`hlc` dead fields are removed from the
  wire; `MembershipEntry` gains `version` and `origin`.
- **Join** (unchanged sequence, §13.1) now runs over the membership plane:
  pull full list → announce self (Alive, incarnation, version++, origin =
  self).

### D5. What is kept unchanged

- Incarnation monotonicity (T8), ADR-0022 rejoin semantics, F1d
  re-admission gating, Dead-retention in the ring, self-rejoin.
- `suspicion_timeout_ms`, `failure_timeout_ms`, `indirect_ping_count`,
  `gossip_interval_ms` config keys and their defaults (spec §13.3).
- Membership events, ring updates, the event-handler task, graceful
  leave/Left flow.
- The existing `GossipConfig` shape; `membership_listen_addr` is added to
  `NodeConfig`.

## Consequences

### Positive

- **Bounded, real detection**: the timeout chain is bound to actual probe
  messages on an isolated plane; detection time no longer depends on the
  data plane's tail.
- **Principled state machine**: seven heuristics collapse into the
  authority-class table; the oscillation classes (t24, the fleet Suspect
  loop, the self-downgrade loop) are closed by construction.
- **Bounded dissemination**: delta-sized messages, k-fanout, ack-carried
  pull; convergence in O(log N) rounds as the protocol intends.
- **The proxy fiction dies**: DK-007's described-but-unbuilt relay
  mechanism is replaced by the actual Probe RPC the spec already lists.

### Negative

- Second listener/port to configure, firewall, and provision (deploy
  scripts, fleet templates, harness).
- Two pools to tune; the announce-address derivation must use the
  membership address.
- The merge rules are a behavioral change: the heuristics' tests must be
  re-expressed against the authority model (they should mostly pass
  unchanged — the rules were chosen to preserve their outcomes).

### Migration

The new plane is additive at the config level (`membership_listen_addr`
defaults to 9002). The gossip/probe traffic moves over in the same release;
the data-plane server drops the Gossip service. No rolling upgrade concern
exists (fleet deploys are all-at-once).

## References

- Spec §12.3 (NodeRPC incl. `Probe`), §13 (join, leave, failure detection
  parameters), §14.1 (node config).
- ADR-0002 (SWIM + consistent hashing; DK-007 proxy design, superseded for
  probing by this ADR).
- ADR-0022 (rejoin with changed address — incarnation bump).
- Audit 2025-08-05 (H1: remote pings never sent).
- Fleet findings: `4d8c172`, `d3239d0`, `4afcf36`, `766b260` and the
  timing-metrics run (`84cb999`).
