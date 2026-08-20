# ADR-0027: Hinted-Handoff Ownership Model — Stable Topology, Debt Ledger, Backstop Owner

**Status:** Accepted
**Date:** 2026-08-20
**Deciders:** User (architecture owner) + Implementer

**Related:** [ADR-0018 hinted handoff](../adr/0018-hinted-handoff.md),
[ADR-0022 rejoin](../adr/0022-rejoin-changed-address-incarnation-bump.md),
[ADR-0025 segment lifecycle](../adr/0025-segment-lifecycle-state-machine.md)

---

## Context

The phase-3 churn test (`e2e/tests/load_cluster_churn.rs`) repeatedly
exposed convergence holes in the replication / hinted-handoff path. The
failure signatures were all variations of one structural smell: **the
handoff subsystem was distributed without a clear owner** — the same
pattern that previously plagued the sealing pipeline (fixed by
ADR-0025's lifecycle coordinator + event WAL).

Concrete manifestations found and fixed during the 2026-08-20 campaign:

1. **The replica set had no owner.** The ring was rebuilt per node from
   that node's own gossip view, and death **removed** a node from the
   ring. A write coordinated during the dead window targeted the alive
   subset, met quorum with it, and the returning node was never hinted
   (the coordinator did not know it existed). The hint queue — the
   convergence debt ledger — was only as complete as the ring view that
   fed it.
2. **Quorum was silently degraded.** `min(write_quorum, ring_size)`
   acked quorum-1 writes (and deletes) on a stale 1-node ring view:
   one durable copy, no replication, no hints — permanent divergence.
3. **Deletes were un-owned.** The delete path replicated to dead
   replicas and merely warned on failure — no hint, no debt record.
   A node that missed a delete kept its stale row forever, and the
   sender-side obsolete pre-check then dropped later write hints for
   keys that were still live elsewhere.
4. **Repair paths were poisonous.** Read-repair wrote winning **remote
   metadata** (foreign chunk references) into local stores: unreadable
   locally, LWW-poisoning against legitimate hint applies, and able to
   regress newer versions. The anti-entropy backstop therefore had no
   safe write path — convergence was *emergent* from hints + LWW, with
   no accountable backstop.

## Decisions

### Decision 1: The topology is owned by config, not by liveness

The ring is the stable N-set. A dead node is **retained** in the
membership table (state=Dead, last-known address) and in the ring; only
a graceful `Left` removes it. Liveness is a **quorum concern** (ack
accounting), never a topology concern.

- The write/delete coordinators always attempt the full N-set. A dead
  member's failed attempt is exactly what becomes hint debt.
- Consumers that need liveness (e.g., the read path's chunk-fetch
  fallback, the forward path) filter by `state_of(...) == Alive` at the
  point of use — they do not mutate the topology.
- The F1d re-admission gate (ADR-0022) now also covers retained-Dead
  entries: equal/lower-incarnation gossip cannot revive a Dead node.

### Decision 2: The coordinating node owns each mutation's convergence

The coordinator of a write or delete is the **owner** of "this mutation
reached all N replicas". Its hint queue is the **durable debt ledger**
(ADR-0018's WAL):

- Complete: guaranteed by Decision 1 — every N-member is either acked
  or owed, and every failed replication attempt records debt (writes
  and deletes alike).
- Cancellable: the sender-side obsolete check is the **debt-cancellation
  rule** — a hint is dropped only when the sender's current state for
  the key is a newer mutation than the hint (the sender coordinated the
  mutation, so its view is authoritative for keys it owns).
- Bounded: hints are enqueued **only after quorum is met** — a failed
  (rolled-back) write must not leave debt for a version that never
  existed.

### Decision 3: Quorum is honest

An unsatisfiable quorum fails (`QuorumNotMet`) — no adaptive capping,
for writes and deletes alike. The client sees an error and retries; an
error is a retry signal, a degraded quorum is not. A quorum-failed
write is **rolled back** locally (fresh-HLC tombstone) so the
unacknowledged version leaves no trace and late hints cannot resurrect
it.

### Decision 4: The receiver's LWW apply is the acceptance gate

Every mutation applies with HLC-LWW against the receiver's local
metadata **and** tombstone: a stale hint/delete/push is discarded. All
delivery orderings (write-then-delete, delete-then-write, cross-sender)
resolve deterministically by timestamp. The gate is not an owner — it
is the acceptance rule that makes the owners' decisions safe.

### Decision 5: The healing service is the backstop owner

Anti-entropy (merkle comparison) is the accountable backstop for keys
whose debt was lost (e.g., pre-restart WAL gaps). Its repair must be
**data-bearing**: fetch the winning version's logical data through the
read path, verify it (size + BLAKE3), and apply it self-contained with
LWW. Metadata-only repair (pushing foreign chunk references) is
prohibited — the 2026-08-20 campaign removed the poisonous
metadata-only paths (read-repair local applies, ungated pushes) and
gated pushes with LWW. The data-bearing backstop repair is the planned
completion of this decision (tracked as a follow-up feature).

## Consequences

- **Invariant:** every mutation is either acknowledged on quorum or
  recorded as debt against every N-member; debt is repaid when the
  member returns; the backstop covers lost debt.
- The churn test went from 11 read-quorum failures per run to 1 (of
  ~108 keys) with convergence, handoff-settle, cache-invalidation, and
  ring-consistency assertions all green; a full outage's hint debt
  (~7000 hints) drains in ~15s (parallel fetch pass + full-queue drain).
- The residual ~1-key/run class (last write's hints to a tombstoned
  pair not materializing) is the target of the data-bearing backstop
  (Decision 5), rather than further patchwork in the delivery path.
