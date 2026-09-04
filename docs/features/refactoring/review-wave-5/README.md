---
feature: "Wave 5: Deferred Design ADRs"
epic: "refactoring/review-wave-5"
status: proposed
priority: low
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# Wave 5: Deferred Design ADRs

> Coordination doc for wave 5 of the 2026-09 review program: design
> discussions that are real but do NOT block the structure gate (wave 2)
> or the healing epics (wave 3). Each becomes an ADR + feature when picked
> up. Land in backlog order; nothing here is urgent.

## Items

### D1 — Seal strategy (review #89)
**Status:** audit written, ADR pending.
**Audit:** `docs/audits/2026-09-04-seal-on-zero-space-waste.md`.
**Decision needed:** seal-on-zero (current) vs seal-on-size-threshold vs
seal-on-full vs hybrid. Hinges on whether an idle-time bound is acceptable
despite the anti-timer principle, and whether pool-slot reads suffice for
long-open segments.

### D2 — Graceful-leave redesign (review #34)
**Status:** deferred.
**Decision needed:** with replicas + g4/g5, shutdown should stop pushing
TBs of data entirely (drain, flush WAL, mark Left; peers detect
under-replication). `NodeLeaveHandler` becomes obsolete. Coordinate with
c1 (which may already delete it) and g7/g8.

### D3 — Replicated lifecycle state (review #30)
**Status:** deferred to wave 3.
**Decision needed:** add an explicit "replicated" state to the lifecycle
machine (event-WAL fold) instead of using empty `storage_locations` as a
proxy; finish merging the compaction state machine. Land before/with g7
(the catch-up flow is a replication flow).

### D4 — Streaming read path (Theme 6, reviews #95/#98/#99)
**Status:** deferred.
**Decision needed:** end-to-end streaming from fetch → verify → response;
the read coordinator currently materializes the full object
(`MultiChunkAssembler` accumulates). Only urgent when large-object SLOs /
multi-part uploads land.

### D5 — Adaptive full-scan strategies (review `node.rs:8` header)
**Status:** deferred until the scheduler (wave 2 ③) exists.
**Decision needed:** threshold-based switching between full-scan and
sampling/round-robin for metadata-space background tasks. The
accounting-based bounded scans (ADR-0034 + `bounded-metadata-scans` epic,
wave 2 ⑥) are the prerequisite.

### D6 — Membership-state resilience (review `membership_state.rs:59`, #31/#42)
**Status:** deferred, small.
**Decision needed:** corrupt `membership_state.toml` currently aborts
startup; fallback = regenerate via gossip seed-pull; also relocate the file
off `{data_dir}` (ADR-0029 pools) per review #42.

### D7 — Bounded metadata scans ~~/ reverse index~~ — **RESOLVED (not wave-5)**
**Status:** moved to **wave 2 ⑥** — ADR-0034 (bounded metadata accounting,
**Accepted 2026-09-04**) + epic `refactoring/bounded-metadata-scans/`.
**Direction (from the ADR, which REJECTED the reverse-index CF):**
accounting-based liveness (live = total − dead) with supersede-capture on
overwrite, a seal-time per-segment contained-objects membership list, and
remap-carrying object keys. Do not re-open here.

## Acceptance bar (epic DoD)

- [ ] Each item above has either a written ADR or an explicit
      drop/defer decision with rationale.
- [ ] Wave 5 does not block waves 0–4.
