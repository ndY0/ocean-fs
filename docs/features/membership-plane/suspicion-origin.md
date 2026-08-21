---
feature: "Membership Plane: Origin-Attributed State + Authority Merge Rules"
epic: "membership-plane"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: membership-plane
    feature: swim-probes
adr: [0028]
perf: [1.3, 7.1, 6.4]
created: 2026-08-21
updated: 2026-08-21
---

# Membership Plane: Origin-Attributed State + Authority Merge Rules

## Summary

ADR-0028 D3: membership entries become attributed facts
`(node_id, state, incarnation, version, address, origin)` and the merge
uses the authority-class table. The seven heuristic guards accumulated in
the phase-3 campaign are deleted; their outcomes are preserved by the
rules.

## Scope

### In Scope

- `NodeEntry`/`GossipDelta` gain `version: u64` and `origin: NodeId`;
  wire mapping from f1's proto fields.
- Detector events carry `origin = detector.node_id` and bump the
  per-`(node, origin)` version; self-announcements carry `origin = self`.
- `GossipProtocol::merge_delta` + `Membership::upsert_node` merge by the
  authority table (ADR-0028 D3):
  1. incarnation: higher wins (rejoin, ADR-0022); lower rejected (T8);
  2. equal incarnation: authority class of origin — self (3) >
     local detector (2) > other member's detector (1) > echo (0);
  3. within a class: higher version wins; equal → idempotent.
- **Deleted heuristics** (each replaced by a table rule):
  - terminality ordering (`Dead > Left > Leaving > Suspect > Alive`);
  - stale-Suspect-over-ping-verified-Alive (`766b260`);
  - self-downgrade guard (`d3239d0`);
  - stale-downgrade-below-recorded guard (part of `4d8c172`).
- **Kept**: F1d re-admission gating (incarnation), Dead-retention in the
  ring, stale-suspicion timer cancellation (re-announced higher
  incarnation), ADR-0022 address update.
- Port every guard test to the authority model; add oscillation regression
  tests (t24 class, the fleet Suspect-loop class, the self-downgrade
  loop class).

### Out of Scope

- Detector probe mechanics (f3).
- Dissemination vectors/watermarks (f5) — version exists as an entry field
  here; f5 computes deltas from it.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-membership` | `membership/state.rs` (entry model), `gossip.rs` (merge), `membership/manager.rs` (upsert), `failure_detector/*` (event origin/version) |
| `oceanfs-core` | `proto_convert` for new fields (if any) |

## Interface (Public API)

- `NodeEntry { node_id, incarnation, version, state, origin, address }`
- `MembershipEvent { node_id, old_state, new_state, incarnation, version, origin, address }`
  (event consumers updated; the node/startup layer only reads state).

## Data Flow

```
detector probe result → event(origin=self, version++) → upsert_node
peer delta entry      → merge_delta: class table → upsert_node
self announce         → event(origin=self, version++) → always wins at equal inc
echo of my own fact   → class 0 → idempotent (oscillation closed)
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` passes; the four deleted
      guards are absent from the merge paths (grep-verifiable)
- [ ] **Tests:** one test per table cell (self beats peer Suspect; local
      detector beats remote Suspect; remote Suspect applies when no newer
      local fact; equal-version echo idempotent; rejoin at higher
      incarnation beats pending Dead; stale timer cancellation); the
      pre-existing guard tests pass unchanged or re-expressed
- [ ] **Docs:** `# Examples`; missing-docs deny passes
- [ ] **ADR:** ADR-0028 D3 satisfied; D5 (kept invariants) verified
- [ ] **Perf:** 1.3 (pre-sized merge loops), 7.1 (short write-lock
      windows), 6.4 (class compare as integer, no dynamic dispatch)
- [ ] **Integration:** local churn field 7/7 with the authority merge;
      the t24/fleet regression scenarios (kill + rejoin + stale-gossip
      replay) converge without oscillation
