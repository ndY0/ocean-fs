---
epic: "membership-plane"
status: proposed
priority: high
created: 2026-08-21
updated: 2026-08-21
---

# Membership Plane — Epic Plan

Epic: `membership-plane`
ADR: [ADR-0028](../adr/0028-membership-plane-full-swim-gossip.md)

## Goal

Replace the membership subsystem's incomplete protocol — bookkeeping
"SWIM" on top of full-state fanout-all gossip, shared with the data
plane — with a **complete, isolated implementation**: a dedicated
membership port hosting real SWIM probes (direct + relayed indirect) and
origin-attributed, watermark-based push-pull gossip. The seven heuristic
guards accumulated during the phase-3 churn campaign collapse into the
authority-class merge rules; the data plane's tail latency can no longer
pollute detection time.

## Feature DAG

```
f1 plane-proto-config
 └── f2 plane-server ──────────────┐
 │      └── f3 swim-probes ──┐     │
 │      └── f5 gossip-dissemination
 └── f3 swim-probes
      └── f4 suspicion-origin
 f4 + f5 ──→ f6 fleet-validation
```

Implementation order: **f1 → f2 → f3 → f4 → f5 → f6**. `f5` can start
once `f2` lands (parallel with `f3/f4`); `f4` needs `f3` (origin-bearing
detector events feed the merge rules). `f6` gates on everything.

| # | Feature | Touches | Depends on |
|---|---|---|---|
| f1 | `plane-proto-config` — wire types, config, membership pool | network, core, membership | — |
| f2 | `plane-server` — dedicated listener + services + deploy | node, membership, scripts | f1 |
| f3 | `swim-probes` — real probe cycle, relay forwarding, proxy removal | membership, network | f2 |
| f4 | `suspicion-origin` — attributed entries, authority merge rules | membership | f3 |
| f5 | `gossip-dissemination` — version vectors, watermarks, fanout | membership | f2 |
| f6 | `fleet-validation` — phase-3 churn field green on the new plane | scripts, e2e | f3, f4, f5 |

## Acceptance bar (epic DoD)

- [ ] ADR-0028 decisions D1–D5 all implemented: dedicated port, real
      direct+indirect probes, origin-attributed merge rules, vector-based
      push-pull gossip, proxy path removed.
- [ ] The seven heuristic guards are deleted or re-expressed as rules of
      the authority table (no `stale-suspect`/`self-downgrade`/`terminality`
      special cases in `merge_delta`/`upsert_node`).
- [ ] The fleet churn quick test (phase-3, 3 nodes) passes **3/3
      consecutive runs**: all 10 assertions, convergence true, 0 read-quorum
      failures, 0 missing keys, no suspect-stuck through the settle.
- [ ] Probe latency is isolated: `probe_duration_microseconds` p99 stays
      under `ping_timeout_ms` while the data plane streams 16 MiB bodies
      (the old push p99 195 ms class is gone from the membership plane).
- [ ] All existing membership unit suites stay green (72 → …), and the
      guard-port tests pass under the authority model.
- [ ] Local churn field spotless (7/7) with the new plane before fleet
      deployment.
