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

- [x] ADR-0028 decisions D1–D5 all implemented: dedicated port, real
      direct+indirect probes, origin-attributed merge rules, vector-based
      push-pull gossip, proxy path removed.
<!-- REVIEW: verified 2026-08-22 (iteration 2): D1 port/pool/announce + socket-opts listener (node.rs:1594-1620, plane.rs:39/69); D2 real direct+relay probes with hard deadlines (ping.rs:121-255, swim_probes.rs integration); D3 authority-class merge (membership/mod.rs:75 matches the AMENDED ADR table; per-cell tests pass); D4 watermarks + k-fanout push-pull + ack-carried pull with divergence healed via per-peer watermark deltas (gossip.rs:597, tested); D5 kept invariants + DK-007 proxy removed (no PingResponse emitted from push). Documented deviations: (a) D1's "fresh channel per probe" is implemented as a timeout-bounded pool acquisition (ping.rs:259 make_client) — hard ping_timeout bound preserved; (b) D4's explicit re-sync Pull RPC is superseded by push-side watermark healing, directly tested (diverged_peer_vector_is_healed_by_the_next_delta). -->
- [x] The seven heuristic guards are deleted or re-expressed as rules of
      the authority table (no `stale-suspect`/`self-downgrade`/`terminality`
      special cases in `merge_delta`/`upsert_node`).
- [ ] The fleet churn quick test (phase-3, 3 nodes) passes **3/3
      consecutive runs**: all 10 assertions, convergence true, 0 read-quorum
      failures, 0 missing keys, no suspect-stuck through the settle.
<!-- REVIEW: DEFERRED (user checkpoint gate) — fleet VMs not deployed; f6 not run. This is a deferred epic-gate item, NOT a code gap. Local half verified 2026-08-22: load_cluster_churn 1/1 (10/10 assertions, 204s) and all cluster_* e2e suites green. -->
- [ ] Probe latency is isolated: `probe_duration_microseconds` p99 stays
      under `ping_timeout_ms` while the data plane streams 16 MiB bodies
      (the old push p99 195 ms class is gone from the membership plane).
<!-- REVIEW: DEFERRED (user checkpoint gate) — the probe-p99-under-data-load capture is an f6 fleet measurement. The isolation is structural (separate listener + pool + socket opts, node.rs:1594-1620) but the p99 evidence remains unproven until f6 runs. -->
- [x] All existing membership unit suites stay green (72 → …), and the
      guard-port tests pass under the authority model.
- [x] Local churn field spotless (7/7) with the new plane before fleet
      deployment.
