---
feature: "Membership Plane: Fleet Validation"
epic: "membership-plane"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: membership-plane
    feature: swim-probes
  - epic: membership-plane
    feature: suspicion-origin
  - epic: membership-plane
    feature: gossip-dissemination
adr: [0028]
perf: []
created: 2026-08-21
updated: 2026-08-21
---

# Membership Plane: Fleet Validation

## Summary

The acceptance gate for the epic: re-run the phase-3 fleet churn campaign
against the new membership plane and prove the class of failures that
spawned the seven heuristics is gone — plus the plane's isolation
property (probe latency unaffected by data-plane load). Also lands the
two test-side fixes identified during the campaign (handoff-delta
counter-reset tolerance; verify settle decision).

## Scope

### In Scope

- Fleet deploy: provision/update the 3-node fleet with the new binary and
  config (9002 open, membership advertise address), harness updated.
- 3 consecutive churn quick runs: all 10 assertions pass, convergence
  true, 0 missing keys, 0 read-quorum failures, no suspect-stuck through
  the settle.
- Isolation proof: while the churn drives 16 MiB data streams,
  `probe_duration_microseconds` p99 < `ping_timeout_ms` (the old
  195 ms push-p99 class gone); capture the metrics via the existing
  federation.
- Test-side fixes (from the campaign):
  - `hinted_handoff_delivery` assertion tolerates counter resets from
    killed nodes (re-base on surviving nodes).
  - Settle/verify grace decision recorded in the harness (config knob,
    default aligned with measured hint-delivery latency).
- Metrics review: `probe_*` + gossip series registered and visible.

### Out of Scope

- Disk-full write-path handling (backlog, unchanged).
- SUT clock drift NTP fix (tracked separately, applied opportunistically).

## Crate Impact

| Crate | Change |
|---|---|
| `e2e/src/harness.rs` | handoff-delta re-base; settle knob |
| `e2e/tests/load_cluster_churn.rs` | assertion tolerance |
| `scripts/*` | fleet deploy with the plane config |

## Interface (Public API)

- `config_cluster_churn` gains `settle_grace_ms` (default documented from
  the measured delivery latency).

## Data Flow

```
build → deploy fleet (9001 data + 9002 membership) → churn runs ×3
     → report assertions + probe-p99 + convergence → gate
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` passes (e2e included)
- [ ] **Tests:** handoff-delta re-base unit test; settle knob honored
- [ ] **Docs:** `# Examples`; missing-docs deny passes
- [ ] **ADR:** ADR-0028's acceptance properties demonstrated (D1
      isolation, D2 detection bounds, D3 no oscillation, D4 convergence)
- [ ] **Perf:** probe p99 under `ping_timeout_ms` during churn
- [ ] **Integration:** 3/3 consecutive churn quick runs fully green on
      the fleet; local churn field spotless before the fleet deploy

<!-- REVIEW: f6 is DEFERRED by the user's checkpoint gate — fleet VMs not
      provisioned, so no fleet deploy, no settle_grace_ms knob in
      e2e/src/harness.rs, no handoff-delta counter-reset re-base in
      load_cluster_churn.rs, no probe-p99 capture, no 3/3 churn runs.
      This is a deferred epic-gate item, NOT a code gap. Local half of
      the Integration bullet is verified (2026-08-22): load_cluster_churn
      1/1 + all cluster_* suites green. -->
