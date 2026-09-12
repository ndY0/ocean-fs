---
feature: "Hint-Debt Hardening & Segment Unrecoverable Signal"
epic: "metadata-anti-entropy"
status: done
priority: high
owner: ""
dependencies:
  - feature: fleet-degradation/f5-degraded-pool-semantics
    reason: The TTL-expiry escalation routes expired debt through f5 D3's HintDropSink (repair-intent conversion); f5 (done) is the substrate, and this feature only extends the routing
  - feature: fleet-degradation/f0-hints-durability-gate
    reason: f0 owns the hint-admission contract; S1 hardens retention/expiry observability without changing the delivery contract (ADR-0027 as amended) or the gate
adr:
  - 0038-metadata-change-journal
  - 0027-hinted-handoff-ownership-model
  - 0028-membership-plane-full-swim-gossip
  - 0029-storage-pools-disk-resilience
  - 0030-re-replication-target-pull
  - 0034-bounded-metadata-accounting
  - 0035-replicated-segment-lifecycle-state
perf:
  - "11.1 atomic counters for the pending-debt and unrecoverable metrics (no lock added to the hint/repair paths)"
  - "1.3 pre-size per-target debt bookkeeping collections with known bounded capacity"
created: 2026-09-12
updated: 2026-09-12
---

# Hint-Debt Hardening & Segment Unrecoverable Signal

## Summary

Harden the two remaining **silent-loss paths** in the index/repair machinery.
They are complements of ADR-0038's change edge and are **independent of the
journal** — they can land before S2:

1. **Hint debt.** Retention follows the membership topology instead of only a
   blind TTL (a retained-Dead target keeps its debt and can replay it when it
   returns); TTL expiry routes expired debt through the existing f5 D3
   `HintDropSink` escalation exactly like give-up, instead of deleting it
   silently; a `hinted_handoff_pending_debt{target}` gauge makes outstanding
   debt observable; hint mirroring stays rejected (it multiplies debt and
   does not converge rows).
2. **Segment-plane unrecoverable signal.** A repair request whose holder set
   has **no live recorded holder** (`storage_locations ∩ live = ∅`) becomes
   terminal after **N consecutive sweeps** instead of parking/retrying
   forever: a bounded dedupe marker stops re-enqueueing, a
   `oceanfs_repair_unrecoverable_total{reason="no_live_holder"}` counter
   surfaces it, and a recorded holder returning clears the marker and resumes
   normal repair.

Both live in `oceanfs-durability` (`hinted_handoff/*`, `repair.rs`), with
metric registration in `oceanfs-core` and at most a small config knob. There
is **no new persisted surface**, no hot-path fsync, and no dependency on
ADR-0038's journal or worker.

## Ratified decisions (encoded, not reopened)

- **Debt hardening (ADR-0038 D7).** Membership-driven retention; TTL expiry
  fires the same escalation as give-up; pending-debt gauge; no mirroring.
- **Unrecoverable signal (ADR-0038 D7).** Terminal classification when no
  live recorded holder remains (after N sweeps, per the ratified hardening);
  metric; bounded stop; clear/resume on return. EC-reconstructible segments
  remain the EC/heal plane's responsibility — this signal is the row/segment
  planes' dead end, not a suppression of EC work.

## Root Causes & Evidence

Grounding is carried from ADR-0038 §Context / D7 (verified 2026-09-12).

### R1 — Hint debt can die silently

- Hint WALs are per-target files, fsync per hint, erased on delivery; give-up
  at 10 attempts; TTL `hint_ttl_sec = 604800` (7 d)
  (`crates/oceanfs-core/src/config/node.rs:668-682,779-782`).
- `prune_expired` (`crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs:298-330`)
  **deletes expired debt with no escalation**, unlike give-up which routes
  through the f5 D3 `HintDropSink`
  (`crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs:271-274`,
  repair-intent conversion). A Dead-but-retained target's debt is therefore
  lost on the clock even though the membership topology still retains the
  node (ADR-0027 D1 / ADR-0028).
- There is **no pending-debt gauge**: `hints_expired_total` alone cannot show
  outstanding debt.

### R2 — Segment repair has no terminal state

- Targetless re-replication requests retry and park; retry exhaustion is a
  log line only (`crates/oceanfs-durability/src/repair.rs:332-363`). When
  `storage_locations` (ADR-0035 replicated lifecycle state) has no live
  holder for a segment, the request re-enqueues forever with no terminal
  classification and no counter.

## Scope

### In Scope

- **Membership-driven hint retention.** Hint debt for a target that remains
  in the ring (Alive/Suspect/**retained Dead** — ADR-0027 D1 / ADR-0028) is
  held while the target remains in the topology; the hard TTL remains a cap
  but is no longer the only fate. Debt for a `Left`/removed target escalates
  through the existing `HintDropSink` path (same as give-up), never a silent
  delete.
- **TTL-expiry escalation.** `prune_expired` routes expired debt through the
  f5 D3 `HintDropSink` repair-intent conversion (reusing the give-up path)
  before removal; no behavior change to delivered hints or the delivery
  contract.
- **Pending-debt gauge.** `hinted_handoff_pending_debt{target}` — outstanding
  debt count/bytes, sourced from the existing per-target pending bookkeeping;
  registered unconditionally (it is not journal-gated).
- **No hint mirroring.** Hints stay node-local; the existing rejection of WAL
  replication/mirroring is documented in the feature and preserved (no new
  code path).
- **Segment unrecoverable signal.** In the repair path: detect
  `storage_locations ∩ live = ∅` for a repair request; after **N consecutive
  sweeps** (`N` configurable or a documented constant — implementer OQ)
  classify terminal; record the segment in a **bounded dedupe set** so it is
  not re-enqueued; emit
  `oceanfs_repair_unrecoverable_total{reason="no_live_holder"}`; log once at
  classification; clear the marker and resume normal repair when a recorded
  holder returns.
- Tests (unit + in-process integration) for both halves.
- Docs for the new metrics and the retention/escalation rules.

### Out of Scope (for this feature)

- The journal, sync worker, watermarks, point fetch, LWW apply — S2
  ([ae2](ae2-metadata-change-journal.md)).
- Bootstrap, range iterator, read-trigger — S3.
- Spot-check/HMAC — S4.
- Changing the hint delivery contract (ADR-0027 as amended) or f0's gate.
- Segment bytes repair mechanics (ADR-0030), reconcile holder-set repair, EC
  reconstruction — unchanged.
- Hint mirroring/WAL replication — explicitly rejected, not built.
- Any new RocksDB CF or persisted surface; any hot-path fsync; C2b.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `hinted_handoff/hint_wal.rs` (retention + expiry escalation routing), `hinted_handoff/hint_delivery.rs` (reuse the existing `HintDropSink` conversion), `hinted_handoff/mod.rs` (pending-debt bookkeeping/gauge feed); `repair.rs` (no-live-holder detection, M-sweep terminal classification, bounded dedupe set, clear/resume). |
| `oceanfs-core` | Metric registration/constants for the two series; if `N` is configurable, a small `[durability]`/dedicated knob with serde default + validation. |
| `oceanfs-server` | Only if the unrecoverable set is surfaced on an existing admin/status surface; otherwise metric-only (implementer OQ). No new route is required by this feature. |
| `oceanfs-node` | None expected; the repair/hint loops are already wired. |
| `fleet-degradation f4` | No change — Pass A can observe the pending gauge (S1 lands before the fleet resumes, per the epic). |

## Interface (Public API)

### Metrics (exact names from ADR-0038 D9)

| Metric | Type | Meaning |
|---|---|---|
| `hinted_handoff_pending_debt{target}` | gauge | Outstanding hint debt (count/bytes; exact series shape — one gauge with a unit label vs two — is an implementer OQ, name fixed) |
| `oceanfs_repair_unrecoverable_total{reason}` | counter | Terminal no-live-holder repair classifications (`reason="no_live_holder"`) |

These register unconditionally (independent of `[metadata_sync] enabled`).

### Config (only if `N` is made configurable — OQ)

A single bounded sweep count with a serde default; validation rejects `0`.
Exact section/key naming is an implementer OQ; if no operator value is
justified, a documented constant is acceptable.

### Admin / observability (OQ)

The counter plus, if an existing repair/status surface can carry it, the
current unrecoverable set (segment ids + detail). Minimum requirement: the
metric exists and classification logs once per segment.

## Data Flow

```
hint enqueue → per-target hint WAL (unchanged)
  target Alive/Suspect/retained-Dead → debt HELD (membership-driven retention)
                                        → hinted_handoff_pending_debt{target}
  target Left/removed                → escalate via HintDropSink (repair intent)
  TTL expiry (cap)                   → same escalation path (never silent delete)

repair request (segment S)
  holder set = storage_locations(S)
  live holders = holder set ∩ live membership
    non-empty → normal retry/enqueue (unchanged)
    empty     → consecutive-miss counter++
                counter ≥ N sweeps?
                  no  → keep parked (bounded, counted)
                  yes → terminal marker in bounded dedupe set
                        stop re-enqueue
                        oceanfs_repair_unrecoverable_total{reason="no_live_holder"}++
                        log once
  recorded holder returns → marker cleared → normal repair resumes
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in the affected crates
      (`oceanfs-durability`, `oceanfs-core`, and `oceanfs-server` only if an
      admin surface is touched); no test-only hooks.
- [x] **Code (retention/escalation):** hint debt for a retained-Dead target
      is held while the target remains in the topology; `Left`/removed debt
      and TTL-expired debt route through the existing f5 D3 `HintDropSink`
      (repair-intent conversion) before removal; delivered hints and the
      delivery contract are unchanged.
- [x] **Code (gauge):** `hinted_handoff_pending_debt{target}` reflects
      outstanding debt (count and bytes), decreases on delivery/escalation,
      and is registered independently of the journal kill-switch.
- [x] **Code (terminal signal):** a repair whose `storage_locations ∩ live =
      ∅` becomes terminal after N consecutive sweeps; the bounded dedupe set
      prevents re-enqueue; the counter increments once per classification;
      the log fires once; a returning recorded holder clears the marker and
      normal repair resumes.
- [x] **Tests (unit):** retention vs `Dead`(retained)/`Left`; TTL expiry
      routes through the sink and not through a bare delete; gauge
      count/bytes transitions; terminal after N sweeps and not before;
      dedupe set stays bounded under repeated sweeps; clear/resume on holder
      return; no panic on a missing/empty holder set.
- [x] **Tests (integration):** an in-process scenario with a target that
      leaves (debt escalates) and one that goes retained-Dead then returns
      (debt replays); a repair whose holders are all dead reaches terminal
      and resumes when a holder is restored; existing hint/repair suites show
      no regression (RocksDB-affected crates with `--test-threads=1`,
      PIPELINE §4.6).
- [x] **Docs:** new metrics documented; the retention/escalation policy and
      the terminal classification rules are documented; every new/changed
      `pub` item has `# Examples`; `#![deny(missing_docs)]` passes.
- [x] **ADR:** ADR-0038 D7 complements are implemented; ADR-0027's contract
      (no delivery-contract change) and ADR-0028's retained-Dead topology
      drive the retention rule; ADR-0030/ADR-0035 define the holder-set
      source; ADR-0034's bounded discipline is preserved (no new persisted
      surface, bounded dedupe set).
- [x] **Perf:** the cited rules are followed — atomic gauges/counters (11.1),
      pre-sized bounded bookkeeping (1.3); no hot-path fsync is added; the
      terminal dedupe set is bounded and O(1) lookup.
- [x] **Integration:** a node-level scenario exercises debt-escalation and
      terminal-no-live-holder classification end to end (both directions:
      classified and resumed).

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **Sweep count `N` and its home.** Config knob vs documented constant;
  exact name/section if config; default value (suggest 3). Must not delay
  classification under a genuinely dead holder set, and must not classify a
  transient membership flap.
- **Sweep definition.** What constitutes one sweep for the miss counter
  (repair-loop tick, reconcile pass, retry attempt) and where the counter is
  persisted — in-memory with restart tolerance is likely acceptable; record
  the choice.
- **Dedupe-set bounds.** Capacity, eviction, and whether it dedupes by
  segment id only; must never grow without bound (ADR-0034).
- **Gauge series shape.** One gauge with a unit label vs two series
  (count/bytes); source from the existing per-target pending bookkeeping
  without adding a lock to the hint path.
- **Retained-Dead detection surface.** Which membership snapshot the hint
  layer reads (it must be the same retained topology as ADR-0027 D1) and
  whether the check is cached per hint-WAL open or per sweep.
- **Admin surface.** Whether to expose the unrecoverable set beyond the
  metric; if an existing route carries it, prefer that over a new one.
- **Interaction with EC/heal.** Confirm the terminal classification never
  suppresses an EC-reconstructible path handled elsewhere; the marker is a
  no-op for those planes.

## Cross-links

- Epic: [metadata-anti-entropy](epic.md).
- Design: [ADR-0038](../../adr/0038-metadata-change-journal.md) §D7 (complements).
- Next stage: [ae2 — change journal](ae2-metadata-change-journal.md) (the
  convergence complement; no dependency).
- Substrate: [f5 D3 hint-drop repair intents](../fleet-degradation/f5-degraded-pool-semantics.md),
  [f0 gate](../fleet-degradation/f0-hints-durability-gate.md).
- Constraints: [ADR-0027](../../adr/0027-hinted-handoff-ownership-model.md),
  [ADR-0028](../../adr/0028-membership-plane-full-swim-gossip.md),
  [ADR-0029](../../adr/0029-storage-pools-disk-resilience.md),
  [ADR-0030](../../adr/0030-re-replication-target-pull.md),
  [ADR-0034](../../adr/0034-bounded-metadata-accounting.md),
  [ADR-0035](../../adr/0035-replicated-segment-lifecycle-state.md).

## Deviations (accepted)

Reviewed 2026-09-12. All deviations below were independently verified
against the working tree and are accepted; the DoD checklist above is
checked on that basis.

- **Terminal-classification home: `oceanfs-node/src/repair.rs`, not
  `oceanfs-durability/src/repair.rs`.** The holder-side `RepairDispatcher`
  (the component that already computes `holders ∩ live` at dispatch time)
  lives in `oceanfs-node`; `oceanfs-durability/src/repair.rs` is the
  acquiring-side `ReRepWorker`. Consequence (contrary to the Crate Impact
  "oceanfs-node: None expected" row): the dispatcher gained the
  `with_unrecoverable_sweeps` builder
  (`crates/oceanfs-node/src/repair.rs:457`), `DurabilityConfig` gained the
  sweep knob (`crates/oceanfs-core/src/config/durability.rs:69`, serde
  default 3, `validate()` rejects 0), and `DurabilityModule::build` gained a
  `metrics: Option<SharedMetricRegistrar>` parameter
  (`crates/oceanfs-node/src/modules/durability.rs:149`). The behavior is as
  specified; only the file/table rows were stale.
- **Metrics registry created earlier in `node.rs`.** The gauges need a
  registrar handle retained past construction (dynamic `{target}` labels),
  so `MetricsRegistry::new()` moved before the durability module
  (`crates/oceanfs-node/src/node.rs:418`) and is passed through
  (`durability.rs:520-522`). No behavior change to other metric
  registration; `register_metrics` is still called once from
  `Node::start`.
- **Gauge series shape (spec OQ): two series, not one.** Count is
  `hinted_handoff_pending_debt{target}` (records) and bytes is
  `hinted_handoff_pending_debt_bytes{target}` (payload bytes), both
  registered lazily on first debt through `with_metric_registrar`
  (`hinted_handoff/hint_delivery.rs:555,1095`). A target never seen with
  debt exposes no series; once seen, the pair stays at 0 after drain (the
  series is not deleted). Count is sourced from the queue length, bytes
  from the new `pending_bytes` bookkeeping maintained under the per-target
  queue lock. Not gated by any kill-switch.
- **TTL semantics pinned.** Retained targets (`Alive`/`Suspect`/retained
  `Dead`) keep debt until the TTL cap; TTL-expired records escalate through
  the f5 D3 `HintDropSink` before removal
  (`hinted_handoff/hint_delivery.rs:1158-1233`). Departed targets (absent
  from membership — `Left` removes the entry, ADR-0027 D1) escalate all
  remaining debt immediately on the prune sweep. A manager without a
  membership handle is TTL-only (`target_is_retained`,
  `hint_delivery.rs:1143`). This is the reading that satisfies both "held
  while retained" and "TTL remains a cap".
- **`HintWal::prune_expired` return type changed** from `Result<usize>` to
  `Result<(usize, Vec<HintRecord>)>` (`hinted_handoff/hint_wal.rs:312`) so
  the caller can escalate the expired records; only the caller and the
  colocated test were updated.
- **Sweep definition and miss-counter persistence (spec OQ).** One sweep =
  one `RepairDispatcher::run` retry tick (fixed 5 s interval,
  `repair.rs:701`). The no-live-holder streak (`unrecoverable_misses`) and
  the terminal set are in-memory: a restart re-derives classification
  within N sweeps (no persisted surface, ADR-0034 preserved). The dedupe
  set is capped at `UNRECOVERABLE_SET_CAPACITY = 10_000` with FIFO eviction
  of the oldest marker (`repair.rs:641-655`); the counter and log fire
  once per new classification (a resumed-and-re-classified segment counts
  again).
- **Hint-half scenarios are colocated tests, not a `tests/` file.** The
  retained-Dead/return replay and departed-escalation scenarios run as
  `#[cfg(test)]` tests in `hinted_handoff/hint_delivery.rs` against the real
  `Membership`, `HintWal`, and manager in-process; the repair half has the
  `crates/oceanfs-node/tests/repair_unrecoverable.rs` integration test
  driving the real `run` loop.

### Review close-out (2026-09-12)

Independent review returned **PASS** (all 10 DoD items checked; the accepted
deviations above were verified against the working tree). Four LOW review
notes were closed post-review before this sync:

1. The pending-debt gauge update now happens while holding the per-target
   queue lock, so a concurrent enqueue cannot expose an interleaved stale
   snapshot.
2. The `unrecoverable` / `unrecoverable_order` collections are pre-sized to
   capacity (perf 1.3).
3. `# Examples` added to `HintWal::prune_expired` and the internal
   `default_repair_unrecoverable_sweeps`.
4. `unrecoverable_set_evicts_oldest_beyond_capacity` — a new
   10,001-classification unit test drives the FIFO eviction path.

One remaining environment failure is pre-existing and out of scope:
`routing_manifests::write_degraded_peer_is_routed_around` reproduces on the
baseline. Accepted design is unchanged.
