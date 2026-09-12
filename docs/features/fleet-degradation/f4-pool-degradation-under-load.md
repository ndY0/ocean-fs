---
feature: "Pool Degradation Under Load (Role Matrix + Dynamic Ops)"
epic: "fleet-degradation"
status: paused
priority: critical
owner: ""
dependencies:
  - feature: f0-hints-durability-gate
    reason: P4 asserts the post-gate contract (no ack without durable debt; hints observability has a producer); f0 must land before this run
  - feature: f1-volume-backed-fleet-topology
    reason: Role mounts/devices and the record's volume map; sysfs/hcloud yank verdict
  - feature: f2-remote-fault-injectors
    reason: SSH yank/replug, per-role fill, segment corruption, POST-with-body admin driving
  - feature: f5-degraded-pool-semantics
    reason: Pre-f4 bug-fix gate — f4's scenarios assume corrected degraded-pool semantics (preferred/fallback tiers, faithful-copy accounting, hint-drop repair intents)
  - feature: test-harness-load-report
    reason: Injection records + assertion blocks in the report (existing Epic 1 types)
  - feature: disk-resilience-scale/d4-cluster-drain
    reason: Drain/detach/source-release is the machinery this feature exercises under fleet load; its residual counters are in scope here
  - feature: disk-resilience-healing
    reason: g7 wal-loss and g8 metadata-loss recovery paths are the assertions' substrate
adr:
  - 0026-phase3-dedicated-node-vms
  - 0027-hinted-handoff-ownership-model
  - 0029-storage-pools-disk-resilience
  - 0030-re-replication-target-pull
  - 0031-remove-single-datadir-legacy-mode
  - 0033-manifest-aware-peer-selection
  - 0034-bounded-metadata-accounting
  - 0035-replicated-segment-lifecycle-state
  - 0036-phase-c-scale-ops-drain
  - 0017-durability-task-abstraction
  - 0019-test-harness-topology-cost-guardrails
perf: []
created: 2026-09-11
updated: 2026-09-11
---

# Pool Degradation Under Load (Role Matrix + Dynamic Ops)

> **PAUSED (2026-09-12, user decision).** This feature does not run while the
> two pool-model product defects exposed by f3/f5 are fixed — see the
> [fleet-degradation Pause](epic.md#paused-product-gaps-before-further-testing-2026-09-12-user-decision)
> and the [pool-runtime-lifecycle epic](../pool-runtime-lifecycle/epic.md):
> a Dead data pool cannot return at runtime (P1b/P2/P3 recovery semantics)
> and pool capacity is stale (C2a/C2b dataset). The cloud fleet is destroyed;
> f4 resumes after those features land with review PASS.

## Summary

Run the **pool model** against real fleet failures under sustained load: a
scenario matrix over the four pool roles (data / wal / metadata / hints) —
each on its own real volume (f1) deformed by the SSH injectors (f2) — plus
the Phase C **dynamic ops** (attach a real spare volume, pool drain on both
intra-node and cluster paths, pause/resume, detach an empty pool, node-level
drain → `leave`) executed live while the cluster serves reads. The feature
proves behaviors that today have **only in-process integration coverage**
(`crates/oceanfs-node/tests/{pool_detach,runtime_attach,cluster_drain,
intra_node_drain,pool_drain_state,wal_pool_recovery,metadata_pool_recovery}.rs`)
on the fleet, and it closes two recorded Phase C residuals:

1. **C2a/C2b decision data.** The `disk-resilience-capacity` deferral is
   explicitly **gated on fleet/load-test data**; this feature's runs produce
   it (capacity reclaim after attach→drain→detach, placement/repair-target
   distribution with heterogeneous volume capacity, skew observations).
2. **d4 `oceanfs_drain_*` counters** ("close or document before the epic
   DoD"): the Integration DoD carries a **close-or-document verdict** —
   register the throughput counters if the fleet assertions need them, or
   record that `DrainCycleStats` + `oceanfs_pool_drain_state` +
   `oceanfs_pool_drain_blocked_reason` are sufficient.

**Correctness only — no performance claims.** Volume-backed runs cross
network block storage and are **not comparable** to local-disk runs. No
scenario asserts throughput or latency; the sustained load exists to keep
the system busy while storage fails, not to measure it.

**Sequencing (2026-09-11):** f4 is gated on
[f5-degraded-pool-semantics](f5-degraded-pool-semantics.md). The f3 fleet
validation exposed product defects in degraded-pool semantics that f4's
scenarios would otherwise re-test on a broken substrate; f5 must land (and
the f3 suite rerun green) before f4 starts.

## Scope

### In Scope

- New `e2e/tests/load_pool_degradation.rs` (name indicative; final at
  implementation) — the pool-role matrix + dynamic ops under load, driven
  by the f2 injectors and the real admin surface.
- **Common harness for every scenario:** sustained PUT/GET/DELETE background
  load (moderate concurrency), a manifest of written keys, live-read
  assertions throughout (**reads must keep succeeding during every
  scenario**, except where the scenario deliberately kills the only replica
  — never the case at RF=3 with one pool down), bounded per-scenario
  timeouts, and per-scenario `FailureInjectionRecord`s.
- **Grounding discipline:** assertions that depend on unverified semantics
  (exact metrics elsewhere; recovery timing bounds) are marked
  **grounding-required before implementation**; the implementer verifies
  the code path and freezes the assertion into this doc before coding.
  **Hints semantics are no longer grounding-required** — the 2026-09-11
  grounding session resolved them into [f0](f0-hints-durability-gate.md)
  and the rewritten P4 below asserts the post-gate contract directly.

#### Scenario matrix (role × fault), with explicit hard-yank vs graceful path

| # | Scenario | Role | Fault mechanism | Path | Data-loss expectation |
|---|---|---|---|---|---|
| P1a | Data-pool hard loss | data | `yank_volume(node, "data")` — fs mounted, device disappears | **Hard yank** (unclean fs intended) | Replicas serve; lost local copies re-replicated; no live data reaped |
| P1b | Data-pool replace & refill | data | reattach/rescan → **format** fresh fs → mount → restart or remount → placement refills | Hard-then-replace | None (replicas + refill) |
| P2a | WAL-pool loss, **live remount** | wal | graceful replace (`umount` → detach → attach → mount) on a fresh device, then `POST /admin/wal-remount` | **Graceful** | None (write gate clears after catch-up) |
| P2b | WAL-pool loss, **boot variant** | wal | graceful replace while the node is stopped, boot with the replaced (empty) device | **Graceful** | None; rebuild-from-holders machinery runs (the variant that matters — `wal_pool_recovery.rs` states the live remount does not exercise it) |
| P3 | Metadata-pool loss | metadata | graceful replace (`umount` → detach → attach → mount, fresh fs) → restart | **Graceful** | None: objects **and deletions** rebuild from peers; segment data untouched |
| P4 | Hints-pool loss | hints | yank/replace the hints volume (live fault; recovery per f0's boot/reopen verdict) | **Hard yank for the live fault; graceful for any replace** | None: no acked write loses debt — a write whose debt cannot be durably recorded is rejected 503 (f0 contract); writes not needing hints are unaffected |
| P5 | Dynamic ops under load | all | `POST /admin/pools` attach a real spare volume; intra-node drain; cluster drain (pool-only + node-level); pause/resume; detach empty; node drain → `leave` | **Graceful** (no yank) | None; zero restart; live reads throughout; manifest re-gossip |

- **P1a — data-pool hard loss** (the flagship):
  - Yank the data volume on one node; assert the pool transitions
    `Degraded`/`Dead` (ADR-0029 §D3 — device unplug is confirmed loss),
    the manifest re-gossips it, and **new placement excludes it** (via the
    existing manifest-aware seams, ADR-0033).
  - Assert reads for every key still serve with correct bytes (replicas
    elsewhere; and/or failover when a read hits the dead pool — ADR-0029 §D5
    error-path fallthrough).
  - Wait for under-replication repair to restore RF; assert via replicated
    lifecycle state / manifest counts, not a timing threshold.
  - **Orphan-reaper/GC safety (the `4c0d216` bug class):** assert that the
    reaper/GC cycle triggered while the pool is dead does **not** delete any
    live segment — every manifest key remains readable and no segment
    referenced by a live key disappears. Also assert the reaper does not
    crash/loop on the missing root (no panic, bounded logs).
  - Reattach → format fresh (fresh fs is the intended recovery for a hard
    yank; no attempt to repair the unclean fs) → mount → assert placement
    starts filling it again (new writes/seals land there) and the node is a
    normal peer.
- **P2a/P2b — WAL pool:**
  - P2a (live): replace the wal device gracefully, `POST /admin/wal-remount`;
    assert the node's write gate clears after catch-up (new writes succeed),
    reads are uninterrupted, and no `.dat` is deleted by the remount
    (`wal_pool_recovery.rs` semantics).
  - P2b (boot): stop the node, replace the wal device, start it; assert the
    **registry rebuild-from-holders** runs (`wal_pool_recovery.rs` / ADR-0035
    D1-D2), segments re-list with valid `storage_locations`, accounting
    (`contained_objects`) behavior matches the documented gap for local-only
    segments, and the node serves again. This is the variant that matters —
    P2a alone is explicitly insufficient.
- **P3 — metadata pool:**
  - Gracefully replace the metadata device, restart the node; assert the
    fresh RocksDB is rebuilt from peers over the node's owned ring ranges —
    **objects and deletions** (`metadata_pool_recovery.rs`:
    `metadata_loss_rebuilds_fresh_store_from_peers`), the pre-loss DELETE is
    honored after rebuild, segment lifecycle state is **not** re-replicated
    by the rebuild (g8 scope boundary, ADR-0035), and reads/writes recover.
  - Assert the rebuild counters/metrics for objects **and deletions** are
    observed in the report (exact metric names grounded at implementation).
- **P4 — hints pool (post-f0 gate contract):**
  - **Precondition: f0 landed** (observability producer + conditional gate).
    This scenario is f0's fleet acceptance test; assertions are grounded in
    f0's contract and are **no longer grounding-required**.
  - Fault: yank the hints volume on one node (live hard fault; the
    graceful-replace/restart path is exercised only per f0's recorded
    boot/reopen verdict). Assert **observability is truthful**:
    `/admin/pools` leaves `Healthy` within the pool detection window
    (Degraded on probe/ENOSPC evidence, Dead on unplug-class confirmation)
    — the producer f0 adds.
  - Assert the gate: with the hints device dead, a write that **needs debt
    recorded** (its replica set has a known-down/missing target) is
    **rejected with 503** and increments
    `hinted_handoff_hints_rejected_total`; the write is not acked. **No
    silent debt loss** — the pre-f0 behavior (warn + ack) is the defect
    this asserts against.
  - Assert writes that **do not need hints** (every replica acks) are
    unaffected and succeed while the hints device is dead.
  - Assert the delete path: a DELETE whose tombstone debt cannot be durably
    recorded is likewise not acked (tombstones are not silently lost).
  - Record the hint WAL/recovery behavior observed on device return per
    f0's verdict (restart-only recommended): after recovery the gate admits
    debt again, pending hints replay/deliver, and the counters/status are
    recorded in the report.
  - **This run is also the AE acceptance harness:** record the residual
    cold-key divergence produced by the scenario (hints death, plus any
    peer-outage window that prevented hinted repair) as the baseline for
    the deferred [metadata-anti-entropy](../metadata-anti-entropy/design-draft.md)
    feature. No AE assertion is made here — the data defines the AE
    acceptance bound.
- **P5 — dynamic ops under load (zero restart):**
  - **Attach:** `POST /admin/pools` with a real spare volume (OQ a — a 5th
    device; if denied, a loopback file on local disk with a recorded realism
    deviation) and assert: the pool registers, the manifest re-gossips
    (peers see the new capacity), placement starts using it, **no restart**,
    live reads unaffected. Requires f2's POST-with-body.
  - **Drain (intra-node and cluster):** `POST /admin/pools/{id}/drain`
    (`intra-node` → sibling; `cluster` → off-node via ADR-0030 target-pull)
    under load; assert reads keep serving from the draining pool until the
    data moves, placement excludes it immediately, `.dat` files move, the
    source releases itself from `storage_locations`, reconciliation stops
    counting it, and the pool reaches `Detachable`.
  - **Pause/resume:** pause a drain, assert progress parks (no movement) and
    reads continue; resume and assert progress continues; state remains
    `Draining` throughout.
  - **Detach:** detach the now-empty pool; assert removal from the registry,
    manifest re-gossip, and no restart.
  - **Node-level drain → leave:** `POST /admin/nodes/{node}/drain` under
    load; the node drains to empty, keeps serving reads until empty, then
    `leave(None)`; assert zero data loss, and the post-leave cluster
    re-converges. This exercises the retirement workflow the d4 integration
    tests substituted node shutdown for (recorded residual).
  - **Ledger:** after every op, the background manifest still verifies and
    no key became unreadable (the "live reads throughout" invariant).
- **Report shape:**
  - Per-scenario assertion blocks (named, each with expected/actual) for
    every row above;
  - all `FailureInjectionRecord`s (role, node, mechanism, path
    hard-yank/graceful, success/detail);
  - the **C2a/C2b decision dataset**: per-node free capacity before/after
    each dynamic op, placement distribution of new segments, repair-target
    distribution, and any observed skew;
  - the **d4 close-or-document verdict** block: which observables carried
    the drain assertions, and the counter verdict;
  - an explicit `perf_assertions: none` marker.
- Tests:
  - Unit-level helpers (record parsing, capacity math) where any exist, in
    `e2e` lib;
  - fleet runs only (PIPELINE §6); the report artifacts are fetched to the
    laptop by the runner (`run-phase4.sh` extended to run this suite —
    OQ e; a separate runner only if the extension proves unwieldy).

### Out of Scope (for this feature)

- Soft degradation (`dm-flakey`, `dm-delay`) and Degraded-vs-Dead
  classifier calibration — follow-up feature (epic non-goal, rationale
  recorded).
- Network partitions, clock skew, multi-failure overlap.
- The four adapted scenarios of the old Phase 4 doc — **f3**.
- Any scenario re-run of in-process node integration coverage; this feature
  adds fleet evidence, it does not replace unit/integration suites.
- C2a/C2b implementation — this feature produces the **decision data**, not
  the rebalance/ring-weighting code (stays in `disk-resilience-capacity`).
- Volume resizing, multi-disk striping, non-Hetzner providers.

## Crate Impact

| Crate | Change |
|---|---|
| `e2e` | New `tests/load_pool_degradation.rs`; may add small helpers to `src/load/fleet_degrade.rs` (f2 module) for drain-state polling and record parsing. |
| `scripts` | `run-phase4.sh` gains a mode/flag to run this suite (OQ e); `vm-test-phase` skill documents it. |
| Product crates | **None** (no test hooks). If the d4 counter verdict elects to register counters, that is a small product-crate change tracked under the d4 residual owner, not hidden in this test feature — record the decision and hand it off. |

## Interface (Public API)

No product `pub` items. Surfaces produced:

- `e2e/tests/load_pool_degradation.rs` — `#[tokio::test]` entry; same env
  contract as f3 (`TARGET_HOSTS`, `TARGET_HOST_SSH`, `TARGET_SERVICE`,
  `LOAD_TEST_VOLUMES_JSON`/record path, `LOAD_TEST_DURATION_SECS`,
  `LOAD_TEST_SEED`) plus `LOAD_TEST_POOL_SCENARIOS` (comma list, default
  all) to run one scenario at a time on the fleet.
- `e2e` helper additions (if any): `pub struct DrainPoller` /
  `wait_for_drain_state(cluster, node, pool_id, state, timeout)`-style
  helpers — exact names at implementation.
- Report artifact: `LoadReport` JSON (`4_load_pool_degradation_*.json`)
  with the scenario blocks + decision dataset described above.
- Consumed unchanged: `GET /admin/pools` (status/drain state/blocked
  reason), `POST /admin/pools`, drain/pause/resume, detach, node drain,
  `POST /admin/wal-remount`, `GET /admin/segments`, `POST /admin/scrub`.

## Data Flow

```
scenario run (fleet, background load active the whole time)
  ├─ P1 data yank:    injector.yank_volume(n,"data") → pool Dead → placement excludes
  │     ├─ reads: replicas / failover → bytes correct
  │     ├─ repair: RF restored (manifest/lifecycle observed, no timing assert)
  │     ├─ reaper/GC: no live segment deleted (4c0d216 class), no panic
  │     └─ replace: format fresh → mount → placement refills
  ├─ P2 wal: live remount (POST /admin/wal-remount) AND boot-with-replaced-device
  │     └─ boot variant: rebuild registry from holders (ADR-0035 D1/D2)
  ├─ P3 meta: graceful replace → restart → objects+deletions rebuilt from peers (g8)
  ├─ P4 hints: f0 pre-epic landed → yank hints device → /admin/pools Degraded/Dead
  │     ├─ write needing debt → 503 + rejected counter (no ack, no silent loss)
  │     ├─ writes not needing hints → unaffected
  │     ├─ delete needing debt → likewise rejected
  │     └─ recovery per f0 verdict → gate re-admits; residual divergence recorded (AE baseline)
  └─ P5 dynamic ops (all live, no restart):
       attach(spare) → manifest re-gossip → placement uses it
       drain intra-node / cluster → reads serve until moved → source-release → Detachable
       pause → parks → resume → continues
       detach(empty) → registry+manifest update
       node drain → empty → leave(None) → cluster re-converges
  └─ report: assertions + injection records + C2a/C2b dataset + d4 verdict
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in `e2e`; the scenarios
      are gated so a missing volume record fails fast with a clear message
      (never silently skipping an injection); `run-phase4.sh` supports this
      suite.
- [ ] **Tests:** `cargo test -p e2e --lib` passes. Fleet runs (PIPELINE §6,
      cloud only) green for: P1a/P1b, P2a/P2b, P3, P4, P5. P4 asserts the
      f0 post-gate contract (503 + counter for a write/delete that needs
      debt; no silent loss; writes not needing hints unaffected; truthful
      Degraded/Dead status), not speculative reconciliation semantics.
- [ ] **Tests:** P1a explicitly asserts the orphan-reaper/GC class: no live
      segment deleted while a pool is dead; a manifest-verified key set stays
      fully readable after the yank + reaper cycle.
- [ ] **Tests:** P2b (not just P2a) exercises rebuild-from-holders; P3
      asserts **objects and deletions** rebuild; P5 asserts live reads
      throughout and **zero restarts** for attach/drain/pause/resume/detach.
- [ ] **Tests:** the P4 report records the residual cold-key divergence
      data (the AE acceptance baseline) and the observed hints recovery
      behavior per f0's verdict; no AE behavior is asserted by this feature.
- [ ] **Docs:** every `pub` helper has `# Examples`; `#![deny(missing_docs)]`
      holds in `e2e`; the test module doc carries the per-row hard-yank vs
      graceful classification and the **volume ≠ performance /
      non-comparability** rule.
- [ ] **ADR:** ADR-0029 §D3/D5/D7 (typed failure semantics; failover on
      error; wal/metadata recovery = fresh store + peer rebuild) and §D3's
      hints row via the f0 gate, ADR-0027 (hint admission honesty per f0;
      delivery contract unchanged), ADR-0030
      (drain moves via target-pull), ADR-0031 (pools mandatory; role
      pinning), ADR-0033 (manifest-aware selection under a dead pool),
      ADR-0034 (bounded metadata discipline — no disk scans in assertions),
      ADR-0035 (rebuild-from-holders, accounting gap), ADR-0036 (drain/detach
      D6 no-destructive-failure under load), ADR-0017 (drain/heal are
      scheduled background work — load does not bypass budgets) satisfied.
- [ ] **Perf:** `perf: []`; no throughput/latency threshold anywhere; the
      report records the `perf_assertions: none` marker; background load is
      deliberately moderate (fleet correctness, not saturation).
- [ ] **Integration:** the full dynamic-ops chain (attach → drain →
      pause/resume → detach → node drain → leave) runs in one fleet session
      under load with a manifest-verified live-read invariant and the
      C2a/C2b decision dataset emitted; **no load suite runs on the dev
      machine** (PIPELINE §6).
- [ ] **Deviations:** the observed P4 hints recovery behavior (f0's
      boot/reopen verdict), the AE residual-divergence baseline, the d4
      counter close-or-document verdict, the spare-volume disposition
      (OQ a), and any scenario adjusted by grounding are recorded here with
      hand-off notes to `disk-resilience-capacity` / the d4 owner / the
      metadata-anti-entropy draft.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **OQ c — hints semantics: RESOLVED 2026-09-11.** The grounding session
  traced the hint WAL, health producer, and coordinator admission path and
  produced [f0](f0-hints-durability-gate.md) (the pre-epic) plus the
  rewritten P4 above. P4 now asserts the **post-gate contract**, not
  speculative reconciliation semantics; the only hints decision still open
  is f0's own Open Questions (gate shape, manifest verdict, missing-root
  policy, ENOSPC classification), which f0 resolves at implementation
  start. No P4 assertion is grounding-required anymore.
- **`leave` observability.** The d4 residual is that the no-op `leave(None)`
  ending was not exercised. Decide the observable: post-leave heal/re-rep
  dispatches should be zero (requires drain/repair observables), or the
  cluster-state convergence itself is the assertion. Record the chosen
  observable; if the `oceanfs_drain_*`/repair counters are required to make
  it observable, that is the counter-close trigger (see next).
- **d4 counter verdict mechanics.** If registering
  `oceanfs_drain_dispatched_total` / `oceanfs_drain_released_total` /
  `oceanfs_drain_remaining{source}` is elected, it is a product change with
  its own review (metric registration only) — record the hand-off; do not
  bury it in the e2e feature.
- **P1 recovery semantics.** Is "format fresh + placement refills" the
  accepted recovery for a hard-yanked data pool at the product level
  (ADR-0029 §D3 says pool returns → healthy; old segments GC'd), or should
  the test also assert the dead-pool GC/residue path? Ground the assertion
  against the data-pool placement/reaper code and record the finding.
- **Scenario isolation.** Each scenario mutates a node/role; decide the
  per-scenario setup/teardown (which node, restore before next) and how the
  manifest survives mid-scenario loses (skip deleted keys, as the existing
  Phase 3 pattern).
- **Dataset format for C2a/C2b.** Fix the JSON shape of the decision dataset
  (per-node capacity timeline, placement histogram, repair target counts)
  so the `disk-resilience-capacity` attempt consumes it without re-runs;
  record it in this doc at implementation start.

## Deviations (accepted)

_None yet — filled at implementation close._
