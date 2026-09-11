---
feature: "Pre-Epic: Hints Durability Gate + Observability"
epic: "fleet-degradation"
status: done
priority: critical
owner: ""
dependencies: []
adr:
  - 0027-hinted-handoff-ownership-model
  - 0029-storage-pools-disk-resilience
  - 0018-durability-wal-consolidation
  - 0031-remove-single-datadir-legacy-mode
  - 0017-durability-task-abstraction
perf:
  - "11.1 atomic counters on hot paths (rejection counter must not lock)"
  - "7.1 minimize lock hold duration (admission check is a status/capability read, no I/O under a lock)"
created: 2026-09-11
updated: 2026-09-11
---

# Pre-Epic: Hints Durability Gate + Observability

> **Product pre-epic.** Like `disk-resilience-scale` d0 this feature precedes the
> epic proper, but unlike d0 it **changes runtime product code** (storage health
> signalling, durability capability, server admission). It is a hard gate for
> f4's hints scenario (P4) — f1/f2/f3 do not depend on it.
>
> **Gate shape: FINAL — conditional, implemented as specified.** The recorded
> verdict is in [Deviations (accepted)](#deviations-accepted); the coarse
> alternative recorded in the Open Questions was not chosen. Independent
> review iteration 3 returned **PASS** (2026-09-11).

## Summary

The 2026-09-11 hints grounding session found **two defects** in the hinted
handoff path — the only metadata-row backfill for a missed replica:

- **D1 — no observability.** The hint WAL uses raw `std::fs`
  (`crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs:21-22`) and
  `HintedHandoffManager::new(wal_dir, client, config)` takes **no
  `IoObserver`** (`crates/oceanfs-node/src/modules/durability.rs:480`).
  Pool health signals are passive — they only reflect observed I/O
  (`crates/oceanfs-storage/src/io/disk_io.rs` `snapshot`;
  `crates/oceanfs-storage/src/pool/health.rs` tick). An idle hints pool
  produces no signals, so a dead/full hints device can leave `/admin/pools`
  **Healthy** indefinitely; the documented ADR-0029 §D3 hints-Dead
  consequence has **no producer**.
- **D2 — best-effort debt, dishonest ack.** `hints_pool_accepts()` rejects
  only `PoolStatus::Dead` (`crates/oceanfs-server/src/write/coordinator.rs:1279-1287`);
  `Degraded` is accepted by design (unit test `:2122-2202`). The rejection
  path is a **warn with no counter** (`:1300-1307`; delete path `:1733-1740`),
  and `enqueue_write_hint` returns `()` and swallows enqueue failures
  (`:1354-1366`). The manager's
  `hinted_handoff_hints_enqueue_failed_total` increments only inside the
  manager on WAL open/write failure
  (`crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs:548-562`).
  A write can therefore be **acked with its debt silently gone**; because
  hints are the metadata-row backfill (read repair explicitly delegates the
  absent/remote-newer case to hinted handoff,
  `crates/oceanfs-server/src/read/coordinator.rs:1142-1149`; the churn
  session recorded "anti-entropy does not backfill missing object *rows*",
  `review/cluster-churn-resolution-2026-09-10.md:72`), a cold key can stay
  under-replicated forever.

**Fix contract (the invariant this feature establishes):**

> **An acked write must have every missing target's debt durably recorded
> (hint), or be fully replicated on its replica set.**

This feature makes hints-pool health truthful (D1) and makes admission
honest (D2). It lives in `oceanfs-storage` (observer/probe),
`oceanfs-durability` (capability/counters), `oceanfs-server` (write/delete
admission), and `oceanfs-node` (wiring/policy).

## Grounding (verified 2026-09-11)

Each fact below was re-verified against the working tree while writing this
doc. The implementer re-verifies the subset it builds on.

1. **Hint WAL does raw std I/O with no observer.** `hint_wal.rs:21-22`
   imports `std::fs::{File, OpenOptions}`; `HintWal::open`/`write_hint` are
   not instrumented. `HintedHandoffManager::new(hints_dir, client, config)`
   (`durability.rs:479-480`) has no observer parameter.
2. **Health is passive and idle-blind.** `HealthMonitor::tick_pool`
   (`pool/health.rs:721`) consumes `PoolSignal` snapshots built from the
   `DiskIoObserver`'s recorded operations
   (`io/disk_io.rs:298,507`); a pool with no I/O has zero-op windows, which
   `decide_transition` treats as clean (`pool/health.rs:846-863`). StorageFull
   **is not mapped** to a confirmed loss
   (`ConfirmedLoss::from_kinds`, `pool/health.rs:307-318`; test
   `:1163-1168`), so even a full hints device can at best be `Degraded`.
   `Dead` is absorbing (`:865-867`).
3. **The gate rejects only Dead and does so silently.** `hints_pool_accepts`
   (`coordinator.rs:1279-1287`) → `status != Dead`; `enqueue_write_hint`
   (`:1289-1367`) warns and returns on rejection (`:1300-1307`) and warns on
   `enqueue` error (`:1354-1366`); `enqueue_delete_hint` (`:1724-1756+`) is
   the same shape. New hints never reach a counter at the coordinator.
4. **The manager's failure counter exists but is unreachable from the
   coordinator's gate.** `hinted_handoff_hints_enqueue_failed_total`
   (`hint_delivery.rs:418-419`, incremented `:548-562`) covers WAL
   open/write failure inside the manager, not the Dead/refused admission
   path.
5. **Ordering makes the gate feasible.** `enqueue_write_hint` runs at step
   6b **before** the `WriteResult`/ack is built (`coordinator.rs:1011-1017`);
   the S3 row is persisted only after `put` returns `Ok`. A pre-ack gate
   failure is therefore expressible without changing the ack protocol.
6. **Ordering carries a rollback obligation.** The quorum-failure path
   already rolls back a locally written Inline row and relies on the orphan
   reaper for unreferenced segment bytes (`coordinator.rs:960-991`); a gate
   failure after local side effects must reuse that discipline.
7. **Recovery does not exist.** Per-node hint WAL handles are cached (cap
   16, lazy-close after ≥60 s idle, `hint_delivery.rs:966-1025`); a handle
   left failing after a device returns keeps failing. There is no
   remount/reopen hook — restart is the only reliable recovery.
8. **Boot with the hints device absent refuses to start.**
   `MissingRootPolicy` defaults to `Fatal` (`crates/oceanfs-core/src/config/storage.rs:170-179`)
   and `sut-deploy.sh` **explicitly pins** `missing_root_policy = "fatal"`
   (`scripts/sut-deploy.sh:256`) — so the node refuses to boot if the hints
   volume is detached.
9. **The delete path has the same honesty hole.** `replicate_delete` skips
   targets with no membership address (`coordinator.rs:1626-1636`) and
   enqueues a hint on channel-acquisition/RPC failure (`:1655`, `:1709`),
   ignoring the result; the S3 handler's quorum check consumes only the
   confirmed count (`crates/oceanfs-server/src/s3_handler/handlers.rs:586`).

## Scope

### In Scope

- **Observability — make hints-pool health have a producer.**
  - Wire hint-root I/O into the health signal path: pass an observer seam
    into the hint manager/WAL path (shape at implementation; today
    `HintedHandoffManager::new` has none) **and/or** add a periodic
    **write-probe on the hints root** through the same observed-I/O path,
    because an idle hints pool produces no signals. The probe must detect
    ENOSPC (`IoErrorKind::StorageFull`), unplug (I/O errors), and permission
    failures, and must be cheap/idempotent (a dotfile create+write+fsync on
    the hints root, cadence ≤ the pool detection window).
  - Ensure the resulting `/admin/pools` status reflects reality: unplug →
    Degraded→Dead via the existing confirmed-loss kinds; ENOSPC → at least
    Degraded with the probe capability failing (see
    [Open Questions](#open-questions-for-the-implementer) for the
    ENOSPC→status decision and the "no global StorageFull→Dead mapping"
    constraint).
- **Metric for the currently-silent rejection path.**
  - Add `hinted_handoff_hints_rejected_total` (labels/reason set at
    implementation: `pool_dead`, `probe_failed`, `enqueue_failed`,
    `path=write|delete`), counted whenever the admission gate refuses a
    needed hint.
  - Keep/align `hinted_handoff_hints_enqueue_failed_total`: define the
    boundary (attempted-and-failed inside the manager vs refused-before-
    attempt at the coordinator) so the two counters are disjoint and the
    loss path is never uncounted.
- **Conditional admission gate (proposed; final shape at implementation
  start).**
  - `enqueue_write_hint`/`enqueue_delete_hint` return `Result<()>` (or an
    equivalent capability result); the callers fail the not-yet-acked write
    when debt cannot be durably recorded.
  - Known-down targets are pre-checked **before dispatch** (membership
    address absent / manifest unavailable): if their debt cannot be
    recorded, fail fast with 503 — no local append/rollback churn.
  - Mid-flight target failure fails the write before ack; any local side
    effect follows the existing quorum-rollback discipline (Inline row
    rollback; segment bytes reaped as unreferenced) — grounding fact 6.
  - No failed targets → the write proceeds and hints health is irrelevant.
  - Hints-`Degraded` does **not** blanket-reject: the trigger is "cannot
    durably record debt" (probe + enqueue evidence), with `Dead` as a hard
    gate. This preserves the ADR-0029 §D3 matrix where Degraded writes
    proceed while the disk still writes.
  - Delete path: a delete whose tombstone debt cannot be recorded must not
    be quorum-acked either (grounding fact 9); propagate the failure.
  - Preserve the legacy no-registry/no-hints-pool fallback (always accept)
    for the non-pool test paths.
- **Boot/recovery semantics (decide and record; product change if
  accepted).**
  - What happens on restart with the hints device absent: recommended
    **per-role missing-root policy for the hints role = `Degraded`** so a
    detached hints volume does not make the node refuse to boot (today:
    global `Fatal`; `sut-deploy.sh` pins it). A `Degraded` hints pool
    boots into the gate contract: writes needing debt are rejected, writes
    not needing hints proceed.
  - Reopen path after device return: recommended **restart-only for v1**
    (the cached-handle failure mode has no reopen hook; adding one is
    speculative). Record the verdict; if the implementer finds restart-only
    untestable on the fleet, the fallback is a small remount/reopen hook —
    either way the choice is written down here.
- **Tests.**
  - Unit: enqueue failure → write rejected **pre-ack**; delete path;
    `Degraded` accepted while the probe is writable; probe transitions
    (Healthy → Degraded → Dead → replacement) with the existing fast-tick
    health config; tombstones unaffected (a rejected delete never acks);
    counters disjoint and incremented exactly once per rejection.
  - Node integration: a `WriteCoordinator` with a failing hints enqueue
    returns the 503 error and leaves no acked row; a write with no failed
    targets succeeds with hints Dead; the Inline rollback path holds.
  - Fleet: **f4's rewritten P4** is the acceptance test (observability
    black-box: yank volume → `/admin/pools` shows Degraded/Dead; write
    needing a hint → 503 + counter, never a silent ack; writes not needing
    hints unaffected).

### Out of Scope (for this feature)

- **Metadata anti-entropy** — the convergence half; its own deferred design
  draft: `docs/features/metadata-anti-entropy/design-draft.md`.
- **Read-path backfill** — dropped with the 2026-09-11 architecture
  decision (AE makes it redundant; no half-measures).
- **Debt-replay repair** — dropped for the same reason (no re-injection of
  lost hints; AE converges the rows).
- A blocking-IO refactor of the hint WAL (e.g. `io_uring`) unless the
  observer/probe work makes it necessary; the WAL is explicitly not on the
  hot path (`hint_wal.rs:63`).
- Soft degradation (`dm-flakey`/`dm-delay`), Degraded-vs-Dead trend
  calibration, and anything network-partition related (epic non-goals).
- Changing the hinted-handoff **delivery** contract (ADR-0027 as amended:
  hints are never dropped at the sender). This feature refuses *admission*
  of debt it cannot record; it does not change delivery/apply semantics.
- Manifest/proto changes (see
  [Open Questions](#open-questions-for-the-implementer) — the recommendation
  is explicitly no signal for v1).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | Hints-root probe + observer plumbing (`io/disk_io.rs`, `pool/health.rs`); classification decision for `StorageFull` (likely no global `ConfirmedLoss` change). |
| `oceanfs-durability` | Observer/capability seam on `HintedHandoffManager`; keep `enqueue` `Result` and align counters (`hint_delivery.rs`, `hinted_handoff/mod.rs`). |
| `oceanfs-server` | Admission gate in `WriteCoordinator::put`/`delete`; `enqueue_*_hint` → `Result`; 503 error mapping; `hinted_handoff_hints_rejected_total` registration. |
| `oceanfs-node` | Composition wiring: observer into the manager, probe task scheduling (Tier-1, ADR-0017), hints missing-root policy. |
| `oceanfs-core` | Only if the per-role missing-root policy route is chosen (config surface for the hints role). |
| `scripts/sut-deploy.sh` | Only if the hints missing-root policy route is chosen (today it pins `missing_root_policy = "fatal"`, line 256). |

## Interface (Public API)

No test-only hooks. Expected public/product surface (exact names and
signatures at implementation):

- `HintedHandoffManager` gains an observer/capability seam (constructor
  parameter or `with_io_observer`); `enqueue` stays `Result`-returning.
- `oceanfs-storage`: a `HintsProbe`/`probe_hints_root` helper (visibility at
  implementation; probed I/O recorded via the existing `IoObserver`).
- `oceanfs-server`:
  - `WriteCoordinator::enqueue_write_hint` / `enqueue_delete_hint` →
    `Result<()>`;
  - the admission check (successor of `hints_pool_accepts`) is consulted by
    `put`/`delete` before dispatch and before ack;
  - an error path that maps to HTTP 503 — reuse `Error::ServiceUnavailable`
    or a dedicated variant (recommended: reuse, with a distinct message;
    exact choice at implementation).
- Metrics:
  - `hinted_handoff_hints_rejected_total` (new);
  - `hinted_handoff_hints_enqueue_failed_total` (existing, boundary
    documented);
  - probe series (name at implementation, e.g.
    `hinted_handoff_hints_probe_failures_total`).
- Config (conditional): per-role missing-root policy for the hints role
  (shape at implementation; e.g. a per-pool field or a
  `missing_root_policy_hints` sibling of the existing global key).

## Data Flow

```
PUT /{bucket}/{key}
  → WriteCoordinator::put
      ├─ local availability gate (metadata/wal)            ── existing
      ├─ known-down targets in the replica set?
      │     └─ debt needed → capability check              ── fail ──▶ 503 + rejected_total (no dispatch)
      ├─ local append + remote dispatch                    ── existing
      ├─ quorum met?  no → rollback + 503 QuorumNotMet     ── existing
      └─ step 6b: for each failed target
            enqueue_write_hint → Err ─▶ rollback (Inline row) + 503 + rejected_total
            Ok                       ─▶ debt durably recorded in the hints WAL
      → S3 row persisted → 200 OK

DELETE /{bucket}/{key}
  → delete replication per target
      ├─ unreachable target → enqueue_delete_hint
      │     Err → the delete must not quorum-ack + rejected_total
      └─ all debt recorded / applied → count confirmed → ack

Observability loop (independent of the gate):
hints root mount
  ├─ real HintWal I/O (open/write) ──▶ IoObserver ──▶ PoolSignals
  ├─ periodic write-probe (idle-safe) ─▶ IoObserver ──▶ PoolSignals
  └─ HealthMonitor tick ──▶ Degraded/Dead ──▶ /admin/pools + manifest gossip
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`,
      `oceanfs-durability`, `oceanfs-server`, `oceanfs-node` (+ `oceanfs-core`
      if the per-role policy route is chosen).
- [x] **Tests:** `cargo test -p oceanfs-storage -p oceanfs-durability
      -p oceanfs-server -p oceanfs-node -- --test-threads=1` (PIPELINE §4.6)
      passes; new units cover: enqueue failure → write rejected **pre-ack**;
      delete path rejected when debt cannot be recorded; Degraded accepted
      while writable; probe transitions (Healthy→Degraded→Dead and
      replacement); counters disjoint; Inline rollback holds.
<!-- REVIEW (iter 3): re-verified on the frozen tree with --test-threads=1: storage lib 535/535, durability lib 290/290, server lib 258/258, node lib 116/116. New tests pass: durability `replay_with_uncreatable_wal_dir_boots_empty`; server `partial_hint_failure_cancels_already_enqueued_hints` (server lib 258/258, all coordinator tests included); node `hints_health_probe` 2/2 in 5.6s including the new `uncreatable_hints_root_still_boots_degraded`. Iter-2 f0 coverage (pre-ack enqueue failure, delete rejection, Degraded accepted while writable, probe transitions, disjoint counters, Inline rollback) stays green in the full lib suites. Preexisting exceptions carried from iter 2 (reproduced on pristine HEAD, no f0/hint interaction): `routing_manifests::write_degraded_peer_is_routed_around` and the `cluster_drain` full-suite timing flake. -->
- [x] **Tests:** no acked write exists whose missing target's debt is not in
      the hint WAL — asserted at the coordinator boundary in a node
      integration test (the invariant, not just the happy path).
<!-- REVIEW (iter 2/3): the DURABLE boundary is asserted. `failed_replica_hinted_even_when_quorum_met_by_others` (crates/oceanfs-server/src/write/coordinator.rs:3896) acks a quorum-with-missing-target write and asserts the on-disk per-node hint WAL `{hints_dir}/n3.wal` exists — not just the in-memory queue. Judgment: this satisfies "no acked write without debt in the hint WAL" at the coordinator boundary. The node-crate multi-node variant is deferred to f4 P4 (recorded in `hints_health_probe.rs`'s module doc): graceful shutdown sends Left and removes the node from the ring, and no in-process kill hook exists ("no test-only hooks"); the spec-writer must record this deferral in Deviations. Iter 3: unchanged by the boot-replay delta — tolerance returns Ok(0) only when the hints directory cannot be created/read (before any ack-time enqueue) and per-file WAL errors stay fatal, so admitted debt is still never silently dropped; the server suite is green 258/258. -->
- [x] **Docs:** every new `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the metric boundary (`rejected` vs `enqueue_failed`) and the
      boot/recovery verdict are documented in code and in this doc's
      Deviations section.
<!-- REVIEW (iter 3): re-verified — running doctests 31/47/18/124 passed, 0 failed; `cargo clippy --lib -- -D warnings` and `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean on all four crates; the crate-level missing_docs deny passes. The metric boundary is documented in code, and the boot/recovery verdict is now documented accurately in code (pool/mod.rs:1041-1052 states Degraded = creation/probe FAILURE, not absent-but-creatable; `replay_and_enqueue`'s doc and warning state dir-level failure boots empty; per-file replay errors stay fatal). STILL OPEN: the boot/recovery verdict is not yet in this doc's Deviations section — spec-writer close step (see the Deviations item). -->
<!-- SPEC-WRITER CLOSE (2026-09-11): closed — the boot/recovery verdict (role-aware Degraded; uncreatable/unreadable hints WAL dir boots empty while per-file WAL errors stay fatal) and the metric boundary are recorded in [Deviations (accepted)](#deviations-accepted); the Docs box is checked. -->
- [x] **ADR:** ADR-0029 §D3 (typed consequences; Degraded still writes,
      Dead cannot owe debt), §D5 (hint target) and §D8 (missing-root policy)
      satisfied; ADR-0027 as amended (delivery contract unchanged — the gate
      refuses admission, never drops an admitted hint) satisfied; ADR-0018
      (hint WAL remains a durability sub-structure) and ADR-0031 (pools
      mandatory; hint WAL on the pinned hints root) satisfied; ADR-0017
      (probe is scheduled background work under the durability budget)
      satisfied for the probe route.
<!-- REVIEW (iter 3): the f0 §D8 gap is FIXED and independently verified. `PoolRegistry::from_config_skipping` forces the hints role to `MissingRootPolicy::Degraded` regardless of the global policy (pool/mod.rs:1053-1057), and `replay_and_enqueue` now tolerates an uncreatable/unreadable WAL directory (warn + Ok(0)); per-file WAL open/replay errors stay fatal, so an admitted hint is never silently dropped (ADR-0027 delivery contract intact — the gate refuses admission, it does not drop debt). New tests pass: durability `replay_with_uncreatable_wal_dir_boots_empty`; node `uncreatable_hints_root_still_boots_degraded` (global policy pinned `Fatal`, uncreatable hints parent → `Node::start` succeeds, `/admin/pools` hints = `degraded`, no-debt PUT 200, both rejection series registered at 0). ADR-0027 D1-D5, ADR-0029 §D3/§D5, ADR-0018, ADR-0031 and ADR-0017 remain satisfied as recorded in iter 2 (excluded/unattempted Step 6b debt, delete no-address hint, Tier-1 probe permit, disjoint counters). Residual, documented and not a §D8 violation: a merely-absent-but-creatable hints root is still created by the startup probe and reports Healthy — out of f0 scope per the deploy contract (`scripts/sut-deploy.sh:251`: "The pool base is created by the startup probe"; hundreds of fixtures boot with non-existent temp roots). The policy/boundary verdict still needs the doc Deviations entry (spec-writer). -->
- [x] **Perf:** the frontmatter rules hold — the rejection counter is an
      atomic counter on the write path (11.1); the admission check is a
      status/capability read with no I/O or blocking under a lock (7.1); the
      probe is bounded, idempotent, and off the client hot path.
- [ ] **Integration:** f4's rewritten P4 fleet scenario (acceptance test):
      yank/replace the hints volume; `/admin/pools` shows Degraded/Dead
      within the pool detection window; a write needing a hint is rejected
      503 with `hinted_handoff_hints_rejected_total` incremented and **no
      silent death**; writes not needing hints are unaffected; the chosen
      boot/recovery verdict (restart-only recommended) is exercised and its
      observed behavior recorded. **No load suite runs on the dev machine**
      (PIPELINE §6).
<!-- REVIEW (iter 3): f4's P4 still does not exist, so the fleet acceptance item stays unexercised (deferred to f4; spec-writer to record the deferral). The node-level boot/recovery half is now EXERCISED: `uncreatable_hints_root_still_boots_degraded` boots a real node with the global policy pinned `Fatal` and an uncreatable hints root, asserting Degraded + needless-write 200 + zero rejection counters; `hints_probe_drives_healthy_degraded_dead_without_gating_needless_writes` remains green (Healthy→Degraded→Healthy→Dead on a real node, 2/2 in 5.6s). No load suite was run on the dev machine (PIPELINE §6). f4's P4 must still drive the live-yank path (probe transitions + 503 + counter) black-box. -->
<!-- SPEC-WRITER CLOSE (2026-09-11): the f4 P4 deferral is recorded in [Deviations (accepted)](#deviations-accepted) (node-level invariant test deferral entry); this box is intentionally left unchecked — no code gap remains. -->
- [x] **Deviations:** the final gate shape (conditional vs coarse), the
      ENOSPC classification, the manifest-semantics verdict, the hints
      missing-root policy, and the reopen-path verdict are recorded here at
      implementation close.
<!-- REVIEW (iter 3): still not recorded in this doc (section below says "None yet"). Spec-writer close step: record verdicts 1-15 from the Implementation Report here (conditional gate; no manifest signal v1; StorageFull→Degraded, Dead reserved for unplug-class; restart-only reopen; probe `PoolRootProbe` in storage, cadence detection_window/6; delete 503; thin observer seam; disjoint counters; counter placement in the durability manager; out-of-scope cluster_drain port fix; excluded/unattempted replica debt now recorded; f4 P4 deferral of the node-level invariant test; empty-blob fast-path boundary) with the iter-3 CORRECTIONS for verdict 3 and the cancel-path item: (a) hints missing-root policy is role-aware Degraded hardcoded at registration (not configurable; oceanfs-core untouched, global key still pinned `fatal` in `sut-deploy.sh`); (b) boot with an uncreatable/unreadable hints dir succeeds with zero replayed hints while per-file WAL open/replay errors remain fatal (accepted design); (c) Degraded applies when creation/probe FAILS, not when the directory is absent-but-creatable (documented in pool/mod.rs; deploy contract); (d) the residual partial-cancel test gap from iter 2 is FIXED by `partial_hint_failure_cancels_already_enqueued_hints`. -->
<!-- SPEC-WRITER CLOSE (2026-09-11): all requested verdicts are recorded in [Deviations (accepted)](#deviations-accepted), including the iter-2/iter-3 corrections; the Deviations box is checked. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

> **Closed at implementation close (2026-09-11).** Every question below has a
> recorded verdict in [Deviations (accepted)](#deviations-accepted); the
> questions are retained as the decision record.

- **Gate shape: conditional vs coarse (the recorded decision point).**
  *Proposed (user-locked recommendation):* the conditional gate — reject
  only writes whose missing-target debt cannot be durably recorded; pre-check
  known-down targets; mid-flight failures fail the not-yet-acked write;
  no-failed-target writes are unaffected; Degraded is not a blanket reject.
  *Coarse alternative (recorded, not chosen):* treat `write_degraded`-style
  hints status as a node-level admission flag and reject every write on a
  node whose hints pool is not Healthy — simpler, but rejects writes that
  need no hints and turns an observability/trend state into a data-plane
  outage. **Confirm at implementation start and record the verdict here.**
- **Manifest semantics: should peers learn "cannot owe debt"?**
  Recommendation: **no manifest signal for v1.** The capability is local to
  the coordinator's own hints pool (a hint is written to the *sender's*
  hints WAL for a remote target), and there is no coordinator-selection
  mechanism that would consume a remote flag. If one is ever added, use a
  capability flag **distinct from `write_degraded`** (they describe
  different roles and different consequences). The alternative — accept no
  signal and rely on local admission only — is the recommendation; record
  it.
- **Hints role missing-root policy.** Today global `Fatal` (and pinned in
  `sut-deploy.sh`). Recommended: per-role override, hints = `Degraded`, so
  the node boots and the gate (not the boot) enforces honesty. Decide the
  config shape (per-pool field vs role-scoped key) and whether
  `sut-deploy.sh` changes. If the global stays Fatal, record the operational
  consequence: a node restarted with the hints volume detached refuses to
  boot, and f4's P4 must stay on the live-yank path.
- **ENOSPC classification.** `StorageFull` is not a `ConfirmedLoss`
  (correct for data/wal/metadata — a full disk has lost nothing). For the
  hints role, a full device cannot record debt. Recommendation: keep the
  global `ConfirmedLoss` mapping unchanged; let the probe record
  `StorageFull`, the pool show **Degraded** (error-rate/trend), and the
  gate reject via the probe/enqueue capability. `Dead` on hints is reserved
  for unplug-class confirmation. If the implementation elects role-specific
  `Dead` for hints-ENOSPC, it MUST also specify the recovery path out of
  the absorbing `Dead` state (there is no g7/g8 reset for hints today);
  otherwise Dead means restart-only.
- **Reopen path verdict.** Recommended: restart-only for v1
  (cached-handle failure mode; no reopen hook exists). Confirm whether the
  fleet can exercise restart-with-replaced-device under the chosen
  missing-root policy; otherwise record the limitation and use live
  yank + restart.
- **Probe cadence/mechanism.** Cadence, probe file location, whether the
  probe lives in `oceanfs-storage` or `oceanfs-node`, and how it avoids
  fighting the hint WAL for the same root. Must be ≤ the pool detection
  window to have a producer before the first Dead confirmation.
- **Delete-path scope of the gate.** Recommended: a delete whose tombstone
  debt cannot be recorded must fail the request (not merely the target),
  matching the write invariant. Confirm against the S3 handler's current
  quorum-count semantics (`handlers.rs:586`) and adjust the returned count
  or the error path accordingly.
- **Observer seam shape.** Whether `HintWal` itself takes an observer (a
  real refactor) or the manager records outcomes around the WAL calls (a
  thin seam). The probe needs the same observer path either way; pick the
  smaller honest seam and record it.

## Deviations (accepted)

Every item below is an **accepted** close-out verdict, recorded after the
independent review iteration 3 returned **PASS** (2026-09-11). They close the
[Open Questions for the Implementer](#open-questions-for-the-implementer)
above; the f4 P4 fleet acceptance stays deferred by design.

- **Accepted — Gate shape: conditional (as approved), implemented as
  specified.** `Dead` is the hard gate; `Degraded` is never a blanket reject.
  A write/delete is refused only when a missing target's debt cannot be
  durably recorded; writes needing no hints proceed on any status. The coarse
  alternative was not chosen.
- **Accepted — Manifest semantics: no signal for v1.** No
  `NodeManifest`/proto change; the capability stays local to the coordinator's
  own hints pool.
- **Accepted — Hints missing-root policy: role-aware fixed `Degraded` for the
  hints role, not a new config surface.** `oceanfs-core` is untouched, the
  global `missing_root_policy = "fatal"` still governs data/wal/metadata, and
  it remains pinned in `scripts/sut-deploy.sh`. `Degraded` applies when root
  creation/probe **fails** (uncreatable parent, read-only/unreadable/detached
  mount, device errors); a merely absent-but-creatable root is still created
  by the boot probe for all roles (deploy contract; documented in
  `crates/oceanfs-storage/src/pool/mod.rs`).
- **Accepted — Hints WAL replay tolerance (iteration-3 fix).** A WAL directory
  that cannot be created or read boots the node with **zero replayed hints +
  a warning**; per-file WAL replay errors remain **fatal** (admitted debt is
  never silently dropped). No counter on the directory-level warning — the
  probe/pool health is the durable record.
- **Accepted — ENOSPC classification.** The global `ConfirmedLoss` mapping is
  unchanged; the probe records `StorageFull` → Degraded via trend/threshold;
  the gate rejects on the actual enqueue failure. Hints-`Dead` remains
  reserved for unplug-class confirmed loss.
- **Accepted — Reopen path: restart-only for v1.** Cached WAL handles are
  unchanged; no remount/reopen hook was added.
- **Accepted — Probe.** `PoolRootProbe` in `oceanfs-storage`, scheduled by
  `oceanfs-node` durability under a Tier-1 permit (ADR-0017), cadence
  `detection_window_secs / 6` (minimum 1 s), fixed probe file, never creates
  the root.
- **Accepted — Delete path.** Debt unrecordable ⇒ the delete returns 503; the
  no-address branch now enqueues the delete hint (previously a silent skip).
- **Accepted — Observer seam.** A thin seam around the manager's
  `HintWal::open`/`write_hint` calls; no `HintWal` refactor.
- **Accepted — Counters.**
  `hinted_handoff_hints_rejected_total{reason="pool_dead",path="write"|"delete"}`
  counts coordinator admission-gate refusals only;
  `hinted_handoff_hints_enqueue_failed_total` counts manager WAL-attempt
  failures. Disjoint; no `probe_failed` label (probe errors feed pool status;
  the enqueue failure is the authoritative mid-flight check). Counters live on
  `HintedHandoffManager` (durability) and are registered by
  `DurabilityModule::register_metrics`.
- **Accepted — Invariant completion (iteration-2 fix).** Replica members
  excluded by the routing hint (or beyond the fan-out cap) are now debt
  targets too — an acked write never leaves a member's copy silently
  abandoned. Hints already persisted when a later enqueue fails are cancelled
  with a fresh-HLC DELETE hint before the local rollback (ADR-0027 D2).
- **Accepted — Node-level invariant test deferral.** The DoD's node-level
  multi-node invariant assertion is covered at the coordinator boundary,
  including a durable per-node WAL assertion; the full fleet multi-node
  scenario (ack with a hard-failed target) is deferred to **f4 P4** because
  graceful in-process shutdown removes a node from the ring (no in-process
  kill hook). This is the only DoD box left open.
- **Accepted — Empty-blob PUT fast path (pre-existing, out of scope).** An
  empty-blob PUT returns 200 without replication/row/hint; recorded as a known
  boundary.
- **Accepted — Out-of-scope test-harness fix carried with this feature.**
  `crates/oceanfs-node/tests/cluster_drain.rs` now reserves explicit HTTP
  ports per node (previously HTTP bound `:0` and could steal a port reserved
  for another node's gRPC listener). Test-only; assertions unchanged.
- **Accepted note — Pre-existing failures at HEAD (not f0 regressions).** The
  exact DoD test command
  (`cargo test -p oceanfs-storage -p oceanfs-durability -p oceanfs-server
  -p oceanfs-node -- --test-threads=1`) cannot be fully green due to
  pre-existing failures at HEAD, recorded in review:
  `routing_manifests::write_degraded_peer_is_routed_around` is a deterministic
  red (3/3 runs on clean HEAD, same `left: 12, right: 24`; d6 scope,
  contradicts honest quorum), and `cluster_drain` / `intra_node_drain` show
  timing flakes (reproduced on clean HEAD; isolation passes). **f0 introduced
  no regressions**: the frozen-tree suites are green (storage lib 535/535,
  durability lib 290/290, server lib 258/258, node lib 116/116) and the
  failures above reproduce on pristine HEAD.
