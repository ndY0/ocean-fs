---
feature: "Regression Gate + Baseline Fix (Pre-Epic)"
epic: "disk-resilience-scale"
status: proposed
priority: high
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-07
updated: 2026-09-07
---

# Regression Gate + Baseline Fix (Pre-Epic)

## Summary

Phase C opens with a **pre-epic cleanup + verification gate**. The design
session that produced ADR-0036 noted that "tests have not been run broadly
for some time" and, when the known pre-existing failures were enumerated
(ADR-0036 follow-up, 2026-09-07), the decision was **not** to record-and-
accept them but to **fix them as a pre-epic**: d0 clears the seven
documented pre-existing failures to *zero*, then runs the full workspace
gate and records a green baseline (commit + counts) so every later Phase-C
failure is attributable. The gate is a prerequisite (d1 ← d0) precisely so
that a red found at d2+ cannot be blamed on the refactor substrate
(ADR-0031…0035) that Phase C sits on.

## The seven pre-existing baseline items to fix (each with its citation)

These were enumerated in the Phase A/B docs as pre-existing and unrelated.
d0 fixes all seven before the gate runs. Each fix lands as its own commit +
review; if one proves large it is still in d0's scope (tracked as a sub-item
with its own review), because d0's DoD is **zero pre-existing failures**.

1. `oceanfs-server` grpc test `grpc_services::swim_death_detection_within_timeout`
   — pre-existing failure (`docs/features/disk-resilience/epic.md`,
   `docs/features/disk-resilience-healing/routing-manifests.md`).
2. `oceanfs-server` 2× `replicated_hlc` test failures — pre-existing
   (`docs/features/disk-resilience-healing/routing-manifests.md`).
3. `oceanfs-server` rustdoc link errors: `RING_PROBE_HASHES` (admin.rs) +
   `HintObjectApplier` (coordinator.rs) — pre-existing
   (`docs/features/disk-resilience-healing/sealed-segment-replication.md`,
   `failure-state-machine.md`).
4. `oceanfs-durability` dead test fn `test_hint_wal_implements_wal_writer_trait`
   (`hint_wal.rs:848`) — pre-existing test-target warning
   (`docs/features/disk-resilience-healing/reconciliation.md`, both
   refactoring session handoffs).
5. `oceanfs-storage` doctest `io/disk_io.rs::ObservedIo:987` — pre-existing
   baseline failure (`docs/features/disk-resilience-healing/metadata-loss-recovery.md`).
6. Flaky `fetch_falls_through_on_replica_error_and_counts_failover`
   (oceanfs-server read/fetch) — pre-existing ~50% flake, nondeterministic
   fetch order (`docs/features/disk-resilience/runtime-attach.md`,
   `routing-cache.md`).
7. In-process node restart blocked by the pre-existing
   seal-worker-not-joined leak (RocksDB lock held after shutdown) —
   documented in `docs/features/disk-resilience/data-pool-placement.md`
   (storage-level restart tests were used as the substitute).

> The implementer must verify each item reproduces at the d0 HEAD before
> fixing (an item may already be resolved by later commits — if so, record
> "already resolved at HEAD <sha>" with the closing commit and move on; the
> DoD is about the *state*, not the archaeology).

## Scope

### In Scope

- **Fix the seven pre-existing baseline items** (above), each in its own
  commit with its own review, matching the repo fix style
  (`fix(server): …`, `fix(durability): …`, etc.).
- **Run the full workspace gate** at the fixed HEAD and record a **green**
  baseline (commit + per-crate counts) in the Definition of Done:
  - `cargo build --workspace --all-targets`;
  - `cargo test --workspace` **lib suites** with the RocksDB-touching
    crates under `--test-threads=1`: `oceanfs-storage`, `oceanfs-node`,
    `oceanfs-durability`, `oceanfs-server` (PIPELINE §4.6 — parallel
    RocksDB open/close aborts with SIGABRT; a `SIGABRT` under parallel
    execution is **not** a defect, only failures that reproduce under
    `--test-threads=1` count);
  - the node **integration binaries**
    (`cargo test -p oceanfs-node --test '*'` — all
    `crates/oceanfs-node/tests/*.rs` binaries that are not load suites)
    under `--test-threads=1`;
  - the **quick functional e2e allowlist** (PIPELINE §6): from
    `crates/e2e/tests/`, only `crash_restart`, `wal_recovery`,
    `segment_lifecycle`, `cluster_lifecycle`, `cluster_write_path`,
    `cluster_read_path`, `data_pool_placement`, `metadata_pool_recovery`,
    `garbage_collection`, `rewrite_leak_test`, `runtime_attach` and the
    other non-`load_*` suites — **never** `load_sustained`,
    `load_cluster_churn`, `load_concurrency`, or any `load_*` binary;
  - `cargo fmt --all -- --check`;
  - `cargo clippy --workspace --lib -- -D warnings` on production code
    (test-code clippy findings are non-gating — `guidelines/coding.md`
    §9.2.1);
  - `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` on the crates Phase C
    touches (`oceanfs-storage`, `oceanfs-node`, `oceanfs-durability`,
    `oceanfs-membership`, `oceanfs-routing`, `oceanfs-core`).
- **Record the green baseline** in the Definition of Done checklist (commit
  + per-crate counts + e2e binary results + clippy/fmt/rustdoc status) so a
  later red can be diffed against a known-good point. This record is the
  deliverable of d0 alongside the seven fixes.
- **Triage any *new* failure** found: a refactor-substrate regression (it
  would poison every Phase C feature's DoD) is fixed before d0 closes; a
  gate-command error is corrected. A failure that is neither one of the
  seven above nor reproducible under `--test-threads=1` is treated as a
  genuine regression and fixed.

### Out of Scope (for this feature)

- Any Phase-C code (d1–d6). d0 changes `crates/` only to the extent needed
  to fix the seven baseline items; all other fixes are substrate fixes with
  their own review.
- Fleet/load-test phase-4 degraded-mode scenarios (harness epic — the
  vm-* skills; PIPELINE §6 hard-prohibits load suites on the dev machine,
  so no load-suite command appears anywhere in this doc's DoD).
- Adopting the seven items as an accepted baseline — explicitly rejected
  (decision 2026-09-07): d0 fixes them.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | Fix items 1, 2, 3, 6 (grpc test failures, rustdoc links, flaky fetch test) |
| `oceanfs-durability` | Fix item 4 (dead test fn) |
| `oceanfs-storage` | Fix item 5 (ObservedIo doctest) |
| `oceanfs-node` | Fix item 7 (seal-worker-not-joined leak blocking in-process restart) if the fix lives node-side; storage-side restart tests are the substitute until then |

## Interface (Public API)

No new `pub` items. The "surface" this feature produces is (a) the seven
fixes and (b) the **green baseline record** in this document:

- `git rev-parse HEAD` — the gate commit;
- per-crate lib/integration test counts (e.g. "storage 424 passed,
  durability 265 passed, node 65 passed", matching the Phase B docs so
  drift is visible);
- e2e allowlist results per binary;
- clippy/fmt/rustdoc verdicts — including **rustdoc clean on
  `oceanfs-server`** (item 3) and **all-targets test builds clean on
  `oceanfs-durability`** (item 4).

## Data Flow

```
HEAD ca8e1c3 (+ item archaeology: verify each of the seven reproduces)
  └─ fix each baseline item in its own commit + review (items 1–7)
  └─ cargo build --workspace --all-targets
  └─ cargo test --workspace (RocksDB crates --test-threads=1, PIPELINE §4.6)
  └─ cargo test -p oceanfs-node --test <integration binaries> (--test-threads=1)
  └─ quick e2e allowlist only (PIPELINE §6 — never load_* locally)
  └─ clippy / fmt / rustdoc gates
  └─ triage any NEW failure: substrate regression (fix) | command error (correct)
  └─ green baseline recorded in DoD (zero pre-existing failures) ──▶ d1 may start
```

## Definition of Done

- [ ] **Code:** all seven pre-existing baseline items are fixed and merged
      (each with its own commit + review); no Phase-C code ships in d0.
- [ ] **Tests:** the full gate above ran **green at the recorded HEAD** and
      the record includes the per-crate counts, the node integration binary
      list + results, and the e2e allowlist results. RocksDB crates ran
      with `--test-threads=1` (PIPELINE §4.6). **Zero pre-existing
      failures remain**: items 1–2 pass, item 3 rustdoc is clean on
      `oceanfs-server`, item 4 is gone, item 5 doctest passes, item 6 flake
      is fixed (deterministic fetch-order test), item 7 in-process restart
      is unblocked (or the substitute storage-level restart coverage is
      recorded with the leak fix tracked). **No load suite ran locally**
      (PIPELINE §6 — the allowlist is explicit per binary).
- [ ] **Docs:** no new `pub` items. The green baseline record (commit,
      counts, gate dates) is written into the DoD checklist of this
      document, and each item's fix notes the closing commit.
- [ ] **ADR:** not applicable — d0 introduces no architecture; it satisfies
      ADR-0036's "regression gate before any code lands" note and the
      design-session decision to fix (not accept) the baseline failures.
- [ ] **Perf:** not applicable — no new code paths; the gate commands are
      the standard repo gates (PIPELINE §4.5 note: system RocksDB keeps
      local storage builds fast). Fixes must not regress the perf
      guidelines on the touched crates.
- [ ] **Integration:** the quick functional e2e allowlist is green at the
      recorded HEAD (crash_restart, wal_recovery, cluster_lifecycle,
      data_pool_placement, metadata_pool_recovery, and the other
      non-`load_*` suites listed in Scope). The result table names every
      binary and its pass/fail so a d2+ red is diffable.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2). This note
> is carried verbatim from the repo template so d0's gate verdict uses the
> same non-gating split as every code feature.

## Open Questions for the Implementer

- **Item 7 scope.** The in-process restart leak (seal-worker-not-joined)
  may be the largest fix; if it cannot be closed cleanly inside d0, the
  DoD requires the fix to be *tracked* (recorded in this doc with its
  owner) while the storage-level substitute restart coverage is verified —
  the substitute is not an acceptance of the leak, only a documented
  interim. The session's decision is to fix all seven; flag early if item 7
  needs to become its own follow-up feature with its own DoD.
- **Item archaeology.** Verify each item reproduces before fixing; an item
  already resolved at HEAD is recorded as resolved with its closing commit
  (this is not an acceptance — the end state is zero).
- **Item 6 root cause.** The flake is a nondeterministic fetch order
  (`shard_batch::group_by_node` returns a std `HashMap`); fix by
  determinizing fetch order in the test or the group_by contract — decide
  at implementation and record in Deviations.
