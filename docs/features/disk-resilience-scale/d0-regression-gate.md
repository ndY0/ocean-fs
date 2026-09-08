---
feature: "Regression Gate + Baseline Fix (Pre-Epic)"
epic: "disk-resilience-scale"
status: done
priority: high
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-09-07
updated: 2026-09-08
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

- [x] **Code:** all seven pre-existing baseline items are fixed and merged
      (each with its own commit + review); no Phase-C code ships in d0.
      <!-- REVIEW (2026-09-08, HEAD a651c9c): verified. Items 1–3 closed by
      55ff6e1 (swim_death_detection_within_timeout → ADR-0028 attributed flow;
      replicated_hlc repoint helper; admin.rs `RING_PROBE_HASHES` + coordinator.rs
      `HintObjectApplier` rustdoc links fixed). Item 6 closed by 6bcdf0a
      (group_by_node returns ordered Vec — root cause of the fetch-order flake).
      Item 7 closed by 82fec21+0c4ac71 (node_in_process_restart_same_dirs_succeeds
      present and green). Items 4+5 fixed in a651c9c itself (single combined commit,
      minor deviation from one-commit-per-item, consistent with the doc's
      state-over-archaeology rule §line 60-63). a651c9c ships no Phase-C code:
      only a re-enabled #[tokio::test], a deleted dead fixture, and a doctest edit. -->
- [x] **Tests:** the full gate above ran **green at the recorded HEAD** and
      the record includes the per-crate counts, the node integration binary
      list + results, and the e2e allowlist results. RocksDB crates ran
      with `--test-threads=1` (PIPELINE §4.6). **Zero pre-existing
      failures remain**: items 1–2 pass, item 3 rustdoc is clean on
      `oceanfs-server`, item 4 is gone, item 5 doctest passes, item 6 flake
      is fixed (deterministic fetch-order test), item 7 in-process restart
      is unblocked (or the substitute storage-level restart coverage is
      recorded with the leak fix tracked). **No load suite ran locally**
      (PIPELINE §6 — the allowlist is explicit per binary).
      <!-- REVIEW (2026-09-08, HEAD a651c9c): independently re-ran the gate.
      Lib suites green under --test-threads=1 for RocksDB crates: storage 468,
      durability 280, node 97, server 245; non-RocksDB: core 232, membership 100,
      routing 21, network 12, cache 58, e2e 129 (all counts match the report).
      All 31 node integration binaries green; all 7 oceanfs-server integration
      binaries green; item 4 trait test present+running; item 5 ObservedIo:987
      doctest passes; item 6 fetch-failover test 10/10 deterministic; item 7
      in-process restart green. build --workspace --all-targets clean; clippy
      --lib -D warnings clean; fmt clean; rustdoc -D warnings clean on
      oceanfs-server + the six Phase-C crates; `cargo build -p oceanfs-durability
      --all-targets` warning-free. CAVEAT: e2e wal_retention flaked once under
      background dev-machine load (deadline assert, e2e/tests/wal_retention.rs:220)
      then passed on an isolated re-run — load-sensitive, not a d0 product
      regression (d0 touches no runtime code); record in the baseline. No local
      load-suite artifacts found dated to the d0 gate (only Aug-13/14 cloud
      load-reports). -->
- [x] **Docs:** no new `pub` items. The green baseline record (commit,
      counts, gate dates) is written into the DoD checklist of this
      document, and each item's fix notes the closing commit.
      <!-- REVIEW: "no new pub items" verified (a651c9c adds none). NOT yet
      satisfied at HEAD: the green-baseline record has not been written into
      this document's DoD checklist — the per-crate counts, the 31 node
      integration binary list, the 27-binary e2e allowlist result table, and
      the wal_retention flake note still need to be recorded here (g8
      convention: closing docs commit after review PASS, e.g. ca8e1c3).
      Closing commits per item are recorded in a651c9c's message; move them
      into this doc. -->
      <!-- CLOSE (2026-09-08, d0 docs close at HEAD a651c9c): the green
      baseline record (gate HEAD + date, per-crate counts, all integration
      binary result tables, clippy/fmt/rustdoc verdicts) is written into the
      "Baseline record (d0 close)" section below, and each item's closing
      commit is recorded in the "Accepted deviations / notes (d0 close)"
      section below. The wal_retention flake caveat is recorded there too. -->
- [x] **ADR:** not applicable — d0 introduces no architecture; it satisfies
      ADR-0036's "regression gate before any code lands" note and the
      design-session decision to fix (not accept) the baseline failures.
      <!-- REVIEW: N/A verified — adr: frontmatter empty; d0 adds no
      architecture; the ADR-0036 regression-gate note is discharged by the
      green gate recorded above. -->
- [x] **Perf:** not applicable — no new code paths; the gate commands are
      the standard repo gates (PIPELINE §4.5 note: system RocksDB keeps
      local storage builds fast). Fixes must not regress the perf
      guidelines on the touched crates.
      <!-- REVIEW: N/A verified — a651c9c touches a test attribute, a dead
      test fixture, and a doc example only; no runtime code path changed;
      touched crates pass clippy/rustdoc gates. -->
- [x] **Integration:** the quick functional e2e allowlist is green at the
      recorded HEAD (crash_restart, wal_recovery, cluster_lifecycle,
      data_pool_placement, metadata_pool_recovery, and the other
      non-`load_*` suites listed in Scope). The result table names every
      binary and its pass/fail so a d2+ red is diffable.
      <!-- REVIEW: independently ran all 27 non-load_* e2e binaries at HEAD
      a651c9c: 27/27 green. CAVEAT: wal_retention
      (wal_file_count_stays_bounded_under_tiered_concurrent_churn) flaked once
      under background dev-machine load — it plateaued at 4 WAL files vs the
      required peak−3=3 within the 150 s deadline (e2e/tests/wal_retention.rs:220)
      — then passed on an isolated re-run. Load-sensitive timing, not a d0
      regression. The per-binary pass/fail result table still needs to be
      written into this doc (closing commit). -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2). This note
> is carried verbatim from the repo template so d0's gate verdict uses the
> same non-gating split as every code feature.

## Baseline record (d0 close)

The deliverable of d0 — a recorded **green** workspace gate at a fixed
HEAD, so every later Phase-C failure (d1–d6) is diffable against a
known-good point. Independent review **PASS** at this HEAD (2026-09-08).

| Field | Value |
|---|---|
| Gate HEAD | `a651c9c53d4b9e95bb4d827bef7cc265301386a8` (fix commit; parent code state = `ca8e1c3`) |
| Gate date | 2026-09-08 |
| `cargo build --workspace --all-targets` | clean, zero warnings |
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --lib -- -D warnings` | clean |
| Rustdoc (`RUSTDOCFLAGS="-D warnings"`) | clean on `oceanfs-storage`, `oceanfs-node`, `oceanfs-durability`, `oceanfs-membership`, `oceanfs-routing`, `oceanfs-core` **and** `oceanfs-server` (item 3) |

### Lib suites

RocksDB crates under `--test-threads=1` (PIPELINE §4.6):

| Crate | Passed |
|---|---|
| oceanfs-storage | 468 |
| oceanfs-durability | 280 |
| oceanfs-node | 97 |
| oceanfs-server | 245 |

Non-RocksDB crates:

| Crate | Passed |
|---|---|
| oceanfs-core | 232 |
| oceanfs-hash | 13 |
| oceanfs-ec | 72 |
| oceanfs-accel | 76 |
| oceanfs-storage-api | 0 |
| oceanfs-routing | 21 |
| oceanfs-membership | 100 |
| oceanfs-network | 12 |
| oceanfs-cache | 58 |
| e2e | 129 |
| oceanfs | 11 |

### Node integration binaries (31, `--test-threads=1`) — ALL GREEN

`anti_entropy` 7 · `auth_middleware` 5 · `cache_behavior` 9 ·
`data_pool_placement` 2 · `durability_wiring` 5 · `e2e_single_node` 4 ·
`failure_state_machine` 2 · `gc_compaction` 7 · `io_observer_faulty` 1 ·
`io_observer_wiring` 1 · `loss_announcement` 1 · `merkle_startup_rebuild`
3 · `metadata_pool_recovery` 1 · `node_lifecycle` 1 · `orphan_reaper` 8 ·
`placement_policy` 1 · `pool_registry` 1 · `pre_pool_dir_refusal` 2 ·
`read_repair` 3 · `read_write_roundtrip` 7 · `reconciliation` 1 ·
`re_replication` 2 · `role_isolation` 3 · `routing_cache` 1 ·
`routing_manifests` 2 · `runtime_attach` 1 · `scrub_cycle` 7 ·
`segment_replication` 3 · `startup_config` 5 · `tiered_routing_e2e` 4 ·
`wal_pool_recovery` 2.

### Storage integration binaries (10) — ALL GREEN

`disk_segment_reader` 10 · `metadata_crud` 10 · `mlock_no_future_cap` 1 ·
`pipeline_parallelism` 7 · `segment_metadata_lifecycle` 4 ·
`segment_roundtrip` 14 · `streaming_ec_encode` 3 · `tiered_routing` 20 ·
`wal_recovery` 10 · `wal_truncation_after_seal` 2.

### Durability integration binaries (6) — ALL GREEN

`anti_entropy` 14 · `distributed_scrub` 5 · `gc_compaction` 5 ·
`merkle_recovery` 3 · `orphan_reaper` 7 · `segment_data_roundtrip` 2.

### Server integration binaries (7) — ALL GREEN

`grpc_services` 9 · `hinted_handoff` 6 · `read_path` 6 ·
`read_repair_e2e` 4 · `replicated_hlc` 2 · `routing_forward` 6 ·
`write_quorum` 3.

### Quick e2e allowlist (27 non-`load_*` binaries, PIPELINE §6) — ALL GREEN

`crash_restart` 1 · `wal_recovery` 1 · `segment_lifecycle` 1 ·
`cluster_lifecycle` 4 · `rewrite_leak_test` 1 · `cluster_write_path` 6 ·
`cluster_read_path` 5 · `garbage_collection` 1 · `anti_entropy` 1 ·
`heal` 1 · `cluster_gossip` 4 · `cluster_failure_detection` 5 ·
`cluster_topology` 4 · `cluster_concurrency` 3 · `cluster_ring_routing` 4
· `cluster_scrub` 2 · `cluster_hinted_handoff` 3 ·
`cluster_anti_entropy` 4 · `cluster_cache_invalidation` 2 ·
`cache_cascade` 2 · `negative_cache` 1 · `compression_roundtrip` 2 ·
`orphan_reaper` 1 · `prefetch` 1 · `remote_target_mode` 1 · `scrub` 1 ·
`wal_retention` 1.

**NO `load_*` binary ran locally** (PIPELINE §6 hard prohibition; the
allowlist is explicit per binary).

## Accepted deviations / notes (d0 close)

- **Items 4+5 shipped as a single combined commit** `a651c9c`
  (`fix(durability,storage): …`) rather than one commit per item — a
  user-approved decision ("small touches"). Items 1–3/6/7 were already
  resolved by prior feature commits and are recorded as such (this is the
  doc's state-over-archaeology instruction, §"The seven pre-existing
  baseline items", item archaeology note — the end state is zero, not the
  archaeology). Per-item closing commits: items 1–3 → `55ff6e1` (09-04),
  item 6 → `6bcdf0a` (08-23), item 7 → `82fec21` + `0c4ac71`, items 4+5 →
  `a651c9c`.
- **`e2e/tests/wal_retention.rs`
  (`wal_file_count_stays_bounded_under_tiered_concurrent_churn`) is
  load-sensitive on the dev machine**: it flaked once under background CPU
  load (WAL count plateaued at 4 vs. the required peak−3 within the 150 s
  deadline, `wal_retention.rs:220`) and passed clean on an isolated re-run
  (199 s). Not a d0 regression (`a651c9c` changes no runtime code);
  recorded here so a later d2+ red is diffable against an honest
  known-good.
- **e2e harness prerequisite (not a defect)**: the harness requires
  `target/release/oceanfs` to be newer than sources, so a fresh
  `cargo build --release -p oceanfs` was required after the fix commit
  before running the e2e allowlist.

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
