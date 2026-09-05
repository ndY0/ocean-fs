---
feature: "c3a: Seal-Pipeline Relocation Storage-Side (c3 Option-A Prerequisite)"
epic: "refactoring/composition-root-decomposition"
status: done
priority: high
owner: ""
dependencies:
  - feature: c1-split-storage-builder
    epic: refactoring/composition-root-decomposition
    reason: The segment pools + sealer and run_startup_recovery live in c1's StorageModule; the storage-side pipeline spawn attaches there
  - feature: c2-split-durability-builder
    epic: refactoring/composition-root-decomposition
    reason: The node-supplied merkle builder and the AE-continuous sealed-segment notifier closure come from the c2-built durability handles
adr: []
perf: []
created: 2026-09-05
updated: 2026-09-05
---

# c3a: Seal-Pipeline Relocation Storage-Side (c3 Option-A Prerequisite)

## Summary

With the seal-worker drain loop living inside `WriteCoordinator`
(oceanfs-server), `StorageModule::run_startup_recovery()` (c1) transitively
depended on a server object — startup recovery's replayed re-seals complete
through the pool seal queues and its `.dat` readiness wait is satisfiable
only by a running seal pipeline. This feature relocates the drain loop
storage-side so the node can start the pipeline and run recovery BEFORE any
server construction, making the future c3 ServerModule a single-phase build.

**Decision lineage.** Approved during c3 planning on 2026-09-04 (user
decision, **Option A**: move the seal pipeline storage-side rather than
keep it in the write coordinator). This is the recorded prerequisite seam
for c3, implemented and reviewed separately from the c3 extraction itself.
Landed as commit `489397a` (`feat(refactoring): relocate the seal pipeline
storage-side (c3-Option-A)`, 2026-09-05) with reviewer **PASS at
iteration 2** (see DoD for the iteration-1 gap record). 8 files,
+594/−414.

The seal pipeline now lives in oceanfs-storage, which must not depend on
durability or server crates — cross-crate inputs are INJECTED at spawn:

```rust
// crates/oceanfs-storage/src/segment/seal_pipeline.rs
pub type SealMerkleBuilder = Arc<dyn Fn(&[u8]) -> Option<HashOutput> + Send + Sync>;
pub type SealedSegmentNotifier = Arc<dyn Fn(SegmentId, HashOutput) + Send + Sync>;

pub fn spawn_seal_pipeline(
    small_pool: Arc<SegmentPool>,
    standard_pool: Arc<SegmentPool>,
    sealer: Arc<SegmentSealer>,
    lifecycle: Arc<SegmentLifecycleCoordinator>,
    merkle_builder: SealMerkleBuilder,
    sealed_notifier: Option<SealedSegmentNotifier>,
) -> JoinHandle<()>;
```

`spawn_seal_pipeline` is a verbatim behavioral move of the removed
`WriteCoordinator::start_seal_worker` drain loop: select-merge of the
per-tier seal receivers, per-tier semaphore permits, race-closing reserve
when the registry entry is missing, merkle root computed on the blocking
pool via the injected builder, and identical buffer recycling. Node-side
startup order is now: storage (§6) → durability (§7) → seal-pipeline start
(detached, `StorageModule::start_seal_pipeline`) → `run_startup_recovery()`
→ all server construction (§8+); the write coordinator is purely the write
path.

## Scope

### In Scope
- **New `crates/oceanfs-storage/src/segment/seal_pipeline.rs`** —
  `spawn_seal_pipeline(...)` as above. oceanfs-storage gained `dashmap`
  (workspace dep, `dashmap.workspace = true` in its Cargo.toml; Cargo.lock
  updated). Node supplies the injected closures: the oceanfs-durability
  MerkleTree builder and the AE-continuous + replicator sealed-segment
  fan-out.
- **`crates/oceanfs-storage/src/segment/pool.rs` entry retention
  invariant** — `SealingWork` carries its entries
  (`Vec<SegmentIndexEntry>`); each pool owns a `blob_entries: DashMap` +
  `record_blob_entry`; the single `seal_work()` construction site COPIES
  entries (never drains at build); `clear_entries()` runs ONLY on a
  successful send — exactly three sites: `finish_seal_handoff_async`
  Ok(Ok(())), `enqueue_seal` (try_send-Ok and blocking_send-Ok), and
  `enqueue_inflight_work` accepted. A failed enqueue keeps the entries for
  the idle driver's retry — the pre-relocation invariant.
- **`crates/oceanfs-server/src/write/coordinator.rs` deletions** —
  `start_seal_worker`, the `segment_entries` map (init +
  `record_blob_entry`), and the `segment_sealed_notifier`
  field/init/setter; two standing review markers closed, including "why is
  the seal worker part of the write coordinator?". The 5 append hooks
  record entries into the pool they append to.
- **Node re-ordering** — `StorageModule::start_seal_pipeline()`
  (`crates/oceanfs-node/src/modules/storage.rs`) spawns the detached
  pipeline before recovery; `run_startup_recovery()` no longer sits behind
  a server object.
- **Test-surface re-pointing** — coordinator test fixtures use a
  `spawn_test_seal_pipeline(&coord, notifier)` helper; `MultiTierFixture`
  wires its `sealed_events` through `spawn_seal_pipeline`.
- **Regression test** `failed_enqueue_keeps_entries_for_the_retry`
  (pool.rs) locking the entry-retention invariant.

### Out of Scope
- **The c3 ServerModule extraction itself** — c3 remains unimplemented and
  its doc stays `proposed`; this feature is the prerequisite seam only and
  is not a c3 slice.
- Any change to notifier semantics — the fan-out to AE-continuous +
  replicator is preserved, now injected at the storage-side spawn point.
- Load suites — none run for this change (PIPELINE.md §6).

## Definition of Done

- [x] **Code:** `cargo build --all-targets` clean; the seal-worker drain
      loop is a verbatim behavioral move into
      `oceanfs-storage/src/segment/seal_pipeline.rs`; the coordinator
      deletions above are complete and the node starts the pipeline before
      recovery. (REVIEW 2026-09-05: verified at PASS, iteration 2 — 8
      files, +594/−414.)
- [x] **Tests:** regression test
      `failed_enqueue_keeps_entries_for_the_retry` present and green;
      per-crate suites green — storage lib 427/427, server lib 244/244,
      node lib 66/66 + 164 integration, durability lib 265/265. Fixtures
      re-pointed to `spawn_test_seal_pipeline(&coord, notifier)`; the
      `MultiTierFixture` wires `sealed_events` through `spawn_seal_pipeline`.
      (REVIEW 2026-09-05: reviewer **FAIL at iteration 1 — G1
      (behavioral)**: entries were drained at work-build time, so an
      enqueue-failure → idle-driver-retry would seal with an empty index.
      FIXED to copy-at-build + clear-on-successful-send, with the
      regression test above. **PASS at iteration 2.**)
- [x] **Docs:** rustdoc `-D warnings` clean (incl. the new
      oceanfs-storage pub items).
- [x] **ADR:** no ADR-level constraint identified for the relocation
      (frontmatter `adr: []`); the cross-crate seam preserves the
      dependency rules — oceanfs-storage must not depend on
      durability/server — by injecting `SealMerkleBuilder` and
      `SealedSegmentNotifier` (same injection pattern as the recovery
      fold).
- [x] **Perf:** no performance guidelines implicated (frontmatter
      `perf: []`); the loop keeps the pre-relocation concurrency shape —
      per-tier semaphore permits, merkle root computed on the blocking
      pool, identical buffer recycling.
- [x] **Integration:** e2e allowlist green at iteration 1 —
      `crash_restart`, `wal_recovery`, `segment_lifecycle`,
      `cluster_write_path` and `rewrite_leak_test` among the allowlist;
      the final e2e re-run after the G-fixes was aborted by the user. No
      load suites ever run (PIPELINE.md §6). (REVIEW 2026-09-05:
      iteration-1 e2e observed green; G-fix re-verification relied on the
      lib suites above — user-aborted e2e re-run recorded as such, not as
      a pass.)
- [x] **Cleanups:** reviewer G2 (doc misattachment on
      `run_startup_recovery`) and G3–G5 (cosmetic) fixed at iteration 1;
      clippy `--lib -D warnings` clean on storage/server/node; fmt clean.
      (See Notes for the pre-existing `--all-targets` CI-gate caveat on
      HEAD itself.)

## Notes

Reviewer INFO items and residuals, recorded as non-blocking:

- **INFO — Reserved-but-never-enqueued blob entries linger until pool
  drop.** `blob_entries` entries for a segment deleted while
  Reserved-but-never-enqueued are not cleared until the pool drops —
  identical leak shape to the old coordinator `segment_entries` map
  (drained only at seal). Not a relocation regression.
- **Pre-existing — unpinned `channel = "nightly"` in
  `rust-toolchain.toml`.** `cargo clippy --all-targets --workspace
  -D warnings` (the CI gate) fails on HEAD itself under the current
  nightly (test-code lints); the changed lib code is clean under
  `--lib -D warnings`. Tracked as a pre-existing toolchain-pinning issue,
  not a gap of this feature.
- **Prerequisite only.** c3 (`c3-split-server-builder.md`) stays
  `proposed`; its ServerModule extraction is unimplemented and is now
  unblocked by this seam (the seal worker and the coordinator's notifier
  field no longer exist to extract).
