---
feature: "Dead-Pool Runtime Recovery"
epic: "pool-runtime-lifecycle"
status: done
priority: critical
owner: ""
dependencies:
  - feature: pr1-capacity-refresh
    reason: Locked order `pr1 → pr2`; the reset path re-probes the root and refreshes capacity, and pr1's periodic task keeps the refreshed value fresh afterwards
  - feature: fleet-degradation/f5-degraded-pool-semantics
    reason: The residue sweep corrects `storage_locations` so f5's reconciliation accounting (live-copy / under-replication) observes the returned pool; the f5 wiring (done) is the substrate this feature feeds
adr:
  - 0029-storage-pools-disk-resilience
  - 0030-re-replication-target-pull
  - 0033-manifest-aware-peer-selection
  - 0035-replicated-segment-lifecycle-state
  - 0036-phase-c-scale-ops-drain
perf: []
created: 2026-09-12
updated: 2026-09-12
---

# Dead-Pool Runtime Recovery

## Summary

Make a **Dead data pool return at runtime**, operator-triggered and
probe-gated, with the return-residue accounting corrected before the pool is
trusted again. Today `Dead` is absorbing (`decide_transition` never leaves
it), the existing `HealthMonitor::reset_pool` hook is wired only to WAL and
metadata recovery, `POST /admin/pools` attach accepts only a **new** root,
detach refuses a Dead pool, and the only data-residue sweep runs once at
boot — so replacing a data device in place requires a node restart, and a
returned-but-empty pool still counts as a live copy for data it no longer
holds. This feature adds the missing runtime path: a probe-gated admin reset
that clears the Dead latch, re-probes the root, refreshes capacity and
re-gossips the manifest, a residue sweep that removes this node/pool from
`storage_locations` for entries whose local `.dat` is absent (via the durable
metadata-refresh path), and the reconciliation hand-off so the lost copies
are re-replicated. The work is product-side: `oceanfs-server` (admin route +
callback), `oceanfs-node` (composition-root reset hook + sweep), reusing
`oceanfs-storage`'s existing probe/reset/refresh/`persist_storage_locations`
surfaces; no test-only hooks.

This is **pr2 of the `pool-runtime-lifecycle` epic** (priority 2 of 2, after
[pr1 capacity-refresh](pr1-capacity-refresh.md)). The paused
`fleet-degradation` f4 resumes after both land with review PASS; its
P1b/P2/P3 recovery assertions use this path where it replaces a restart.

## Root Causes & Evidence (verified 2026-09-12 at HEAD b3eba7f)

### R1 — `Dead` is absorbing; there is no `Dead→*` transition

`decide_transition` (`crates/oceanfs-storage/src/pool/health.rs:901-955`)
only leaves `Healthy→Degraded` and `Degraded→Dead` on `ConfirmedLoss`
(`:922-925`); the `Dead` arm returns `Dead` unconditionally, with the comment
"Dead is absorbing until replacement (g7/g8) — the node explicitly resets it
after a fresh WAL/store + catch-up" (`:940-942`). The `Draining` arm likewise
treats Dead as absorbing (`:943-955`).

### R2 — The reset hook exists but is wired only to WAL and metadata recovery

- `HealthMonitor::reset_pool` (`crates/oceanfs-storage/src/pool/health.rs:750-757`)
  clears the monitor's internal Dead state / history / clean windows.
- Its only production callers are the WAL write-gate handoff
  (`crates/oceanfs-node/src/modules/wal_recovery.rs:1120-1129`, esp. `:1127`)
  and the metadata rebuild completion
  (`crates/oceanfs-node/src/modules/metadata_recovery.rs:278-281`).
- Note the caller pattern: `reset_pool` only clears the **monitor** latch; the
  WAL path pairs it with `pool_registry.set_status(Healthy)` and
  `set_write_degraded(false)` (`wal_recovery.rs:1125-1127`). Any data-pool
  reset must do both.
- Nothing for data pools: a Dead data pool's monitor latch can never clear.

### R3 — Every runtime path refuses the same Dead entry

- **Boot is the only re-probe.** `PoolRegistry::from_config_skipping` probes
  each root and registers `Healthy` (or `Degraded` on policy); with the data
  device still yanked, the default `missing_root_policy="fatal"` refuses node
  start (`crates/oceanfs-storage/src/pool/mod.rs:1078-1097`).
- **Attach accepts only a new root.** `POST /admin/pools` validates against
  the live registry; a duplicate name or root returns 409, and registration
  re-probes the (new) root to `Healthy`
  (`crates/oceanfs-server/src/admin.rs:1251-1301`;
  `crates/oceanfs-storage/src/pool/mod.rs:1551-1583`).
- **Detach refuses a Dead pool.** `POST /admin/pools/{id}/detach` requires an
  empty `Detachable` pool (409 otherwise, incl. Dead)
  (`admin.rs:1324-1364`).
- Net effect: the **same** Dead entry cannot be revived at runtime; replacing
  the data device in place requires a node restart.

### R4 — No pool-return residue handling

- The only data-residue sweep is the once-per-boot `.dat`-vs-registry
  classification: `is_startup_residue`
  (`crates/oceanfs-storage/src/segment/relocate.rs:242-274`) invoked from
  `crates/oceanfs-node/src/modules/storage.rs:815-884`. It **unlinks** files;
  it does not correct `storage_locations`, and it runs only at boot.
- The orphan reaper is dead-byte accounting only
  (`crates/oceanfs-durability/src/gc/orphan_reaper.rs:152-199`) — it cannot
  discover "the registry claims a local copy that no longer exists".
- After a fresh-format recovery, the registry still believes the pool holds
  segments, and `storage_locations` still includes self. f5 D2's live-copy
  accounting counts holders by **manifest health** (`live_copy_count`,
  `crates/oceanfs-durability/src/reconcile.rs:105-162`), not disk truth, so a
  Healthy-but-empty returned pool counts as a live copy for data it no longer
  holds and masks under-replication until repair/scrub happens to notice.

## Proposed mechanism (recommended; the user may veto at review)

Recorded as the intended shape; the exact route name, error taxonomy, and
sweep mechanics are implementer Open Questions.

1. **Operator-triggered, probe-gated admin reset.**
   `POST /admin/pools/{id}/reset` (proposed name — implementer OQ) is valid
   for a **Dead** pool whose root probes healthy. It clears the Dead latch via
   `HealthMonitor::reset_pool` (paired with `PoolRegistry::set_status` +
   `set_write_degraded(false)`, mirroring `reset_wal_pool`), re-probes the
   root, refreshes capacity, and re-gossips the manifest. A probe failure
   **keeps the pool Dead** and answers with a conflict/server-error plus the
   reason — never a silent `Healthy`.
2. **Recovery residue sweep.** After a successful reset, sweep the recovered
   pool's registry entries against the pool root: for each entry naming that
   pool whose **local `.dat` is absent**, remove this node/pool from
   `storage_locations` through the durable metadata-refresh path
   (`SegmentLifecycleCoordinator::persist_storage_locations`,
   `crates/oceanfs-storage/src/segment/lifecycle.rs:2260-2279`), so
   reconciliation repairs the lost copies. Entries whose files are present
   stay untouched. Without this, the returned pool counts as a live copy for
   data it no longer holds (R4).
3. **Reconciliation observes the corrected state.** The storage-locations
   notifier updates the reconciliation loop's holder index
   (`crates/oceanfs-node/src/modules/durability.rs:221-224` →
   `ReconciliationLoop::on_storage_locations`, f5), so the live-copy count
   drops below RF and the ADR-0030 target-pull repair re-replicates the lost
   copies; the returned pool is also a fresh placement target for new seals
   (ADR-0029 §D5).
4. **Rejected alternative (recorded): automatic periodic re-probe of Dead
   pools.** A stale or dirty filesystem can probe writable, and an automatic
   `Dead→Healthy` would re-admit a pool whose contents are not honest — the
   return must be operator-confirmed after the device is known-good. A
   **restart also still works and remains supported**: the boot probe path is
   unchanged (`pool/mod.rs:1078-1097`), and f4's scenarios may use either.

### ADR hand-off (recorded, not written here)

ADR-0029 §D3's data-pool row already states the intended semantics — "Pool
returns → healthy; old segments GC'd; placement resumes". This feature
implements that at runtime, but adds normative detail the ADR does not
currently carry: (a) the return is **operator-triggered and probe-gated**
(no automatic Dead→Healthy), (b) the return-residue/`storage_locations`
correction runs **before** the pool is trusted as a live copy, and (c) the
reconciliation hand-off is the repair trigger for the lost copies.
**Recommendation:** a short amendment to ADR-0029 §D3 (smallest surface) or,
if review prefers, a new short ADR; the implementer records the disposition
and the amendment text at implementation close. No ADR is written by this
feature doc.

## Scope

### In Scope

- New operator-triggered admin route for a probe-gated runtime reset of a
  Dead data pool (proposed `POST /admin/pools/{id}/reset`; name is an
  implementer OQ), plumbed through `oceanfs-server`'s existing admin callback
  pattern.
- Reset effect: re-probe the root; clear the health monitor's Dead latch
  (`reset_pool`) and set the registry status/`write_degraded` consistently
  (the WAL-recovery pairing, `wal_recovery.rs:1125-1127`); refresh capacity
  (see [pr1](pr1-capacity-refresh.md)); rebuild and re-gossip the manifest
  (reuse the attach hook's shape,
  `crates/oceanfs-node/src/modules/server.rs:515-536`).
- Probe failure keeps the pool Dead with a 4xx/5xx status plus the reason —
  no silent `Healthy`.
- Recovery residue sweep: registry entries naming the recovered pool whose
  local `.dat` is absent lose this node from `storage_locations` through the
  durable metadata-refresh path; present files are kept.
- Reconciliation hand-off: the corrected state reaches f5's
  `ReconciliationLoop`/holder index so lost copies are repaired by the
  ADR-0030 target-pull path, and the returned pool is a placement target for
  new seals.
- Tests (see Definition of Done): route + error taxonomy, health-reset
  effect, residue-sweep correctness, probe-failure keeps Dead, reconciliation
  observes the returned state, node-integration coverage.
- Docs: `# Examples` on new/changed `pub` items; the admin endpoint and the
  ADR hand-off are documented.
- Keep the restart path unchanged and supported.

### Out of Scope (for this feature)

- WAL-pool and metadata-pool recovery flows — they already have their runtime
  paths (g7/g8: `wal_recovery.rs`, `metadata_recovery.rs`) and stay as they
  are; whether the reset route is data-only or role-generic is an
  implementer OQ, but no g7/g8 behavior is reworked here.
- Changing boot recovery or `missing_root_policy`.
- Soft degradation (`dm-flakey`, `dm-delay`) — epic non-goal.
- Any f4 scope rework beyond using the new recovery path where it replaces a
  restart (epic non-goal); f4 stays paused until both features land.
- C2a/C2b rebalance work (pr1 supplies fresh capacity; the decision stays in
  `disk-resilience-capacity`).
- Test-only hooks; fleet/load runs while paused (PIPELINE §6/§7); any
  performance assertion.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-server` | `admin.rs`: new `POST /admin/pools/{id}/reset` route, `PoolResetCallback`/`with_pool_reset` plumbing + `AdminState` field (mirroring attach/detach), and the error-status mapping (404 unknown, 409 not-Dead/conflict, 500/409 probe failure, 501 unwired, 200 success). |
| `oceanfs-node` | Composition-root reset hook (probe → registry/monitor reset → capacity refresh → residue sweep → manifest rebuild/re-gossip), analogous to the attach/detach hooks in `modules/server.rs:505-670`; possibly a small `pool_reset.rs` module next to `pool_detach.rs`. |
| `oceanfs-storage` | Reuse `HealthMonitor::reset_pool` (`pool/health.rs:750`), `PoolRegistry::set_status`/`set_write_degraded`/`refresh_capacity`, and `SegmentLifecycleCoordinator::persist_storage_locations` (`segment/lifecycle.rs:2260`). `probe_root` is private (`pool/mod.rs:650`), so the re-probe/act step likely needs a storage-side entry point (e.g. `PoolRegistry::reset_dead_pool(...)`) or exposure — exact shape OQ. |
| `oceanfs-durability` | No change expected: the existing `persist_storage_locations` notifier already feeds `HolderIndex`/reconciliation (f5), so the sweep's effects are observed without new durability code. |
| `oceanfs-core` | None expected. |
| `e2e` | None — f4 resumes on the new route; no harness change in this feature. |

## Interface (Public API)

### Admin surface (sketch — route name and taxonomy are OQs)

```rust
// oceanfs-server/src/admin.rs — sketch, mirrors PoolAttachCallback/PoolDetachCallback
pub type PoolResetCallback =
    Arc<dyn Fn(u32) -> Result<PoolResetOutcome, String> + Send + Sync>;

impl AdminHandler {
    /// Wires `POST /admin/pools/{id}/reset`; without it the route answers 501.
    pub fn with_pool_reset(mut self, on_reset: PoolResetCallback) -> Self;
}
```

```
POST /admin/pools/{id}/reset      # data pool currently Dead; device replaced + mounted
  200 {"pool_id": <u32>, "status": "healthy", "capacity": {...}}   success
  404 unknown pool
  409 pool is not Dead (operator action conflicts with lifecycle state)
  409 | 500 root probe failed — pool stays Dead, {"error": "<reason>"} (taxonomy OQ)
  501 reset surface not wired
```

### Node-internal reset hook (sketch; names finalized in implementation)

```rust
// oceanfs-node — pool_reset / modules::server hook
//  1. registry.pool_by_id(id) must be Dead            → else 409
//  2. probe the root                                  → failure: keep Dead, error
//  3. registry.set_status(id, Healthy);
//     registry.set_write_degraded(id, false);
//     health_monitor.reset_pool(id, Healthy)           // clears the Dead latch
//  4. registry.refresh_capacity()
//  5. residue sweep:
//       for each registry entry S with entry.metadata.pool_id == id:
//         local .dat present → keep
//         local .dat absent  → lifecycle.persist_storage_locations(S, locations − self)
//  6. build_node_manifest + membership.set_self_manifest (+ manifest_cache.update)
```

### Reused (unchanged)

- `probe_root` (`crates/oceanfs-storage/src/pool/mod.rs:650`; private today).
- `HealthMonitor::reset_pool` (`crates/oceanfs-storage/src/pool/health.rs:750`).
- `PoolRegistry::set_status` (`pool/mod.rs:1339`), `set_write_degraded`
  (`:1374`), `refresh_capacity` (`:1300`), `pool_by_id` (`:1205`).
- `SegmentLifecycleCoordinator::persist_storage_locations`
  (`crates/oceanfs-storage/src/segment/lifecycle.rs:2260-2279`) and the
  storage-locations notifier that feeds reconciliation (ADR-0030).
- `SegmentDataStore::list_segment_files` for the local-presence check
  (used at `crates/oceanfs-node/src/modules/storage.rs:837`).
- The manifest rebuild/re-gossip shape of the attach hook
  (`crates/oceanfs-node/src/modules/server.rs:515-536`).

## Data Flow

```
operator: POST /admin/pools/{id}/reset   (data pool currently Dead; device replaced, mounted)
  → AdminHandler → on_pool_reset(id)                    [node composition root]
      ├─ pool must be Dead                              → else 409
      ├─ probe root                                     → failure: stays Dead, 409/500 + reason
      ├─ registry.set_status(id, Healthy); set_write_degraded(id, false)
      ├─ health_monitor.reset_pool(id, Healthy)         [clears the Dead latch]
      ├─ registry.refresh_capacity()
      ├─ return-residue sweep (new): for each registry entry with pool_id == id
      │     local .dat present? → keep the entry unchanged
      │     local .dat absent?  → persist_storage_locations(S, locations − self)
      ├─ build_node_manifest + membership.set_self_manifest (re-gossip)
      └─ manifest_cache.update(self, manifest)
  → 200 { pool_id, status: "healthy" }

reconciliation: storage-locations notifier → HolderIndex/ReconciliationLoop (f5)
  → live_copy_count < RF → ADR-0030 repair pulls the lost copies
placement: pool Healthy + refreshed capacity → new seals may land on it
boot/restart: unchanged (still a supported path)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in the affected crates
      (`oceanfs-server`, `oceanfs-node`, and `oceanfs-storage` if the probe
      entry point is added); the route is registered and answers 501 when the
      hook is unwired; a probe failure keeps the pool Dead and returns the
      reason (no silent `Healthy`); no test-only hooks.
- [x] **Tests:** route + error taxonomy (unknown 404; non-Dead 409;
      probe-failure keeps Dead with the documented status + reason; success
      200 and pool observable Healthy); health-reset effect (the monitor's
      Dead latch clears and a subsequent health tick cannot re-Dead from the
      stale latch); residue-sweep correctness (an entry whose local `.dat` is
      absent loses this node from `storage_locations`; an entry whose file is
      present is untouched); reconciliation observes the returned state (the
      holder index/live-copy count reflects the removal and an ADR-0030
      repair intent follows); node-integration coverage of the full
      Dead → reset → corrected accounting → repaired scenario; no regression
      in the existing suites (RocksDB-affected crates with
      `--test-threads=1`, PIPELINE §4.6).
- [x] **Docs:** every new/changed `pub` item has `# Examples`;
      `#![deny(missing_docs)]` passes; the endpoint and the reset contract
      (probe-gated, operator-confirmed, residue-corrected) are documented.
- [x] **ADR:** ADR-0029 §D3's data-pool return row is implemented at runtime;
      the amendment/new-ADR disposition is recorded with what it must state
      (operator-triggered + probe-gated; residue/`storage_locations`
      correction before trust; reconciliation hand-off) — see
      [ADR hand-off](#adr-hand-off-recorded-not-written-here). ADR-0030
      (repair via target-pull), ADR-0033 (manifest re-gossip/selection),
      ADR-0035 (durable lifecycle state/`storage_locations`), and ADR-0036
      (detach/drain semantics not weakened; no destructive failure)
      constraints are addressed; no ADR is written here.
- [x] **Perf:** `perf: []` — the reset is operator-triggered and serialized
      by the lifecycle state (a concurrent call sees not-Dead → 409; no
      lock), bounded by the recovered pool's registry entries plus one root
      directory listing (no full-disk scans in the normal path; ADR-0034
      discipline); no hot-path change and no performance assertion.
- [x] **Integration:** a node-level integration test exercises a Dead data
      pool returning at runtime with the residue-corrected accounting
      observed by reconciliation and a new seal landing on the returned pool;
      restart remains a documented, supported alternative.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

Implementation-shape only — the operator-triggered, probe-gated mechanism is
the recorded recommendation (the user may veto at review).

- **Route name and payload.** Confirm `POST /admin/pools/{id}/reset` (or an
  alternative); request body (none vs options); response body shape.
- **Error taxonomy.** Probe-failure status (409 vs 500) and body; behavior
  when the pool is Healthy (reject as 409 vs idempotent no-op); the exact
  class names for each error.
- **Role scope.** Data pools only first (the defect), or any Dead pool role?
  WAL/metadata have their own recovery paths; hints has its own semantics
  (f0). Ground it and record.
- **Probe/activate entry point.** `probe_root` is private to
  `oceanfs-storage` (`pool/mod.rs:650`). Decide between a storage-side
  `PoolRegistry::reset_dead_pool(...)` (probe + status + capacity in one
  place) and exposing the probe to the node — and where the manifest
  re-gossip triggers from.
- **Residue-sweep mechanics.** How to enumerate the pool's registry entries
  (registry iteration vs per-segment reads); the local-presence check
  (`list_segment_files` set vs stat per entry); handling of on-disk `.dat`
  files with **no** registry entry (the boot sweep unlinks such residue —
  should reset reuse that classification or leave it to the next boot?);
  batching and failure policy if one `persist_storage_locations` write fails.
- **Ordering/visibility.** Is the pool allowed to receive new seals before
  the residue sweep completes (self-consistent, since new seals have files),
  or is placement gated until the sweep finishes? Record the choice.
- **Concurrency/single-flight.** Reset vs in-flight reads, a concurrent
  reset, and a drain began on the same pool.
- **Observability.** Whether the reset outcome should be observable through
  logs alone or an explicit counter/gauge (and its name), given f4 may
  assert on it later. Keep it minimal.

## Deviations (accepted)

- **Route + scope.** `POST /admin/pools/{id}/reset`, **data pools only**
  (WAL/metadata keep g7/g8; hints keeps f0 semantics). Error taxonomy:
  `404` unknown pool; `400` non-data role; `409` not Dead or root probe
  failed (both operator-precondition conflicts — the probe-failure case
  keeps the pool `Dead`); `501` unwired; `200` with
  `{pool_id, status:"healthy", segments_released, sweep_failures}`.
- **Probe/activate entry point.** New public
  `PoolRegistry::reset_dead_pool(id) -> Result<(), PoolResetError>`
  (probe → `Healthy` → `write_degraded(false)` → capacity refresh);
  `probe_root` stays private. The node hook pairs it with
  `HealthMonitor::reset_pool(id, Healthy)` exactly like the g7/g8 reset
  pairing, then re-declares the self manifest.
- **Residue-sweep mechanics.** Local presence comes from one bounded root
  listing via the store's `list_segment_files` (the same layout source as
  the boot residue sweep, never path guessing); only `Sealed` entries naming the recovered pool
  with `self` still in `storage_locations` are candidates; the stale set
  is snapshotted before the async durable writes (no registry lock across
  I/O). Per-entry `persist_storage_locations` failures increment
  `sweep_failures` and are logged — the route still answers `200` because
  the pool did return; the drift scan re-detects the leftovers. A listing
  failure changes nothing (logged). On-disk `.dat` files without a
  registry entry are left to the next boot sweep (out of scope).
- **Ordering/visibility.** The pool is set `Healthy` before the sweep and
  the sweep runs synchronously inside the request; new seals racing the
  sweep have their files present by construction, so they are kept.
  Placement may target the returned pool immediately — intended.
- **Concurrency.** No new single-flight lock: a concurrent reset sees the
  pool not-Dead on the second call (`409`); a drain on a Dead pool is
  already rejected by the registry (`409`). The operation is
  operator-triggered and bounded by the pool's registry entries plus one
  root directory listing.
- **Observability.** Logs + the response counters only; no new metric
  series (minimal per the spec OQ).
- **ADR hand-off (recorded for the ADR owner).** Recommendation: a short
  **ADR-0029 §D3 amendment** stating (a) the data-pool return is
  operator-triggered and probe-gated — never an automatic `Dead→Healthy`;
  (b) the return-residue/`storage_locations` correction runs before the
  pool is trusted as a live copy; (c) the reconciliation hand-off is the
  repair trigger for the lost copies. No ADR is written by this feature;
  f4's P1b/P2/P3 use this path where it replaces a restart, and restart
  remains supported.
- **Review note (non-blocking).** The per-entry
  `persist_storage_locations` failure branch is untested — exercising it
  needs a failing event-WAL.
- **Review note (non-blocking).** `#[non_exhaustive]` is intentionally
  omitted on the two new error enums, consistent with the existing
  `oceanfs_storage::Error` / `TransitionError` precedent.
