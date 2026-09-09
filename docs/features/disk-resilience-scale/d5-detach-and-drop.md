---
feature: "Pool Detach & Drop (Inverse of f8 Attach)"
epic: "disk-resilience-scale"
status: done
priority: high
owner: ""
dependencies: ["d3-intra-node-drain"]
adr: [0036, 0029, 0031]
perf: []
created: 2026-09-07
updated: 2026-09-09
---

# Pool Detach & Drop (Inverse of f8 Attach)

## Summary

The inverse of f8 `attach` (ADR-0036 D1 "detach"): once a pool has been
drained to emptiness — its `DrainState` is `Detachable`, produced by the
d3 (C1a) or d4 (C1b) worker — an operator removes it from the **live
`PoolRegistry`** (`PoolRegistry::detach(id)`, the missing inverse of
`PoolRegistry::attach` at `crates/oceanfs-storage/src/pool/mod.rs:1297`),
drops it from the node's topology/config view, and rebuilds + re-gossips
the `NodeManifest` (the f6 path) so peers stop treating the node as having
that capacity. **No restart** anywhere in the path. Validation mirrors
attach in reverse (unique name/root released, role cardinality preserved);
`wal`/`metadata`/`hints` role pools cannot be detached — those roles are
cardinality-1 and their replacement is the g7/g8 boot/remount path, not
drain (ADR-0031 role pinning). A detached pool's root/name becomes
available for a fresh `attach` (e.g. a hot-swapped device re-attached with
its original identity — today impossible because the old entry cannot be
removed, `runtime-attach.md` deviation). Lives in `oceanfs-storage`
(registry detach) + `oceanfs-node`/`oceanfs-server` (admin + manifest
wiring). The full lifecycle this feature closes: **attach → drain →
detach**.

## Scope

### In Scope

- **`PoolRegistry::detach(pool_id)`** (new; `crates/oceanfs-storage/src/pool/mod.rs`
  next to `attach:1297` and `validate_attach:1378`):
  - **Precondition: the pool is empty** — `drain_state(pool_id) ==
    Detachable` (d1's record, set only by a worker that found zero
    registered segments with that `pool_id`). Detach re-checks emptiness
    atomically under the registry lock via an injected segment-count
    predicate (the registry does not know the lifecycle registry; the node
    wires the check, mirroring how the registry is constructed with its
    role/capacity knowledge). If the pool is not empty or not
    `Detachable`, detach is **rejected** — the no-destructive-failure
    rule (ADR-0036 D6): detach only on empty; nothing half-removed.
  - Role rule: only `data` pools detach. `wal`/`metadata`/`hints` pools
    return a specific error (their replacement is g7/g8; ADR-0031).
  - Effect: the `StoragePool` leaves the registry (`data_pools`,
    `pool_by_role`, snapshots — placement, sealer, and reader all read
    registry snapshots, so a detached pool is immediately invisible to new
    placement and root resolution); its `PoolMetrics` are unregistered;
    its id/name/root are released (a later `attach` with the same root —
    hot-swap — is no longer a duplicate, closing the f8 deviation).
  - The pool **root directory is not deleted** by detach — the pool is
    empty, but removing a directory is the operator's/device's concern;
    detach leaves the (empty) root in place and records that the operator
    may remove the device.
- **Config/topology drop — PERSISTENT removed-overlay (decision
  2026-09-07).**
  - Detach is **durable**: a restart must honor the removal without a
    config edit. The pool set at boot is `config − removed`, where
    `removed` is a small persistent **removed-pool record** consulted by
    `PoolRegistry::from_config`. The record is keyed by **name + root**
    (never pool id — ids are config-order and shift when pools are
    removed/reordered).
  - **Why persistent (not the f8-symmetric ephemeral overlay):** an
    ephemeral detach would resurrect an empty pool on the next restart and
    placement would refill it — a footgun in exactly the disk-replacement
    workflow detach exists for. Config stays untouched (no config-file
    rewrite — Option C rejected; rewriting user/fleet-managed
    `oceanfs.toml` is the config-magic the 2026-09 review fought). The
    boot-consulted marker has precedent in g7's wal-pool replacement
    marker (ADR-0035 D4) and `membership_state.toml`.
  - **Reconciliation rules** (specify precisely in the feature):
    - a removed record suppresses a config-declared pool **only while both
      name and root match**;
    - re-attaching the same name+root via the admin API **clears** the
      record (a hot-swap round-trip removes the tombstone);
    - an operator config edit that drops the pool leaves a harmless stale
      record that never matches (may be GC'd on boot);
    - boot applies `config − removed` **before** role/cardinality
      validation (ADR-0031's ≥1 data pool check sees the *post-overlay*
      set).
  - **Storage of the record:** a small crash-safe file written under the
    node's state directory (same family as `membership_state.toml` /
    g7's marker), written transactionally (write temp → fsync → rename).
    The record is node-local; it is never gossiped (peers only see the
    manifest the node re-gossips after detach).
  - Node-level retirement note: a node that drains and detaches its **last
    data pool** ends with zero data pools and is headed for `leave(None)`.
    Boot validation requires ≥1 data pool on the *post-overlay* set
    (ADR-0031), so a restart of a fully-detached node before `leave`
    refuses boot — document this as the expected retirement sequence
    (detach → leave; never restart a fully-detached node expecting zero
    data pools).
- **Manifest rebuild + re-gossip.** After a successful detach the node
  re-runs `build_node_manifest` (`crates/oceanfs-node/src/pool_manifest.rs:52`)
  and `Membership::set_self_manifest`
  (`crates/oceanfs-membership/src/membership/manager.rs:752`), so peers
  stop counting the node's capacity. Wired through the same
  `on_pool_attached`-style hook f8 uses (`crates/oceanfs-server/src/admin.rs:430`)
  — an `on_pool_detached` counterpart. Peers observe the version-bumped
  manifest through the proven f7 propagation path (no new gossip code).
- **Admin surface.**
  - `POST /admin/pools/{id}/detach` — accepted only when
    `drain_state == Detachable`; responses: `200` detached, `409` not
    empty / not `Detachable`, `400` wrong role (`wal`/`metadata`/`hints`),
    `404` unknown pool. (Route wording finalized with d4's drain verbs —
    the epic leaves exact routes to d4/d5.)
  - Status payload (d1) reflects the pool's removal (the pool disappears
    from `/admin/pools`).
- Tests:
  - unit: detach succeeds on a `Detachable` (empty) data pool — pool gone
    from registry/data-pools/role lookups; id/name/root released (a
    subsequent attach with the same root/name succeeds — hot-swap closure);
  - unit: detach rejected when not empty / not `Detachable` (both the
    `Draining` and the non-draining-healthy cases) — nothing removed;
    **no-destructive-failure scenario**;
  - unit: detach rejected for `wal`/`metadata`/`hints` role pools with the
    role-specific error;
  - unit: **persistence** — removed-pool record round-trips (temp→fsync→
    rename crash-safe write), and `from_config` over `config − removed`
    omits the detached pool; re-attach of the same name+root clears the
    record; a stale record whose name/root no longer matches any config
    pool is inert and GC'd at boot; config validation sees the
    post-overlay set;
  - unit: manifest rebuild after detach drops the pool row (2→1 data
    pools) and `set_self_manifest` is invoked;
  - integration (local node, 2 data pools): PUT → drain pool 0 (C1a mode)
    to empty → `POST /admin/pools/0/detach` → registry 2→1 data pools,
    manifest 2→1, placement writes only to pool 1, GETs of pre-drain
    objects still served from pool 1, **no restart**, and a fresh `attach`
    of the same root succeeds (hot-swap round-trip);
  - integration: **restart honors detach** — after detach, an in-process
    restart (or node restart where the leak fix from d0 allows it) does
    NOT resurrect the pool (config still lists it; `config − removed`
    drops it);
  - integration: detach of a non-empty pool returns 409 and leaves the
    pool fully serving.

### Out of Scope (for this feature)

- Draining a pool to emptiness (d3 C1a / d4 C1b) — detach consumes the
  `Detachable` they produce.
- `wal`/`metadata`/`hints` pool replacement — the g7/g8 boot/remount path,
  never drain/detach (ADR-0031).
- Deleting the pool root directory / wiping the device — operator action.
- Ring re-weighting on detach (d6/C2a) — freed capacity is reclaimed via
  new writes + repair targeting (documented interim behavior); peers stop
  treating the node as having the capacity, but the ring is not re-weighted
  here.
- Graceful-leave redesign (leave stays `leave(None)`), migration-plane
  isolation (ADR-0030 D4), C2b/C3 — epic non-goals.

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `PoolRegistry::detach` (+ reverse validation, role guard, metrics unregister); doc/`validate_attach` updates for the released name/root path |
| `oceanfs-node` | `on_pool_detached` hook → manifest rebuild + re-gossip; empty-check predicate wiring into detach |
| `oceanfs-server` | Admin `POST /admin/pools/{id}/detach` route (alongside the d4 drain verbs) |

## Interface (Public API)

- `PoolRegistry::detach(&self, pool_id: u32) -> Result<(), DetachError>` —
  removes an empty (`Detachable`) data pool from the live registry.
  `<!-- TODO(spec): verify anchor -->` the empty check needs a segment-
  count predicate the registry does not own; confirm how the node wires it
  (an injected `pool_is_empty: Arc<dyn Fn(u32) -> bool>` at registry
  construction, or a node-side pre-check immediately before `detach`).
- `pub enum DetachError { NotDetachable, PoolNotEmpty, WrongRole(PoolRole),
  UnknownPool, … }`.
- `PoolRegistry::attach` now accepts a root/name previously detached
  (duplicate validation against the *live* registry, `validate_attach` at
  `pool/mod.rs:1378`, so released identities are reusable); re-attach
  clears the removed-pool record.
- **Removed-pool record** (new persistent overlay, decision 2026-09-07):
  - `PoolRemovedRecord { name: String, root: PathBuf, removed_at }` —
    node-local, keyed by name+root;
  - `RemovedPoolStore::record(...)` / `::clear(name, root)` /
    `::list()` — crash-safe write (temp → fsync → rename) under the node
    state dir; read at boot by `PoolRegistry::from_config` (`config −
    removed`).
- Admin: `POST /admin/pools/{id}/detach` (verbs finalized with d4).

## Data Flow

```
operator ──▶ POST /admin/pools/{id}/detach
   ├─ drain_state(id) == Detachable ∧ segment-count predicate == 0?   ← else 409 (no-destructive-failure)
   ├─ role == data?                                                   ← else 400 (wal/meta/hints → g7/g8 path)
   ├─ registry: remove StoragePool + metrics (registry lock)
   ├─ write removed-pool record (name+root; temp → fsync → rename)
   ├─ on_pool_detached ──▶ build_node_manifest (pool dropped) ──▶ set_self_manifest ──▶ gossip
   │     └─ peers: node's capacity excludes the pool (repair/write target filtering)
   ├─ 200 detached
   └─ same root/name now attachable (re-attach clears the record; hot-swap round-trip closes f8 deviation)
restart: boot = config − removed ──▶ the detached pool does NOT return (persistent decision 2026-09-07)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`,
      `oceanfs-node`, `oceanfs-server`.
<!-- REVIEW: PASS (verified 2026-09-09, worktree on 4fddf9e). `cargo build --all-targets -p oceanfs-storage -p oceanfs-node -p oceanfs-server` green; `cargo fmt -- --check` clean; `cargo clippy --workspace --lib -- -D warnings` clean; `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean on the three crates (missing_docs denied in each crate lib.rs). DoD Code item PASSES. -->
- [x] **Tests:** `cargo test -p oceanfs-storage -p oceanfs-node -p
      oceanfs-server --lib -- --test-threads=1` passes; the Scope scenario
      list is green — including the **no-destructive-failure** detach
      rejections (non-empty / not-`Detachable` pools are refused and keep
      serving), the role guard, the released-name/root re-attach
      (hot-swap) case, and the integration scenario: 2-pool node drains a
      pool, detaches it, manifest 2→1, reads continue, no restart.
<!-- REVIEW: PASS (iteration-2 re-verified 2026-09-09). All lib suites green single-threaded: oceanfs-storage 530, oceanfs-node 114, oceanfs-server 245. New d5 unit coverage still passes: pool::removed::tests 3/3, pool::drain::tests::detach_* 5/5, pool::tests::overlay_* 4/4, src pool_detach::tests 7/7, removed_pools::tests 7/7; the data_store regression write_to_a_segment_whose_pool_was_detached_fails (data_store.rs:781) passes. The iteration-1 FAIL is FIXED: crates/oceanfs-node/tests/pool_detach.rs now passes 3/3 single-threaded — scenario 3 inserts `steer(&node.pool_registry(), 0)` (tests/pool_detach.rs:362) after the 201 attach assert (:354) and before the put (:364), so placement lands the post-attach write on the re-attached pool 0, the tombstone-clear path runs, and the boot-3 assertions pass. Scenario 1's post-detach wait is no longer vacuous (tests/pool_detach.rs:187-193: records `pool1_dats_before` and waits for a strictly larger `.dat` count, asserting root0 stays empty). No-destructive-failure rejections (409 healthy / 409 non-empty Draining / 400 wal / 404 unknown), the role guard, and the released-name/root re-attach hot-swap case are exercised and green. Regression integration re-verified green single-threaded: runtime_attach 1/1, pool_registry 1/1, pool_drain_state 3/3, data_pool_placement 2/2, placement_policy 1/1, intra_node_drain 3/3, cluster_drain 3/3, node_lifecycle 1/1. Server doctests 18/18 (4 ignore-tagged). DoD Tests item PASSES. -->
- [x] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the **persistent removed-overlay rule** (`config − removed`;
      record keyed by name+root; re-attach clears) and the
      zero-data-pool-at-boot restriction (ADR-0031, post-overlay set) are
      documented on `detach` and `from_config`.
<!-- REVIEW: PASS (verified 2026-09-09; nits closed in iteration 2). `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean on oceanfs-storage/node/server; missing_docs is denied at crate level (lib.rs of each). All new pub items carry doc comments and the removed-overlay + zero-data-pool rule is documented on `PoolRegistry::detach` (drain.rs:723-736), `PoolRegistry::from_config_skipping` (pool/mod.rs:927-940), `PoolRemovedRecord`/`matches` (removed.rs:37-90), `RemovedPoolStore` (removed_pools.rs:46-156), and the admin handler (admin.rs:1288-1304). The two iteration-1 doc nits are now FIXED: pub `MetricsRegistry::remove_counter` gained an `# Examples` block (admin.rs:205-217; server doctests 18/18), and the stale attach comment at pool/mod.rs now describes lowest-free-id computed inside the write lock (pool/mod.rs:1503-1529). DoD Docs item PASSES. -->
- [x] **ADR:** ADR-0036 D1 (detach = inverse of f8; empty-pool only; no
      restart) + D6 (detach accepted only on an empty/`Detachable` pool)
      + **D8 (persistent removed-overlay)** satisfied; ADR-0029 §D8
      (runtime pool topology change via admin) and ADR-0031 (role pinning;
      wal/meta/hints replacement is the g7/g8 path; pools mandatory at
      boot) satisfied.
<!-- REVIEW: PASS (verified 2026-09-09). D1/D6 — `PoolRegistry::detach` (drain.rs:726-820) removes only a `Detachable` `data` pool under a short write-lock section; healthy/Draining/non-`data`/unknown pools return typed errors, nothing half-removed; runtime-only (no restart). The empty proof is the node-side `pool_is_empty` (pool_detach.rs:84-109) using d4's Deviations-f.i definition (no Reserved with pool_id==X; no Sealed with pool_id==X that lists self in storage_locations) — d4 f.i is thereby addressed. d4 f.ii (post-Detachable re-materialization) is addressed by refusing detach over a re-materialized resident copy (pool_detach.rs:300-321 test) and by the store write failing loudly after removal (data_store.rs:771-798 regression). D8 — record keyed by name+root (`PoolRemovedRecord::matches`, removed.rs:88-90); re-attach clears the tombstone (modules/server.rs:495-510 + admin.rs:1257-1263); stale records inert + overlay applied before the ≥1-data-pool boot check (pool/mod.rs:994-1056; unit tests overlay_stale_record_is_inert / overlay_removing_every_data_pool_refuses_boot); crash-safe temp→fsync→rename (removed_pools.rs:139-156); full-detach boot refusal confirmed end-to-end via a reviewer Node::start probe. ADR-0031 role pinning: wal/metadata/hints detach refused (WrongRole). ADR-0029 §D8 runtime attach continues to work (runtime_attach regression 1/1). Deviations from the sketch to record (see section below): the empty check is a node-side pre-check rather than a registry-injected predicate; `DetachError::WrongRole` carries (u32, PoolRole). DoD ADR item PASSES. -->
- [x] **Perf:** frontmatter `perf: []`; prose constraints: detach is a
      rare admin op; the registry removal is a short write-lock section
      (perf rule 7.1); the manifest rebuild is once-per-change
      (`pool_manifest.rs` module doc, perf rule 2.4).
<!-- REVIEW: PASS (verified 2026-09-09). `perf: []` in frontmatter. `PoolRegistry::detach` holds the `pools` write lock only for the removal itself (drain.rs:793-805) and takes the metrics/drain/drain_mode maps sequentially AFTER releasing `pools` (no cross-lock hold; LOCK ORDER note drain.rs:43-50). Node-side emptiness scan is a lifecycle-registry `for_each` over a cold-path pool detach (never per-write). Manifest rebuild + set_self_manifest + routing-cache update run exactly once per detach in the composition-root closure (modules/server.rs:572-633). DoD Perf item PASSES. -->
- [x] **Integration:** integration test at the node boundary exercises the
      full attach → drain → detach workflow (a 2-data-pool node drains pool
      0 to pool 1, detaches it, and the manifest 2→1; GETs keep serving;
      no restart), the **restart-honors-detach** case (config − removed),
      and the non-empty detach rejection. **No load suite is run locally**
      (PIPELINE §6).
<!-- REVIEW: PASS (iteration-2 re-verified 2026-09-09). `cargo test -p oceanfs-node --test pool_detach -- --test-threads=1` → 3/3 (4.45s; implementer-reported 2.05s — timing only). Scenario 1 drain_detach_keeps_serving_and_restart_honors_the_removal: seed pool 0 → drain to Detachable → POST detach 200 → registry 2→1 (pool_by_id(0) None; survivor keeps durable id 1) → manifest 2→1 → post-detach write lands on pool 1 (strictly larger `.dat` count) while detached root0 stays empty → all GETs serve → restart over the same data dir: overlay keeps data-0 out, survivor id 1, all objects read. Scenario 2 no-destructive-failure refusals (409 healthy / 409 non-empty Draining / 400 wal / 404 unknown); pool keeps serving. Scenario 3 reattach_same_identity_after_detach_round_trips: drain + detach → boot 2 (overlay) → re-attach same name+root → 201, freed id 0 reused → steer placement to pool 0 (tests/pool_detach.rs:362 — the iteration-1 gap fix) → post-attach write lands on pool 0 → boot 3 shows BOTH pools (tombstone cleared), ids 0 and 1 in config order, all objects read from both roots. No load/e2e suite run locally (PIPELINE §6). DoD Integration item PASSES. -->

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Resolved Decisions

Recorded 2026-09-09 at final spec close (implementation complete;
independent review PASS, iteration 2). Every question under "Open
Questions for the Implementer" is resolved; each outcome is
cross-referenced to [Deviations (accepted)](#deviations-accepted) where
it deviates from the proposed sketch.

1. **Removed-record storage location & format.** One crash-safe TOML
   file, `{data_dir}/removed_pools.toml` — the state-dir family of
   `membership_state.toml` / g7's wal-replacement marker. A single file
   with a small list, keyed by name+root; written temp → fsync → rename
   (Deviations d).
2. **Record GC.** No boot GC of stale non-matching records — re-attach of
   the same name+root is the authoritative clear (Deviations d). A stale
   record whose name/root no longer matches any config pool is inert and
   harmless until cleared.
3. **Empty-check authority.** Node-side pre-check
   (`pool_detach::pool_is_empty`) runs immediately before
   `PoolRegistry::detach`, which re-checks role/`Detachable` under its
   write lock. NOT an injected predicate — lifecycle and pool registries
   are different lock domains, and a concurrent re-add writes through
   lifecycle (Deviations b).
4. **Root directory handling.** Detach leaves the empty root in place and
   never wipes/renames it — removing the device is the operator's step,
   and a dirty-disk re-attach fails loudly via f8's existing write+read
   probe on attach.
5. **Zero-data-pool runtime state.** Runtime tolerates zero data pools
   after the last detach until `leave(None)` (operator responsibility;
   remote reads still served from other nodes' holders, new writes
   rejected as the manifest advertises no healthy data pool). Only *boot*
   validation requires ≥1 data pool on the post-overlay set (ADR-0031)
   (Deviations f).

## Deviations (accepted)

Recorded 2026-09-09 at final spec close after implementation review
(PASS, iteration 2) — each validated by the stakeholder at/after
implementation.

- **a. Overlay id numbering / survivor id stability.** `config − removed`
  (`PoolRegistry::from_config_skipping`) keeps ORIGINAL config-order ids
  for surviving pools — the removed pool's slot goes vacant (sparse ids) —
  because sealed segments durably stamp `pool_id`; a dense renumber would
  silently re-point survivors. `attach` now assigns the LOWEST FREE id
  (previously `pools.len()`), so a re-attach reuses the freed slot. Boot
  refuses when the post-overlay set has zero data pools (ADR-0031 on the
  post-overlay set; documented detach → leave retirement sequence).
- **b. Empty-check authority.** The d4-emptiness proof (no Reserved entry
  with `pool_id == X` and no Sealed entry with `pool_id == X` where self ∈
  `storage_locations`) is a NODE-side pre-check
  (`oceanfs-node` `pool_detach::pool_is_empty`) immediately before
  `PoolRegistry::detach`, which re-checks role/`Detachable` under its
  write lock. NOT an injected predicate: lifecycle and pool registries
  are different lock domains, and a concurrent re-add writes through
  lifecycle (holding the pools write lock while scanning lifecycle would
  create a new lock order).
- **c. `DetachError` shape** is `{UnknownPool, WrongRole(u32, PoolRole),
  NotDetachable}`; "PoolNotEmpty" is a node-layer 409 outcome from the
  pre-check (the registry cannot know emptiness).
- **d. Removed-pool record store.** Single TOML file
  `{data_dir}/removed_pools.toml` (state-dir family like
  `membership_state.toml`), crash-safe temp→fsync→rename, keyed by
  name+root; corrupt file ⇒ boot error (ambiguous removal intent refuses
  loudly); and NO boot GC of stale non-matching records (re-attach is the
  authoritative clear). Record-loss recovery semantics: advisory overlay,
  config authoritative, no data-loss path (documented in `oceanfs-node`
  `removed_pools.rs`).
- **e. Option A (d4 Deviations f.ii post-`Detachable` re-materialization)
  resolved WITHOUT a `pool_id` remap.** Grounded finding: writes are
  registry-resolved (`DiskSegmentStore::resolve_pool` maps the entry's
  `pool_id` → root), so a copy whose recorded pool was detached fails
  loudly at write time and never stamps the holder set; while the pool is
  still registered, a stale re-add makes `pool_is_empty` false → detach
  refused 409. Covered by data_store test
  `write_to_a_segment_whose_pool_was_detached_fails` + `pool_detach`
  units.
- **f. Runtime zero-data-pool state** after the last detach is tolerated
  (operator responsibility until `leave(None)`); only BOOT refuses
  (post-overlay zero-data check).
- **g. d4 Deviations f.i honored.** Detach emptiness uses the d4
  definition — released Sealed entries keeping `pool_id == source` but
  self not in `storage_locations` do not count as resident.

## Implementation Review (2026-09-09)

**Verdict (iteration 1): FAIL** — 2 DoD items incomplete (Tests,
Integration); product code verified correct; one deterministic defect in
the feature's own integration test plus doc/recording nits. Review
iterations: 1 (of 3 cap).

**Verdict (iteration 2): PASS** — all six iteration-1 gaps fixed and
independently re-verified; the two unchecked DoD items (Tests,
Integration) now carry `[x]` with REVIEW evidence above. The only
remaining step is the LOW doc-process item (flip frontmatter to
`status: done` + record the accepted Deviations), which is the
spec-writer's post-PASS step — see the iteration-2 table below. Review
iterations: 2 (of 3 cap).

### Implementer self-claim cross-reference

| Claim | Verdict | Evidence |
|---|---|---|
| `from_config_skipping` validates original config, skips matching pools by name+root, keeps config-order ids, refuses boot at zero data pools post-overlay | ✅ TRUE | pool/mod.rs:977-1077; overlay unit tests 4/4; Node::start boot-refusal probe passed |
| Attach assigns lowest free id instead of `pools.len()` | ✅ TRUE | pool/mod.rs:1526-1529; `detach_succeeds_on_detachable_pool_and_releases_identity` |
| `PoolRegistry::detach` fast role/Detachable check then removal under write lock; drops pool + metrics + drain/drain_mode entries | ✅ TRUE | drain.rs:726-820 |
| `RemovedPoolStore` crash-safe temp→fsync→rename; corrupt file = load error; idempotent record/clear | ✅ TRUE | removed_pools.rs:80-156; unit tests 7/7 |
| `pool_is_empty` d4 definition + `try_detach_pool` orchestration | ✅ TRUE | pool_detach.rs:84-141; unit tests 10/10 |
| Node::start loads removed records and passes them into the registry build | ✅ TRUE | node.rs:357-366, 445 |
| Attach hook signature `Fn(&StoragePoolConfig)` + clears tombstone; detach hook full orchestration | ✅ TRUE | modules/server.rs:483-510, 572-633 |
| POST `/admin/pools/{id}/detach` + typed callbacks + `remove_gauge`/`remove_counter` | ✅ TRUE | admin.rs:196-207, 483-509, 1291-1349 |
| data_store regression (write to segment whose pool was detached fails) | ✅ TRUE | data_store.rs:771-798 |
| Integration scenarios 1+2 (drain→detach→restart-honors; refusals) | ✅ TRUE | pool_detach.rs tests 1-2 pass single-threaded |
| Integration scenario 3 (hot-swap round-trip) green | ❌ FALSE | `reattach_same_identity_after_detach_round_trips` times out (pool_detach.rs:356-357); test missing `steer()` after re-attach |

### Gaps (prioritized)

1. **HIGH — deterministic integration-test failure (DoD Tests + Integration
   cannot close).** `crates/oceanfs-node/tests/pool_detach.rs:356-357` —
   scenario 3 re-attaches pool 0 but never steers placement toward it; both
   pools live on one temp filesystem, so the survivor (pool 1, earlier free
   snapshot) is always picked by `PlacementPolicy` (max free/weight,
   placement.rs:312-327) and root0 never receives the `.dat`, so
   `wait_until("the re-attached pool receives writes", …)` at line 115
   times out. **Fix:** insert `steer(&node.pool_registry(), 0);` after the
   attach returns 201 (mirroring boot 1 and scenarios 1–2) before the
   `put(&client, &addr, "swap-after", …)`, or assert the write lands on
   either data pool. The product behavior is verified correct with that
   steer (id 0 reuse, tombstone cleared, boot 3 both pools, reads serve).
2. **MEDIUM — the d4 f.ii accounting note / module-doc inaccuracy.**
   `pool_detach.rs:22-26` claims the in-flight re-materialization window
   between the emptiness pre-check and `registry.detach` "is closed by the
   receiver's pool_id remap (the d5 re-materialization rule)" — no such
   remap exists in the code; the actual mechanisms are (a) `pool_is_empty`
   refusing detach over a resident copy (pool_detach.rs:300-321) and (b)
   `resolve_pool` failing writes loudly once the pool id is gone
   (data_store.rs:361-376). Either implement a remap or correct the comment
   to describe the real rule.
3. **LOW — stale attach comment.** `pool/mod.rs:1504-1508` still says the
   id is "`pools.len()` under the lock" and construction happens outside the
   lock; the code now computes lowest-free-id and constructs inside the write
   lock (pool/mod.rs:1514-1541). Update the comment.
4. **LOW — scenario-1 latent test weakness.** `pool_detach.rs:188` waits
   for `!root_dats(&root1).is_empty()` after the post-detach write, but
   root1 is already non-empty from the drain, so the wait passes trivially
   and does not prove the post-detach write landed on pool 1 (it must, being
   the only data pool, but the assertion is vacuous).
5. **LOW — doc process.** The d5 feature doc was left `status: proposed`
   with no accepted-Deviations record. Record the implemented resolutions:
   empty-check authority = node-side pre-check (not an injected registry
   predicate), `DetachError::WrongRole(u32, PoolRole)` shape, removed-record
   path `{data_dir}/removed_pools.toml`, record-GC = clear-on-re-attach only
   (no boot GC), and root-dir left in place.
6. **LOW — `MetricsRegistry::remove_counter` lacks `# Examples`** (admin.rs:
   203-207); examples are non-gating per the Lint note above, and rustdoc
   with `-D warnings` passes.

### Verified green (evidence for the [x] DoD items)

- `cargo build --all-targets -p oceanfs-storage -p oceanfs-node -p
  oceanfs-server` — PASS.
- `cargo test -p oceanfs-storage -p oceanfs-node -p oceanfs-server --lib --
  --test-threads=1` — 530 / 114 / 245 all PASS.
- New d5 unit suites — storage `pool::removed::tests` 3/3,
  `pool::drain::tests::detach_*` 5/5, `pool::tests::overlay_*` 4/4,
  data_store regression 1/1; node `removed_pools::tests` 7/7,
  `pool_detach::tests` 10/10.
- `cargo test -p oceanfs-node --test pool_detach -- --test-threads=1` —
  2/3 (scenario 3 fails, gap 1).
- Regression integration (single-threaded): runtime_attach, pool_registry,
  pool_drain_state, data_pool_placement, placement_policy, intra_node_drain
  3/3, cluster_drain 3/3, node_lifecycle — all PASS.
- `cargo clippy --workspace --lib -- -D warnings` — PASS.
- `cargo fmt -- --check` — PASS.
- `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` (3 crates) — PASS.
- No load/e2e suite run locally (PIPELINE §6).

### Iteration 2 — gap fixes verified (2026-09-09)

| Iteration-1 gap | Fix claim | Verdict | Evidence |
|---|---|---|---|
| HIGH — scenario 3 never steered placement after re-attach (tests/pool_detach.rs:356) | `steer(&node.pool_registry(), 0)` after the 201 attach | ✅ TRUE | tests/pool_detach.rs:362 (between the 201 assert :354 and the put :364); suite passes 3/3 (4.45s) |
| MEDIUM — module doc claimed a nonexistent "receiver's pool_id remap" (src/pool_detach.rs:22-26) | Doc now describes the real guards | ✅ TRUE | src/pool_detach.rs:19-31 — node-side pre-check; "not by a pool_id remap (none exists — writes are registry-resolved)"; a stale push while the pool is still registered ⇒ `pool_is_empty` false ⇒ detach refused (`PoolNotEmpty`); after removal the write fails loudly at `resolve_pool` |
| LOW — stale attach-id comment ("`pools.len()` under the lock", pool/mod.rs:1504-1508) | Comment describes lowest-free under the write lock | ✅ TRUE | pool/mod.rs:1420 + 1503-1529 — id is the lowest id not already registered, computed inside the write lock; `pools.len()` only when no holes |
| LOW — vacuous wait on root1 (tests/pool_detach.rs:188) | Wait on a strictly larger count + root0 empty assert | ✅ TRUE | tests/pool_detach.rs:187-193 — `pool1_dats_before` recorded, `len() > pool1_dats_before`, and root0 asserted empty |
| LOW — d5 doc left `status: proposed`, no Deviations record | Spec-writer records after reviewer PASS | ⏳ PENDING (by design) | frontmatter still `status: proposed`; spec-writer flips to done and records the accepted deviations (empty-check authority = node-side pre-check; `DetachError` shape {UnknownPool, WrongRole(u32, PoolRole), NotDetachable} + node-layer `PoolNotEmpty`→409; sparse overlay ids + lowest-free attach; single `{data_dir}/removed_pools.toml`, corrupt⇒boot error, no boot GC; no pool_id remap — Option A resolution; zero-data-pool tolerated at runtime, boot refuses post-overlay) |
| LOW — `MetricsRegistry::remove_counter` lacked `# Examples` (admin.rs:203-207) | Example added | ✅ TRUE | admin.rs:205-217 (`# Examples`); server doctests 18/18 (4 ignore-tagged) |

All gates re-run this iteration: `cargo build --all-targets -p
oceanfs-storage -p oceanfs-node -p oceanfs-server` PASS; lib suites
single-threaded 530 / 114 / 245 all PASS; `cargo test -p oceanfs-node
--test pool_detach -- --test-threads=1` 3/3 PASS; server doctests 18/18;
`cargo fmt -- --check` PASS; `cargo clippy -p oceanfs-storage -p
oceanfs-node -p oceanfs-server --lib -- -D warnings` PASS;
`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` PASS on the three crates.
`cargo clippy --all-targets -- -D warnings` still flags ONLY pre-existing
test-code hygiene in files the d5 changeset did not touch
(modules/storage.rs, pool/placement.rs, segment/event_wal.rs,
metadata/store.rs, tests/wal_pool_recovery.rs) — non-gating per the Lint
note. No load/e2e suite run locally (PIPELINE §6).

## Implementation (final gates, 2026-09-09)

Feature closed after review PASS (iteration 2); the open questions and
stakeholder-validated resolutions are recorded under Resolved Decisions
and Deviations (accepted) above. Final gate results, re-verified by the
independent reviewer:

- **Lib tests** (single-threaded): oceanfs-storage 530, oceanfs-node 114,
  oceanfs-server 245 — all PASS.
- **Integration** `cargo test -p oceanfs-node --test pool_detach --
  --test-threads=1` — 3/3: drain→detach→restart-honors with survivor id
  preserved; no-destructive-failure refusals (409 healthy / 409 non-empty
  Draining / 400 wal / 404 unknown); hot-swap re-attach round-trip over
  two restarts (tombstone cleared, freed id reused, both pools on boot 3).
- **Lint/docs**: `cargo fmt -- --check` clean; `cargo clippy --workspace
  --lib -- -D warnings` clean; `RUSTDOCFLAGS="-D warnings" cargo doc
  --no-deps` clean on oceanfs-storage/node/server.
- **Doctests**: oceanfs-storage 118, oceanfs-server 18.
- No load/e2e suite run locally (PIPELINE §6).
