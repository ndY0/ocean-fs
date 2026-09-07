---
feature: "Pool Detach & Drop (Inverse of f8 Attach)"
epic: "disk-resilience-scale"
status: proposed
priority: high
owner: ""
dependencies: ["d3-intra-node-drain"]
adr: [0036, 0029, 0031]
perf: []
created: 2026-09-07
updated: 2026-09-07
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

- [ ] **Code:** `cargo build --all-targets` succeeds in `oceanfs-storage`,
      `oceanfs-node`, `oceanfs-server`.
- [ ] **Tests:** `cargo test -p oceanfs-storage -p oceanfs-node -p
      oceanfs-server --lib -- --test-threads=1` passes; the Scope scenario
      list is green — including the **no-destructive-failure** detach
      rejections (non-empty / not-`Detachable` pools are refused and keep
      serving), the role guard, the released-name/root re-attach
      (hot-swap) case, and the integration scenario: 2-pool node drains a
      pool, detaches it, manifest 2→1, reads continue, no restart.
- [ ] **Docs:** Every `pub` item has `# Examples`; `#![deny(missing_docs)]`
      passes; the **persistent removed-overlay rule** (`config − removed`;
      record keyed by name+root; re-attach clears) and the
      zero-data-pool-at-boot restriction (ADR-0031, post-overlay set) are
      documented on `detach` and `from_config`.
- [ ] **ADR:** ADR-0036 D1 (detach = inverse of f8; empty-pool only; no
      restart) + D6 (detach accepted only on an empty/`Detachable` pool)
      + **D8 (persistent removed-overlay)** satisfied; ADR-0029 §D8
      (runtime pool topology change via admin) and ADR-0031 (role pinning;
      wal/meta/hints replacement is the g7/g8 path; pools mandatory at
      boot) satisfied.
- [ ] **Perf:** frontmatter `perf: []`; prose constraints: detach is a
      rare admin op; the registry removal is a short write-lock section
      (perf rule 7.1); the manifest rebuild is once-per-change
      (`pool_manifest.rs` module doc, perf rule 2.4).
- [ ] **Integration:** integration test at the node boundary exercises the
      full attach → drain → detach workflow (a 2-data-pool node drains pool
      0 to pool 1, detaches it, and the manifest 2→1; GETs keep serving;
      no restart), the **restart-honors-detach** case (config − removed),
      and the non-empty detach rejection. **No load suite is run locally**
      (PIPELINE §6).

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Open Questions for the Implementer

- **Removed-record storage location & format.** Same state dir family as
  `membership_state.toml` and g7's wal-replacement marker; confirm the
  exact path and whether a single overlay file or a directory of records is
  cleaner (single file with a small list is the sketch's default).
- **Record GC.** A stale record (config edited to drop the pool) is inert;
  GC it at boot or leave it until a re-attach of the same name+root needs
  the clear path? (Sketch: clear-on-re-attach is authoritative; boot GC of
  never-matching records is a hygiene nicety.)
- **Empty-check authority.** The registry does not know the lifecycle
  registry; detach must re-verify emptiness at mutation time. Decide
  between an injected predicate vs. a node-side pre-check racing the
  registry lock (the injected predicate under the registry write lock is
  the race-free option).
- **Root directory handling.** Detach leaves the empty root in place
  (operator removes the device). Confirm no hidden requirement to wipe or
  rename the root (e.g. to make a re-attach of a *dirty* disk fail loudly —
  the f8 probe already write+read-probes on attach).
- **Zero-data-pool runtime state.** After the last data pool detaches, the
  node runs with zero data pools until `leave`. Confirm the node tolerates
  this at runtime (reads of remote objects may still be served from other
  nodes' holders; new writes are rejected as the manifest advertises no
  healthy data pool) and that only *boot* validation requires ≥1 data pool
  (ADR-0031) — on the post-overlay set.

## Deviations (accepted)

None yet — this document is proposed. Expected-deviation candidates: the
removed-record storage format and path, the record-GC rule, the
empty-check wiring, and the root-directory rule. Record each with its
resolution at implementation. The config-drop semantics are **settled** by
the 2026-09-07 decision (persistent removed-overlay — Option P of the
ADR-0036 discussion), not an open item.
