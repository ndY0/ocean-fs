---
feature: "Composition-Root Decomposition — Program Coordination"
epic: "refactoring/composition-root-decomposition"
status: done
priority: critical
owner: ""
dependencies:
  - epic: refactoring
    reason: Part of the 2026-09 review triage program (wave 2, item 1); see review-2026-09-roadmap.md
adr:
  - 0031-remove-single-datadir-legacy-mode
created: 2026-09-04
updated: 2026-09-05
---

# Composition-Root Decomposition — Program Coordination

> **This is the coordination document for the composition-root epic.** If
> you are implementing any feature under this epic, read this first — it
> tells you where your work sits in the whole, what must exist before you
> start, and what must not regress while you work. The per-feature docs
> are the authority for your feature; this document is the map.

> **STATUS: EPIC COMPLETE (2026-09-05).** c1, c2, the c3a prerequisite
> seam (seal-pipeline relocation storage-side, commit `489397a`), c3, c4,
> and c5 all LANDED — each with independent review PASS (c5: iteration
> 1, 0 blocking gaps). `Node::start()` is 122 lines with zero
> `tokio::spawn`; `node.rs` is 1,744 lines. This document is retained as
> the coordination record; nothing here is pending — see the Landing
> order and the Acceptance bar below for the per-item gate record.

---

## Summary

`crates/oceanfs-node/src/node.rs` is **4,458 lines** and
`Node::start()` — the composition root — spans **~565→2795 (~2,230
lines)** of sequential construction: stores, pools, lifecycle machinery,
durability workers, caches, coordinators, gRPC services, background
spawns, and shutdown wiring, interleaved with inline closures, adapter
structs defined at file top (`PrefetchStoreAdapter`, `WorkerQueueSink`,
`NodeLeaveHandler`), and ~20 numbered `// ----` sections whose numbering
is internally inconsistent (`6` appears four times; `6a/6b/6c` collide
with the later `16b/16c`).

Review comments `node.rs:546 (#37)`, `node.rs:369 (#36)`, `node.rs:8
(#33)`, `node.rs:1932 (#68)`, and `node.rs:2620 (#70)` all target this.
The **decision** (2026-09-04 triage session, stakeholder + architect):

> **Do NOT adopt a compile-time DI framework (shaku et al.). Decompose
> `Node::start()` into plain module builders** — one function per
> subsystem bundle, each returning a typed struct, each owning its own
> `spawn`. The builder-chain `with_*` style survives *inside* modules.
> Add a guideline: **no `tokio::spawn` in `Node::start()`** — modules
> expose their own spawn entry points.

Rationale (from the triage session):

1. The problem is not missing DI — it is missing *decomposition*; the
   explicit graph at startup is a feature (guidelines §4.1), and a
   container would make it implicit.
2. `with_*` is already the DI mechanism; it has been applied
   inconsistently (sometimes builder, sometimes inline spawn, sometimes
   top-of-file adapter structs). The fix is consistency + boundaries.
3. DI containers fight Rust ownership/lifetimes and add macro tax for
   zero runtime-swap benefit in a boot-time composition root.

## Target structure

```
crates/oceanfs-node/src/
  node.rs                 # Node struct, start(), shutdown(), accessors
                          # start() = ~5 builder calls + readiness gate
  modules/                # NEW
    storage.rs            # StorageModule::build(cfg, paths) -> StorageModule
    durability.rs         # DurabilityModule::build(cfg, storage) -> DurabilityModule
    server.rs             # ServerModule::build(cfg, storage, durability) -> ServerModule
    membership.rs         # MembershipModule::build + start_plane_and_join (membership plane)
    data_plane.rs         # DataPlaneModule::build + serve (data-plane HTTP/gRPC binds)
    background.rs         # move spawn_background_tasks + task handles here
                          # → AS LANDED (c5): bundler only — spawn_all glue
                          #   + cancellable metric poller (see note below)
  adapters.rs             # PrefetchStoreAdapter, WorkerQueueSink, NodeLeaveHandler
                          # → AS LANDED: no adapters.rs — the two surviving
                          #   adapters live in modules/server.rs; NodeLeaveHandler
                          #   deleted by c1 (see note below)
```

| Builder | Owns (moved out of `start()`) | Produces |
|---|---|---|
| `StorageModule::build` | registry + role-pinned paths (sec 0), metadata store (1), accel (2), ring/routing (3), lifecycle registry + coordinator + event WAL/checkpoint (6b), pools + sealer (6), **single** shared segment store, replicator (6c), I/O infra + reader (11), startup recovery (6a) | `StorageModule { registry, lifecycle, sealer, event_wal, checkpoint, reader, replicator, ... }` |
| `DurabilityModule::build` | GC, AE (+ merkle tree), scrub, reaper, heal pipeline, reconcile, re-rep worker + dispatcher (7), op timeouts (7d) — and later the ADR-0017 `DurabilityScheduler` wrapper | `DurabilityModule { gc, ae, scrub, reaper, heal, reconcile, rep_worker, ... }` |
| `ServerModule::build` | caches + policies (8), prefetch (9), adapters (10), coordinators (write/read), S3 handler, admin handler (12-13), gRPC services (15) | `ServerModule { s3, admin, grpc_services, ... }` |
| `MembershipModule::build` + `start_plane_and_join` | membership + rejoin state + incarnation bump (4/4a, ADR-0022), peer-side routing cache (4b), membership-plane pool + announce address (5/ADR-0028 D1), gossip/probe construction (re-seated from c3), membership-plane bind (15b), `membership.start()` + metrics, bootstrap + join/rejoin + fallback-seed snapshot (15c), manifest declaration + cache self-seed (15d), routing-cache subscriber (15e), `cluster_ready_gate_opens` | `MembershipModule { membership, manifest_cache, membership_state_store, announce_incarnation, is_cluster_node, grpc_addr }` |
| `DataPlaneModule::build` + `serve` | data-plane `ConnectionPool` + `RpcConfig` (5), HTTP bind (14), data-plane gRPC bind + 4-line `.add_service` assembly (15) | `DataPlaneModule { pool, grpc_addr }`; `serve` → `BoundDataPlane { server_addr, grpc_addr, http_shutdown, grpc_shutdown, grpc_server_handle }` |
| `BackgroundModule::spawn_all` | all `tokio::spawn` loops currently inline (16, 16b-e, 17) | `BackgroundTasks` |

`Node::start()` shrinks to: validate config → infra (§0-3) → build
membership + data plane (early module builds) → build storage + durability
+ §7b recovery → §11 hinted handoff + ready gate → build server →
`data_plane.serve` (HTTP/gRPC binds) → `membership.start_plane_and_join`
(membership-plane bind + join) → spawn background → return `Node`.
Bind-before-join is preserved as the documented serve →
start_plane_and_join sequence. Shutdown (`node.rs:3107-3213`) stays on
`Node` but moves its hard-coded timeouts to config (review #71).

> **As landed (2026-09-05, epic close — full record in the c5 doc).**
> The sketch above was the working target; the landed shape differs in
> two placements: there is **no `adapters.rs`** — the two surviving
> adapters (`PrefetchStoreAdapter`, `WorkerQueueSink`) live inside
> `modules/server.rs` (`NodeLeaveHandler` was deleted, not moved, by c1)
> — and **`modules/background.rs` is a bundler only**:
> `spawn_all` (background.rs:48) glues the module-owned spawn entries
> (`DurabilityModule::spawn_loops` durability.rs:479,
> `StorageModule::spawn_loops` storage.rs:716,
> `modules/server.rs::spawn_prefetch_loop` server.rs:708,
> `MembershipModule::spawn_ready_gate` membership.rs:500,
> `DataPlaneModule::serve`) and owns the cancellable metric poller; it
> spawns no loops itself. Landed measures: `node.rs` **1,744 lines**;
> `start()` **122 lines** (node.rs:327-448) with **zero `tokio::spawn`**;
> `BackgroundTasks` holds 16 `Option<JoinHandle<()>>` handles built via
> `new()`; shutdown (node.rs:990-1092) drains **all** handles under
> configurable grace — `shutdown_grace_secs` (10) /
> `shutdown_fast_grace_secs` (5), defaults preserving the old 10s/5s.

## Feature DAG

```
c1 split-storage-builder
 └── c2 split-durability-builder
      └── c3a seal-pipeline relocation storage-side (c3 Option-A prerequisite)
           └── c3 split-server-builder
                └── c4 split-network-builder → membership.rs + data_plane.rs (ONE pass)
                     └── c5 background-spawn-extraction + start() slimming
```

Ordering: **c1 → c2 → c3a → c3 → c4 → c5**. Each c1–c5 builder step is a
pure-move refactor: the builder returns the same `Arc`s the inline code
produced; behavior is identical; the regression bar is "node boots, e2e
write/read passes, existing node tests pass." `c5` (the final `start()`
slimming + guideline update) only makes sense once the c1–c4 extractions
exist. **c4 implements BOTH post-c4 node-side modules —
`MembershipModule` (`modules/membership.rs`) + `DataPlaneModule`
(`modules/data_plane.rs`) — in one pass, split along the ADR-0028 planes;
there is no c4a/c4b split and `modules/network.rs` is retired**
(user-approved amendment 2026-09-05; full record in the c4 doc). The
amendment also re-seats c3's `gossip_service`/`probe_service` (and drops
`ServerModule`'s `membership_pool` build param) — recorded as a deviation
on the c3 doc. `c3a` is the user-approved Option-A prerequisite seam for c3 (c3
planning, 2026-09-04): the seal pipeline had to move storage-side so
`run_startup_recovery()` no longer depends on a server object — it is NOT
one of the c1–c5 builder steps and does not change what c3 extracts.

**Landing order (per-feature gate record):**

- **c1 split-storage-builder — LANDED 2026-09-04 (review PASS,
  iteration 3).** `StorageModule` extracted; stores consolidated 8 → 2;
  `NodeLeaveHandler` deleted (B1 closed). Approved plan dispositions
  recorded in the c1 doc (Scope DISPOSITION + deviations note).
- **c2 split-durability-builder — LANDED 2026-09-04 (review PASS — 0
  blocking gaps; 2 LOW items, both fixed by the implementer; node lib
  re-verified 66 passed).** `DurabilityModule` extracted (§7; 12-handle
  bundle); user-approved deviations D1–D5 recorded once in the c2 doc's
  Accepted Deviations.
- **c3a seal-pipeline relocation (c3 Option-A prerequisite) — LANDED
  2026-09-05 (review PASS, iteration 2; commit `489397a`).** Storage-side
  seal pipeline (`oceanfs-storage::segment::seal_pipeline`); `SealingWork`
  carries entries cleared only on successful enqueue; node order storage →
  durability → seal pipeline → recovery → server construction. Full
  details in its own doc (`c3a-seal-pipeline-relocation.md`); it
  unblocked c3, which LANDED 2026-09-05 (next bullet).
- **c3 split-server-builder — LANDED 2026-09-05 (review PASS, iteration
  1 — 0 blocking gaps).** `ServerModule` extracted (§8-13 + §15
  construction; node.rs 3465→2937, start() ~1592→~1179); binds + §15b-e
  + §16-17 stay for c4/c5; deviations recorded in the c3 doc.
- **c4 split-network-builder (membership + data-plane modules) — LANDED
  2026-09-05 (review PASS, iteration 1 — 0 blocking gaps).** Planes
  split: `MembershipModule` (identity/rejoin/plane/gossip+probe/
  bootstrap) + `DataPlaneModule` (pool + HTTP/gRPC binds); gossip/probe
  re-seated from c3 (deviation #9); review #64 closed (strict
  `membership_listen_addr`); node.rs 2937→2606.
- **c5 background-spawn-extraction — LANDED 2026-09-05 (review PASS,
  iteration 1 — 0 blocking).** start() 720→122 lines, no `tokio::spawn`
  in start(); node.rs 1744; module-owned spawns + bundler; shutdown
  drains all handles under configurable grace; reviews #68/#71 +
  guideline §4.1 closed. **EPIC COMPLETE.**

`c1` includes the `NodeLeaveHandler` supersession — the handler is
**deleted** (review #34), which closes wave-0/1 f1 B1 (review #35; B1 was
deferred to this deletion by DECISION 2026-09-04, see
`review-wave-0-1/f1-correctness-bug-batch.md`) — and the `start()`
store-consolidation precondition (one shared `DiskSegmentStore` per
kind, review #57/#59/#60) because the builder is where the 8 store
instances become the 2 shared ones (one `DiskSegmentStore` + one
`DiskSegmentShardStore` — see the acceptance bar; the final one-unified-
store state is store-unification f3, ADR-0032).

## Constraints / non-goals

- **No DI framework.** Plain functions + typed bundles only.
- **No behavior change** in any subsystem. This is a move, not a
  redesign; the ADR-0017 scheduler (wave 2 ③) is a *separate* epic that
  lands after the modules exist.
- **No crate-boundary changes.** Everything stays in `oceanfs-node`
  (guidelines §4.1 composition root).
- Do not renumber the `// ----` sections during c1–c4; do it once in c5
  when the sections have moved.

## Acceptance bar (epic DoD)

- [x] `Node::start()` under ~300 lines; every background loop spawned
      from a module, none in `start()`.
      Verified (c5): `start()` = node.rs:327-448, 122 lines; 0
      `tokio::spawn` in `start()`; loops module-owned
      (`DurabilityModule::spawn_loops`, `StorageModule::spawn_loops`,
      `modules/server.rs::spawn_prefetch_loop`,
      `MembershipModule::spawn_ready_gate`, `DataPlaneModule::serve`).
- [x] `node.rs` under ~2,000 lines (from 4,458); no top-of-file adapter
      structs (moved to `adapters.rs`).
      Verified (c5): node.rs = 1,744 lines; no adapter structs at file
      top — `PrefetchStoreAdapter` + `WorkerQueueSink` live in
      `modules/server.rs` (no `adapters.rs` was created;
      `NodeLeaveHandler` deleted, not moved, by c1).
- [x] **Two** shared store instances (one `DiskSegmentStore` + one
      `DiskSegmentShardStore`) constructed in `StorageModule` and shared by
      all durability subsystems. NOTE: this is the c1 precondition — the
      final **one** unified store (review #57/#59/#60 closure) is the
      store-unification epic's f3 end state (ADR-0032), NOT this epic.
      See `refactoring/store-unification/` and roadmap §4 sequencing.
      Verified (c1): stores consolidated 8 → 2 in `StorageModule::build`.
- [x] Review #64 (B2) is fixed in the wave-0/1 epic, NOT here. Review
      #35 (B1) was **deferred to this epic** (DECISION 2026-09-04): c1's
      `NodeLeaveHandler` deletion closes it — the disposition is recorded
      in wave-0/1 f1 and in c1's doc.
      Verified: #35 B1 closed by c1 (2026-09-04); #64 B2 closed by
      wave-0/1 f1 (2026-09-04) + c4's strict `membership_listen_addr`
      parse (2026-09-05) — dispositions in the wave-0/1 f1, c1, and c4
      docs.
- [x] All existing node tests + e2e write/read green; no behavior delta.
      Verified (per-feature, c1–c5): node lib 66, doc 38, all 30
      integration suites, e2e allowlist (crash_restart, wal_recovery,
      segment_lifecycle, cluster_lifecycle, cluster_write_path,
      cluster_read_path, garbage_collection, rewrite_leak_test) green;
      no load suites (PIPELINE.md §6).
- [x] Guideline update: "composition root §4.1 — no `tokio::spawn` in
      `start()`; modules expose their own `spawn`."
      Verified (c5): guidelines/architecture.md §4.1 updated — no
      `tokio::spawn` in `Node::start()`; module-owned spawn entries +
      bundler (`spawn_all`); clippy/rustdoc/fmt clean.

## References

- Review comments: `node.rs:8,163,202,369,546,594,715,1233,1269,1285,1450,
  1501,1932,2356,2620,3141`
- Guidelines `architecture.md §4.1` (composition root), §3.3 (file per type)
- Triage program: `features/refactoring/review-2026-09-roadmap.md`
- In-flight: g7/g8 in `features/disk-resilience-healing/` must land after
  this epic + the store/scheduler epics
