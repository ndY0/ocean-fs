---
feature: "Composition-Root Decomposition — Program Coordination"
epic: "refactoring/composition-root-decomposition"
status: proposed
priority: critical
owner: ""
dependencies:
  - epic: refactoring
    reason: Part of the 2026-09 review triage program (wave 2, item 1); see review-2026-09-roadmap.md
adr:
  - 0031-remove-single-datadir-legacy-mode
created: 2026-09-04
updated: 2026-09-04
---

# Composition-Root Decomposition — Program Coordination

> **This is the coordination document for the composition-root epic.** If
> you are implementing any feature under this epic, read this first — it
> tells you where your work sits in the whole, what must exist before you
> start, and what must not regress while you work. The per-feature docs
> are the authority for your feature; this document is the map.

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
    network.rs            # NetworkModule::build(cfg, ...) -> NetworkModule handles
    background.rs         # move spawn_background_tasks + task handles here
  adapters.rs             # PrefetchStoreAdapter, WorkerQueueSink, NodeLeaveHandler
```

| Builder | Owns (moved out of `start()`) | Produces |
|---|---|---|
| `StorageModule::build` | registry + role-pinned paths (sec 0), metadata store (1), accel (2), ring/routing (3), lifecycle registry + coordinator + event WAL/checkpoint (6b), pools + sealer (6), **single** shared segment store, replicator (6c), I/O infra + reader (11), startup recovery (6a) | `StorageModule { registry, lifecycle, sealer, event_wal, checkpoint, reader, replicator, ... }` |
| `DurabilityModule::build` | GC, AE (+ merkle tree), scrub, reaper, heal pipeline, reconcile, re-rep worker + dispatcher (7), op timeouts (7d) — and later the ADR-0017 `DurabilityScheduler` wrapper | `DurabilityModule { gc, ae, scrub, reaper, heal, reconcile, rep_worker, ... }` |
| `ServerModule::build` | caches + policies (8), prefetch (9), adapters (10), coordinators (write/read), S3 handler, admin handler (12-13), gRPC services (15) | `ServerModule { s3, admin, grpc_services, ... }` |
| `NetworkModule::build` | membership + pools (4/5), HTTP bind (14), gRPC bind (15), membership plane bind (15b), bootstrap + join (15c), manifest (15d), routing-cache subscriber (15e) | `NetworkModule { http_handle, grpc_handle, membership, ... }` |
| `BackgroundModule::spawn_all` | all `tokio::spawn` loops currently inline (16, 16b-e, 17) | `BackgroundTasks` |

`Node::start()` shrinks to: validate config → build storage → build
durability → build server → build network → recover/join gate → spawn
background → return `Node`. Shutdown (`node.rs:3107-3213`) stays on
`Node` but moves its hard-coded timeouts to config (review #71).

## Feature DAG

```
c1 split-storage-builder
 └── c2 split-durability-builder
      └── c3 split-server-builder
           └── c4 split-network-builder
                └── c5 background-spawn-extraction + start() slimming
```

Ordering: **c1 → c2 → c3 → c4 → c5**. Each is a pure-move refactor: the
builder returns the same `Arc`s the inline code produced; behavior is
identical; the regression bar is "node boots, e2e write/read passes,
existing node tests pass." `c5` (the final `start()` slimming + guideline
update) only makes sense once all four builders exist. `c1` includes the
`NodeLeaveHandler`/`data[76..]` bug fix (review #35) and the `start()`
store-consolidation precondition (one shared `DiskSegmentStore`, review
#57/#59/#60) because the builder is where the 8 store instances become 1.

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

- [ ] `Node::start()` under ~300 lines; every background loop spawned
      from a module, none in `start()`.
- [ ] `node.rs` under ~2,000 lines (from 4,458); no top-of-file adapter
      structs (moved to `adapters.rs`).
- [ ] **Two** shared store instances (one `DiskSegmentStore` + one
      `DiskSegmentShardStore`) constructed in `StorageModule` and shared by
      all durability subsystems. NOTE: this is the c1 precondition — the
      final **one** unified store (review #57/#59/#60 closure) is the
      store-unification epic's f3 end state (ADR-0032), NOT this epic.
      See `refactoring/store-unification/` and roadmap §4 sequencing.
- [ ] Bug fixes from review #35/#64 ride in the wave-0/1 epic, NOT here
      (wave-0/1 f1 B1/B2 owns them; c1 only supersedes the leave handler
      per review #34).
- [ ] All existing node tests + e2e write/read green; no behavior delta.
- [ ] Guideline update: "composition root §4.1 — no `tokio::spawn` in
      `start()`; modules expose their own `spawn`."

## References

- Review comments: `node.rs:8,163,202,369,546,594,715,1233,1269,1285,1450,
  1501,1932,2356,2620,3141`
- Guidelines `architecture.md §4.1` (composition root), §3.3 (file per type)
- Triage program: `features/refactoring/review-2026-09-roadmap.md`
- In-flight: g7/g8 in `features/disk-resilience-healing/` must land after
  this epic + the store/scheduler epics
