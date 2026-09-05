---
feature: "c3: Extract ServerModule Builder"
epic: "refactoring/composition-root-decomposition"
status: done
priority: high
owner: ""
dependencies:
  - feature: c2-split-durability-builder
    epic: refactoring/composition-root-decomposition
    reason: Server coordinator wiring consumes the durability hint-applier / heal-data-store paths built by c1+c2
adr: []
perf: []
created: 2026-09-04
updated: 2026-09-05
---

# c3: Extract ServerModule Builder

## Summary

Extract construction of caches, prefetch, adapters, coordinators, S3 +
admin handlers, and gRPC service instances (node.rs sections 8, 9, 10,
12, 13, 15) into `modules/server.rs`. Returns a `ServerModule` bundle:
the axum router pieces, the write/read coordinators (with their notifier
closures wired to c1's replicator + AE), and the gRPC services list.

```rust
pub struct ServerModule {
    pub write_coordinator: Arc<WriteCoordinator>,
    pub read_coordinator: Arc<ReadCoordinator>,
    pub s3_handler: S3Handler,
    pub admin_handler: AdminHandler,
    pub grpc_services: Vec<tonic::service::NamedService>, // or explicit fields
    pub metrics: Arc<MetricsRegistry>,
}
```

## Prerequisite — c3a seal-pipeline relocation (LANDED 2026-09-05)

c3's Option-A prerequisite seam — moving the seal pipeline storage-side —
landed 2026-09-05 as commit `489397a` (review PASS, iteration 2); it is
recorded in its own feature doc:
[`c3a-seal-pipeline-relocation.md`](./c3a-seal-pipeline-relocation.md)
(status: done). Consequences for this feature:

- The seal-worker drain loop no longer lives in `WriteCoordinator`: the
  pipeline is spawned storage-side via `StorageModule::start_seal_pipeline()`
  **before** `run_startup_recovery()` and all server construction.
- The sealed-segment notifier wiring (which this feature's Summary
  attributed to the write coordinator's `with_*` chain) is now the
  AE-continuous + replicator fan-out closure built at the pipeline spawn
  point and injected into `oceanfs-storage::segment::seal_pipeline` as a
  `SealedSegmentNotifier` — the coordinator's `segment_sealed_notifier`
  field/setter is deleted.
- c3's extraction therefore moves the **remaining** server sections
  (caches/policies, prefetch, adapters, coordinators, S3/admin handlers,
  gRPC services — node.rs sections 8–15 as scoped below) with no
  seal-worker or notifier-field surface to carry over from the
  coordinator.

This feature LANDED 2026-09-05 (status: done) with the Scope and DoD
below unchanged; c3a was a prerequisite only, not a c3 slice. The
extraction's landed shape and reviewer-verified deviations are recorded
in "Implementation Notes / Accepted Deviations" at the foot of this doc.

## Scope

### In Scope
- Move cache/policy construction (L1/L2/negative) + prefetch engine into a
  `Caches` sub-struct.
- Move the read/write coordinator builders and their `.with_*` chains
  (write coordinator notifiers → replicator/AE; read coordinator routing
  hints) into this module.
- Move the S3/admin handler construction and the notifier closure wiring.
- Move gRPC service construction (segment/healing/cache/scrub/gossip/
  probe) here; the actual `tonic` bind stays in c4 (network).

### Out of Scope
- HTTP/gRPC listener binding and membership plane bootstrap (c4).
- Backend behavior changes (including the write-coordinator
  `shard_small`/`shard_standard` dead-field removal — that is wave-4
  cleanup, tracked separately; do not conflate).

## Definition of Done

- [x] `node.rs` shrinks by the server/handler sections; construction moved
      to `modules/server.rs` — node.rs 3465 → 2937 lines; `start()` body
      ~1592 → ~1179.
- [x] Coordinators' sealed-segment notifier still fans out to replicator +
      AE (behavior preserved) — that wiring moved storage-side in the c3a
      prerequisite (injected `SealedSegmentNotifier` at the seal-pipeline
      spawn point); nothing to carry over from the coordinator.
- [x] gRPC services constructed in the same order and with the same
      message-size caps (segment + healing 64 MiB decode cap; cache/scrub
      default).
- [x] Node tests green: lib 66 passed, doc 38 passed, all integration
      suites green. E2e write/read green on the sanctioned allowlist
      (crash_restart, wal_recovery, segment_lifecycle,
      cluster_lifecycle, cluster_write_path, cluster_read_path,
      garbage_collection, rewrite_leak_test) — no load suites run
      locally (PIPELINE.md §6).

## Implementation Notes / Accepted Deviations

Recorded at landing (2026-09-05). Reviewer-verified and behavior-neutral:
the extraction is a pure move, and each note documents where the landed
shape differs from the plan sketch in the Summary above or from the c1/c2
precedent.

1. **Minimal `ServerModule` bundle.** Landed fields: `router`
   (`axum::Router`), `grpc` (`DataPlaneServices`: the four tonic-wrapped
   data-plane services with their caps), `gossip_service`,
   `probe_service`, `prefetch_engine`. The Summary sketch's
   `s3_handler`/`admin_handler`/`write_coordinator`/`read_coordinator`-
   as-fields shape is impossible — the handlers are consumed by the axum
   `Router::merge` inside `build()`, and the coordinators/caches are
   module internals (no post-start consumer exists; no field was added to
   `Node`).
2. **gRPC service wrapping/caps live in the module** (the chosen option):
   services are wrapped and decode-capped inside `modules/server.rs`;
   `start()` keeps only the 4-line `.add_service` tonic assembly (segment
   /healing/cache/scrub) at the §15 bind site.
3. **`build()` is synchronous** — `pub(crate) fn build(...) ->
   Result<Self, String>` — unlike c1/c2's async builds: the moved span
   contains no awaits, and the only fallible step is the re-rep worker's
   queue-sender creation.
4. **The metrics `Arc` is created node-side before the build call.**
   Module-owned series (3 caches, s3_handler, healing_service) register
   inside `build()`; the remaining node-side series register right after
   the call returns (registry insert order is not observable).
5. **The `on_pool_attached` closure is built inside the module**,
   self-contained over its inputs: membership, `storage.registry`,
   `manifest_cache`, `announce_incarnation`, and metrics.
6. **`PrefetchStoreAdapter` and `WorkerQueueSink` moved into
   `modules/server.rs`** alongside their uses; the root-level
   `adapters.rs` consolidation remains c5's, per the epic README target
   structure.
7. **The §15 construction span's explanatory comments travelled with the
   code** into `modules/server.rs` (hint-materialization rationale, fleet
   disk-fill comment, gRPC message-size-cap rationale), so the
   construction context is not lost in the move.
8. **Reviewer PASS, iteration 1 (0 blocking gaps).** Verification matrix:

   | Gate | Result |
   |---|---|
   | `cargo build` (workspace, `--all-targets`) | PASS |
   | `cargo clippy --lib -- -D warnings` | PASS |
   | rustdoc / doc tests (`#![deny(missing_docs)]`) | PASS (38 doc tests) |
   | `cargo fmt` | PASS |
   | node lib tests | PASS (66) |
   | node integration suites | PASS (all suites) |
   | e2e write/read (allowlist: crash_restart, wal_recovery, segment_lifecycle, cluster_lifecycle, cluster_write_path, cluster_read_path, garbage_collection, rewrite_leak_test) | PASS — no load suites (PIPELINE.md §6) |

9. **Post-landing follow-up (recorded by c4, 2026-09-05): the
   `gossip_service`/`probe_service` re-seat to the membership module.**
   c3 landed with `gossip_service` + `probe_service` constructed inside
   `ServerModule::build` (reviewer-verified, notes 2/7 above) and with a
   `membership_pool` build parameter. The user-approved c4 amendment
   splits the remaining node-side wiring along the ADR-0028 planes, and c4
   re-seats gossip/probe construction into
   `MembershipModule::start_plane_and_join` (`modules/membership.rs`):
   the two services wrap only membership-plane inputs (membership,
   membership_pool, node_id, `gossip.failure_timeout_ms`) and bind on the
   membership-plane listener, so they belong to the membership module, not
   the server module. Net effect on this feature's landed shape:
   `ServerModule` loses the `membership_pool` build param and the
   `gossip_service`/`probe_service` fields; it keeps `router`,
   `DataPlaneServices` (segment/healing/cache/scrub), and
   `prefetch_engine`. c3's `done` status is unchanged — this is a
   deliberate follow-up correction made by c4 for ownership reasons, not a
   c3 rework.
