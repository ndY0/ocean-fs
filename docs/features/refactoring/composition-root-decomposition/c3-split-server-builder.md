---
feature: "c3: Extract ServerModule Builder"
epic: "refactoring/composition-root-decomposition"
status: proposed
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

This feature's status stays `proposed` and its Scope/DoD are unchanged;
c3a was a prerequisite only, not a c3 slice.

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

- [ ] `node.rs` shrinks by the server/handler sections; construction moved
      to `modules/server.rs`.
- [ ] Coordinators' sealed-segment notifier still fans out to replicator +
      AE (behavior preserved).
- [ ] gRPC services constructed in the same order and with the same
      message-size caps.
- [ ] Node tests green; e2e write/read green.
