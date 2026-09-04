---
feature: "f3: One DiskSegmentStore Instance in StorageModule, Injected Everywhere"
epic: "refactoring/store-unification"
status: proposed
priority: high
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: f3 rewires the StorageModule builder (modules/storage.rs) that c1 creates; c1 already consolidated the 8 node.rs constructions to two shared StorageModule fields
  - feature: f1-unify-trait
    reason: The StorageModule fields are typed with the unified oceanfs_storage_api::SegmentDataStore trait after f1
  - feature: f2-single-impl
    reason: f3 constructs the merged oceanfs_storage::DiskSegmentStore that f2 introduces; the durability impl types no longer exist
adr:
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf: []
created: 2026-09-04
updated: 2026-09-04
---

# f3: One DiskSegmentStore Instance in StorageModule, Injected Everywhere

## Summary

Close ADR-0032 D4: after c1 the composition root constructs two shared
store instances inside `StorageModule` (`data_store` and `shard_store`
fields on `crates/oceanfs-node/src/modules/storage.rs`'s
`StorageModule`); after f1/f2 both fields are the same unified
`oceanfs_storage_api::SegmentDataStore` trait and the only impl is
`oceanfs_storage::DiskSegmentStore`. f3 collapses the pair to **exactly
one construction site** — `StorageModule.data_store:
Arc<dyn oceanfs_storage_api::SegmentDataStore>`, built once in
`StorageModule::build` — and injects that single `Arc` into every
consumer: GC, orphan reaper, anti-entropy, heal worker, scrub + scrub
gRPC, reconciliation's repair worker, the re-replication worker, the
segment replicator, and the segment/healing gRPC services. Consumers that
today hold separate `data_store`/`shard_store` fields (the GC compactor,
the healing service) drop the second field and share one `Arc`. The
8-instance sprawl (`node.rs:1005,1059,1112,1118,1253,1273,1291,2142`
before c1) ends as a single `StorageModule.data_store`.

## Scope

### In Scope
- In `crates/oceanfs-node/src/modules/storage.rs` (`StorageModule` from
  c1), replace the `data_store` + `shard_store` field pair with one
  field; construct `oceanfs_storage::DiskSegmentStore` exactly once in
  `StorageModule::build` and `Arc::clone` it into the consumers below.
- Update every consumer constructor call in the durability/server wiring
  to receive the single `Arc`:
  - `GarbageCollector` — remove the separate `.with_shard_store(...)`
    (`node.rs:1118` pre-c1); the compactor's data and delete/list needs
    are served by the one shared store (its
    `shard_store: Arc<dyn SegmentShardStore>` field was migrated to the
    unified trait in f1).
  - `OrphanReaper` — drop its store field (and f1-era second root source)
    in favor of the shared store + the injected pool roots it lists.
  - Anti-entropy engine, scrub coordinator, scrub gRPC service, heal
    worker, healing service (`with_data_store` /
    `with_shard_store` → one `with_store` or shared `Arc`),
    `ReRepWorker`, `SegmentReplicator`
    (`segment_replicator.rs:305`), server segment gRPC service, admin
    (`admin.rs:419` data_store field).
  - Reconciliation itself performs no `.dat` I/O (it enqueues repairs via
    the `RepairDispatcher` sink, whose acquiring side — the local
    `ReRepWorker` — holds the shared store); verify and document that the
    shared `Arc` reaches the worker through that path.
- Delete now-dead `shard_store`-typed fields/builder methods on
  `GarbageCollector`/`HealingService` once no caller passes a distinct
  store.
- Assert single-instancehood in a node wiring test: `StorageModule`
  exposes the store and the test checks pointer-identity/shared `Arc`
  across GC/AE/heal consumers (exactly one construction).
- Close the ADR-0032 review anchors for instance sprawl
  (`node.rs:1233,1269,1285,1450`) with `[resolved]` annotations or
  removal once the wiring is single-instance.

### Out of Scope
- Any further trait/impl work (f1/f2 done).
- The ADR-0017 scheduler and ADR-0033 manifest-aware AE — separate
  epics that will consume this single store later.
- g7/g8 recovery flows (roadmap wave 3 — they must land on this
  substrate after the epic).

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-node` | `modules/storage.rs`: `StorageModule` loses `shard_store`; `data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore>` is the single field/construction; wiring calls updated; one new node test asserts a single shared instance |
| `oceanfs-durability` | `GarbageCollector`/`SegmentCompactor` and `HealingService` drop the separate shard-store field/builder (now that the unified trait covers delete/list); no behavioral change |
| `oceanfs-server` | unchanged (consumes the injected `Arc`); admin keeps its existing `Option` field if used |

## Interface (Public API)

No new public API. Public-shape changes only:

- `StorageModule` (post-c1, `pub(crate)`) — fields reduce to:
  ```rust
  pub struct StorageModule {
      // ...
      /// The ONE segment data store (ADR-0032 D4). Shared by GC, orphan
      /// reaper, AE, heal, scrub, re-replication, the replicator, and the
      /// segment/healing gRPC services. Constructed exactly here.
      pub data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore>,
      // shard_store field REMOVED
  }
  ```
- `GarbageCollector` / `HealingService` — the 
  `with_shard_store(...)` builder (and any second store field) is
  removed; delete/list callers use the same injected store.

## Data Flow

```
StorageModule::build (modules/storage.rs)
  ├─ pools/registry/lifecycle coordinator + io reader (from c1)
  └─ data_store = Arc::new(oceanfs_storage::DiskSegmentStore::new(...))   ← the ONLY construction
        │
        ├─► GarbageCollector (compaction data + delete/list)  ── one Arc
        ├─► OrphanReaper (list per root + delete_shards_with_pool)
        ├─► AntiEntropy engine (read/repair)
        ├─► ScrubCoordinator + scrub gRPC service (read)
        ├─► HealWorker (read + coordinated rewrite)
        ├─► HealingService (fetch_shard read; push_repaired_shard write)
        ├─► ReRepWorker / repair worker (pull + write + stamp)
        ├─► SegmentReplicator (read owner-side .dat to push replicas)
        └─► server SegmentGrpcService (fetch/append .dat) + admin
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds;
      `grep -rn "DiskSegmentStore::new\|DiskSegmentStore {" crates/oceanfs-node
      --include=*.rs` returns exactly **one** construction site; no
      `shard_store` field/builder remains on
      `GarbageCollector`/`HealingService`/`StorageModule`.
- [ ] **Tests:** `cargo test -p oceanfs-node --lib -- --test-threads=1`
      green (PIPELINE.md §4.6); NEW wiring test asserts GC, AE, heal,
      and the healing-service share one `Arc` (pointer identity through
      `StorageModule.data_store`) and that only one store exists;
      `cargo test -p oceanfs-durability --lib -- --test-threads=1` and
      `cargo test -p oceanfs-server --lib -- --test-threads=1` green.
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `StorageModule` field
      docs state the single-construction invariant.
- [ ] **ADR:** ADR-0032 D4 satisfied (one `DiskSegmentStore` constructed
      in `StorageModule::build`; injected into every consumer);
      ADR-0031 pools-mandatory wiring intact; ADR-0025 unchanged.
- [ ] **Perf:** no new locks on hot paths; sharing one store removes the
      duplicate fs layer (one read/write/delete path per pool).
- [ ] **Integration:** `cargo test -p oceanfs-node --test durability_wiring
      -- --test-threads=1` and e2e write/read green on a pools config;
      a GC compaction → heal → scrub cycle exercises the shared store
      end-to-end.

> **Lint & Doc Examples (non-gating):** `cargo clippy --lib -- -D warnings`
> should pass on production code. Test-code clippy warnings (`.unwrap()`,
> `.expect()` in `#[cfg(test)]` modules) and `ignore`-tagged doc examples
> are non-blocking for feature completeness — they are structural codebase
> hygiene tracked separately (see `guidelines/coding.md` §9.2.1). Do NOT
> include Lint or Manual items in the Definition of Done checklist.

## Migration path

This is the tail of the epic: c1's two fields become one, and every
`with_data_store(...)`/`with_shard_store(...)` call collapses to passing
the same `Arc`. Because f1 made the trait uniform and f2 deleted the
second impl, the collapse is a pure wiring change with no behavioral
delta — the regression bar is the node boot + durability integration
tests listed above. After f3 the epic-level DoD in this directory's
README is the acceptance gate.
