---
feature: "f3: One DiskSegmentStore Instance in StorageModule, Injected Everywhere"
epic: "refactoring/store-unification"
status: done
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
updated: 2026-09-05
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

- [x] **Code:** `cargo build --all-targets` succeeds;
      `grep -rn "DiskSegmentStore::new\|DiskSegmentStore {" crates/oceanfs-node
      --include=*.rs` returns exactly **one** construction site; no
      `shard_store` field/builder remains on
      `GarbageCollector`/`HealingService`/`StorageModule`.
<!-- REVIEW: verified 2026-09-05 (iteration 1) — `cargo build --all-targets` EXIT 0; the only warning is the pre-existing hint_wal.rs:848 cfg(test) dead-code (untouched file — same warning recorded by the f1/f2 reviews). Grep gates re-run independently: `DiskSegmentStore::new|DiskSegmentStore {` in crates/oceanfs-node = exactly 1 hit (modules/storage.rs:441, inside `StorageModule::build`); `SegmentShardStore` / `DiskSegmentShardStore` / `with_shard_store` / `segment_store_impl` across crates/*.rs = 0 hits. Field/builder deletions verified in-file: GarbageCollector lost `shard_store` Option field + `with_shard_store` builder (garbage_collector.rs — compactor now constructed with one store at :286); HealingGrpcService lost `shard_store` field + `with_shard_store` builder (healing_service.rs); StorageModule lost the `shard_store` field (storage.rs struct — only `data_store` remains, field docs state the single-construction invariant :72-80); SegmentCompactor's data_store+shard_store pair collapsed to one `store` field, accessor renamed `store()`. Remaining `shard_store` strings are test-local naming only: `test_shard_store()` helper (gc/orphan_reaper.rs:382, after the :350 `#[cfg(test)]`), test fn `shard_store_lists_and_unlinks_across_pool_roots` (garbage_collector.rs:1180, after the :611 `#[cfg(test)]`), and node/tests/orphan_reaper.rs harness naming. All other `DiskSegmentStore::new` hits workspace-wide are cfg(test)/integration-test fixtures (compaction_crash.rs:198 [cfg(test) file], garbage_collector.rs:1165, orphan_reaper.rs:825, segment_data_roundtrip.rs:79). -->
- [x] **Tests:** `cargo test -p oceanfs-node --lib -- --test-threads=1`
      green (PIPELINE.md §4.6); NEW wiring test asserts GC, AE, heal,
      and the healing-service share one `Arc` (pointer identity through
      `StorageModule.data_store`) and that only one store exists;
      `cargo test -p oceanfs-durability --lib -- --test-threads=1` and
      `cargo test -p oceanfs-server --lib -- --test-threads=1` green.
<!-- REVIEW: verified 2026-09-05 (iteration 1) — all lib suites re-run independently under `--test-threads=1`: oceanfs-node 66/66, oceanfs-durability 263/263 (incl. all 9 compaction_crash matrix rows), oceanfs-server 244/244, oceanfs-storage 435/435. NEW wiring test `build_returns_module_with_single_shared_store` (modules/storage.rs tests, renamed from the f2-era test): asserts `Arc::ptr_eq(&module.data_store, &module.segment_replicator.data_store())` via the new `#[cfg(test)]` `SegmentReplicator::data_store()` accessor (segment_replicator.rs:426-431) — the module's one instance is the replicator's instance — plus a lifecycle-routed write→read→delete round trip and recovery completion on the shared store. The module struct no longer HAS a `shard_store` field, so the field-pair collapse is compile-enforced; durability_wiring (node integration, 4/4) now wires GC/AE/reaper/scrub from ONE shared Arc (f3 shape). Coverage note (LOW, non-blocking): pointer identity is asserted for the replicator — the only store consumer constructed inside `StorageModule::build`; GC/AE/heal/reaper/scrub/rep-worker/healing-service/segment-service receive their Arc in `DurabilityModule::build` / `ServerModule::build` exclusively as `storage.data_store.clone()` (those module builds take `&StorageModule` and hold no other store source — durability.rs:198,329,346,376,153 and server.rs:596,615,669,518), so their single-instancehood is structurally forced by the exactly-one-construction grep + same-field clones rather than asserted per consumer. -->
- [x] **Docs:** `#![deny(missing_docs)]` passes; `StorageModule` field
      docs state the single-construction invariant.
<!-- REVIEW: verified 2026-09-05 (iteration 1) — `#![deny(missing_docs)]` present in all touched crates (oceanfs-durability lib.rs:23, oceanfs-node lib.rs:21, storage/server/api likewise); `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p oceanfs-storage-api -p oceanfs-storage -p oceanfs-durability -p oceanfs-server -p oceanfs-node` EXIT 0. `StorageModule.data_store` doc states the invariant: "a single `oceanfs_storage::DiskSegmentStore` instance constructed exactly here, shared by the replicator, GC, orphan reaper, AE, heal, scrub, re-replication, the healing/segment gRPC services and startup recovery" (modules/storage.rs:72-80). -->
- [x] **ADR:** ADR-0032 D4 satisfied (one `DiskSegmentStore` constructed
      in `StorageModule::build`; injected into every consumer);
      ADR-0031 pools-mandatory wiring intact; ADR-0025 unchanged.
<!-- REVIEW: verified 2026-09-05 (iteration 1) — ADR-0032 D4: one construction at modules/storage.rs:441; the same `data_store` Arc reaches the replicator (storage.rs:471-482), GC (durability.rs:196-199), AE (durability.rs:329), reaper (durability.rs:343-349), heal worker (durability.rs:376), ReRepWorker (durability.rs:151-158 — the g4/g5 repair path documented in In-Scope), scrub cycle (durability.rs:578), scrub gRPC + segment gRPC + healing service + admin (server.rs:518,595-600,611-617,667-670), and the startup recovery sweep — now `self.data_store.delete_shards_with_pool` (storage.rs:640). Healing-service remap unlink now runs through the shared `self.data_store` (healing_service.rs:1569), no `with_shard_store` chain exists anywhere. ADR-0031: the diff is wiring-only — no legacy_dir/empty-pools branch introduced, pool resolution untouched (f2's registry-only resolve). ADR-0025: every reserve/seal/delete/refresh remains coordinator-routed; GC's compactor still requests all transitions from the coordinator and unlinks only after the durable delete (garbage_collector.rs:270-281); no lifecycle call changed. -->
- [x] **Perf:** no new locks on hot paths; sharing one store removes the
      duplicate fs layer (one read/write/delete path per pool).
<!-- REVIEW: verified 2026-09-05 (iteration 1) — frontmatter `perf: []` (no rules cited); diff review confirms the f3 change set adds no lock/sync primitive, no collection, and no allocation on any hot path (wiring + field-deletion only). The deleted second instance removes the duplicate fs layer: one shared read/write/delete path per pool through the single DiskSegmentStore. -->
- [x] **Integration:** `cargo test -p oceanfs-node --test durability_wiring
      -- --test-threads=1` and e2e write/read green on a pools config;
      a GC compaction → heal → scrub cycle exercises the shared store
      end-to-end.
<!-- REVIEW: verified 2026-09-05 (iteration 1) — `durability_wiring` 4/4 green; ALL 30 node integration suites green under `--test-threads=1` (incl. e2e_single_node 4/4 — a full-node boot + write/read on the real shared store wiring — plus scrub_cycle 7/7, re_replication 2/2, gc_compaction 7/7, orphan_reaper 8/8); durability integration green (anti_entropy 14, distributed_scrub 5, gc_compaction 5, merkle_recovery 3, orphan_reaper 7, segment_data_roundtrip 2); server integration 7/7 + storage integration 10/10. E2e functional allowlist re-run against a freshly rebuilt release binary (target/release/oceanfs 2026-09-05 20:48, staleness-checked newer than every source): crash_restart 1, wal_recovery 1, segment_lifecycle 1, cluster_write_path 6, cluster_read_path 5, garbage_collection 1, rewrite_leak_test 1, cluster_lifecycle 4 = 20/20 green on a pools config; no load suites run (PIPELINE.md §6). Shared-store end-to-end coverage note (LOW, non-blocking): no single literal GC→heal→scrub three-phase test exists; the cycle is exercised across the crash-matrix post-compaction scrub row (compaction_crash.rs `post_compaction_segment_scrubs_healthy_against_the_machine_root`), node scrub_cycle 7/7, durability gc_compaction 5/5 and e2e garbage_collection 1/1 — all through the shared store on real wiring. -->
<!-- REVIEW: LOW (not a DoD gap) — reconciliation itself still performs no .dat I/O (verified: ReconciliationLoop::new at durability.rs:169-176 takes the repair_dispatcher sink only); the shared Arc reaches the acquiring side because ReRepWorker is constructed with storage.data_store (durability.rs:151-158) and the healing-service `request_re_replication` handler enqueues into that worker's queue (server.rs:655-661) — the path documented at durability.rs:131-150. f3 adds no .dat reader/writer of its own. -->

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

## Implementation notes (accepted)

Status: independent review **PASS**, iteration 1 (2026-09-05) — zero
blocking findings; all DoD items are verified and ticked with evidence
comments above. The reviewer recorded two LOWs (non-blocking, kept open
for context below) and the notes below record the closure decisions so
the document reflects what was built.

- **LOW (non-blocking) — pointer identity is asserted for the
  replicator consumer only.** The wiring test
  `build_returns_module_with_single_shared_store` asserts
  `Arc::ptr_eq` between `StorageModule.data_store` and the replicator's
  instance via the `#[cfg(test)]`
  `SegmentReplicator::data_store()` accessor
  (segment_replicator.rs:426-431) — the one store consumer constructed
  inside `StorageModule::build`. Every other consumer (GC, AE, heal
  worker, orphan reaper, scrub, re-rep worker, healing service, segment
  gRPC service, admin) receives the `Arc` exclusively as
  `storage.data_store.clone()` in `DurabilityModule::build` /
  `ServerModule::build` (durability.rs:153,198,329,346,376 and
  server.rs:518,596,615,669): those builders take `&StorageModule` and
  hold no other store source, so a second instance is structurally
  unrepresentable. Their single-instancehood is enforced by that type
  shape plus the exactly-one-construction grep gate — not by a
  per-consumer identity assertion. Accepted as-is.
- **LOW (non-blocking) — no single literal GC→heal→scrub three-phase
  test.** The DoD's "compaction → heal → scrub cycle through the shared
  store" is exercised end-to-end but spread across suites: the
  crash-matrix scrub row
  (`compaction_crash.rs` `post_compaction_segment_scrubs_healthy_against_the_machine_root`),
  node `scrub_cycle` (7/7), durability `gc_compaction` (5/5), and the
  e2e `garbage_collection` suite (1/1) — all on the real shared-store
  wiring. No one test drives all three phases as one literal sequence;
  accepted as-is.
- **End state — `StorageModule` is now a single-`data_store` module.**
  The struct's only store field is
  `data_store: Arc<dyn oceanfs_storage_api::SegmentDataStore>`
  (modules/storage.rs:72-80), and `oceanfs_storage::DiskSegmentStore`
  is constructed exactly once inside `StorageModule::build`
  (modules/storage.rs:441) — the f3 end state and ADR-0032 D4. The
  f2-era two-field/one-instance arrangement (both `data_store` and
  `shard_store` fields holding clones of the one unified store) is gone:
  the `shard_store` field and every `with_shard_store` builder/field
  on `GarbageCollector`, `HealingService`, `StorageModule`, and
  `SegmentCompactor` were deleted with the collapse.
- **Review-marker closure table.** The ADR-0032 review anchors tracked
  by this epic now stand as follows:

  | Anchor | Fate |
  |---|---|
  | `garbage_collector.rs:29,548` — "data store and shard store are the same abstraction" / "multiplication of abstraction of segment data access" | Annotated `[resolved]` (comments at garbage_collector.rs:30 and :523 cite f1/f3, ADR-0032 D1/D4; the collector holds one store for the data and delete/list roles) |
  | `healing_service.rs:1327` — concurrent tasks writing through the store | Annotated `[resolved]` (comment at healing_service.rs:1331 cites f2, ADR-0032 D3 — one shared store serializes writers per `.dat`) |
  | `segment_service.rs:895` — parallel writer to the same segment | Annotated `[resolved]` (comment at grpc/segment_service.rs:895 cites f2, ADR-0032 D3 — per-`.dat` lock + reserve-before-write) |
  | `node.rs:1233,1269,1285,1450` — composition-root instance sprawl | The markers moved with c1 into `modules/storage.rs` and `modules/durability.rs`; both surviving bodies are annotated `[resolved]` (storage.rs:453 — "3 abstractions to access disk", durability.rs:306 — "AE no longer creates its own data store"; both cite the f2/f3 unification) |
  | `segment_store_impl.rs:16,92` and the `DiskSegmentShardStore` duplication marker (`garbage_collector.rs:613`) | Deleted with their files/structs in f2 — no annotation needed |
  | `InMemoryShardStore` cfg-guard marker (`garbage_collector.rs:542`, `[review][code smell][high]` — "if this is only used in tests, it should be guarded with a cfg macro") | Retained → wave-4 hygiene sweep (test-local double, not this epic's scope) |
  | `anti_entropy/engine.rs:199` — "reading terabytes of data, unbounded" | Retained → ADR-0034 bounded-metadata-scans; the two-reader divergence half of the same marker is closed with a `[resolved-half]` note (f2, ADR-0032 D2 — engine.rs:200-215) |
