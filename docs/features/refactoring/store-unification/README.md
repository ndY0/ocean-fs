---
feature: "Segment Store Unification (ADR-0032) — Program Coordination"
epic: "refactoring/store-unification"
status: done
priority: critical
owner: ""
dependencies:
  - epic: refactoring/composition-root-decomposition
    reason: ADR-0032 lands behind c1 so trait/impl migrations touch exactly one wiring point (StorageModule). c1 first consolidates the 8 node.rs store constructions (node.rs:1005,1059,1112,1118,1253,1273,1291,2142) to two shared instances inside StorageModule::build; this epic then folds those two into one unified store.
adr:
  - 0032-unify-segment-data-access
  - 0031-remove-single-datadir-legacy-mode
  - 0025-segment-lifecycle-state-machine
perf:
  - "3.2 O_DIRECT for segment data files"
  - "3.3 mmap for hot segment reads"
created: 2026-09-04
updated: 2026-09-05
---

# Segment Store Unification — Program Coordination

> **This is the coordination document for the store-unification epic
> (ADR-0032, review triage Theme 1, wave 2 ②).** If you are implementing
> any feature under `refactoring/store-unification/`, read this first — it
> tells you where your work sits in the whole, what must exist before you
> start, and what must not regress while you work. The per-feature docs
> (`f1-*`, `f2-*`, `f3-*`) are the authority for each feature; this
> document is the map.

## Summary

Segment `.dat` data access has proliferated into two traits, two
field-for-field-identical disk impls, two divergent read paths, eight
composition-root instances, and uncoordinated multi-writer data writes.
ADR-0032 collapses all of it into **one trait, one impl, one construction
site, lifecycle-routed writes**:

| Today (verified) | After ADR-0032 |
|---|---|
| `SegmentDataStore` (read/write) in `oceanfs-durability/src/anti_entropy/merkle_tree.rs:40` | One `SegmentDataStore` trait in **`oceanfs-storage-api`** (5 methods: read/write/delete/delete-with-pool/list) — ADR-0032 D1 |
| `SegmentShardStore` (delete/list) in `oceanfs-durability/src/gc/garbage_collector.rs:561` | Folded into the single trait; the name is deleted |
| `DiskSegmentStore` in `segment_store_impl.rs:27` and `DiskSegmentShardStore` in `garbage_collector.rs:626` (duplicated structs, same `data_pools`/`legacy_dir`/`pool_id_for` fields) | One `DiskSegmentStore` in **`oceanfs-storage`** beside the lifecycle coordinator — ADR-0032 D2 |
| 5× `DiskSegmentStore::new` + 3× `DiskSegmentShardStore::new` in `node.rs` (1005, 1059, 1112, 1118, 1253, 1273, 1291, 2142) | One construction in `StorageModule::build` (`modules/storage.rs` post-c1); `StorageModule.data_store` is the only site — ADR-0032 D4 |
| Heal (`heal/worker.rs:411`), re-rep (`repair.rs:437`), GC compaction (`segment_compactor.rs:328`), healing-service `push_repaired_shard` (`healing_service.rs:1333`), segment-service `push_sealed_segment` (`segment_service.rs:837`) each call raw `std::fs` whole-file writes on their own store instance | Writes serialized per `.dat` (per-segment lock / coordinator grant) and routed through the storage `io` layer (`SegmentReader`/`IoBackend`/per-pool `IoObserver`) — ADR-0032 D3 |
| Durability reads raw files; server reads via `io::DiskSegmentReader` (mmap/O_DIRECT) — divergent readers (`anti_entropy/engine.rs:199` review) | One read path: the unified store reads through the same `io` layer as the server |

The epic deletes 7 of 8 store instances and the duplicated impl pair,
restores the single-writer invariant at the data-file level (the same
invariant ADR-0025 restored for lifecycle *state*), and closes the
two-reader divergence.

---

## The Epic at a Glance

```
refactoring/store-unification/
├── README.md                 ← this document (map)
├── f1-unify-trait.md         [critical]  fold SegmentShardStore → one storage-api SegmentDataStore
├── f2-single-impl.md         [critical]  one DiskSegmentStore in oceanfs-storage (io layer + locks)
└── f3-single-instance-wiring.md [high]   one construction site in StorageModule; inject everywhere
```

| Feature | Kills (by construction) | Delivers |
|---|---|---|
| **f1** | The `SegmentShardStore` trait and the read/write-vs-delete/list abstraction split; consumers' hand-rolled 76/92-byte header logic | Single async `SegmentDataStore` trait + `SegmentFile` (parsed header + payload) in `oceanfs-storage-api`; every consumer migrated (heal/repair/AE/GC/reaper/healing-service/server segment-service/node) |
| **f2** | `oceanfs-durability`'s `DiskSegmentStore` (`segment_store_impl.rs`) and `DiskSegmentShardStore` (`garbage_collector.rs:626`) — the verbatim-duplicate pair; raw-`std::fs` whole-file `.dat` writes; uncoordinated concurrent writers to one `.dat` | One `DiskSegmentStore` in `crates/oceanfs-storage/src/segment/data_store.rs`; writes serialized per segment and routed through `io::SegmentReader`/`IoBackend` + per-pool `IoObserver`; delete/list carried over from the shard store |
| **f3** | 7 of the 8 composition-root instances | Exactly one `Arc<dyn oceanfs_storage_api::SegmentDataStore>` in `StorageModule`, injected into GC/AE/heal/scrub/reconcile/re-rep/segment+healing gRPC/replicator |

## Dependency Graph (implementation order)

```
refactoring/composition-root-decomposition/c1   ← MUST EXIST FIRST (single wiring point)
                     │
                     ▼
refactoring/store-unification/f1-unify-trait
                     │
                     ▼
refactoring/store-unification/f2-single-impl
                     │
                     ▼
refactoring/store-unification/f3-single-instance-wiring   ← final ADR-0032 D4 state
```

Ordering rules:

1. **c1 precedes this whole epic** (ADR-0032 Consequences: "Must land
   behind the composition root decomposition (c1)"). c1's
   `StorageModule` turns the 8 inline constructions into two shared
   instances (`data_store` + `shard_store` fields in
   `modules/storage.rs`). Nothing in this epic should widen the 8-site
   sprawl further; f2's impl deletion would otherwise break eight
   construction sites at once.
2. **f1 → f2 → f3 within the epic.** The trait folds first
   (ADR-0032 Consequences: "fold `SegmentShardStore` into the new trait
   before deleting impls; dual-impl during transition is acceptable"),
   the impl merges second, the wiring collapses third. Each step lands
   green (build + tests + clippy + fmt per PIPELINE.md) before the next.
3. **f1 and f2 may both be implemented while the durability impls still
   exist** (the dual-impl transition window); **f3 may not** — it is the
   end state where only the storage impl remains and only one instance
   is constructed.

## Sequencing vs the roadmap and other epics

- **Wave 2 ① (composition-root c1) must be merged first** — see above.
- **Wave 2 ③ (ADR-0017 scheduler) and ④ (manifest-aware AE/scrub) land
  AFTER this epic** (roadmap §4: wave 2 ① precedes ②③; ④⑤ parallel where
  independent). Do not build new `.dat` writers against the old store
  while this epic is in flight.
- **Wave 3 (g7/g8) must not start** before this epic + the scheduler are
  done — g7/g8 add more `.dat` writers to exactly the surfaces being
  fixed.
- **ADR-0031 (legacy removal) is a precondition of f2** (and is already
  accepted): the new storage impl has **no** `legacy_dir` field and **no**
  "empty `data_pools` = legacy mode" resolution branch. `pool_id` always
  maps to a real, registered pool.

## Key Design Decisions to Respect (do not re-litigate)

- **The trait lives in `oceanfs-storage-api`** (ADR-0032 D1; ADR-0009
  precedent for a trait with consumers in two DAG branches). It must not
  live in `oceanfs-durability` (server's segment service would depend on
  durability) and its **impl must not** live in durability either (it must
  sit beside the lifecycle coordinator and `io::SegmentReader`).
- **The name `SegmentDataStore` is retained** (already used), and the
  `SegmentShardStore` name is dropped (ADR-0032 Neutral).
- **Writes are single-writer per `.dat`** (ADR-0032 D3 + ADR-0025): a
  per-segment write lock or the coordinator's exclusive-transition grant
  makes concurrent writers to the same `.dat` unrepresentable. Lifecycle
  state transitions (reserve/seal/delete/stamp) keep going through
  `SegmentLifecycleCoordinator` exactly as they do today; the data-file
  write is the surface being serialized.
- **The durability side stops using raw `std::fs`** for whole-file I/O
  where the optimized layer applies — reads/writes go through
  `oceanfs-storage::io` (`SegmentReader`, `IoBackend`, atomic
  `SegmentWriteMode`, per-pool `IoObserver`).
- **No legacy mode** (ADR-0031): pool resolution reads the lifecycle
  registry; `data_dir`/`pool_id=0`-sentinel branches are gone.
- **`MetadataStore` and `SegmentStore` (the logical write-path trait) do
  NOT move** (ADR-0032 Out of scope). Objects stay in RocksDB
  (ADR-0025). This epic is the *data*-access unification only.

## What an Implementer Should Do When Picking Up a Feature

1. Read this document (you are here).
2. Read the feature doc's `adr:` frontmatter and the cited ADR sections.
3. Read the composition-root `c1` doc (`modules/storage.rs` target shape)
   — its `StorageModule` struct is the object every feature reshapes.
4. Identify your **inputs** (features in `dependencies:` — done) and your
   **outputs** (who consumes you). c1's tests and the durability/server
   test suites listed in each DoD are your regression safety net.
5. Land green: build, tests (with `--test-threads=1` for RocksDB-touching
   crates per PIPELINE.md §4.6), clippy, fmt.

## Epic-level DoD (ADR-0032 acceptance)

- [x] **One trait:** `oceanfs_storage_api::SegmentDataStore` (async;
      read/write/delete/delete-with-pool/list) is the only segment
      data-access trait. `grep -rn "SegmentShardStore" crates --include=*.rs`
      returns nothing (f1; the durability trait definitions were deleted).
- [x] **One impl:** `oceanfs_storage::DiskSegmentStore` in
      `crates/oceanfs-storage/src/segment/data_store.rs` is the only disk
      impl. `crates/oceanfs-durability/src/segment_store_impl.rs` and the
      `DiskSegmentShardStore` struct are deleted (f2);
      `grep -rn "DiskSegmentShardStore" crates --include=*.rs` returns
      nothing.
- [x] **One construction site:** `StorageModule.data_store`
      (`crates/oceanfs-node/src/modules/storage.rs`) is the only
      `DiskSegmentStore::new`/construction in the node crate
      (`grep -rn "DiskSegmentStore::new" crates/oceanfs-node --include=*.rs`
      returns exactly one site — f3).
- [x] **Single-writer + optimized I/O:** every `.dat` mutation (heal,
      re-rep, GC compaction, `push_repaired_shard`,
      `push_sealed_segment`) is serialized per segment (per-segment
      exclusive locks) and routed through the `oceanfs-storage::io` layer
      (shared file core reads + atomic observed writes); no
      `std::fs::write`/`std::fs::read` whole-file calls remain in the
      durability data paths (f2 — the durability crate holds no disk
      impl).
- [x] **No legacy:** the unified store has no `legacy_dir` and no
      empty-pools branch (ADR-0031; registry-only resolution, f2).
- [x] **Green:** `cargo build --all-targets`; `cargo test -p
      oceanfs-storage --lib -- --test-threads=1` (435), `-p
      oceanfs-durability --lib -- --test-threads=1` (263), `-p
      oceanfs-server --lib -- --test-threads=1` (244), `-p oceanfs-node
      --lib -- --test-threads=1` (66) — all green; e2e functional
      allowlist green (crash_restart, wal_recovery, segment_lifecycle,
      cluster_write/read_path, garbage_collection, rewrite_leak_test,
      cluster_lifecycle — 20/20) with the release binary.
- [x] **Review markers closed:** the `[review]` blocks at
      `merkle_tree.rs:22` (no marker present), `segment_store_impl.rs:16,92`
      (file deleted in f2), `garbage_collector.rs:29,548` (annotated
      `[resolved]`; `:599` — the test-only double's cfg-guard marker —
      retained for the wave-4 hygiene sweep; `:613` — the duplication
      marker — deleted with the struct in f2),
      `healing_service.rs:1327` (`[resolved]`), `segment_service.rs:825`
      (`[resolved]`), `node.rs:1233,1269,1285,1450` (the moved markers in
      `modules/storage.rs` + `modules/durability.rs` are annotated
      `[resolved]`).
- [x] **Docs:** `#![deny(missing_docs)]` passes in all touched crates;
      `RUSTDOCFLAGS="-D warnings" cargo doc` clean on all five crates.

## References

- ADR-0032 (this epic's decision), ADR-0031 (legacy removal —
  precondition), ADR-0025 (lifecycle coordinator = single writer),
  ADR-0029 §D3/D4/f5 (pools + observability), ADR-0009 (crate split /
  trait placement), ADR-0024 (event log / delete-before-unlink)
- `docs/features/refactoring/composition-root-decomposition/c1-split-storage-builder.md`
  — the `StorageModule` builder this epic lands inside
- `docs/features/refactoring/review-2026-09-roadmap.md` §2 Theme 1, §3
  wave 2 ②
- Review anchors cited in ADR-0032 §References
