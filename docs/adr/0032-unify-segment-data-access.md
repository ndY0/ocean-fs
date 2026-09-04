# ADR-0032: Unify Segment Data Access — One Trait, One Store, Lifecycle-Routed Writes

**Status:** Accepted
**Date:** 2026-09-04
**Deciders:** OceanFS architecture team

---

## Context

The 2026-08-25/09-03 review (triage Theme 1) found segment data access
proliferated across the durability layer. Verified in today's code:

- **Two traits over the same `.dat` files**: `SegmentDataStore`
  (read/write whole segment; defined in `anti_entropy/merkle_tree.rs:40`)
  and `SegmentShardStore` (delete/list shards;
  `gc/garbage_collector.rs:561`). Review `gc/garbage_collector.rs:29`:
  "data store and shard store are the same abstraction."
- **Two disk implementations that are field-for-field identical**:
  `DiskSegmentStore` (`segment_store_impl.rs:27`) and
  `DiskSegmentShardStore` (`garbage_collector.rs:626`) carry the same
  fields (`data_pools`, `legacy_dir`, `pool_id_for`) and the same
  `new()` signature. Review `gc/garbage_collector.rs:613`: "this struct
  is verbatim the same as the one in `segment_store_impl`."
- **Eight store instances wired in the composition root**: `node.rs`
  constructs 5× `DiskSegmentStore::new` (1005, 1059, 1112, 1253, 1291)
  and 3× `DiskSegmentShardStore::new` (1118, 1273, 2142) with identical
  args. Reviews `node.rs:1233` ("the anti-entropy worker creates its own
  data store… there should only be one data store"),
  `node.rs:1269`, `node.rs:1285`, `node.rs:1450` ("we have 3 abstractions
  to access disk").
- **Writes bypass the lifecycle coordinator and the optimized I/O path.**
  Heal (`heal/worker.rs:411`), re-replication (`repair.rs:437`), GC
  compaction (`segment_compactor.rs:326-329`), and the healing service's
  `push_repaired_shard` (`healing_service.rs:1320-1351`) each call
  `write_segment_data` → `std::fs::write` on their own store instance.
  ADR-0025 made the `SegmentLifecycleCoordinator` the only writer of
  lifecycle *state*; the review's point (reviews `gc/garbage_collector.rs:268`,
  `heal/worker.rs:97`, `healing_service.rs:1327`, `segment_service.rs:825`)
  is that *data-file writes* are still uncoordinated and multi-writer.
- **Two divergent read paths.** Durability reads via the plain-fs
  `SegmentDataStore`; the server read path serves via
  `oceanfs_storage::io::DiskSegmentReader` (mmap / O_DIRECT / buffered,
  with `SegmentFileCache`). Review `anti_entropy/engine.rs:199`: "we have
  two divergent data readers, for the server and for the durability side."

Trait-consumer analysis (ADR-0005 / ADR-0009 apply): `SegmentDataStore` is
consumed by `oceanfs-durability` (heal, repair, AE, GC) and by
`oceanfs-server` (the segment gRPC service uses `data_store`). Two
consumers in different DAG branches → the shared trait has no natural home
in either crate; ADR-0009 already solved this pattern with
`oceanfs-storage-api`.

## Decision

### D1. One trait in `oceanfs-storage-api`: `SegmentDataStore`

`SegmentShardStore`'s delete/list responsibilities fold into a single
`SegmentDataStore` trait moved to `oceanfs-storage-api`:

```rust
pub trait SegmentDataStore: Send + Sync {
    /// Full-file read (returns version + payload; headers parsed).
    async fn read_segment_data(&self, id: &SegmentId)
        -> Result<Option<SegmentFile>>;
    /// Full-file write (authoritative persistence — see D3).
    async fn write_segment_data(&self, id: &SegmentId, data: &[u8])
        -> Result<()>;
    /// Delete a segment's .dat file(s).
    async fn delete_shards(&self, id: &SegmentId) -> Result<()>;
    /// Delete .dat under an explicit pool (recovery path).
    async fn delete_shards_with_pool(&self, id: &SegmentId, pool: PoolId) -> Result<()>;
    /// List .dat files under a root (orphan sweep, multi-root).
    fn list_segment_files(&self, root: &Path) -> Result<Vec<PathBuf>>;
}
```

`oceanfs-storage-api` depends only on `oceanfs-core` (types), so both
`oceanfs-durability` and `oceanfs-server` consume the trait without a new
crate edge — the same reasoning as ADR-0009 Part 2. ADR-0025's read-only
`MetadataStore`-style boundary for GC/scrub/AE is preserved; this trait is
the *data* boundary complementing it.

### D2. One implementation in `oceanfs-storage`: `DiskSegmentStore`

The unified implementation lives in `oceanfs-storage` (beside the
lifecycle coordinator and the `io::SegmentReader`/`SegmentFileCache` it
must share). `oceanfs-durability`'s `DiskSegmentStore` and
`DiskSegmentShardStore` are deleted; their logic moves into the single
storage-side implementation. Pool resolution (`pool_id → root`) reads the
lifecycle registry; the legacy `data_dir`/`pool_id=0-sentinel` branches are
gone per ADR-0031.

### D3. Writes are lifecycle-coordinated and use the optimized I/O layer

- Background `.dat` writers (heal, re-replication, GC compaction, replica
  append / `push_repaired_shard`) go through the **lifecycle coordinator**
  as the single writer (ADR-0025): `request_reserve` →
  coordinated write → `request_seal` → metadata stamp, exactly as the
  seal pipeline does today. No subsystem calls `write_segment_data`
  directly on a segment it does not own.
- The store's read/write paths use the same `io::SegmentReader` /
  `IoBackend` (mmap / O_DIRECT / buffered) and per-pool `IoObserver`
  signals as the server path. The durability side stops using raw
  `std::fs` for whole-file I/O where the optimized layer applies.
- A per-segment write lock (or the coordinator's exclusive-transition
  grant) makes concurrent writers to the same `.dat` unrepresentable
  (reviews `healing_service.rs:1327`, `segment_service.rs:825`).

### D4. Single instance, injected once

`node.rs` constructs **one** `DiskSegmentStore` in `StorageModule::build`
(c1) and injects it into GC, AE, heal, scrub, reconcile, re-replication,
the segment/healing gRPC services, and the replicator. The
`StorageModule.data_store` field is the only construction site.

### Out of scope

- Moving `MetadataStore` or `SegmentStore` (the *logical* segment trait
  used by the write path) — those stay per ADR-0009.
- The ADR-0017 scheduler (separate ADR; it consumes this unified store).
- The read-path streaming design (review Theme 6) — independent.
- Compression/encryption bit flags on `ChunkRef` (review #19) —
  independent.

## Consequences

### Positive

- One trait, one implementation, one construction site — deletes the
  duplicated struct pair and 7 of 8 store instances.
- Restores the "single writer" invariant at the data-file level, matching
  what ADR-0025 did for lifecycle state; g7/g8 recovery flows land on a
  coordinated store instead of adding more uncoordinated writers.
- Durability reads/writes gain the optimized I/O path (mmap/O_DIRECT,
  per-pool I/O signals), closing the two-reader divergence.
- Clean ADR-0005 compliance for a trait with two consumer branches.

### Negative

- Wide blast radius: touches every durability subsystem + the segment
  gRPC service + the composition root. Must land behind the composition
  root decomposition (c1) so there is one wiring point.
- Trait growth: `read_segment_data` must expose parsed header info
  (version, data_end) so callers stop hand-rolling the 76/92-byte logic
  (review #35).
- Migration ordering matters: fold `SegmentShardStore` into the new trait
  before deleting impls; dual-impl during transition is acceptable.

### Neutral

- The trait name `SegmentDataStore` is retained (already used) rather than
  introducing a new name.
- `InMemorySegmentStore` test impls move with the trait to
  `oceanfs-storage-api` (or stay test-local per review #17/#26).

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Keep two traits, share one instance** (status quo + c1) | Smallest change | Leaves the duplicated impl structs and the read/write/delete split that produced the divergence; review explicitly calls the split an abstraction bug | Rejected: unification is the review's demand, sharing alone does not remove the duplication |
| **One trait in `oceanfs-durability`** | Near the consumers (heal, GC) | `oceanfs-server`'s segment gRPC service would depend on `oceanfs-durability` to get its data trait — inverts the natural dependency; repeats the ADR-0009 Option-B rejection | Rejected: two consumers in different DAG branches → shared trait crate (ADR-0009 precedent) |
| **Keep impls in `oceanfs-durability`, trait in storage-api** | Trait home is clean | The impl would sit far from the lifecycle coordinator and `io::SegmentReader` it must share; ADR-0025 already placed the coordinator in storage | Rejected: impl belongs beside the coordinator + optimized I/O it coordinates with |
| **Full native store now (ADR-0023 Phase 2 entire)** | One rewrite | Much larger correctness surface; out of proportion to the review's data-access complaint | Rejected: this ADR unifies access to the existing file layout; ADR-0023 stays separate |

## References

- Review comments (Theme 1): `gc/garbage_collector.rs:29,548,613`,
  `node.rs:1233,1269,1285,1450`, `heal/worker.rs:97`,
  `healing_service.rs:1327`, `segment_service.rs:825`,
  `anti_entropy/engine.rs:199`, `segment_store_impl.rs:92`
- ADR-0009 (storage-api crate + trait placement), ADR-0005 (trait in
  consuming crate), ADR-0025 (lifecycle coordinator = single writer),
  ADR-0031 (legacy removal — precondition for D2)
- Roadmap: `docs/features/refactoring/review-2026-09-roadmap.md` (Theme 1,
  wave 2 ②)
- Epic: `docs/features/refactoring/composition-root-decomposition/` (c1 =
  the single wiring point this ADR depends on)
