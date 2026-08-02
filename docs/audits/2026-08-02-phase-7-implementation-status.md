---
audit_date: 2026-08-02
scope: targeted
target_crates: oceanfs-storage
severity_counts:
  critical: 1
  high: 4
  medium: 6
  low: 3
---

# Audit Report: Phase 7 Durability — Implementation Status

## Summary

Phase 7 (Durability) consists of four features: Anti-Entropy & Merkle Tree
Exchange, Distributed Scrubbing, Garbage Collection & Segment Compaction, and
Orphaned Segment Reaper. All four are declared `in_progress` — accurately. Every
feature has solid type definitions, configuration structs, background-task
lifecycles, and passing unit + integration tests (93 total, 0 failures).
However, **every feature shares the same critical gap: the hot-path logic that
actually does the work is stubbed.** Merkle trees compute correctly, but
`run_cycle` does not exchange them with peers. The scrub coordinator partitions
segments correctly, but `scrub_segment` never verifies a single hash. GC
computes liveness ratios precisely, but `compact_segment` never re-packs a
single blob. The orphan reaper detects orphans, but never deletes a shard from
disk. **Phase 7 is approximately 40% complete overall** — the architecture is
sound, the types are correct, but none of these features work in a real
multi-node cluster.

---

## Findings

### Critical

| # | Location | Description | Recommendation |
|---|---|---|---|
| C1 | `crates/oceanfs-storage/src/anti_entropy.rs:512-524` | `AntiEntropy::run_cycle()` returns `AntiEntropyStats::default()` — always zero. No peer selection, no Merkle root exchange, no tree descent, no repair. An anti-entropy cycle that does nothing means silent data corruption goes undetected forever. | Implement the full run_cycle: (1) select random peer from membership, (2) exchange Merkle roots via MerkleExchangeProtocol/gRPC, (3) call `diff()` on mismatched trees, (4) descend with `descend_diff()` to find diverged leaves, (5) fetch and repair corrupt shards. This is blocking correctness. |

### High

| # | Location | Description | Recommendation |
|---|---|---|---|
| H1 | `crates/oceanfs-storage/src/scrub.rs:148-181` | `ScrubWorker::scrub_segment()` always returns `healthy: true`. Never reads shard data from disk, never computes BLAKE3 hash, never recomputes Merkle tree. A full cluster-wide scrub that finds zero corruption is dangerously misleading. | Implement actual verification: read shard data from segment store, compute BLAKE3 per shard, compare to stored hashes, recompute Merkle tree and compare root. On mismatch, enqueue segment for EC healing. |
| H2 | `crates/oceanfs-storage/src/gc.rs:203-248` | `SegmentCompactor::compact_segment()` is a no-op stub. It counts bytes but never reads blobs from old segments, never writes to new segments, never updates object metadata chunk refs, and never deletes old segment shards from disk. Compaction claims success without moving a single byte. | Implement real compaction: (1) read live blobs from old segment using SegmentIndex, (2) classify each blob via TierRouter, (3) write to new active segments in appropriate tier pools, (4) batch-update ObjectMetadata chunk refs, (5) delete old segment shards from disk and metadata. |
| H3 | `crates/oceanfs-storage/src/gc.rs:421-471` | `process_tombstones()` skips the tombstone TTL check. The comment at line 462 reads: "Since we can't easily get the tombstone's timestamp without an iterator, we treat all present tombstones as eligible." This means tombstones younger than `tombstone_ttl_sec` (default 3 days) are immediately eligible for GC, which is a data-loss risk if a tombstone was created by a client error and then rolled back. | Add a tombstone iteration API to MetadataStore that exposes `deletion_time`, and filter tombstones by `now - deletion_time > tombstone_ttl_sec`. Without this, the TTL configuration knob has no effect. |
| H4 | `crates/oceanfs-storage/src/gc.rs:567-576` | `OrphanReaper::run_cycle()` increments `orphans_deleted` without actually deleting anything. The comment reads: "The MetadataStore doesn't have a direct delete_segment... For now, we track it as a stat." Orphaned segments accumulate unbounded disk usage because the reaper never reclaims them. | Add `MetadataStore::delete_segment()` and `SegmentStore::delete_shards()` APIs. Call them from the reaper's double-check path. Without this, the reaper is a monitoring tool, not a reclamation tool. |

### Medium

| # | Location | Description | Recommendation |
|---|---|---|---|
| M1 | `crates/oceanfs-storage/src/anti_entropy.rs:234-292` | `MerkleTree::descend_diff()` exists but is marked `#[allow(dead_code)]` and never called from `diff()`. The `diff()` method does flat leaf-by-leaf comparison instead of tree descent. For large trees (e.g., 64 leaves), flat comparison transmits 64×32B = 2KB of leaf hashes; tree descent would transmit ~log₂(64)×32B = 192B. The feature doc specifies tree descent for bandwidth efficiency. | Either remove `descend_diff` if flat comparison is acceptable, or integrate it into `diff()` to provide bandwidth-efficient divergence detection. The feature spec (§Data Flow) explicitly specifies "binary descent" on root mismatch. |
| M2 | `crates/oceanfs-storage/src/anti_entropy.rs:555-578` | `MerkleExchangeProtocol` exists but has zero functionality. It holds a config and has no methods for encoding, decoding, or exchanging Merkle root sets. The `run_cycle` stub doesn't use it. This dead struct should either be fleshed out or removed. | Implement `MerkleExchangeProtocol` with `encode_roots()`, `decode_roots()`, and a gRPC exchange method, or remove it and inline the logic into `run_cycle`. Currently it's a misleading placeholder. |
| M3 | `crates/oceanfs-storage/src/scrub.rs:290-346` | `ScrubCoordinator::run_cycle()` runs as a single-node local scan using `list_segments()` from the local MetadataStore. The feature spec requires: (1) coordinator election from membership, (2) distributed partition assignment across healthy nodes, (3) parallel verification with concurrent node workers, (4) aggregate reporting. None of these are implemented. | Add coordinator election (random node selection from membership), partition assignment to remote nodes via gRPC, and aggregate result collection. The partitioning logic (`partition_segments`) is correctly implemented — it just needs to be used with multiple nodes. |
| M4 | `crates/oceanfs-storage/src/gc.rs:251-267` | `SegmentCompactor::find_objects_in_segment()` does a full O(n) scan of all objects via `list_objects()`, filtering in-memory. For a store with millions of objects, this is prohibitively slow. The feature doc notes: "in production, a reverse index (segment → objects) would accelerate this." | Implement a reverse index: either a RocksDB column family mapping `segment_id → [object_key]`, or maintain an in-memory index updated on write. This is a known scalability bottleneck documented in the code. |
| M5 | `crates/oceanfs-storage/src/anti_entropy.rs:483-485` | `AntiEntropy` struct takes only `config` in its constructor. The feature spec requires `membership: Arc<Membership>`, `metadata: Arc<MetadataStore>`, and `pool: Arc<ConnectionPool>`. Without these, `run_cycle` cannot select peers or exchange data. The constructor was simplified to return empty stats for testing — but the accepted public API doesn't match the spec. | Restore the full constructor signature. Add optional parameters or builder pattern if some dependencies are not yet available. The current stub constructor is misleading because it appears fully functional. |
| M6 | `crates/oceanfs-storage/src/scrub.rs:322` | `ScrubCoordinator::run_cycle()` acquires a semaphore and then calls `scrub_partition()` synchronously on the caller's thread — defeating the purpose of the semaphore. The semaphore is acquired once, then released at end of scope. No concurrency is actually bounded because everything runs sequentially. | Wrap `scrub_partition` in `tokio::spawn` for each partition, using `Semaphore` to bound concurrent partition verification. Alternatively, remove the semaphore if single-threaded operation is intentional. |

### Low

| # | Location | Description | Recommendation |
|---|---|---|---|
| L1 | `docs/features/phase-7-durability/` | No epic-level `README.md` exists. Each of the four feature files is self-contained, but there is no phase-level status table, integration test matrix, or cross-feature dependency graph. Progress across the phase requires reading all four files and cross-referencing manually. | Create `docs/features/phase-7-durability/README.md` with: status table, integration test matrix (GC+Reaper combined pipeline, AntiEntropy+Scrub interaction), and dependency tracking. Follow the pattern from the Phase 4 audit recommendation. |
| L2 | `crates/oceanfs-storage/src/anti_entropy.rs:538-548` | `AntiEntropy::start_background()` spawns a `tokio::task` but never returns a way to gracefully shut down. The task runs `loop { sleep; run_cycle }` with no cancellation token. In a production system, this leaks tasks on node shutdown. | Add a `CancellationToken` parameter or use `tokio::select!` with a shutdown signal. Same issue exists in `ScrubCoordinator::start_background()` and `GarbageCollector::start_background()`. |
| L3 | `crates/oceanfs-storage/src/anti_entropy.rs:12` | `DEFAULT_LEAF_SIZE` constant is marked `#[allow(dead_code)]`. It's used in the `build()` doctest example at line 29, but never referenced in production code flow. The constant is defined but unused in `build()` (callers pass `leaf_size` explicitly). | Either remove the constant or make it the default parameter for `build()`. A dead-code annotation is a signal that the constant has no purpose. |

---

## Per-Feature Detailed Assessment

### Anti-Entropy & Merkle Tree Exchange — Status: `in_progress` ✅ ACCURATE

| DoD Item | Status | Evidence |
|---|---|---|
| `MerkleTree` struct (build, root, diff, leaf_hash) | ✅ Done | `anti_entropy.rs:33-355` |
| `MerkleRoot`, `LeafRange`, `MerkleProof` types | ✅ Done | `anti_entropy.rs:366-423` |
| Merkle tree built at segment seal time | 🔶 Partial | Merkle tree computation works, but not yet integrated into segment sealer |
| `AntiEntropy` struct + `run_cycle` | 🔶 Scaffold | `run_cycle` returns zero stats — **C1** |
| `start_background` lifecycle | 🔶 Scaffold | Spawns but runs empty cycles — **C1**, **L2** |
| gRPC Merkle exchange protocol | ❌ Missing | `MerkleExchangeProtocol` is a dead struct — **M2** |
| Tree descent on root mismatch | ❌ Missing | `descend_diff` unused — **M1** |
| Leaf repair (fetch + verify + replace) | ❌ Missing | No repair logic |
| Diff identifies exact leaf index for corruption | ✅ Done | `diff_*` tests verify: single-bit change → correct leaf range |
| Empty segment → valid single-leaf tree | ✅ Done | `build_empty_data_returns_none`, `empty_segment_returns_valid_single_leaf_tree` |
| BLAKE3 SIMD (perf 5.1) | ✅ Done | Uses `blake3` crate with auto-detection |
| Streaming hash (perf 5.2) | ✅ Done | `build_from_hashes` avoids full-data buffering; `build()` buffers but is documented |
| Rayon parallel diff (perf 2.1) | ✅ Done | `MerkleTree::diff()` uses `rayon::prelude::*` for `max_leaves > 4` |
| `cargo build --all-targets` | ✅ Done | Builds clean |
| `#![deny(missing_docs)]` | ✅ Done | Docs pass |
| Unit tests | ✅ Done | 28 unit tests pass |
| Integration tests | ✅ Done | 4 integration tests pass (basic comparison, no real peer exchange) |
| Coverage ≥ 80% | 🔶 Below | Previously reported 77.7% for anti_entropy.rs |

**Assessment:** The Merkle tree data structure is complete and well-tested. The
anti-entropy *service* is a hollow shell — `run_cycle` does nothing,
`MerkleExchangeProtocol` is dead code, and no peer communication exists. ~35%
complete. The risk: tree computation is correct but never used operationally.

---

### Distributed Scrubbing — Status: `in_progress` ✅ ACCURATE

| DoD Item | Status | Evidence |
|---|---|---|
| `ScrubConfig`, `ScrubReport`, `ScrubResult` types | ✅ Done | `scrub.rs:33-104` |
| `ScrubCoordinator`, `ScrubWorker` structs | ✅ Done | `scrub.rs:127-401` |
| `partition_segments()` (no gaps, no overlaps) | ✅ Done | 5 partition tests pass |
| `run_cycle()` | 🔶 Scaffold | Single-node only, no actual verification — **H1**, **M3** |
| `trigger_manual()` | 🔶 Scaffold | Spawns but only triggers empty verification |
| `start_background()` | 🔶 Scaffold | Works but runs empty cycles — **L2** |
| BLAKE3 shard verification | ❌ Missing | `scrub_segment` returns healthy: true always — **H1** |
| Merkle root recomputation | ❌ Missing | Debug trace only, no actual recomputation — **H1** |
| Auto-heal via EC decode | ❌ Missing | No healing logic |
| Coordinator election | ❌ Missing | Single-node only — **M3** |
| Distributed partition assignment | ❌ Missing | All segments assigned to local node — **M3** |
| Scrub report aggregation | 🔶 Partial | Report structure exists, but all segments are "healthy" |
| Semaphore-bounded concurrency (perf 2.7) | 🔶 Incorrect | Semaphore acquired once, not used for bounding — **M6** |
| `cargo build --all-targets` | ✅ Done | Builds clean |
| `#![deny(missing_docs)]` | ✅ Done | Docs pass |
| Unit tests | ✅ Done | 10 unit tests pass |
| Integration tests | ✅ Done | 5 integration tests pass (all healthy, no corruption detection) |
| Coverage ≥ 80% | 🔶 Below | Previously reported 78.7% for scrub.rs |

**Assessment:** The partitioning logic is correct and well-tested, but the
verification logic is entirely placeholder. The scrub coordinator can partition
segments but cannot verify them. The gap between "partitions work" and "scrub
works" is large. ~30% complete.

---

### Garbage Collection & Segment Compaction — Status: `in_progress` ✅ ACCURATE

| DoD Item | Status | Evidence |
|---|---|---|
| `GcConfig`, `GcStats`, `LivenessTracker` types | ✅ Done | `gc.rs:48-172` |
| `SegmentCompactor`, `GarbageCollector` structs | ✅ Done | `gc.rs:183-472` |
| Liveness ratio computation | ✅ Done | All edge cases tested (0.0, 0.5, 1.0, unknown segment) |
| Compaction candidate detection | ✅ Done | `compaction_candidates()` with threshold filtering |
| Bounded mpsc channel (perf 2.6) | ✅ Done | `compaction_queue_capacity=64` |
| Semaphore-bounded compaction (perf 2.7) | ✅ Done | `max_concurrent_compactions=4` |
| `run_cycle()` | 🔶 Partial | Tombstones processed, liveness computed, compaction spawned — but compactor is a no-op — **H2** |
| `compact_segment()` (repack, update metadata, delete old) | ❌ Missing | All three steps stubbed — **H2** |
| Tombstone TTL enforcement | ❌ Missing | All tombstones treated as eligible — **H3** |
| TierRouter integration for re-packing | ❌ Missing | TierRouter constructed but never used for actual classification — **H2** |
| Old segment shard deletion | ❌ Missing | Comment: "In production: delete shards from disk" — **H2** |
| Object metadata chunk ref update | ❌ Missing | No metadata mutation during compaction — **H2** |
| Reverse index (segment → objects) | ❌ Missing | O(n) full scan — **M4** |
| `start_background()` | 🔶 Scaffold | Loop works but compactions are no-ops — **L2** |
| Concurrent write-during-GC test | ❌ Missing | Not implemented |
| `cargo build --all-targets` | ✅ Done | Builds clean |
| `#![deny(missing_docs)]` | ✅ Done | Docs pass |
| Unit tests | ✅ Done | 22 unit tests pass |
| Integration tests | ✅ Done | 3 integration tests pass (smoke tests only) |
| Coverage ≥ 80% | ❌ Below | Previously reported 65.6% for gc.rs |

**Assessment:** The liveness tracking and candidate selection are correct and
well-tested. The compaction itself is entirely stubbed — it computes what to
compact but never performs the compaction. ~40% complete.

---

### Orphaned Segment Reaper — Status: `in_progress` ✅ ACCURATE

| DoD Item | Status | Evidence |
|---|---|---|
| `OrphanStats`, `OrphanReaper` types | ✅ Done | `gc.rs:479-635` |
| `build_referenced_set()` | ✅ Done | Scans objects CF, builds HashSet |
| `is_segment_referenced()` double-check | ✅ Done | Re-checks before deletion |
| TTL enforcement (sealed_at) | ✅ Done | `now_ms - sealed_at > ttl_ms` check works |
| `run_cycle()` | 🔶 Partial | Detects orphans but never deletes them — **H4** |
| Shard deletion from disk | ❌ Missing | Stat-only placeholder — **H4** |
| SegmentMetadata deletion from RocksDB | ❌ Missing | No `delete_segment` API exists — **H4** |
| Idempotent double-check | ✅ Done | `is_segment_referenced` called before deletion |
| `start_background()` | 🔶 Scaffold | Loop works but deletions are no-ops — **L2** |
| 0-ref=orphan, 1-ref=not-orphan tests | ✅ Done | Both pass |
| Too-young segment not orphan test | ✅ Done | Passes |
| Empty CF yields no orphans test | ✅ Done | Passes |
| Race condition test (concurrent write) | 🔶 Partial | Double-check exists but no concurrent write test |
| `cargo build --all-targets` | ✅ Done | Builds clean |
| `#![deny(missing_docs)]` | ✅ Done | Docs pass |
| Unit tests | ✅ Done | 6 orphan unit tests pass |
| Integration tests | ✅ Done | 4 orphan integration tests pass (detection-only) |
| Coverage ≥ 80% | ❌ Below | Previously reported 65.6% (shared file with GC) |

**Assessment:** The detection logic is complete and well-tested — orphans are
correctly identified. The reclamation step is entirely stubbed. The reaper is
effectively a monitoring tool that reports orphan counts but never reclaims
space. ~50% complete.

---

## Phase 7 Progress Summary

| Feature | Status | Real Completeness | Critical Gap |
|---|---|---|---|
| Anti-Entropy & Merkle | `in_progress` | 35% | `run_cycle` returns empty stats (C1) |
| Distributed Scrubbing | `in_progress` | 30% | `scrub_segment` always healthy (H1) |
| GC & Compaction | `in_progress` | 40% | `compact_segment` is no-op (H2) |
| Orphan Reaper | `in_progress` | 50% | Deletion is stat-only (H4) |
| **Phase 7 aggregate** | — | **~39%** | All features compute what to do but don't do it |

## Dependency Graph & Remediation Order

```
                    ┌─────────────────────────────┐
                    │  MetadataStore extensions    │
                    │  (tombstone iterator,        │
                    │   delete_segment, shard      │
                    │   deletion APIs)             │
                    └──────────┬──────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ GC Compaction   │  │ Orphan Reaper   │  │ Anti-Entropy    │
│ (H2, H3, M4)    │  │ (H4)            │  │ (C1, M1, M2, M5)│
│                 │  │                 │  │                 │
│ Needs: shard     │  │ Needs: shard    │  │ Needs: gRPC     │
│ read, TierRouter │  │ delete, segment │  │ peer exchange,  │
│ write, metadata  │  │ delete APIs     │  │ membership      │
│ update APIs      │  │                 │  │ integration     │
└─────────────────┘  └─────────────────┘  └─────────────────┘
          │                                       │
          └───────────────┬───────────────────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Scrubbing       │
                 │ (H1, M3, M6)    │
                 │                 │
                 │ Depends on:     │
                 │ • GC for heal   │
                 │ • Anti-entropy  │
                 │   for Merkle    │
                 │ • gRPC for      │
                 │   distributed   │
                 │   assignment    │
                 └─────────────────┘
```

The critical path runs through a shared bottleneck: **real disk operations and
gRPC peer communication**. All four features share the same root cause: they
compute what to do (liveness ratios, orphan detection, Merkle diffs, partition
assignments) but none perform the actual data mutations.

---

## Test Coverage Summary

| Module | Unit Tests | Integration Tests | Total | All Pass? |
|---|---|---|---|---|
| Anti-Entropy | 28 | 4 | 32 | ✅ |
| Scrubbing | 10 | 5 | 15 | ✅ |
| GC & Compaction | 22 | 3 | 25 | ✅ |
| Orphan Reaper | 6 | 4 | 10 | ✅ |
| Shared (error, etc.) | 3 | — | 3 | ✅ |
| Other storage tests | — | 8 | 8 | ✅ |
| **Total** | **69** | **24** | **93** | ✅ 93/93 |

All 93 tests pass. However, test quality is limited by the stubs — many tests
verify that no-ops don't panic rather than that real work was performed. For
example, `run_cycle_returns_stats` verifies that `segments_compared == 0`, and
`scrub_worker_healthy_segment` verifies that the always-healthy stub returns
healthy.

---

## Recommendations

1. **Immediate — implement MetadataStore extension APIs.** Three new APIs are
   needed across all four features: (a) a tombstone iterator exposing
   `deletion_time` (unblocks H3), (b) `delete_segment()` to remove segment
   metadata from RocksDB (unblocks H4, H2), (c) shard deletion via
   `SegmentStore` (unblocks H4, H2). These are small, well-scoped additions to
   `oceanfs-storage` that unblock all four features simultaneously.

2. **Second — implement real compaction.** Fill in `compact_segment()` (H2):
   read live blobs from old segment, classify via TierRouter, write to new
   active segments, update object metadata, delete old shards. This is the
   highest-impact single change because it also validates the TierRouter and
   metadata update paths that Orphan Reaper needs.

3. **Third — add real scrub verification.** Replace the placeholder in
   `scrub_segment()` (H1) with actual BLAKE3 hash computation, Merkle tree
   recomputation, and comparison. This also exercises the Merkle tree
   construction in the operational path.

4. **Fourth — wire up anti-entropy peer exchange.** Restore the full
   `AntiEntropy::new()` constructor (M5) with membership and connection pool,
   implement `run_cycle()` to exchange Merkle roots (C1), and use
   `descend_diff()` for bandwidth-efficient tree comparison (M1).

5. **Fifth — implement shard deletion for orphan reaper.** Once the
   `delete_segment` and shard deletion APIs exist (Rec #1), fill in the
   `OrphanReaper::run_cycle()` deletion path (H4).

6. **Ongoing — create Phase 7 epic README** (L1). Follow the format established
   in the Phase 4 audit. Include a status table, integration test matrix
   (GC+Reaper combined pipeline, AntiEntropy+Scrub interaction), and a
   dependency graph showing that Orphan Reaper depends on GC infrastructure and
   Scrub depends on Anti-Entropy's Merkle trees.

7. **Ongoing — add cancellation tokens** (L2) to all three `start_background()`
   methods. Graceful shutdown is a cross-cutting concern that applies to
   AntiEntropy, ScrubCoordinator, and GarbageCollector.

