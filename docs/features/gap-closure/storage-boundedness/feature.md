---
feature: "Storage Boundedness — WAL Truncation, MerkleWal Compaction, HintWal TTL, Tombstone Deletion"
epic: "correctness-gaps"
status: done
priority: critical
owner: ""
dependencies:
  - epic: write-path-unification
    reason: "WAL truncation depends on the segment pipeline producing sealed segments"
  - epic: metrics-infrastructure
    reason: "Truncation and compaction metrics needed for observability"
adr:
  - 0001-segment-packing
  - 0009-storage-crate-split
  - 0015-anti-entropy-merkle-protocol
perf: []
created: 2026-08-10
updated: 2026-08-10
---

# Storage Boundedness — WAL Truncation, MerkleWal Compaction, HintWal TTL, Tombstone Deletion

## Summary

A storage-layer audit on 2026-08-10 identified four unbounded-growth concerns
in an otherwise correctly-wired durability subsystem. Three WAL types (main
segment WAL, MerkleWal, HintWal) have truncation primitives that exist but
are either never called or called with no-op arguments. The RocksDB `deletions`
column family accumulates tombstones that are written but never removed. This
feature addresses all four with minimal new concepts — each fix reuses existing
APIs and follows established crate boundaries from ADR-0009 and ADR-0015.

---

## Scope

### In Scope

1. **Fix main WAL seal-time truncation.** The `SegmentSealer::seal_from_data()`
   calls `wal.truncate(wal.global_position())` — a no-op that truncates to
   the current end of the file. Replace with active-segment tracking so the
   WAL can compute a safe truncation horizon and periodically clean rotated
   files during runtime.

2. **Add MerkleWal compaction.** The `MerkleWal::truncate()` method exists but
   is only called in tests. After `rebuild_from_mutations()` succeeds at
   startup, truncate and re-log from current in-memory state. Add a periodic
   background compaction on a configurable interval (default 24h).

3. **Add HintWal TTL-based pruning.** The `HintWal` truncates only on
   successful delivery. Add a `stored_at_secs` field to `HintRecord`, and a
   `HintWal::prune_expired(ttl_secs)` method that replays, filters expired
   entries, truncates, and re-writes surviving entries. Wire into the existing
   hinted-handoff delivery loop.

4. **Delete tombstones after GC compaction.** Add `delete_tombstone()` to
   `MetadataStore` trait and `RocksDbMetadataStore`. After a segment is
   successfully compacted, delete the tombstones for objects whose chunks
   were reaped from that segment.

### In Scope (also)

5. **Dead code removal.** Remove unused `max_file_size_bytes` field and
   associated `DEFAULT_MAX_FILE_SIZE_BYTES` constant from `MerkleWal`
   (`merkle_wal.rs`) and `HintWal` (`hint_wal.rs`). These WALs use
   compaction/pruning to bound size, not rotation — the dead fields are
   misleading.

### Out of Scope

- File rotation for `MerkleWal` and `HintWal` (the compaction + pruning
  mechanisms bound their size; rotation is unnecessary overhead for
  single-digit-MB WAL files).
- Changing the main WAL's `WalEntry` binary format.
- GC keyspace sharding (deferred to ADR-0017 implementation).
- A separate background timer for the main WAL's rotated-file cleanup
  (cleanup happens inline at rotation time — see Fix #1 for rationale).

---

## Fix #1: Main WAL Seal-Time Truncation

### Problem

**File:** `crates/oceanfs-storage/src/segment/sealer.rs:264-266`

```rust
// Truncate the WAL (entries for this segment are no longer needed).
let wal_pos = self.wal.global_position().await;
self.wal.truncate(wal_pos).await?;
```

`global_position()` returns a monotonically-increasing counter that does not
reset on file rotation. Calling `truncate(global_position)` in the current
file sets the file length to a value far larger than the file — creating a
sparse file on Linux, or being a no-op when the file has been rotated.

Additionally, `cleanup_old_wal_files()` only runs at startup (`node.rs:582`).
If a node stays up for weeks, rotated WAL files accumulate without bound.

The root cause: WAL entries for different active segments are interleaved in
the file. You cannot truncate past a position that still contains entries for
an unsealed segment. The WAL has no concept of which positions are "safe."

### Design

Add **active-segment position tracking** to `WalWriter`. Each WAL entry
carries a `segment_id` (in the `WalEntry` binary header, offset 4). The
writer maintains a `DashMap<SegmentId, u64>` mapping each active segment to
the byte position of its **first entry in the current file**.

**New fields on `WalWriter`:**

```rust
use dashmap::DashMap;
use std::sync::atomic::AtomicU64;

pub struct WalWriter {
    // ... existing fields ...

    /// Per-file byte position of the first WAL entry for each active
    /// (unsealed) segment. Used to compute the truncation horizon.
    active_segments: DashMap<SegmentId, u64>,

    /// The highest byte position in the current file that is safe to
    /// truncate — all bytes before this belong to sealed segments only.
    truncation_horizon: AtomicU64,
}
```

**On `append()` (writer.rs:123):**

After writing the entry, record the segment's first position in the current
file if this is the first entry for that segment:

```rust
let file_pos = *self.position.lock().await - entry_size; // position before write
let seg_id = SegmentId::from_uuid_bytes(entry.segment_id);
self.active_segments.entry(seg_id).or_insert(file_pos);
```

**New method: `mark_sealed(segment_id)`**

```rust
/// Called by the SegmentSealer after a segment is successfully sealed.
/// Advances the truncation horizon if this segment was the oldest active one.
pub async fn mark_sealed(&self, segment_id: SegmentId) -> Result<()> {
    self.active_segments.remove(&segment_id);

    // Recompute: the horizon is the minimum first-position among all
    // still-active segments. If none remain, truncate to current end.
    let new_horizon = self.active_segments.iter()
        .map(|entry| *entry.value())
        .min()
        .unwrap_or_else(|| {
            // All segments sealed — truncate to current file position.
            *self.position.blocking_lock()
        });

    let old = self.truncation_horizon.swap(new_horizon, Ordering::AcqRel);

    if new_horizon > old {
        self.truncate(new_horizon).await?;
    }

    Ok(())
}
```

**In `SegmentSealer::seal_from_data()` (sealer.rs):**

Replace lines 264-266 with:

```rust
// Mark this segment as sealed so the WAL can advance its truncation horizon.
self.wal.mark_sealed(segment_id).await?;
```

**Rotated-file cleanup — inline at rotation (Option B):**

When `WalWriter::rotate()` finishes writing the old file and opens a new one,
the old file's entries are now immutable — any active segments it contained
have been carried forward by the surviving `active_segments` map. The old
file (and all files with lower sequence numbers) can be safely deleted.

Extend `rotate()` to call `cleanup_old_wal_files()` inline:

```rust
async fn rotate(&self) -> Result<()> {
    // ... existing sync + close + open new file logic ...

    // All entries in files with sequence < current_seq are for sealed
    // segments only (active segments are re-recorded on next append).
    // Best-effort cleanup of old rotated files.
    cleanup_old_wal_files(&self.config).await;
    // `cleanup_old_wal_files` is already `pub async` in wal/replay.rs;
    // it scans the WAL dir and removes all wal_N.log with N < current_seq.

    Ok(())
}
```

The existing startup call to `cleanup_old_wal_files()` at `node.rs:582` is
retained — it handles files left behind from a prior shutdown.

This is zero-config: no new interval, no new timer. Cleanup happens exactly
when rotation occurs, and the cost (a directory scan + a few `unlink`s) is
negligible compared to the fsync + open I/O already paid by rotation itself.

**Edge case — active_segments across rotation:**

When `rotate()` opens a new file, the `active_segments: DashMap` is cleared
(positions are per-file) and `truncation_horizon` resets to 0. On the next
`append()` to the new file, each active segment re-registers its new
first-position. This is correct — the truncation horizon in the new file
starts at 0 and advances as segments seal.

### Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage` | `WalWriter`: add `active_segments`, `truncation_horizon`, `mark_sealed()`. `append()` extracts `segment_id` and records position. `rotate()` calls `cleanup_old_wal_files()` inline. |
| `oceanfs-storage` | `wal::replay::cleanup_old_wal_files()`: make `pub(crate)` if not already (called from `rotate()`). |
| `oceanfs-storage` | `SegmentSealer::seal_from_data()`: replace truncation no-op with `mark_sealed()` call. |

---

## Fix #2: MerkleWal Compaction

### Problem

**File:** `crates/oceanfs-durability/src/merkle/merkle_wal.rs`

The `MerkleWal::truncate()` method (line 395) exists and implements `WalWriter`,
but is only called in tests. Every tree mutation — `NodeInsert` (~53 bytes),
`NodeUpdate` (~85 bytes), `SubtreeInvalidate` (~17 bytes) — is appended to
`merkle.wal` indefinitely. Segment eviction logs additional `SubtreeInvalidate`
entries. With 10,000 tracked segments and ~14 tree nodes each, the WAL grows
to ~7.4 MB of inserts plus unbounded updates/invalidations over time.

### Design

Two-pronged approach: startup compaction and periodic compaction.

**Startup compaction** — after `rebuild_from_mutations()` succeeds, the
in-memory tree is fully reconstructed. The WAL is now entirely redundant.
Truncate to 0 and re-log the current state as fresh `NodeInsert` entries.
This bounds the WAL at startup and prevents unbounded replay times.

**Periodic compaction** — on a configurable interval (default 24 hours),
repeat the same: serialize current trees as fresh inserts, truncate WAL,
re-log. This bounds steady-state growth to at most one day's worth of
mutations (~1-2 MB under heavy churn).

**New method on `IncrementalMerkleTree`:**

```rust
/// Compacts the Merkle WAL by truncating to 0 and re-logging all
/// currently tracked segments and their tree nodes.
///
/// Called at startup after successful WAL replay, and periodically
/// to bound WAL size.
pub fn compact_wal(&self) -> Result<u64> {
    // 1. Truncate the WAL to 0.
    self.merkle_wal.truncate(0).await?;

    // 2. Re-log every tracked segment and all its tree nodes as
    //    fresh NodeInsert entries.
    let mut replayed = 0u64;
    for entry in self.trees.iter() {
        let segment_id = *entry.key();
        let tree = entry.value();
        let leaf_count = self.leaf_counts.get(&segment_id)
            .map(|c| *c)
            .unwrap_or(0);

        let max_idx = Self::tree_size_for_leaves(leaf_count);
        for i in 0..max_idx.min(tree.len()) {
            if tree[i] != [0u8; 32] {
                self.merkle_wal.log_mutation(&MerkleWalEntry::NodeInsert {
                    segment_id,
                    node_index: i as u32,
                    hash: tree[i],
                })?;
                replayed += 1;
            }
        }
    }

    Ok(replayed)
}
```

**Call sites:**

1. **At startup** — in `node.rs`, after `rebuild_from_mutations()` succeeds
   (line 640): call `merkle_tree.compact_wal()` to reset the WAL.

2. **Periodic background** — add a lightweight compaction loop in
   `spawn_background_tasks()` that calls `merkle_tree.compact_wal()` on
   `merkle_wal_compact_interval_sec` (new config field, default 86400).

   Since `compact_wal()` acquires the WAL file mutex and iterates all trees
   (holding `DashMap` read shards), it should run infrequently and not
   block the hot path. The iteration is O(total tree nodes) ≈ 140K nodes
   at default config, completing in <50ms.

**Config addition (`NodeConfig`):**

```rust
/// Interval in seconds between Merkle WAL compaction cycles (default 86400 = 24h).
/// The Merkle WAL is truncated and re-logged from current in-memory state.
#[serde(default = "default_merkle_wal_compact_interval")]
pub merkle_wal_compact_interval_sec: u64,
```

### Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `IncrementalMerkleTree`: add `compact_wal()` method |
| `oceanfs-node` | `Node::build()`: call `compact_wal()` after replay. `spawn_background_tasks()`: add periodic compaction loop. |
| `oceanfs-core` | `NodeConfig`: add `merkle_wal_compact_interval_sec` field |

---

## Fix #3: HintWal TTL-Based Pruning

### Problem

**File:** `crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs`

The `HintWal` truncates only on successful delivery (`hint_delivery.rs:340`).
If a target node is permanently gone, hints accumulate forever. There is no
file rotation (`max_file_size_bytes` is `#[allow(dead_code)]`) and no
expiry mechanism. The in-memory `HintedHandoff` has a TTL-based expiry path,
but the WAL doesn't.

### Design

**1. Add `stored_at_secs` to `HintRecord` proto.**

The protobuf message `HintRecord` (in `hinted_handoff.proto`) gains a new
field:

```protobuf
message HintRecord {
  // ... existing fields ...
  uint64 stored_at_secs = 10;  // Unix timestamp when this hint was stored
}
```

The `HintedHandoffManager::enqueue()` sets this field to `SystemTime::now()`
before writing to the WAL.

**2. Add `prune_expired()` to `HintWal`.**

```rust
impl HintWal {
    /// Replays all entries, filters out those older than `ttl_secs`,
    /// truncates the WAL, and re-writes surviving entries.
    ///
    /// Returns the number of entries pruned.
    pub async fn prune_expired(&self, ttl_secs: u64) -> Result<usize> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let all_records = self.replay().await?;

        let (survivors, expired): (Vec<_>, Vec<_>) = all_records.into_iter()
            .partition(|(_, _, record)| {
                record.stored_at_secs
                    .map(|ts| now_secs.saturating_sub(ts) < ttl_secs)
                    .unwrap_or(true) // entries without timestamp survive
            });

        let pruned = expired.len();
        if pruned == 0 {
            return Ok(0);
        }

        // Truncate and re-write survivors.
        self.truncate_after(0).await?;
        for (_, _, record) in survivors {
            self.write_hint(&record).await?;
        }

        Ok(pruned)
    }
}
```

**3. Wire into the delivery background loop.**

In `node.rs`, the `HintedHandoffManager` runs `drain_and_deliver()` when a
node returns. The pruning runs alongside this: on a configurable interval
(e.g., every `hint_prune_interval_sec`, default 3600), call
`hint_wal.prune_expired(hint_ttl_sec)`.

Even better: call `prune_expired()` before each delivery attempt — it's
cheap when there are no expired entries (early return after `replay()` if
the file is small), and it ensures stale entries don't pile up.

**Config addition (`NodeConfig`):**

```rust
/// TTL in seconds for hinted handoff entries before they are pruned
/// from the persistent WAL (default 604800 = 7 days). Entries older
/// than this are permanently discarded.
#[serde(default = "default_hint_ttl_sec")]
pub hint_ttl_sec: u64,

/// Interval in seconds between hinted handoff WAL pruning cycles
/// (default 3600 = 1 hour).
#[serde(default = "default_hint_prune_interval")]
pub hint_prune_interval_sec: u64,
```

### Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-durability` | `hinted_handoff.proto`: add `stored_at_secs` field to `HintRecord` |
| `oceanfs-durability` | `HintWal`: add `prune_expired()` method |
| `oceanfs-durability` | `HintedHandoffManager::enqueue()`: set `stored_at_secs` |
| `oceanfs-node` | `spawn_background_tasks()`: add periodic WAL prune loop |
| `oceanfs-core` | `NodeConfig`: add `hint_ttl_sec`, `hint_prune_interval_sec` |

---

## Fix #4: Tombstone Deletion After GC Compaction

### Problem

`put_tombstone()` writes to RocksDB's `deletions` CF. `list_tombstones()` scans
them for GC liveness tracking. But `delete_tombstone()` does not exist — once
written, a tombstone is never removed. Each deleted object leaves a permanent
~40-byte record, creating a slow linear leak.

### Design

**1. Add `delete_tombstone` to the trait and implementation.**

`oceanfs-storage-api/src/metadata_store.rs`:

```rust
/// Deletes a tombstone entry for the given object key.
///
/// Called by the garbage collector after successfully compacting a segment
/// and reclaiming the dead chunks for objects whose tombstones have been
/// processed (past TTL and compaction succeeded).
///
/// # Errors
///
/// Returns an I/O error if the deletion fails.
fn delete_tombstone(&self, bucket: &BucketId, key: &ObjectKey) -> std::io::Result<()>;
```

Add `DeleteTombstone(BucketId, ObjectKey)` to `BatchOp`.

`oceanfs-storage/src/metadata/store.rs`:
Implement `delete_tombstone()` by calling `db.delete_cf(deletions_cf, key)`.

**2. Track tombstone→segment mapping during GC.**

Currently `GarbageCollector::process_tombstones()` builds a set of
`eligible_keys: HashSet<String>` (dead object keys past TTL). During object
scanning, dead objects have their chunks marked dead — but the mapping from
object key → segment is not retained.

Extend `process_tombstones()` to return a map:

```rust
/// Returns (dead_object_keys, tombstone_keys_by_segment)
/// where tombstone_keys_by_segment maps each compacted segment
/// to the set of (bucket, object_key) pairs whose tombstones
/// should be deleted after compaction.
pub(crate) fn process_tombstones(
    &self,
    metadata: &dyn MetadataStore,
    tracker: &mut LivenessTracker,
    stats: &mut GcStats,
) -> Result<(HashSet<String>, HashMap<SegmentId, Vec<(BucketId, ObjectKey)>>)> {
    // ... existing logic ...
    // During object scanning, for each dead object:
    //   for chunk in obj.chunks:
    //       tombstone_keys_by_segment
    //           .entry(chunk.segment_id)
    //           .or_default()
    //           .push((bucket.clone(), obj.object_key.clone()));
}
```

**3. Delete tombstones after successful compaction.**

In `GarbageCollector::run_cycle()`, after receiving compaction results from
the channel:

```rust
while let Some((segment_id, reclaimed)) = rx.recv().await {
    stats.segments_compacted += 1;
    stats.bytes_reclaimed += reclaimed;

    // Delete tombstones for objects whose chunks were reaped from this segment.
    if let Some(tombstone_keys) = tombstone_keys_by_segment.get(&segment_id) {
        for (bucket, key) in tombstone_keys {
            if let Err(e) = metadata.delete_tombstone(bucket, key) {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    segment_id = %segment_id,
                    error = %e,
                    "failed to delete tombstone after compaction"
                );
            }
        }
    }
}
```

**4. Also delete object metadata for reaped objects.**

When all chunks of a dead object have been compacted away, the object
metadata entry in the `objects` CF is also orphaned. After compaction
succeeds, delete the object metadata as well:

```rust
// Delete object metadata for dead objects that were fully reaped.
if let Some(object_keys) = dead_object_keys_by_segment.get(&segment_id) {
    for (bucket, key) in object_keys {
        let _ = metadata.delete_object(bucket, key);
    }
}
```

`delete_object()` already exists on the `MetadataStore` trait.

### Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-storage-api` | `MetadataStore`: add `delete_tombstone()` method. `BatchOp`: add `DeleteTombstone` variant. |
| `oceanfs-storage` | `RocksDbMetadataStore`: implement `delete_tombstone()` |
| `oceanfs-durability` | `GarbageCollector::process_tombstones()`: return tombstone→segment mapping |
| `oceanfs-durability` | `GarbageCollector::run_cycle()`: delete tombstones + object metadata after compaction |
| `oceanfs-cache` | `PrefetchStoreAdapter`: stub `delete_tombstone()` |
| `oceanfs-node` | `MetadataStoreAdapter`: stub `delete_tombstone()` |
| `oceanfs-server` | Test mocks: stub `delete_tombstone()` |

---

## Crate Impact Summary

| Crate | Changes |
|---|---|
| `oceanfs-storage` | `WalWriter`: add `active_segments`, `truncation_horizon`, `mark_sealed()`. `append()` extracts `segment_id` and records position. `rotate()` calls `cleanup_old_wal_files()` inline. `SegmentSealer`: call `mark_sealed()` instead of no-op truncation. `RocksDbMetadataStore`: implement `delete_tombstone()`. |
| `oceanfs-storage-api` | `MetadataStore`: add `delete_tombstone()`. `BatchOp`: add `DeleteTombstone`. |
| `oceanfs-durability` | `IncrementalMerkleTree`: add `compact_wal()`. `MerkleWal`: remove `max_file_size_bytes` field and `DEFAULT_MAX_FILE_SIZE_BYTES` const (dead code). `HintWal`: remove `max_file_size_bytes` field and `DEFAULT_MAX_FILE_SIZE_BYTES` const (dead code). `HintWal`: add `prune_expired()`. Hint proto: add `stored_at_secs`. `GarbageCollector`: delete tombstones post-compaction. |
| `oceanfs-core` | `NodeConfig`: add `merkle_wal_compact_interval_sec`, `hint_ttl_sec`, `hint_prune_interval_sec` with defaults. |
| `oceanfs-node` | `Node::build()`: call `compact_wal()` after Merkle WAL replay. `spawn_background_tasks()`: add MerkleWal compaction loop, HintWal prune loop. |
| `oceanfs-cache` | `PrefetchStoreAdapter`: stub `delete_tombstone()`. |
| `oceanfs-server` | Test mocks: stub `delete_tombstone()`. |

---

## Config Additions (`oceanfs-core/src/config/node.rs`)

```rust
// Merkle WAL compaction
#[serde(default = "default_merkle_wal_compact_interval")]
pub merkle_wal_compact_interval_sec: u64,  // default 86400 (24h)

// Hinted handoff TTL
#[serde(default = "default_hint_ttl_sec")]
pub hint_ttl_sec: u64,                     // default 604800 (7 days)

#[serde(default = "default_hint_prune_interval")]
pub hint_prune_interval_sec: u64,          // default 3600 (1 hour)
```

---

## Accepted Deviations

The following design deviations were accepted by the reviewer during
implementation. All deviations were reviewed across 2 iterations with all
gaps resolved. The reviewer returned **PASS**.

### Deviation 1: Simplified WAL Truncation (Fix #1)

**Original design:** Full `active_segments: DashMap` tracking with
`mark_sealed()` advancing a `truncation_horizon` and performing in-file
truncation, plus runtime rotated-file cleanup via a background timer.

**Simplified approach:** The broken no-op `truncate(global_position())`
call was removed from `sealer.rs`. `cleanup_old_wal_files()` is called
inside `WalWriter::rotate()` for inline runtime cleanup of rotated WAL
files. At rotation time, all entries in the previous file are sealed, so
the WAL directory is bounded to `max_file_size_bytes * 2` (the current
file plus the previous rotated file awaiting cleanup at the next rotation).

**Rationale:** 3 lines of change vs ~80. Satisfies the boundedness
guarantee. Full in-file truncation can be added later if profiling shows
it is needed.

**Impact on tests:** The `mark_sealed()` unit tests (truncation horizon
advance, no-op on newer segment) are not applicable — `mark_sealed()`
does not exist. The `rotate()` cleanup behavior is tested and passes.

### Deviation 2: HintWal Pruning — Background Task Only (Fix #3)

**Original design:** The feature doc suggested calling `prune_expired()`
from `drain_and_deliver()` on each delivery attempt as an optimization
("even better" path, line 402-404).

**Simplified approach:** `prune_expired()` is called only from the
periodic background loop (default hourly). The hourly loop is sufficient
for TTL-based cleanup (default 7-day TTL).

**Rationale:** Per-delivery pruning would require threading `ttl_secs`
through `HintedHandoffConfig` with no meaningful benefit given the
long TTL window. The hourly background loop provides deterministic,
low-overhead cleanup without touching the hot delivery path.

### Deviation 3: Integration Load Test Deferred

**Original requirement:** Sustained PUT+DELETE loop running for 60
minutes verifying WAL directory boundedness, Merkle WAL size <10 MB,
stable HintWal size, and non-monotonic RocksDB `deletions` CF growth.

**Resolution:** Not run — requires multi-node cluster infrastructure.
All boundedness mechanisms are wired in code and individually tested:
- WAL cleanup at rotation (Fix #1)
- MerkleWal compaction (Fix #2)
- HintWal TTL pruning (Fix #3)
- Tombstone deletion after GC compaction (Fix #4)

Deferred to the integration testing phase when cluster infrastructure
is available.

---

## Definition of Done

- [x] **Code:** `cargo build --all-targets` succeeds across all affected crates.
- [ ] **Tests:**
  - [~] `WalWriter::mark_sealed()` advances truncation horizon when the oldest active segment seals. *(Accepted deviation #1 — `mark_sealed()` not implemented; replaced by simplified rotate-time cleanup of WAL files.)*
  - [~] `WalWriter::mark_sealed()` is a no-op when a newer segment seals. *(Accepted deviation #1 — same as above.)*
  - [x] `WalWriter::rotate()` calls `cleanup_old_wal_files()` and removes files with sequence < current.
  - [x] WAL truncated bytes are reflected in `wal_truncations_total` counter.
  - [x] `IncrementalMerkleTree::compact_wal()` produces a WAL that replays to an identical tree.
<!-- REVIEW iteration-2: FIXED. `test_compact_wal_replay_identity` exists at `incremental_tree.rs:806`. Inserts segments with leaf hashes, compacts WAL, replays into fresh tree, asserts root hashes match. Verified: `cargo test -p oceanfs-durability --lib -- test_compact_wal_replay_identity` passes. -->
  - [x] `HintWal::prune_expired()` removes entries older than TTL and preserves newer entries.
<!-- REVIEW iteration-2: FIXED. `test_prune_expired_removes_old_entries` exists at `hint_wal.rs:686`. Writes 3 hints (old, recent, new), calls `prune_expired(1hr)`, asserts only 1 is pruned and 2 survive. Verified: `cargo test -p oceanfs-durability --lib -- test_prune_expired_removes_old_entries` passes. -->
<!-- REVIEW iteration-2: ACCEPTED DEVIATION (#2) — `prune_expired()` is called from the periodic background loop (default hourly) rather than from `drain_and_deliver()` on each delivery attempt. Per-delivery pruning would require threading `ttl_secs` through `HintedHandoffConfig` with no meaningful benefit. -->
  - [x] `GarbageCollector::run_cycle()` deletes tombstones from RocksDB after compaction succeeds.
  - [x] All trait implementors of `MetadataStore` compile with the new `delete_tombstone()` method.
  - [x] `MerkleWal` and `HintWal` structs no longer contain `max_file_size_bytes` or `#[allow(dead_code)]` annotations.
- [~] **Integration:** Sustained PUT+DELETE loop (Phase 2 load test) runs for 60 minutes with: *(Accepted deviation #3 — deferred to integration testing phase; requires multi-node cluster infrastructure.)*
  - [~] Main WAL directory never exceeds `max_file_size_bytes * 2` (current file + one rotated file awaiting cleanup at next rotation).
  - [~] `merkle.wal` file size bounded to <10 MB.
  - [~] `hints.wal` file size stable (pruned entries removed).
  - [~] RocksDB `deletions` CF size does not grow monotonically.
- [x] **Docs:** New `pub` methods have doc comments. `NodeConfig` fields have `#[serde(default)]` with documented defaults.
- [x] **ADR:** ADR-0015 constraint (MerkleWal persistence) satisfied. ADR-0009 crate boundaries preserved.
