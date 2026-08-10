# ADR-0018: Durability WAL Consolidation — Remove MerkleWal, Per-Node HintWal, Segment-Ref Hints

**Status:** Proposed
**Date:** 2026-08-10
**Deciders:** OceanFS design team

---

## Context

OceanFS currently has four independent write-ahead log / persistence domains:

| # | Component | Crate | Format | fsync Strategy | Compaction |
|---|---|---|---|---|---|
| 1 | Segment WAL (`WalWriter`) | `oceanfs-storage` | 80-byte header + inline blob data | Group commit via `WalSyncGroup` | Truncation on seal |
| 2 | Hint WAL (`HintWal`) | `oceanfs-durability` | Length-prefixed protobuf + CRC32 | Per-entry `sync_all()` | Byte-position truncation |
| 3 | Merkle WAL (`MerkleWal`) | `oceanfs-durability` | Tree-node mutation records | Per-entry | Background compaction task |
| 4 | RocksDB WAL | RocksDB (via `oceanfs-storage`) | RocksDB internal | RocksDB-managed | LSM compaction |

This creates four separate failure domains, four recovery paths, four format implementations, and four consistency boundaries. An architectural review identified two of these as unnecessary or structurally flawed.

### HintWal: Data Duplication and Internode Pollution

The coordinator creates a hint whenever a replica write fails during quorum collection (`crates/oceanfs-server/src/write/coordinator.rs:342`):

```rust
let hint = HintRecord::new_inline(target, bucket, key, req.data.clone());
let _ = self.hinted_handoff.enqueue(hint).await;
```

Two problems:

1. **Data duplication.** `req.data` is the full blob payload (up to 1 MB). This data was already written to the Segment WAL on line 249 (`self.write_wal_entry(...)`) — the same data exists in two WALs. The `HintRecord::new_segment_ref()` constructor and `HintedHandoffConfig::inline_threshold_bytes` (default 4096) exist in the codebase but are not wired at the call site.

2. **Internode pollution.** All hints for all target nodes are interleaved in a single `hints.wal` file. When node-A returns and its hints are delivered, the file can only be truncated at the position of the earliest undelivered hint across ALL nodes. A single node that stays down for hours prevents the entire file from being truncated, causing unbounded dead-space growth.

### MerkleWal: Persisting Derived State

ADR-0015 §2 established a `MerkleWal` to persist incremental Merkle tree mutations for crash recovery. The Merkle tree is a derived index over sealed segments — the authoritative source of truth is the `segments` column family in RocksDB (each `SegmentMetadata` contains a `blake3_hash`). Persisting derived state in a separate WAL creates:

- A **dual-write consistency gap**: the sealer writes to RocksDB (`segments` CF) and emits a notification via `mpsc` channel to the Merkle tree updater, which writes to `MerkleWal`. A crash between these two operations leaves the segment in RocksDB but not in the Merkle tree.
- A **background compaction task** (`merkle_wal_compact` in `BackgroundTasks`) that exists solely to reclaim space in the Merkle WAL.
- An **additional fsync domain** with its own format and recovery path.

The Merkle tree is an optimization — it spreads the cost of tree construction across seals rather than paying it at anti-entropy time. It is not required for correctness.

---

## Decision

### Decision 1: Remove MerkleWal — Rebuild Merkle Tree from Segments CF on Startup

The `MerkleWal` is removed. The `IncrementalMerkleTree` remains as a pure in-memory structure, updated incrementally on each segment seal via the existing `mpsc` notification channel from `SegmentSealer`. On node restart, the tree is rebuilt from scratch by scanning the `segments` column family in RocksDB.

**Affected code:**
- **Remove:** `crates/oceanfs-durability/src/merkle/merkle_wal.rs` (the `MerkleWal` struct and its `open`, `append`, `replay`, `compact` methods)
- **Remove:** Background compaction task in `crates/oceanfs-node/src/node.rs` (`merkle_wal_compact` field in `BackgroundTasks`, `merkle_wal_compact_cancel` token, the `tokio::spawn` call in `spawn_background_tasks`)
- **Modify:** `crates/oceanfs-node/src/node.rs:628-671` — replace `MerkleWal::open` + `rebuild_from_mutations` with a direct scan of the `segments` CF via `RocksDbMetadataStore::list_segments()`
- **Modify:** `crates/oceanfs-node/src/node.rs` — remove the segment seal → Merkle tree notification background task (lines 675-719), since the tree is now updated synchronously during seal
- **Modify:** `crates/oceanfs-durability/src/merkle/mod.rs` — remove `pub mod merkle_wal` and the `MerkleWal` re-export
- **Supersedes:** ADR-0015 §2 ("MerkleWal for Crash Recovery"). ADR-0015 §1 (incremental tree) and §3 (sampling) remain unchanged.

**Startup rebuild:**
```
for each SegmentMetadata in metadata_store.list_segments():
    tree.insert_leaf(segment_id, segment_metadata.blake3_hash)
```

Cost: O(N) sequential scan of `segments` CF. For 1M segments: ~1 second. For 10M segments: ~10 seconds. Startup is infrequent; anti-entropy runs every 300 seconds by default.

**Rationale:**
- Eliminates one WAL domain (format, fsync, recovery path)
- Eliminates the dual-write consistency gap between sealer and MerkleWal
- Removes a background compaction task
- The tree is derived state; the authoritative source is `segments` CF

### Decision 2: Per-Node HintWal Files

The single `hints.wal` is replaced with per-node files under `{data_dir}/hints/{node_id}.wal`. Each file is independently managed and truncated.

**Affected code:**
- **Modify:** `crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs` — `HintWal` already accepts a path via `open(path)`. The change is in the caller: instead of opening a single file, the manager maintains a `DashMap<NodeId, Arc<HintWal>>`.
- **Modify:** `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs` — `HintedHandoffManager` (line 155):
  - Replace `hint_wal: Arc<HintWal>` with `wal_dir: PathBuf` (the directory containing per-node files)
  - Replace `enqueue()` (line 243): resolve `target_node.wal`, create if absent, call `write_hint()` on that node's WAL
  - Replace `drain_and_deliver()` (line 281): after successful delivery, call `truncate(0)` on the node's WAL and remove the entry from the map
  - Replace `replay_and_enqueue()` (line 217): scan the directory for `*.wal` files, replay each, populate queues
  - Lazy open/close: open a node's WAL on first write; close it after 60 seconds of inactivity; cap at 16 concurrently open files
- **Modify:** `crates/oceanfs-durability/src/hinted_handoff/mod.rs` — update `HintedHandoffConfig` to use `wal_dir: PathBuf` instead of `wal_path: PathBuf`
- **Modify:** `crates/oceanfs-node/src/node.rs` — the `HintedHandoffManager` construction passes `config.data_dir.join("hints")` as the directory

**File lifecycle per node:**
```
Node goes down:
  → create hints/{node_id}.wal
  → append hint records (one per failed replica write)
  → each record: [u32 LE: payload_len][protobuf: HintRecord][u32 LE: crc32]

Node returns:
  → drain_and_deliver(node_id)
  → truncate(0) on hints/{node_id}.wal
  → remove file (or keep empty for reuse)
```

**Rationale:**
- Truncation is instant and complete — no fragmentation, no dead space
- fsync is per-node — hints for node-A don't contend with hints for node-B
- Number of files ≤ number of distinct unreachable nodes; in practice 1-3 at any time
- FD pressure is bounded by lazy open/close with a concurrent-open cap

### Decision 3: Segment References for Non-Inline Hints

The coordinator uses `HintRecord::new_segment_ref()` instead of `HintRecord::new_inline()` for blobs exceeding `HintedHandoffConfig::inline_threshold_bytes` (default 4096).

**Affected code:**
- **Modify:** `crates/oceanfs-server/src/write/coordinator.rs:342-347` — replace the unconditional `new_inline()` call with a size check:
  ```rust
  let hint = if req.data.len() as u64 <= self.hint_config.inline_threshold_bytes {
      HintRecord::new_inline(target, bucket, key, req.data.clone())
  } else {
      // Use the segment_id, offset, and length from the append above.
      let chunk = &chunks[0]; // single chunk for Small/Standard tier
      HintRecord::new_segment_ref(target, bucket, key, chunk.segment_id, chunk.offset, chunk.length)
  };
  ```
- The `HintedHandoffConfig` struct and `HintRecord::new_segment_ref()` already exist and are tested — only the call site wiring is needed.

**Rationale:**
- HintWal records shrink from up to 1 MB to ~40 bytes (segment_id + offset + length) for all non-inline blobs
- The data is already durable in the Segment WAL — the hint only needs to point to it
- Per-entry fsync on a 40-byte record is negligible
- `HintInline` remains for inline blobs (≤4 KB) that were stored directly in RocksDB metadata and never touched a segment

---

## Consequences

### Positive

- **4 WAL domains → 2.** Segment WAL (blob data in flight) + RocksDB WAL (metadata). The HintWal files are a sub-structure within durability, not an independent persistence domain — they use the same format and recovery pattern, just partitioned by node.
- **No cross-node truncation pollution.** Each node's hint file is independently truncated to zero on successful delivery.
- **No data duplication.** Hint records point to segment data rather than copying it.
- **No MerkleWal compaction task.** Background task count decreases by one.
- **No dual-write consistency gap.** The Merkle tree's correctness depends solely on `segments` CF (RocksDB).
- **Simpler crash recovery.** Two WAL formats to replay instead of four.

### Negative

- **Startup rebuild cost.** Scanning `segments` CF on startup takes O(N) time (~10s for 10M segments). This replaces the previous O(1) MerkleWal replay. Accepted for infrequent restarts.
- **Per-node file FD management.** Requires lazy open/close logic in `HintedHandoffManager`. Mitigated by a concurrent-open cap (default 16).
- **ADR-0015 partial supersession.** ADR-0015 §2 (MerkleWal) is superseded. The remainder of ADR-0015 (incremental tree, sampling) is unchanged.
- **Deletes recently-written code.** `MerkleWal` (~200+ lines) and the single-file `HintWal` management (~100 lines of wiring) are removed.

### Neutral

- The `IncrementalMerkleTree` struct and its `insert_leaf` / `compute_root` methods are unchanged. Only persistence is removed.
- The `HintWal` struct itself is unchanged — it already operates on a per-file basis via `open(path)`. Only the call site changes from single-file to per-node.
- `HintRecord` protobuf definitions are unchanged. Both `HintInline` and `HintSegmentRef` already exist.

---

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **Columnar HintWal (index + data file)** | Two files total, no internode pollution, group-committed fsync | Compaction complexity, random reads on delivery, medium implementation effort | Over-engineered for the common case (1-3 unreachable nodes). Per-node files achieve the same isolation with simpler code. |
| **Bucketed HintWal (hash → 64 files)** | Bounded FD count, reduced (but not eliminated) pollution | Dead space still accumulates within each bucket; the 1:64 reduction may not be enough for pathological cases | Sits in an awkward middle ground that inherits drawbacks from both extremes. |
| **RocksDB-backed HintWal** | No additional fsync, LSM compaction handles space reclamation | Blob data (up to 1 MB for inline hints) in RocksDB values causes write amplification; couples hint persistence to metadata store | Decision 3 (segment references) makes this moot — hints are now tiny pointers. If we still stored inline data, this would be a stronger alternative. |
| **Ephemeral hints (no persistence)** | Zero I/O on write path, no persistence code | Hints lost on coordinator crash; returning node must rely on anti-entropy (slower recovery) | Acceptable if anti-entropy ran frequently, but adds network load on node return. Retaining durability for hints is worth the small I/O cost after Decision 3. |
| **Keep MerkleWal (status quo)** | Instant tree recovery on restart | Extra WAL domain, background compaction, dual-write consistency gap | The tree is derived state; persisting it separately is wasteful. Startup rebuild cost is negligible. |

---

## References

- [ADR-0015: Anti-Entropy Merkle Tree Protocol](./0015-anti-entropy-merkle-protocol.md) — §2 is superseded by this ADR
- [ADR-0009: Storage Crate Split](./0009-storage-crate-split.md) — defines crate boundaries for `oceanfs-durability`
- [Spec §7.2: Hinted Handoff](../spec.md#72-hinted-handoff)
- [Spec §7.4: Anti-Entropy](../spec.md#74-anti-entropy-background)
- Coordinator hint creation: `crates/oceanfs-server/src/write/coordinator.rs:331-351`
- HintWal implementation: `crates/oceanfs-durability/src/hinted_handoff/hint_wal.rs`
- HintDeliveryManager: `crates/oceanfs-durability/src/hinted_handoff/hint_delivery.rs`
- Node startup WAL wiring: `crates/oceanfs-node/src/node.rs:564-719`
- Segments CF schema: `crates/oceanfs-storage/src/metadata/cf.rs`
