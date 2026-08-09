# ADR-0015: Anti-Entropy Merkle Tree Protocol — Incremental Trees, MerkleWal, and Sampling

**Status:** Proposed
**Date:** 2026-08-09
**Deciders:** OceanFS design team

---

## Context

The spec §7.4 defines anti-entropy in one paragraph: "exchange Merkle roots,
descend on mismatch." The current implementation rebuilds Merkle trees from
scratch for every anti-entropy cycle, reconstructs peer trees locally rather
than receiving them over the wire, and runs on the full metadata keyspace
unconditionally.

A manual code review on 2026-08-09 identified five related issues:

| # | Finding |
|---|---|
| **#15** | Anti-entropy rebuilds Merkle trees from scratch each cycle |
| **#16** | Peer Merkle tree is reconstructed locally; the `MerkleExchange` gRPC exists but doesn't send pre-built tree structure |
| **#17** | AE runs on full keyspace; a random subset would reduce resource pressure and allow more frequent cycles |
| **#18** | AE inconsistently uses EC optimisations — sometimes pushes to heal pool, sometimes uses local Cauchy matrices |
| **#27** | Scrub worker raises the same questions as anti-entropy |

The constraint is that any new design must work within the existing crate
boundaries defined by ADR-0009 (storage/durability split) and must be
recoverable after a node restart without degrading to a full segment scan
on the critical path.

## Decision

### 1. Incremental Merkle Tree in `oceanfs-durability`

The Merkle tree for each segment is maintained incrementally rather than
rebuilt from scratch each cycle. When a segment is sealed and written to
storage, `oceanfs-durability` observes the event (via a notifier from
`oceanfs-storage` or by polling the `segments` column family) and updates
the tree:

- Each sealed segment corresponds to one leaf hash in a per-segment Merkle tree
- Internal node hashes are recomputed in O(log n) along the path from leaf to root
- The in-memory tree structure is a binary Merkle tree with BLAKE3 as the hash
  function, matching the existing `merkle_tree` module in `oceanfs-durability`

The tree is owned entirely by `oceanfs-durability`. `oceanfs-storage` has no
knowledge of Merkle trees — it writes segments and notifies durability of new
sealed segments. This preserves the boundary established by ADR-0009.

### 2. MerkleWal for Crash Recovery

Tree mutations are persisted to a dedicated `MerkleWal` — a write-ahead log
specialised for small tree-node records (unlike `SegmentWal` which handles
multi-kilobyte segment data). The `MerkleWal` implements the `WalWriter` trait
from `oceanfs-storage-api` (ADR-0009, Part 2).

**Record format:**

```
MerkleWalEntry {
    segment_id: SegmentId,
    node_index: u32,          // position in the binary tree
    hash: [u8; 32],           // BLAKE3 hash of this node
    operation: NodeInsert | NodeUpdate | SubtreeInvalidate,
}
```

**Recovery on restart:**
1. Replay `MerkleWal` — rebuild in-memory tree from logged mutations
2. If replay fails (corrupted WAL): rebuild all trees from a full scan of the
   `segments` RocksDB column family, write a fresh `MerkleWal`, log `WARN`
3. The in-memory tree is ready before the first AE cycle begins

**Why MerkleWal over RocksDB CF:** The tree mutations are an append-only log
of incremental changes — naturally a WAL, not a random-access key-value store.
A dedicated WAL keeps the `segments` CF clean and avoids mixing derivable
metadata with source-of-truth data. If the WAL is lost, the tree is
reconstructable from segment data at the cost of one full scan.

### 3. Two Anti-Entropy Modes

| Mode | Trigger | Mechanism | Resource Usage |
|---|---|---|---|
| **Continuous** | Every N segment writes, or every `gossip_interval_ms` | Exchange incremental Merkle roots with neighbours. Root mismatch → descend tree to find divergent leaves. | Low (single root hash, incremental descent only on mismatch) |
| **Sampling** | Every `ae_interval_sec` (configurable, default 300s) | Exchange roots for a random subset of segments (configurable fraction, default 5%). Detect divergence probabilistically. | Very low (5% of segments × one root each) |

**Interaction:** Continuous mode catches divergence in actively-written segments
quickly. Sampling mode covers cold segments that haven't been written recently
(and therefore haven't triggered a continuous exchange). If sampling detects a
mismatch, it triggers a full descent on that segment — same as continuous.

**Full scrub** (spec §7.5) remains the heavyweight option — full scan of all
segments, every `scrub_interval_sec` (default 7 days). It is not affected by
this ADR.

**Configuration:**

```toml
[anti_entropy]
continuous_enabled       = true
continuous_max_segments  = 10000     # max segments tracked in continuous mode
sampling_enabled         = true
sampling_interval_sec    = 300
sampling_fraction        = 0.05      # 5% of segments per cycle
```

### 4. gRPC MerkleExchange Protocol — Pre-Built Tree Sending

The `MerkleExchange` gRPC (`spec §12.3`) is extended so the responding node
can send its pre-built tree structure rather than requiring the requestor to
reconstruct it:

```protobuf
message MerkleRequest {
  repeated bytes segment_ids = 1;
  bool include_full_tree = 2;     // requestor asks for full tree
}

message MerkleResponse {
  bytes merkle_root = 1;
  repeated TreeNode internal_nodes = 2;  // only populated if include_full_tree=true
                                          // OR if roots mismatch (server decides)
  bool full_tree_included = 3;
}

message TreeNode {
  uint32 node_index = 1;
  bytes hash = 2;
  repeated uint32 children = 3;
}
```

**Protocol flow:**
1. Node A sends `MerkleRequest { segment_ids, include_full_tree: false }`
2. Node B responds with `{ merkle_root }`
3. If roots match → done
4. If roots mismatch → Node A sends `MerkleRequest { segment_ids, include_full_tree: true }`
5. Node B responds with `{ merkle_root, internal_nodes, full_tree_included: true }`
6. Node A compares internal nodes, identifies divergent leaves, triggers repair

Since both nodes maintain incremental trees (Decision 1), step 5 is a cheap
serialisation of the in-memory tree — no on-demand rebuild. Bandwidth cost:
O(n) for n leaves, same as local reconstruction, but CPU cost on the requestor
is eliminated.

### 5. EC Optimisation Unification

Anti-entropy and scrub must use the same EC decode path consistently:

- All Merkle-detected divergence routes to the **heal pool** (which uses
  `AccelDispatcher` for tiered EC decode, per spec §9)
- Remove local Cauchy matrix usage from the AE path
- Scrub worker follows the same pattern — detect via Merkle, repair via heal pool

This eliminates the inconsistency flagged in finding #18.

### Scope

**In scope:**
- Incremental Merkle tree maintenance in `oceanfs-durability`
- `MerkleWal` implementing `WalWriter` from `oceanfs-storage-api`
- Continuous + sampling AE modes
- Pre-built tree exchange over gRPC
- EC optimisation unification (AE and scrub both use heal pool)

**Out of scope:**
- Full scrub redesign (scrub remains full-scan, per spec §7.5)
- Merkle tree for individual blobs within a segment (the tree is per-segment;
  intra-segment integrity is covered by BLAKE3 per-blob checksums)
- Adaptive sampling rate (static 5% fraction, configurable)

## Consequences

### Positive

- **Faster AE cycles.** Continuous mode exchanges one root hash per segment
  (32 bytes) instead of rebuilding a full tree. Sampling mode processes 5% of
  segments instead of 100%. Combined, AE becomes near-continuous at negligible
  resource cost.
- **Crash-safe incremental state.** `MerkleWal` survives restarts. No
  degradation to full-scan on the first cycle after boot.
- **Reduced network traffic.** Pre-built tree exchange (Decision 4) avoids
  peer-side CPU cost. Root-only exchange (32 bytes per segment) keeps bandwidth
  minimal in the common case (no divergence).
- **Clean crate boundary.** The tree lives in `oceanfs-durability`, observing
  storage events. Storage has no knowledge of Merkle trees. ADR-0009 boundary
  preserved.
- **Unified EC path.** All repair triggers (AE, scrub, heal) use the same
  acceleration tier via the heal pool. No duplication of Cauchy matrices.

### Negative

- **New WAL type.** `MerkleWal` is the third WAL implementation (`SegmentWal`,
  `HintWal` per review finding #25, `MerkleWal`). Each adds maintenance burden.
  However, all three implement the same `WalWriter` trait, so the API surface
  is shared.
- **Event observation mechanism.** `oceanfs-durability` must observe segment
  writes from `oceanfs-storage`. This requires either a notifier channel passed
  from `oceanfs-node` (composition root), or periodic polling of the `segments`
  CF. The notifier approach adds a cross-crate dependency that `oceanfs-node`
  resolves at startup — architecturally clean but adds wiring code.
- **In-memory tree memory.** Each tracked segment requires O(log n) tree nodes
  in memory. For `continuous_max_segments = 10000` with a tree height of ~14:
  10,000 × 14 × 32 bytes ≈ 4.5 MB. Acceptable. If the operator configures a
  larger max, memory scales linearly — documented in the config field comment.

### Neutral

- **Sampling is probabilistic.** A 5% sample means a single corrupted segment
  has a 5% chance of detection per cycle. Over 20 cycles (~100 minutes at
  default 300s interval), detection probability exceeds 64%. Operators who
  require deterministic detection should use full scrub at a shorter interval
  or increase `sampling_fraction`.
- **Scrub is unchanged.** This ADR does not modify the full-scrub path. Scrub
  continues as a heavyweight background task with distributed partition
  assignment per spec §7.5.

## Considered Alternatives

| Alternative | Pros | Cons | Why Rejected |
|---|---|---|---|
| **A. Rebuild from scratch every cycle** | Simplest implementation; no persistence complexity | O(n) scan per cycle; resource-intensive; finding #15 flagged this as unacceptable | Rejected: the scan cost grows with segment count; becomes a bottleneck in production |
| **B. Persist tree nodes in RocksDB `merkle` column family** | Atomic with segment writes; crash recovery via RocksDB | RocksDB write amplification; mixes derivable metadata with source-of-truth data; `segments` CF already stores segment metadata — adding tree nodes bloats it | Rejected: a WAL is a more natural fit for an append-only mutation log; RocksDB adds compaction overhead for data that is entirely reconstructable |
| **C. Persist tree mutations to the segment WAL** | Single WAL file; no new WAL type | Mixes segment data (multi-KB) with tree metadata (~32 bytes per node); WAL format becomes heterogeneous; complicates segment replay | Rejected: violates single-responsibility for WAL types; the segment WAL's job is segment durability, not tree maintenance |
| **D. No incremental tree — sampling only** | Simplest; no tree maintenance at all | Sampling is probabilistic — guaranteed to miss rare corruption; no continuous mode for actively-written segments | Rejected: continuous mode is the primary value of this redesign; sampling is a complement, not a replacement |

## References

- [Spec §7.4: Anti-Entropy](../../docs/spec.md#74-anti-entropy-background)
- [Spec §7.5: Distributed Scrubbing](../../docs/spec.md#75-distributed-scrubbing)
- [Spec §12.3: Internal gRPC — MerkleExchange](../../docs/spec.md#123-internal-grpc-node-to-node)
- [ADR-0009: Storage Crate Split](./0009-storage-crate-split.md)
- [Review 2026-08-09, findings #15-18, #27](../../review/08-09-2026.md)
