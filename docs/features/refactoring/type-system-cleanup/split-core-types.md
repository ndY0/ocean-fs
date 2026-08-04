---
feature: "Split Core Types File"
epic: "refactoring/type-system-cleanup"
status: done
priority: critical
owner: ""
dependencies: []
adr: []
perf: []
created: 2026-08-03
updated: 2026-08-04
---

# Split Core Types File

## Summary

Split `crates/oceanfs-core/src/types.rs` (~2,198 lines, 45+ public types) into a
`types/` directory with one file per type category. This eliminates the god-file
that concentrates 12 of the top-20 most-depended-upon symbols and violates the
"one public type per file" rule (architecture guideline §3.3). The existing
`pub mod types;` in `oceanfs-core/src/lib.rs` must continue working with zero
downstream changes — the new `types/mod.rs` serves as a transparent re-export
facade.

## Scope

### In Scope

- Create `types/id.rs` containing `SegmentId`, `NodeId`, `BucketId`, `ObjectKey`
  and their `impl` blocks
- Create `types/hash.rs` containing `HashOutput` and `HashKey`
- Create `types/metadata.rs` containing `ObjectMetadata`, `SegmentMetadata`,
  `ChunkRef`, `SegmentIndexEntry`, `Tombstone`, and `StorageLocation`
- Create `types/config.rs` containing `RpcConfig`, `PoolConfig`, `GpuConfig`,
  `GossipConfig`, `NvcompConfig`, `CompressConfig`, `CompressionTier`, and any
  other config types currently in `types.rs`
- Create `types/codec.rs` containing `CodecType`, `CodecConfig`, and `EncodingPlan`
- Create `types/heal.rs` containing `HealRequest`, `HealStats`, and `ShardIndex`
- Create `types/node.rs` containing `NodeState`, `VnodeRange`, `Incarnation`,
  `IntendedFor`, `PeerAddress`, `WriteQuorum`, `WriteResult`, and `WriteAck`
- Create `types/cache.rs` containing `CacheInvalidateRequest`
- Create `types/mod.rs` as the re-export facade — identical public API to the
  old `types.rs`
- Migrate every test function from the `#[cfg(test)] mod tests` block in the
  old `types.rs` into the file that owns the type under test
- Delete `types.rs` and replace with the `types/` directory

### Out of Scope

- Renaming, refactoring, or changing any type's fields, methods, or visibility
- Moving types between crates (e.g., `HashOutput` to `oceanfs-hash` — that is
  feature `resolve-hash-crate`)
- Splitting `oceanfs-core/src/config.rs` (that is Epic 6, feature `split-config`)
- Moving the `MetadataStore` trait out of `types.rs` — if it exists in
  `types.rs`, it stays; the trait move is Epic 3, feature `move-metadata-store-trait`
- Any downstream crate changes — the re-export facade ensures zero breakage

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | Delete `src/types.rs`; create `src/types/` directory with 9 files (`id.rs`, `hash.rs`, `metadata.rs`, `config.rs`, `codec.rs`, `heal.rs`, `node.rs`, `cache.rs`, `mod.rs`) |

## Interface (Public API)

No new public items. No removed public items. The facade in `types/mod.rs`
re-exports every public item that was previously exported from `types.rs`.
Downstream consumers (`use oceanfs_core::types::SegmentId`) continue to work
unchanged.

The re-export facade follows the pattern established in `architecture.md` §3.1:

```rust
// oceanfs-core/src/types/mod.rs
mod id;
mod hash;
mod metadata;
mod config;
mod codec;
mod heal;
mod node;
mod cache;

pub use id::{SegmentId, NodeId, BucketId, ObjectKey};
pub use hash::{HashOutput, HashKey};
pub use metadata::{ObjectMetadata, SegmentMetadata, ChunkRef, SegmentIndexEntry, Tombstone, StorageLocation};
pub use config::{RpcConfig, PoolConfig, GpuConfig, GossipConfig, NvcompConfig, CompressConfig, CompressionTier};
pub use codec::{CodecType, CodecConfig, EncodingPlan};
pub use heal::{HealRequest, HealStats, ShardIndex};
pub use node::{NodeState, VnodeRange, Incarnation, IntendedFor, PeerAddress, WriteQuorum, WriteResult, WriteAck};
pub use cache::CacheInvalidateRequest;
```

All types remain in the `oceanfs_core::types` namespace.

## Data Flow

This is a pure structural refactor. No runtime data flow changes.

```
Old:  crates use oceanfs_core::types::SegmentId
            ↓
      oceanfs-core/src/types.rs (2,198 lines, monolithic)

New:  crates use oceanfs_core::types::SegmentId
            ↓
      oceanfs-core/src/types/mod.rs (re-exports)
            ↓
      oceanfs-core/src/types/id.rs (SegmentId definition)
```

## Definition of Done

- [x] **Code:** `cargo build --all-targets -p oceanfs-core` succeeds; workspace has pre-existing `oceanfs-server` test compilation errors (unrelated — `replicate_write` missing argument)
<!-- REVIEW: verified — `cargo build --all-targets -p oceanfs-core` passes clean. Workspace build: `oceanfs-server` test compile fails (pre-existing, confirmed via git stash). -->
- [x] **Tests:** 119 unit tests + 5 integration tests + 45 doc-tests pass; all 66 tests from old `types.rs` migrated correctly
<!-- REVIEW: verified — all 119 unit tests pass. 66 tests from old types.rs matched exactly across new files. Per-file test counts by implementer inflated (id.rs: 14 not 16, metadata.rs: 11 not 12, config.rs: 17 not 21, node.rs: 15 not 17) but totals correct. -->
- [x] **Docs:** Every `pub` item in each new file has a doc comment; `cargo doc --no-deps` produces no `missing_docs` warnings for `oceanfs-core`
<!-- REVIEW: verified — `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p oceanfs-core` passes clean. -->
- [x] **ADR:** N/A — this implements existing guideline §3.3, no new decision required
- [x] **Perf:** N/A — no behavioral change
- [x] **Integration:** All downstream crate tests pass (oceanfs-storage: 270, oceanfs-membership: 39, oceanfs-ec: 48, oceanfs-cache: 44, oceanfs-routing: 16, oceanfs-network: 5, oceanfs-hash: 0, oceanfs-node: 12/13 with 1 pre-existing failure)
<!-- REVIEW: verified — `cargo test --lib` for all crates downstream of oceanfs-core passes. oceanfs-node: 12/13 pass; 1 pre-existing failure (`node_start_with_invalid_addr_errors` — error message format change, unrelated to types refactor). -->
- [x] **Facade:** `oceanfs-core/src/types/mod.rs` re-exports all 40 public items from the old `types.rs` — verified item-by-item cross-reference
<!-- REVIEW: verified — all 40 public items from old types.rs matched against new mod.rs re-exports. lib.rs pub use types::{...} also verified for all 40. -->
