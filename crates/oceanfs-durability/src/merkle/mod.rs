//! Incremental Merkle tree protocol.
//!
//! Implements the design from ADR-0015: per-segment binary Merkle trees
//! maintained incrementally in memory, with a dedicated `MerkleWal` for
//! crash recovery. Supports continuous and sampling anti-entropy modes,
//! pre-built tree exchange over gRPC, and unified EC repair through
//! the heal pool.
//!
//! ## Module Structure
//!
//! - `tree_node` — `TreeNode` wire type and `MerkleWalEntry` mutation log enum
//! - `merkle_wal` — Write-ahead log for persisting tree mutations
//! - `incremental_tree` — The incremental, in-memory Merkle tree data structure
//!
//! ## LOCK ORDER
//!
//! No multi-lock acquisitions occur in this module. The `DashMap` in
//! `IncrementalMerkleTree` provides internal sharding; the `insertion_order`
//! `Mutex` is acquired independently.

pub mod incremental_tree;
pub mod merkle_wal;
pub mod tree_node;

pub use incremental_tree::{IncrementalMerkleTree, MerkleTreeConfig};
pub use merkle_wal::MerkleWal;
pub use tree_node::{MerkleWalEntry, TreeNode};
