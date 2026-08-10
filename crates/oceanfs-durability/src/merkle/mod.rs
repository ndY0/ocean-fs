//! Incremental Merkle tree protocol.
//!
//! Implements the design from ADR-0015 (amended by ADR-0018 Decision 1):
//! per-segment binary Merkle trees maintained incrementally in memory.
//! On node restart, the tree is rebuilt from the `segments` column family
//! in RocksDB — no dedicated persistence domain is required.
//!
//! Supports continuous and sampling anti-entropy modes, pre-built tree
//! exchange over gRPC, and unified EC repair through the heal pool.
//!
//! ## Module Structure
//!
//! - `tree_node` — `TreeNode` wire type and `MerkleWalEntry` mutation log enum
//! - `incremental_tree` — The incremental, in-memory Merkle tree data structure
//!
//! ## LOCK ORDER
//!
//! No multi-lock acquisitions occur in this module. The `DashMap` in
//! `IncrementalMerkleTree` provides internal sharding; the `insertion_order`
//! `Mutex` is acquired independently.

pub mod incremental_tree;
pub mod tree_node;

pub use incremental_tree::{IncrementalMerkleTree, MerkleTreeConfig};
pub use tree_node::{MerkleWalEntry, TreeNode};
