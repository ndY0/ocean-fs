//! Anti-entropy — Merkle tree exchange for background data integrity.
//!
//! Implements the anti-entropy protocol using Merkle tree exchange between
//! neighbor nodes. Merkle trees are built at segment seal time and compared
//! periodically. On root mismatch, nodes descend the tree to identify
//! diverged leaves and repair only the affected data.
//!
//! ## Dependencies
//!
//! Requires `Membership` for peer discovery, `ConnectionPool` for gRPC
//! transport to peers, and [`oceanfs_storage::RocksDbMetadataStore`] for segment metadata.

mod config;
mod engine;
mod merkle_proof;
mod merkle_root;
mod merkle_tree;

pub use config::AntiEntropyConfig;
pub use engine::{AntiEntropy, AntiEntropyStats};
pub use merkle_proof::{LeafRange, MerkleProof};
pub use merkle_root::MerkleRoot;
pub use merkle_tree::{InMemorySegmentStore, MerkleTree, SegmentDataStore};
