//! Hashing subsystem — BLAKE3 streaming hasher and batch verification.
//!
//! Provides content-addressable hashing for blob data, segment checksums,
//! and Merkle tree nodes. Uses the `blake3` crate with runtime SIMD
//! detection (AVX-512, AVX2, SSE4.1, NEON).

#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    missing_docs
)]

mod batch;
mod hash_output;
mod hasher;

pub use batch::{BatchHasher, Blake3BatchHasher};
pub use hash_output::HashOutput;
pub use hasher::{Blake3Hasher, Hasher};
