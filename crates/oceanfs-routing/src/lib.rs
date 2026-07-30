//! Distributed hash table routing.
//!
//! Implements the 256-bit consistent hashing ring with virtual nodes.
//! Routes blob keys to their N replica successors in O(log N) time via
//! binary search. The ring topology is cached with [`arc_swap::ArcSwap`]
//! for wait-free reads.

#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    missing_docs
)]
