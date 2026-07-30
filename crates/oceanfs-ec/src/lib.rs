//! Erasure coding engine.
//!
//! Defines the `Encoder` and `Decoder` traits and a Cauchy Reed-Solomon
//! implementation over GF(2⁸). Supports CPU SIMD via ISA-L (feature-gated)
//! and GPU acceleration via CUDA (Phase 8).
//!
//! # Unsafe Code
//!
//! This crate is permitted to use `unsafe` for SIMD-accelerated Galois
//! field arithmetic intrinsics.

#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    missing_docs
)]
