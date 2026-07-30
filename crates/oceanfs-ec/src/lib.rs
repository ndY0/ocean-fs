//! Erasure coding engine.
//!
//! Defines the `Encoder` and `Decoder` traits and a Cauchy Reed-Solomon
//! implementation over GF(2^8). Supports rayon-based stripe parallelism
//! via `ParallelEncoder` and `ParallelDecoder`.
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

mod cauchy;
mod error;
mod gf;
mod stripe;
mod traits;

pub use cauchy::CauchyEncoder;
pub use oceanfs_core::EncodingPlan;
pub use stripe::{ParallelDecoder, ParallelEncoder, StripeBatch, StripeLayout};
pub use traits::{Decoder, Encoder};
