//! Erasure coding engine.
//!
//! Defines the `Encoder` and `Decoder` traits and a Cauchy Reed-Solomon
//! implementation over GF(2^8). Supports rayon-based stripe parallelism
//! via `ParallelEncoder` and `ParallelDecoder`. Zero-copy shard access
//! via `ShardData` and `bytemuck`.
//!
//! # Feature Flags
//!
//! - `isa-l`: Enable ISA-L SIMD-accelerated encode/decode on x86.
//!
//! # Unsafe Code
//!
//! This crate is permitted to use `unsafe` for SIMD-accelerated Galois
//! field arithmetic intrinsics and `bytemuck` zero-copy casts.

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
pub mod gf;
#[cfg(feature = "isa-l")]
mod isal;
mod shard;
mod stripe;
mod traits;

pub use cauchy::CauchyEncoder;
pub use error::{Error, Result};
pub use oceanfs_core::EncodingPlan;
pub use shard::{cast_shard_slice, cast_shard_slice_mut, ShardData, ShardPod};
pub use stripe::{ParallelDecoder, ParallelEncoder, StripeBatch, StripeLayout};
pub use traits::{Decoder, Encoder};

#[cfg(feature = "isa-l")]
pub use isal::isal::IsalEncoder;
