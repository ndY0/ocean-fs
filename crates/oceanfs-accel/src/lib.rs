//! Hardware acceleration subsystem.
//!
//! Provides tiered acceleration for erasure coding and hashing:
//!
//! - **Tier 0:** CPU SIMD (portable)
//! - **Tier 1:** ISA-L (x86, feature-gated)
//! - **Tier 2:** GPU / CUDA (feature-gated)
//!
//! The [`AccelDispatcher`] selects the best available backend at runtime.
//!
//! # Unsafe Code
//!
//! This crate is permitted to use `unsafe` for GPU FFI and SIMD intrinsics.

#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    missing_docs
)]

mod dispatcher;

#[cfg(feature = "cuda")]
mod cuda;

pub use dispatcher::{AccelDispatcher, AccelTier};
