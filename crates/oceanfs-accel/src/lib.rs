//! Hardware acceleration subsystem.
//!
//! Provides tiered acceleration for erasure coding, hashing, and compression:
//!
//! - **Tier 0:** CPU SIMD (portable) — always available
//! - **Tier 1:** ISA-L (x86, feature-gated) — `IsalEncoder`
//! - **Tier 2:** GPU / CUDA (feature-gated) — `CudaBackend`
//!
//! The [`AccelDispatcher`] selects the best available backend at runtime
//! and delegates all encode/decode/compress operations transparently.
//!
//! ## Fallback Chain
//!
//! When a configured tier is unavailable, the dispatcher falls back to
//! the next available tier. The system **never panics** due to missing
//! hardware (per ADR-0006 §2):
//!
//! ```text
//! GpuCuda → IsaL → CpuSimd   (always terminates at CpuSimd)
//! ```
//!
//! ## Feature Flags
//!
//! - `cuda`: Enables the CUDA EC backend (requires CUDA toolkit at build time)
//! - `isa-l`: Enables the ISA-L SIMD-accelerated encoder (x86 only)
//!
//! With no features enabled, only Tier 0 (CPU SIMD) is available.
//!
//! # Unsafe Code
//!
//! This crate is permitted to use `unsafe` for GPU FFI and SIMD intrinsics
//! (per architecture.md §7.2). Every `unsafe` block is documented with a
//! `// SAFETY:` comment.

#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    missing_docs
)]

mod arm_sve;
mod compressor;
mod dispatcher;
mod error;
mod tier0;

#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
mod igzip;

#[cfg(feature = "isa-l")]
mod isal;

#[cfg(feature = "cuda")]
mod cuda;

// Public types (facade)
pub use arm_sve::ArmEncoder;
pub use compressor::{Compressor, ZstdCompressor};
pub use dispatcher::{AccelDispatcher, AccelTier};
pub use error::{AccelError, Result};

// Re-exports from oceanfs-core for dependents
pub use oceanfs_core::{AccelConfig, CompressionTier, GpuConfig};

// Feature-gated backends
#[cfg(feature = "cuda")]
pub use cuda::CudaBackend;
#[cfg(feature = "cuda")]
pub use cuda::nvcomp::NvcompCompressor;

#[cfg(feature = "isa-l")]
pub use isal::IsalEncoder;

#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
pub use igzip::IgzipCompressor;

// Re-export Encoder/Decoder traits for convenience
pub use oceanfs_ec::{Decoder, Encoder};
