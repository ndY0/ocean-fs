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
//! - `cuda`: Enables CUDA EC backend + nvCOMP compression (auto-detects toolkit)
//! - `isa-l`: Enables the ISA-L SIMD-accelerated encoder (x86 only)
//!
//! When CUDA tools are absent at build time, the `cuda` feature compiles
//! but GPU backends are unavailable at runtime (degrade gracefully).
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
// Custom cfgs set by build.rs when CUDA/nvCOMP tools are absent.
#![allow(unexpected_cfgs)]

mod arm_sve;
mod compressor;
mod dispatcher;
mod error;
mod tier0;

#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
mod igzip;

#[cfg(feature = "isa-l")]
mod isal;

#[cfg(all(feature = "cuda", not(no_cuda_toolkit)))]
mod cuda;

// Public types (facade)
pub use arm_sve::ArmEncoder;
pub use compressor::{Compressor, ZstdCompressor};
pub use dispatcher::{AccelDispatcher, AccelTier};
pub use error::{AccelError, Result};

// Re-exports from oceanfs-core for dependents
pub use oceanfs_core::{AccelConfig, CompressionTier, GpuConfig};

// Feature-gated backends
#[cfg(all(feature = "cuda", not(no_cuda_toolkit)))]
pub use cuda::CudaBackend;
#[cfg(all(feature = "cuda", not(no_cuda_toolkit), not(no_nvcomp)))]
pub use cuda::nvcomp::NvcompCompressor;

#[cfg(feature = "isa-l")]
pub use isal::IsalEncoder;

#[cfg(all(target_arch = "x86_64", feature = "isa-l"))]
pub use igzip::IgzipCompressor;

// Re-export Encoder/Decoder traits for convenience
pub use oceanfs_ec::{Decoder, Encoder};
