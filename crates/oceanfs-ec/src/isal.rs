//! ISA-L accelerated erasure coding (feature-gated).
//!
//! When the `isa-l` feature is enabled and the target is x86/x86_64,
//! this module provides SIMD-accelerated encode/decode using Intel's
//! Intelligent Storage Acceleration Library (ISA-L).
//!
//! On non-x86 targets or when `isa-l` is not enabled, falls back to
//! the portable GF(2^8) implementation.

/// Stub for ISA-L accelerated encoder.
///
/// When the `isa-l` feature is enabled, this is backed by ISA-L's
/// `ec_encode_data()` using AVX/AVX2/AVX512 instructions.
#[cfg(feature = "isa-l")]
pub mod isal {
    //! ISA-L accelerated encoder (placeholder).
    //!
    //! TODO: Integrate `isal-rs` or direct FFI bindings for
    //! `ec_encode_data()` and `ec_init_tables()`.

    /// ISA-L encoder stub.
    pub struct IsalEncoder;

    impl IsalEncoder {
        /// Creates a new ISA-L encoder.
        pub fn new() -> Self {
            Self
        }
    }
}

/// Portable fallback when ISA-L is not available.
#[cfg(not(feature = "isa-l"))]
pub mod isal {
    //! Portable fallback — delegates to the Cauchy RS implementation.

    /// Portable encoder (delegates to Cauchy).
    pub struct IsalEncoder;

    impl IsalEncoder {
        /// Creates a new encoder using the portable fallback.
        pub fn new() -> Self {
            Self
        }
    }
}
