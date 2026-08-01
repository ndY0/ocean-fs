//! Acceleration subsystem error types.

use oceanfs_core::CompressionTier;

/// Hardware acceleration errors.
///
/// Variants are grouped by cause: GPU errors, ISA-L FFI errors,
/// backend availability errors, and compression errors.
#[derive(Debug, thiserror::Error)]
pub enum AccelError {
    /// GPU ran out of memory during an operation.
    #[error("GPU out of memory: requested {requested}, available {available}")]
    GpuOutOfMemory {
        /// Bytes requested for the allocation.
        requested: u64,
        /// Bytes available (free VRAM).
        available: u64,
    },

    /// The GPU device was lost (driver crash, hot-unplug, etc.).
    #[error("GPU device lost")]
    GpuDeviceLost,

    /// A GPU data transfer (H→D or D→H) failed.
    #[error("GPU data transfer error")]
    GpuTransferError(#[source] std::io::Error),

    /// An ISA-L FFI call returned an unexpected error.
    #[error("ISA-L FFI error: {0}")]
    IsalFfi(String),

    /// The requested backend is temporarily unavailable (e.g., GPU cooldown).
    #[error("Backend temporarily unavailable: {backend}")]
    BackendUnavailable {
        /// Name of the unavailable backend.
        backend: String,
    },

    /// Compression operation failed (corrupt data, codec error, etc.).
    #[error("compression error: {reason}")]
    CompressionError {
        /// Human-readable reason for the failure.
        reason: String,
    },

    /// The requested compression backend is unavailable.
    #[error("compression backend unavailable: {requested:?}")]
    CompressionBackendUnavailable {
        /// The compression tier that was requested but unavailable.
        requested: CompressionTier,
    },
}

/// Convenience alias for `std::result::Result<T, AccelError>`.
pub type Result<T, E = AccelError> = std::result::Result<T, E>;

#[cfg(test)]
mod assertions {
    use static_assertions::assert_impl_all;

    use super::AccelError;

    assert_impl_all!(AccelError: std::error::Error, Send, Sync);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn gpu_out_of_memory_displays_sizes() {
        let err = AccelError::GpuOutOfMemory {
            requested: 1024,
            available: 512,
        };
        let s = err.to_string();
        assert!(s.contains("1024"));
        assert!(s.contains("512"));
    }

    #[test]
    fn gpu_device_lost_displays_message() {
        let err = AccelError::GpuDeviceLost;
        assert!(err.to_string().contains("device lost"));
    }

    #[test]
    fn isal_ffi_displays_message() {
        let err = AccelError::IsalFfi("encode failed".into());
        assert!(err.to_string().contains("encode failed"));
    }

    #[test]
    fn backend_unavailable_displays_backend_name() {
        let err = AccelError::BackendUnavailable {
            backend: "cuda".into(),
        };
        assert!(err.to_string().contains("cuda"));
    }

    #[test]
    fn result_type_alias_works() {
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }
        fn returns_error() -> Result<i32> {
            Err(AccelError::GpuDeviceLost)
        }
        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_error().is_err());
    }

    #[test]
    fn compression_error_displays_reason() {
        let err = AccelError::CompressionError {
            reason: "bad data".into(),
        };
        assert!(err.to_string().contains("bad data"));
    }

    #[test]
    fn compression_backend_unavailable_displays_tier() {
        let err = AccelError::CompressionBackendUnavailable {
            requested: CompressionTier::CpuIgzip,
        };
        assert!(err.to_string().contains("CpuIgzip"));
    }
}
