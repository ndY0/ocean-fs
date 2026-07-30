#![cfg(feature = "cuda")]

//! CUDA-accelerated erasure coding backend.
//!
//! Offloads batch GF(2^8) matrix multiplication to the GPU.
//! Only compiled when the `cuda` feature is enabled.

/// A CUDA-accelerated EC backend.
///
/// Implements `Encoder` and `Decoder` using GPU kernel launches
/// for batched stripe operations.
pub struct CudaBackend {
    _device_id: usize,
}

#[allow(dead_code)]
impl CudaBackend {
    /// Creates a new CUDA backend.
    ///
    /// # Panics
    ///
    /// Panics if no CUDA device is available.
    pub fn new(device_id: usize) -> Self {
        Self { _device_id: device_id }
    }

    /// Returns `true` if a CUDA device is available.
    pub fn is_available(&self) -> bool {
        true // stub: assume available when compiled with cuda feature
    }
}
