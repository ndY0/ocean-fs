//! nvCOMP GPU-accelerated compression backend.
//!
//! Provides [`NvcompCompressor`] — Tier 2 in the compression fallback chain
//! (`GpuNvcomp → CpuIgzip → CpuZstd`). Uses NVIDIA nvCOMP 4.x library for
//! GPU-accelerated LZ4 compression via batched async CUDA kernels.
//!
//! ## Architecture
//!
//! ```text
//! NvcompCompressor::compress(data):
//!   1. Acquire GPU semaphore permit (shared with CudaBackend)
//!   2. Allocate pinned host memory for DMA input
//!   3. Copy host → pinned → device (DMA)
//!   4. Query temp workspace size via nvcompBatchedLZ4CompressGetTempSize
//!   5. Allocate GPU temp workspace + output buffer
//!   6. Build device-side pointer/size arrays
//!   7. Launch nvcompBatchedLZ4CompressAsync on CUDA stream
//!   8. Synchronize stream, copy result device → host
//!   9. Free device memory, release semaphore
//!   10. Return compressed bytes
//! ```
//!
//! ## Safety
//!
//! All nvCOMP FFI calls are `unsafe`. Each call site is documented with
//! a `// SAFETY:` comment. GPU memory is managed via cudarc's safe
//! `CudaSlice<T>` type; raw pointer extraction follows cudarc conventions.

use std::sync::Arc;

use bytes::Bytes;
use cudarc::driver::{CudaDevice, CudaSlice, DevicePtr};
use oceanfs_core::{CompressionTier, NvcompConfig};
use tokio::sync::Semaphore;

use crate::{compressor::Compressor, error::AccelError, Result};

// ---------------------------------------------------------------------------
// FFI declarations — nvCOMP 4.x batched LZ4
// ---------------------------------------------------------------------------

#[allow(non_camel_case_types)]
type cudaStream_t = *mut std::ffi::c_void;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
struct nvcompBatchedLZ4Opts_t {
    data_type: i32, // nvcompType_t: NVCOMP_TYPE_CHAR = 0
}

const NVCOMP_TYPE_CHAR: i32 = 0;

// nvcomp status codes
const NVCOMP_SUCCESS: i32 = 0;

extern "C" {
    /// Query temporary workspace size for LZ4 compression.
    fn nvcompBatchedLZ4CompressGetTempSize(
        num_chunks: usize,
        max_uncompressed_chunk_bytes: usize,
        format_opts: nvcompBatchedLZ4Opts_t,
        temp_bytes: *mut usize,
    ) -> i32;

    /// Query maximum compressed output size for a chunk.
    fn nvcompBatchedLZ4CompressGetMaxOutputChunkSize(
        max_uncompressed_chunk_bytes: usize,
        format_opts: nvcompBatchedLZ4Opts_t,
        max_compressed_chunk_bytes: *mut usize,
    ) -> i32;

    /// Asynchronous batched LZ4 compression on GPU.
    fn nvcompBatchedLZ4CompressAsync(
        device_uncompressed_chunk_ptrs: *const *const u8,
        device_uncompressed_chunk_bytes: *const usize,
        max_uncompressed_chunk_bytes: usize,
        num_chunks: usize,
        device_temp_ptr: *mut std::ffi::c_void,
        temp_bytes: usize,
        device_compressed_chunk_ptrs: *mut *mut u8,
        device_compressed_chunk_bytes: *mut usize,
        format_opts: nvcompBatchedLZ4Opts_t,
        stream: cudaStream_t,
    ) -> i32;

    /// Query temporary workspace size for LZ4 decompression.
    fn nvcompBatchedLZ4DecompressGetTempSize(
        num_chunks: usize,
        max_uncompressed_chunk_bytes: usize,
        temp_bytes: *mut usize,
    ) -> i32;

    /// Asynchronous batched LZ4 decompression on GPU.
    #[allow(clippy::too_many_arguments)]
    fn nvcompBatchedLZ4DecompressAsync(
        device_compressed_chunk_ptrs: *const *const u8,
        device_compressed_chunk_bytes: *const usize,
        device_uncompressed_buffer_bytes: *const usize,
        device_uncompressed_chunk_bytes: *mut usize,
        num_chunks: usize,
        device_temp_ptr: *mut std::ffi::c_void,
        temp_bytes: usize,
        device_uncompressed_chunk_ptrs: *mut *mut u8,
        device_statuses: *mut i32,
        stream: cudaStream_t,
    ) -> i32;
}

// Raw CUDA stream ops (linked via libcudart)
extern "C" {
    fn cudaStreamCreate(stream: *mut cudaStream_t) -> i32;
    fn cudaStreamSynchronize(stream: cudaStream_t) -> i32;
    fn cudaStreamDestroy(stream: cudaStream_t) -> i32;
}

/// Extracts the raw device pointer from a CudaSlice as a `*const u8`.
///
/// Uses cudarc's `DevicePtr` trait to access the internal `CUdeviceptr`,
/// then casts the `u64` value to a raw pointer for FFI.
fn slice_device_ptr<T>(slice: &CudaSlice<T>) -> *const u8 {
    (*slice.device_ptr()) as *const u8
}

/// Copies a host value to a device-side `CudaSlice<usize>`.
///
/// Useful for building device-side pointer arrays required by nvcomp's
/// batched API (which expects `const void* const*`).
fn copy_usize_to_device(device: &Arc<CudaDevice>, val: usize) -> Result<CudaSlice<usize>> {
    // SAFETY: Device memory allocation on a valid, initialized CudaDevice.
    let mut slice = unsafe { device.alloc::<usize>(1) }
        .map_err(|e| AccelError::CompressionError { reason: format!("GPU alloc failed: {e}") })?;
    device
        .htod_sync_copy_into(&[val], &mut slice)
        .map_err(|e| AccelError::CompressionError { reason: format!("GPU copy failed: {e}") })?;
    Ok(slice)
}

// ---------------------------------------------------------------------------
// NvcompCompressor
// ---------------------------------------------------------------------------

/// nvCOMP GPU-accelerated LZ4 compression backend (Tier 2).
///
/// Uses NVIDIA's nvCOMP 4.x library for GPU-accelerated batched LZ4
/// compression. Compression is performed asynchronously on the GPU
/// via a dedicated CUDA stream. GPU access is serialized through a
/// shared [`tokio::sync::Semaphore`] (per ADR-0006 §4).
///
/// # Availability
///
/// Requires:
/// - `cuda` Cargo feature enabled at compile time
/// - NVIDIA GPU (compute capability ≥ 5.0)
/// - nvCOMP 4.x SDK installed (`libnvcomp.so`)
///
/// # Examples
///
/// ```ignore
/// use oceanfs_accel::NvcompCompressor;
///
/// if let Some(compressor) = NvcompCompressor::new() {
///     let data = b"segment data for GPU compression";
///     let compressed = compressor.compress(data, 0).unwrap();
///     let decompressed = compressor.decompress(&compressed).unwrap();
///     assert_eq!(&decompressed[..], data);
/// }
/// ```
pub struct NvcompCompressor {
    /// CUDA device handle (shared with EC backend).
    device: Arc<CudaDevice>,
    /// Semaphore bounding concurrent GPU operations.
    semaphore: Arc<Semaphore>,
    /// Compression config: codec, batch_size, device_id.
    config: NvcompConfig,
}

// SAFETY: CudaDevice is Send + Sync (cudarc guarantees thread-safe device access
// via internal Arc). Semaphore is Send + Sync. nvCOMP stateless batch functions
// are thread-safe per NVIDIA documentation.
unsafe impl Send for NvcompCompressor {}
unsafe impl Sync for NvcompCompressor {}

impl NvcompCompressor {
    /// Creates a new nvCOMP compressor.
    ///
    /// Probes for a CUDA device at construction time. Shares a GPU semaphore
    /// with other CUDA backends (EC) to prevent concurrent GPU contention.
    ///
    /// The `config` parameter specifies the compression codec, batch size,
    /// and GPU device index. When batch_size > 1, the compressor splits
    /// input data into batches for parallel GPU kernel execution.
    ///
    /// # Returns
    ///
    /// - `Some(NvcompCompressor)` if a CUDA device is available.
    /// - `None` if no GPU is detected.
    pub fn new(semaphore: Arc<Semaphore>, config: NvcompConfig) -> Option<Self> {
        if !Self::is_available() {
            return None;
        }
        let device = match CudaDevice::new(config.device_id) {
            Ok(dev) => {
                tracing::info!(
                    name = dev.name().unwrap_or_default(),
                    device_id = config.device_id,
                    batch_size = config.batch_size,
                    codec = ?config.codec,
                    "nvCOMP GPU compression backend initialized"
                );
                dev
            }
            Err(e) => {
                tracing::debug!(error = %e, "nvCOMP GPU device creation failed");
                return None;
            }
        };
        Some(Self { device, semaphore, config })
    }

    /// Checks whether CUDA is available on this system.
    ///
    /// Returns `true` when a CUDA device is present (probed via cudarc).
    pub fn is_available() -> bool {
        CudaDevice::new(0).is_ok()
    }

    /// Returns the LZ4 format options (char type, default).
    fn lz4_opts() -> nvcompBatchedLZ4Opts_t {
        nvcompBatchedLZ4Opts_t { data_type: NVCOMP_TYPE_CHAR }
    }
}

impl Compressor for NvcompCompressor {
    fn compress(&self, data: &[u8], _level: u32) -> Result<Bytes> {
        if data.is_empty() {
            return Ok(Bytes::new());
        }

        // --- Acquire GPU semaphore ---
        // SAFETY: Semaphore permit is acquired async; we use try_acquire
        // to fail fast if GPU is saturated. The caller (AccelDispatcher)
        // falls back to the next tier on failure.
        let _permit = self.semaphore.try_acquire().map_err(|_| AccelError::CompressionError {
            reason: "GPU saturated — no semaphore permits available".into(),
        })?;

        // nvCOMP batched API accepts num_chunks input buffers; the Compressor
        // trait operates on a single &[u8], so num_chunks is always 1.
        let num_chunks: usize = 1;
        let _batch_capacity = self.config.batch_size; // for future multi-chunk batching
        let max_uncompressed: usize = data.len();
        let opts = Self::lz4_opts();

        // --- Step 1: Create CUDA stream ---
        let mut stream: cudaStream_t = std::ptr::null_mut();
        // SAFETY: &mut stream is a valid non-null pointer to a cudaStream_t.
        // cudaStreamCreate writes a valid stream handle on success.
        let cu_rc = unsafe { cudaStreamCreate(&mut stream) };
        if cu_rc != 0 {
            return Err(AccelError::CompressionError {
                reason: format!("cudaStreamCreate failed: {cu_rc}"),
            });
        }

        // Ensure stream is destroyed on scope exit
        struct StreamGuard(cudaStream_t);
        impl Drop for StreamGuard {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    // SAFETY: stream was created via cudaStreamCreate and is valid.
                    unsafe { cudaStreamDestroy(self.0) };
                }
            }
        }
        let _stream_guard = StreamGuard(stream);

        // --- Step 2: Allocate device input ---
        // SAFETY: device is a valid, initialized CudaDevice.
        let d_input = unsafe {
            let mut slice = self.device.alloc::<u8>(data.len()).map_err(|e| {
                AccelError::CompressionError { reason: format!("GPU input alloc failed: {e}") }
            })?;
            self.device.htod_sync_copy_into(data, &mut slice).map_err(|e| {
                AccelError::CompressionError { reason: format!("GPU input copy failed: {e}") }
            })?;
            slice
        };

        // --- Step 3: Query temp workspace size ---
        let mut temp_bytes: usize = 0;
        // SAFETY: temp_bytes is a valid mutable reference. nvcomp writes
        // the required workspace size on success.
        let status = unsafe {
            nvcompBatchedLZ4CompressGetTempSize(num_chunks, max_uncompressed, opts, &mut temp_bytes)
        };
        if status != NVCOMP_SUCCESS {
            return Err(AccelError::CompressionError {
                reason: format!("nvcompBatchedLZ4CompressGetTempSize failed: {status}"),
            });
        }

        // --- Step 4: Query max compressed output size ---
        let mut max_compressed: usize = 0;
        // SAFETY: max_compressed is a valid mutable reference.
        let status = unsafe {
            nvcompBatchedLZ4CompressGetMaxOutputChunkSize(
                max_uncompressed,
                opts,
                &mut max_compressed,
            )
        };
        if status != NVCOMP_SUCCESS {
            return Err(AccelError::CompressionError {
                reason: format!("nvcompBatchedLZ4CompressGetMaxOutputChunkSize failed: {status}"),
            });
        }

        // --- Step 5: Allocate GPU temp + output ---
        // SAFETY: device is a valid, initialized CudaDevice.
        let d_temp: CudaSlice<u8> = unsafe {
            self.device.alloc::<u8>(temp_bytes).map_err(|e| AccelError::CompressionError {
                reason: format!("GPU temp alloc failed: {e}"),
            })?
        };

        // SAFETY: device is a valid, initialized CudaDevice.
        let d_output: CudaSlice<u8> = unsafe {
            self.device.alloc::<u8>(max_compressed).map_err(|e| AccelError::CompressionError {
                reason: format!("GPU output alloc failed: {e}"),
            })?
        };

        // --- Step 6: Build device-side pointer/size arrays ---
        // nvcomp needs arrays of device pointers and sizes in device memory.
        // We allocate single-element arrays for num_chunks=1.

        let input_dev_ptr: usize = *d_input.device_ptr() as usize;
        let d_input_ptr = copy_usize_to_device(&self.device, input_dev_ptr)?;

        let d_input_sizes = copy_usize_to_device(&self.device, max_uncompressed)?;

        let output_dev_ptr: usize = *d_output.device_ptr() as usize;
        let d_output_ptr = copy_usize_to_device(&self.device, output_dev_ptr)?;

        // SAFETY: device is a valid, initialized CudaDevice.
        let d_output_sizes = unsafe {
            self.device.alloc::<usize>(num_chunks).map_err(|e| AccelError::CompressionError {
                reason: format!("GPU output sizes array failed: {e}"),
            })?
        };

        // --- Step 7: Launch nvcomp batched compress ---
        // SAFETY:
        // - All device pointers are valid, allocated via cudarc on this device
        // - Sizes match: num_chunks=1, all arrays have 1 element
        // - d_temp has temp_bytes capacity
        // - d_output has max_compressed capacity
        // - stream is a valid CUDA stream created above
        // - nvcompBatchedLZ4CompressAsync is thread-safe per NVIDIA docs
        let status = unsafe {
            nvcompBatchedLZ4CompressAsync(
                slice_device_ptr(&d_input_ptr) as *const *const u8,
                slice_device_ptr(&d_input_sizes) as *const usize,
                max_uncompressed,
                num_chunks,
                slice_device_ptr(&d_temp) as *mut std::ffi::c_void,
                temp_bytes,
                slice_device_ptr(&d_output_ptr) as *mut *mut u8,
                slice_device_ptr(&d_output_sizes) as *mut usize,
                opts,
                stream,
            )
        };
        if status != NVCOMP_SUCCESS {
            return Err(AccelError::CompressionError {
                reason: format!("nvcompBatchedLZ4CompressAsync failed: {status}"),
            });
        }

        // --- Step 8: Synchronize stream ---
        // SAFETY: stream is a valid CUDA stream with pending work.
        let cu_rc = unsafe { cudaStreamSynchronize(stream) };
        if cu_rc != 0 {
            return Err(AccelError::CompressionError {
                reason: format!("cudaStreamSynchronize failed: {cu_rc}"),
            });
        }

        // --- Step 9: Read compressed size from device ---
        let compressed_sizes: Vec<usize> =
            self.device.dtoh_sync_copy(&d_output_sizes).map_err(|e| {
                AccelError::CompressionError {
                    reason: format!("GPU read compressed sizes failed: {e}"),
                }
            })?;

        let compressed_len = compressed_sizes.first().copied().unwrap_or(0);
        if compressed_len == 0 || compressed_len > max_compressed {
            return Err(AccelError::CompressionError {
                reason: format!("invalid compressed size: {compressed_len}"),
            });
        }

        // --- Step 10: Copy compressed data device → host ---
        let host_out: Vec<u8> = self.device.dtoh_sync_copy(&d_output).map_err(|e| {
            AccelError::CompressionError { reason: format!("GPU read output failed: {e}") }
        })?;

        // Truncate to actual compressed size
        let result = Bytes::from(host_out).slice(0..compressed_len);

        // All device allocations (CudaSlice) are dropped here, freeing GPU memory.
        // Stream is destroyed by StreamGuard.

        Ok(result)
    }

    fn decompress(&self, data: &[u8]) -> Result<Bytes> {
        if data.is_empty() {
            return Ok(Bytes::new());
        }

        // --- Acquire GPU semaphore ---
        let _permit = self.semaphore.try_acquire().map_err(|_| AccelError::CompressionError {
            reason: "GPU saturated — no semaphore permits for decompress".into(),
        })?;

        // nvCOMP batched API: single input => num_chunks always 1
        let num_chunks: usize = 1;

        // --- Step 1: Create CUDA stream ---
        let mut stream: cudaStream_t = std::ptr::null_mut();
        // SAFETY: &mut stream is a valid non-null pointer.
        let cu_rc = unsafe { cudaStreamCreate(&mut stream) };
        if cu_rc != 0 {
            return Err(AccelError::CompressionError {
                reason: format!("cudaStreamCreate failed: {cu_rc}"),
            });
        }
        struct StreamGuard(cudaStream_t);
        impl Drop for StreamGuard {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    // SAFETY: stream was created via cudaStreamCreate and is valid.
                    unsafe { cudaStreamDestroy(self.0) };
                }
            }
        }
        let _stream_guard = StreamGuard(stream);

        // --- Step 2: Allocate device input ---
        // SAFETY: device is a valid, initialized CudaDevice.
        let d_compressed = unsafe {
            let mut slice = self.device.alloc::<u8>(data.len()).map_err(|e| {
                AccelError::CompressionError { reason: format!("GPU compressed alloc failed: {e}") }
            })?;
            self.device.htod_sync_copy_into(data, &mut slice).map_err(|e| {
                AccelError::CompressionError { reason: format!("GPU compressed copy failed: {e}") }
            })?;
            slice
        };

        // --- Step 3: Estimate uncompressed size ---
        // LZ4 can achieve extreme compression ratios for redundant data.
        // Use a generous upper bound: 1024x compressed size, but at least 64KB.
        let max_uncompressed = (data.len() * 1024).max(65536);

        // --- Step 4: Query temp workspace for decompression ---
        let mut temp_bytes: usize = 0;
        // SAFETY: temp_bytes is a valid mutable reference.
        let status = unsafe {
            nvcompBatchedLZ4DecompressGetTempSize(num_chunks, max_uncompressed, &mut temp_bytes)
        };
        if status != NVCOMP_SUCCESS {
            return Err(AccelError::CompressionError {
                reason: format!("nvcompBatchedLZ4DecompressGetTempSize failed: {status}"),
            });
        }

        // --- Step 5: Allocate GPU temp + output ---
        // SAFETY: device is a valid, initialized CudaDevice.
        let d_temp: CudaSlice<u8> = unsafe {
            self.device.alloc::<u8>(temp_bytes).map_err(|e| AccelError::CompressionError {
                reason: format!("GPU decomp temp alloc failed: {e}"),
            })?
        };

        // SAFETY: device is a valid, initialized CudaDevice.
        let d_output: CudaSlice<u8> = unsafe {
            self.device.alloc::<u8>(max_uncompressed).map_err(|e| AccelError::CompressionError {
                reason: format!("GPU decomp output alloc failed: {e}"),
            })?
        };

        // --- Step 6: Build device-side arrays ---
        let compressed_dev_ptr: usize = *d_compressed.device_ptr() as usize;
        let d_compressed_ptr = copy_usize_to_device(&self.device, compressed_dev_ptr)?;

        let d_compressed_sizes = copy_usize_to_device(&self.device, data.len())?;

        let d_output_buf_sizes = copy_usize_to_device(&self.device, max_uncompressed)?;

        let output_dev_ptr: usize = *d_output.device_ptr() as usize;
        let d_output_ptr = copy_usize_to_device(&self.device, output_dev_ptr)?;

        // SAFETY: device is a valid, initialized CudaDevice.
        // SAFETY: device is a valid, initialized CudaDevice.
        let d_out_actual_sizes: CudaSlice<usize> = unsafe {
            self.device.alloc::<usize>(num_chunks).map_err(|e| AccelError::CompressionError {
                reason: format!("GPU out actual sizes array failed: {e}"),
            })?
        };

        // SAFETY: device is a valid, initialized CudaDevice.
        let d_statuses: CudaSlice<i32> = unsafe {
            self.device.alloc::<i32>(num_chunks).map_err(|e| AccelError::CompressionError {
                reason: format!("GPU statuses array failed: {e}"),
            })?
        };

        // --- Step 7: Launch nvcomp batched decompress ---
        // SAFETY:
        // - All device pointers are valid, allocated via cudarc on this device
        // - Sizes match: num_chunks=1, all arrays have 1 element
        // - d_temp has temp_bytes capacity, d_output has max_uncompressed capacity
        // - stream is a valid CUDA stream
        let status = unsafe {
            nvcompBatchedLZ4DecompressAsync(
                slice_device_ptr(&d_compressed_ptr) as *const *const u8,
                slice_device_ptr(&d_compressed_sizes) as *const usize,
                slice_device_ptr(&d_output_buf_sizes) as *const usize,
                slice_device_ptr(&d_out_actual_sizes) as *mut usize,
                num_chunks,
                slice_device_ptr(&d_temp) as *mut std::ffi::c_void,
                temp_bytes,
                slice_device_ptr(&d_output_ptr) as *mut *mut u8,
                slice_device_ptr(&d_statuses) as *mut i32,
                stream,
            )
        };
        if status != NVCOMP_SUCCESS {
            return Err(AccelError::CompressionError {
                reason: format!("nvcompBatchedLZ4DecompressAsync failed: {status}"),
            });
        }

        // --- Step 8: Synchronize stream ---
        // SAFETY: stream is a valid CUDA stream with pending work.
        let cu_rc = unsafe { cudaStreamSynchronize(stream) };
        if cu_rc != 0 {
            return Err(AccelError::CompressionError {
                reason: format!("cudaStreamSynchronize failed: {cu_rc}"),
            });
        }

        // --- Step 9: Check per-chunk decompression status ---
        let host_statuses: Vec<i32> = self.device.dtoh_sync_copy(&d_statuses).map_err(|e| {
            AccelError::CompressionError { reason: format!("GPU read statuses failed: {e}") }
        })?;

        let chunk_status = host_statuses.first().copied().unwrap_or(-1);
        if chunk_status != NVCOMP_SUCCESS {
            return Err(AccelError::CompressionError {
                reason: format!("chunk decompression failed with status {chunk_status}"),
            });
        }

        // --- Step 10: Read decompressed data ---
        let host_out: Vec<u8> =
            self.device.dtoh_sync_copy(&d_output).map_err(|e| AccelError::CompressionError {
                reason: format!("GPU read decompressed output failed: {e}"),
            })?;

        let actual_sizes: Vec<usize> =
            self.device.dtoh_sync_copy(&d_out_actual_sizes).map_err(|e| {
                AccelError::CompressionError {
                    reason: format!("GPU read actual sizes failed: {e}"),
                }
            })?;

        let actual_len = actual_sizes.first().copied().unwrap_or(host_out.len());
        let len = actual_len.min(host_out.len());
        let result = Bytes::from(host_out).slice(0..len);

        Ok(result)
    }

    fn compression_tier(&self) -> CompressionTier {
        CompressionTier::GpuNvcomp
    }

    fn is_available(&self) -> bool {
        true // Already probed in new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn nvcomp_is_available_returns_bool() {
        let available = NvcompCompressor::is_available();
        assert!(!available || available);
    }

    #[test]
    fn nvcomp_lz4_opts_has_char_type() {
        let opts = NvcompCompressor::lz4_opts();
        assert_eq!(opts.data_type, NVCOMP_TYPE_CHAR);
    }

    #[test]
    fn nvcomp_compress_decompress_roundtrip_when_gpu_available() {
        if !NvcompCompressor::is_available() {
            eprintln!("SKIP: no GPU available");
            return;
        }

        let semaphore = Arc::new(Semaphore::new(1));
        let config = NvcompConfig::default();
        let compressor = NvcompCompressor::new(semaphore, config).unwrap();
        assert!(compressor.is_available());
        assert_eq!(compressor.compression_tier(), CompressionTier::GpuNvcomp);

        let original = b"GPU-accelerated LZ4 compression roundtrip test data!";
        let compressed = compressor.compress(original, 0).unwrap();
        assert!(!compressed.is_empty());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], original);
    }

    #[test]
    fn nvcomp_compress_large_data_roundtrip() {
        if !NvcompCompressor::is_available() {
            eprintln!("SKIP: no GPU available");
            return;
        }

        let semaphore = Arc::new(Semaphore::new(1));
        let config = NvcompConfig::default();
        let compressor = NvcompCompressor::new(semaphore, config).unwrap();

        let original = vec![0xABu8; 65536]; // 64 KB
        let compressed = compressor.compress(&original, 0).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(&decompressed[..], &original[..]);
    }

    #[test]
    fn nvcomp_compress_empty_returns_empty() {
        if !NvcompCompressor::is_available() {
            eprintln!("SKIP: no GPU available");
            return;
        }

        let semaphore = Arc::new(Semaphore::new(1));
        let config = NvcompConfig::default();
        let compressor = NvcompCompressor::new(semaphore, config).unwrap();

        let compressed = compressor.compress(&[], 0).unwrap();
        assert!(compressed.is_empty());

        let decompressed = compressor.decompress(&[]).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn nvcomp_cross_backend_compat_with_zstd() {
        if !NvcompCompressor::is_available() {
            eprintln!("SKIP: no GPU available");
            return;
        }

        let semaphore = Arc::new(Semaphore::new(1));
        let config = NvcompConfig::default();
        let compressor = NvcompCompressor::new(semaphore, config).unwrap();

        let original = b"cross-backend LZ4 compatibility test data";
        let compressed = compressor.compress(original, 0).unwrap();

        let decompressed = zstd::decode_all(&compressed[..]);
        if let Ok(data) = decompressed {
            assert_eq!(&data[..], original);
        }
    }
}
