//! Acceleration subsystem observability metrics.
//!
//! Provides atomic counters for monitoring acceleration tier usage,
//! fallback events, and backend status. All counters use `Ordering::Relaxed`
//! since metrics are non-critical for memory ordering.

use std::sync::atomic::{AtomicU64, Ordering};

/// Aggregated acceleration metrics for observability.
///
/// All counters are `AtomicU64` with relaxed ordering — reads and writes
/// are eventually consistent, which is sufficient for metrics reporting.
///
/// # Examples
///
/// ```
/// use oceanfs_accel::AccelMetrics;
///
/// let metrics = AccelMetrics::default();
/// metrics.record_encode(1024);
/// metrics.record_decode(512);
/// metrics.record_ec_fallback();
/// assert_eq!(metrics.bytes_encoded(), 1024);
/// assert_eq!(metrics.ec_fallback_count(), 1);
/// ```
#[derive(Default)]
pub struct AccelMetrics {
    /// Total bytes processed by encode operations.
    bytes_encoded: AtomicU64,
    /// Total bytes processed by decode operations.
    bytes_decoded: AtomicU64,
    /// Number of EC tier fallback events (per ADR-0006 §2).
    ec_fallback_total: AtomicU64,
    /// Number of compression tier fallback events.
    compression_fallback_total: AtomicU64,
    /// Number of runtime backend failures that triggered fallback.
    runtime_fallback_total: AtomicU64,
    /// Total number of encode operations attempted.
    encode_ops_total: AtomicU64,
    /// Total number of decode operations attempted.
    decode_ops_total: AtomicU64,
}

impl AccelMetrics {
    /// Records an encode operation processing `byte_count` bytes.
    pub fn record_encode(&self, byte_count: u64) {
        self.bytes_encoded.fetch_add(byte_count, Ordering::Relaxed);
        self.encode_ops_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a decode operation processing `byte_count` bytes.
    pub fn record_decode(&self, byte_count: u64) {
        self.bytes_decoded.fetch_add(byte_count, Ordering::Relaxed);
        self.decode_ops_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an EC tier fallback event.
    pub fn record_ec_fallback(&self) {
        self.ec_fallback_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a compression tier fallback event.
    pub fn record_compression_fallback(&self) {
        self.compression_fallback_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a runtime backend failure fallback event.
    pub fn record_runtime_fallback(&self) {
        self.runtime_fallback_total.fetch_add(1, Ordering::Relaxed);
    }

    // -- Getters --

    /// Returns total bytes processed by encode operations.
    pub fn bytes_encoded(&self) -> u64 {
        self.bytes_encoded.load(Ordering::Relaxed)
    }

    /// Returns total bytes processed by decode operations.
    pub fn bytes_decoded(&self) -> u64 {
        self.bytes_decoded.load(Ordering::Relaxed)
    }

    /// Returns total number of EC tier fallback events.
    pub fn ec_fallback_count(&self) -> u64 {
        self.ec_fallback_total.load(Ordering::Relaxed)
    }

    /// Returns total number of compression fallback events.
    pub fn compression_fallback_count(&self) -> u64 {
        self.compression_fallback_total.load(Ordering::Relaxed)
    }

    /// Returns total number of runtime fallback events.
    pub fn runtime_fallback_count(&self) -> u64 {
        self.runtime_fallback_total.load(Ordering::Relaxed)
    }

    /// Returns total encode operations attempted.
    pub fn encode_ops(&self) -> u64 {
        self.encode_ops_total.load(Ordering::Relaxed)
    }

    /// Returns total decode operations attempted.
    pub fn decode_ops(&self) -> u64 {
        self.decode_ops_total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn metrics_start_at_zero() {
        let m = AccelMetrics::default();
        assert_eq!(m.bytes_encoded(), 0);
        assert_eq!(m.bytes_decoded(), 0);
        assert_eq!(m.ec_fallback_count(), 0);
        assert_eq!(m.compression_fallback_count(), 0);
        assert_eq!(m.runtime_fallback_count(), 0);
        assert_eq!(m.encode_ops(), 0);
        assert_eq!(m.decode_ops(), 0);
    }

    #[test]
    fn record_encode_increments_counters() {
        let m = AccelMetrics::default();
        m.record_encode(1024);
        m.record_encode(2048);
        assert_eq!(m.bytes_encoded(), 3072);
        assert_eq!(m.encode_ops(), 2);
    }

    #[test]
    fn record_decode_increments_counters() {
        let m = AccelMetrics::default();
        m.record_decode(512);
        assert_eq!(m.bytes_decoded(), 512);
        assert_eq!(m.decode_ops(), 1);
    }

    #[test]
    fn record_fallback_events() {
        let m = AccelMetrics::default();
        m.record_ec_fallback();
        m.record_ec_fallback();
        m.record_compression_fallback();
        m.record_runtime_fallback();
        assert_eq!(m.ec_fallback_count(), 2);
        assert_eq!(m.compression_fallback_count(), 1);
        assert_eq!(m.runtime_fallback_count(), 1);
    }

    #[test]
    fn metrics_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AccelMetrics>();
    }
}
