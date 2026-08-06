//! Acceleration subsystem observability metrics.
//!
//! Provides counters for monitoring acceleration tier usage,
//! fallback events, and backend status. All counters use
//! `Counter` from `oceanfs_core` for integration with
//! the centralized `MetricsRegistry`.

use oceanfs_core::{Counter, LabelSet, MetricRegistrar};

/// Aggregated acceleration metrics for observability.
///
/// All fields are `Counter` — they are self-registering
/// metrics that can be wired into a central metrics registry
/// via [`register_metrics`](Self::register_metrics).
///
/// # Examples
///
/// ```
/// use oceanfs_accel::AccelMetrics;
///
/// let metrics = AccelMetrics::new();
/// metrics.record_encode(1024);
/// metrics.record_decode(512);
/// metrics.record_ec_fallback();
/// assert_eq!(metrics.bytes_encoded(), 1024);
/// assert_eq!(metrics.ec_fallback_count(), 1);
/// ```
pub struct AccelMetrics {
    /// Total bytes processed by encode operations.
    pub bytes_encoded: Counter,
    /// Total bytes processed by decode operations.
    pub bytes_decoded: Counter,
    /// Number of EC tier fallback events (per ADR-0006 §2).
    pub ec_fallback_total: Counter,
    /// Number of compression tier fallback events.
    pub compression_fallback_total: Counter,
    /// Number of runtime backend failures that triggered fallback.
    pub runtime_fallback_total: Counter,
    /// Total number of encode operations attempted.
    pub encode_ops_total: Counter,
    /// Total number of decode operations attempted.
    pub decode_ops_total: Counter,
    /// Total number of encode errors.
    pub encode_errors_total: Counter,
    /// Total number of decode errors.
    pub decode_errors_total: Counter,
}

impl AccelMetrics {
    /// Creates new acceleration metrics with unregistered counters.
    ///
    /// Use [`register_metrics`](Self::register_metrics) to wire them
    /// into a registry.
    pub fn new() -> Self {
        Self {
            bytes_encoded: Counter::new(
                "accel_bytes_encoded_total".into(),
                "Total bytes encoded by acceleration backends".into(),
                LabelSet::empty(),
            ),
            bytes_decoded: Counter::new(
                "accel_bytes_decoded_total".into(),
                "Total bytes decoded by acceleration backends".into(),
                LabelSet::empty(),
            ),
            ec_fallback_total: Counter::new(
                "accel_ec_fallback_total".into(),
                "EC tier fallback events".into(),
                LabelSet::empty(),
            ),
            compression_fallback_total: Counter::new(
                "accel_compression_fallback_total".into(),
                "Compression tier fallback events".into(),
                LabelSet::empty(),
            ),
            runtime_fallback_total: Counter::new(
                "accel_runtime_fallback_total".into(),
                "Runtime backend failures triggering fallback".into(),
                LabelSet::empty(),
            ),
            encode_ops_total: Counter::new(
                "accel_encode_ops_total".into(),
                "Total encode operations attempted".into(),
                LabelSet::empty(),
            ),
            decode_ops_total: Counter::new(
                "accel_decode_ops_total".into(),
                "Total decode operations attempted".into(),
                LabelSet::empty(),
            ),
            encode_errors_total: Counter::new(
                "accel_encode_errors_total".into(),
                "Total encode errors".into(),
                LabelSet::empty(),
            ),
            decode_errors_total: Counter::new(
                "accel_decode_errors_total".into(),
                "Total decode errors".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Registers all counters with a metrics registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.bytes_encoded.clone());
        registrar.register_counter(self.bytes_decoded.clone());
        registrar.register_counter(self.ec_fallback_total.clone());
        registrar.register_counter(self.compression_fallback_total.clone());
        registrar.register_counter(self.runtime_fallback_total.clone());
        registrar.register_counter(self.encode_ops_total.clone());
        registrar.register_counter(self.decode_ops_total.clone());
        registrar.register_counter(self.encode_errors_total.clone());
        registrar.register_counter(self.decode_errors_total.clone());
    }

    /// Records an encode operation processing `byte_count` bytes.
    pub fn record_encode(&self, byte_count: u64) {
        self.bytes_encoded.add(byte_count);
        self.encode_ops_total.inc();
    }

    /// Records a failed encode operation.
    pub fn record_encode_error(&self) {
        self.encode_errors_total.inc();
    }

    /// Records a failed decode operation.
    pub fn record_decode_error(&self) {
        self.decode_errors_total.inc();
    }

    /// Records a decode operation processing `byte_count` bytes.
    pub fn record_decode(&self, byte_count: u64) {
        self.bytes_decoded.add(byte_count);
        self.decode_ops_total.inc();
    }

    /// Records an EC tier fallback event.
    pub fn record_ec_fallback(&self) {
        self.ec_fallback_total.inc();
    }

    /// Records a compression tier fallback event.
    pub fn record_compression_fallback(&self) {
        self.compression_fallback_total.inc();
    }

    /// Records a runtime backend failure fallback event.
    pub fn record_runtime_fallback(&self) {
        self.runtime_fallback_total.inc();
    }

    // -- Getters --

    /// Returns total bytes processed by encode operations.
    pub fn bytes_encoded(&self) -> u64 {
        self.bytes_encoded.get()
    }

    /// Returns total bytes processed by decode operations.
    pub fn bytes_decoded(&self) -> u64 {
        self.bytes_decoded.get()
    }

    /// Returns total number of EC tier fallback events.
    pub fn ec_fallback_count(&self) -> u64 {
        self.ec_fallback_total.get()
    }

    /// Returns total number of compression fallback events.
    pub fn compression_fallback_count(&self) -> u64 {
        self.compression_fallback_total.get()
    }

    /// Returns total number of runtime fallback events.
    pub fn runtime_fallback_count(&self) -> u64 {
        self.runtime_fallback_total.get()
    }

    /// Returns total encode operations attempted.
    pub fn encode_ops(&self) -> u64 {
        self.encode_ops_total.get()
    }

    /// Returns total decode operations attempted.
    pub fn decode_ops(&self) -> u64 {
        self.decode_ops_total.get()
    }
}

impl Default for AccelMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn metrics_start_at_zero() {
        let m = AccelMetrics::new();
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
        let m = AccelMetrics::new();
        m.record_encode(1024);
        m.record_encode(2048);
        assert_eq!(m.bytes_encoded(), 3072);
        assert_eq!(m.encode_ops(), 2);
    }

    #[test]
    fn record_decode_increments_counters() {
        let m = AccelMetrics::new();
        m.record_decode(512);
        assert_eq!(m.bytes_decoded(), 512);
        assert_eq!(m.decode_ops(), 1);
    }

    #[test]
    fn record_fallback_events() {
        let m = AccelMetrics::new();
        m.record_ec_fallback();
        m.record_ec_fallback();
        m.record_compression_fallback();
        m.record_runtime_fallback();
        assert_eq!(m.ec_fallback_count(), 2);
        assert_eq!(m.compression_fallback_count(), 1);
        assert_eq!(m.runtime_fallback_count(), 1);
    }
}
