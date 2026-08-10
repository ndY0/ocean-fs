//! Per-operation timeout configuration.
//!
//! Defines distinct timeout durations per operation type as required
//! by performance guideline §4.5 (adaptive per-operation timeouts).
//!
//! Using per-operation timeouts allows the system to detect failures
//! at the appropriate granularity — fast for metadata, slower for
//! large data transfers — instead of a single global timeout.

/// Adaptive per-operation timeout configuration.
///
/// # Examples
///
/// ```
/// use oceanfs_core::OperationTimeouts;
///
/// let timeouts = OperationTimeouts::default();
/// assert!(timeouts.wal_write_ms <= 500);
/// assert!(timeouts.metadata_read_ms <= 50);
/// ```
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct OperationTimeouts {
    /// Timeout for WAL write acknowledgments (ms). Default: 500ms.
    pub wal_write_ms: u64,
    /// Timeout for metadata reads (cache miss → RocksDB). Default: 50ms.
    pub metadata_read_ms: u64,
    /// Timeout for segment shard fetches. Default: 30_000ms (30s).
    pub shard_fetch_ms: u64,
    /// Timeout for EC encode operations. Default: 60_000ms (60s).
    pub ec_encode_ms: u64,
    /// Timeout for gossip ping operations. Default: 5_000ms (5s).
    pub gossip_ping_ms: u64,
    /// Timeout for hint delivery operations. Default: 10_000ms (10s).
    pub hint_delivery_ms: u64,
    /// Default generic write timeout. Default: 5_000ms (5s).
    pub write_default_ms: u64,
    /// Default generic read timeout. Default: 10_000ms (10s).
    pub read_default_ms: u64,
    /// Timeout for segment seal + EC encode post-seal operations (ms). Default: 120_000ms.
    pub segment_seal_ms: u64,
    /// Timeout for gossip roundtrip (push-pull sync message) (ms). Default: 10_000ms.
    pub gossip_roundtrip_ms: u64,
}

impl Default for OperationTimeouts {
    fn default() -> Self {
        Self {
            wal_write_ms: 500,
            metadata_read_ms: 50,
            shard_fetch_ms: 30_000,
            ec_encode_ms: 60_000,
            gossip_ping_ms: 5_000,
            hint_delivery_ms: 10_000,
            write_default_ms: 5_000,
            read_default_ms: 10_000,
            segment_seal_ms: 120_000,
            gossip_roundtrip_ms: 10_000,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn operation_timeouts_default_values() {
        let t = OperationTimeouts::default();
        assert_eq!(t.wal_write_ms, 500);
        assert_eq!(t.metadata_read_ms, 50);
        assert_eq!(t.shard_fetch_ms, 30_000);
        assert_eq!(t.ec_encode_ms, 60_000);
        assert_eq!(t.gossip_ping_ms, 5_000);
        assert_eq!(t.hint_delivery_ms, 10_000);
        assert_eq!(t.write_default_ms, 5_000);
        assert_eq!(t.read_default_ms, 10_000);
        assert_eq!(t.segment_seal_ms, 120_000);
        assert_eq!(t.gossip_roundtrip_ms, 10_000);
    }

    #[test]
    fn operation_timeouts_custom_values() {
        let t = OperationTimeouts {
            wal_write_ms: 100,
            metadata_read_ms: 10,
            shard_fetch_ms: 5_000,
            ec_encode_ms: 10_000,
            gossip_ping_ms: 1_000,
            hint_delivery_ms: 2_000,
            write_default_ms: 2_000,
            read_default_ms: 3_000,
            segment_seal_ms: 60_000,
            gossip_roundtrip_ms: 5_000,
        };
        assert_eq!(t.wal_write_ms, 100);
    }

    #[test]
    fn operation_timeouts_serde_roundtrip() {
        let t = OperationTimeouts {
            wal_write_ms: 100,
            metadata_read_ms: 10,
            shard_fetch_ms: 5_000,
            ec_encode_ms: 10_000,
            gossip_ping_ms: 1_000,
            hint_delivery_ms: 2_000,
            write_default_ms: 2_000,
            read_default_ms: 3_000,
            segment_seal_ms: 60_000,
            gossip_roundtrip_ms: 5_000,
        };
        let toml_str = toml::to_string(&t).unwrap();
        let roundtripped: OperationTimeouts = toml::from_str(&toml_str).unwrap();
        assert_eq!(roundtripped.wal_write_ms, 100);
        assert_eq!(roundtripped.metadata_read_ms, 10);
        assert_eq!(roundtripped.shard_fetch_ms, 5_000);
        assert_eq!(roundtripped.segment_seal_ms, 60_000);
        assert_eq!(roundtripped.gossip_roundtrip_ms, 5_000);
    }
}
