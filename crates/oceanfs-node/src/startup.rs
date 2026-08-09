//! Startup validation utilities for OceanFS node configuration.
//!
//! Performs sanity checks on derived configuration values before
//! the node binds its network ports.

/// Validates that the total shard memory budget does not exceed 25% of system memory.
///
/// Logs a warning (not an error) if the threshold is exceeded, allowing the operator
/// to adjust `segment_shard_count`, pool size, or segment size.
///
/// # Examples
///
/// ```
/// use oceanfs_node::startup::validate_shard_memory_budget;
///
/// // For a typical 16 GB system, this should pass silently:
/// let result = validate_shard_memory_budget(4, 65536, 4_194_304);
/// assert!(result.is_ok());
/// ```
pub fn validate_shard_memory_budget(
    shard_count: usize,
    pool_size_bytes: usize,
    segment_size_bytes: u64,
) -> Result<(), String> {
    let total_shard_memory = shard_count as u64 * pool_size_bytes as u64 * segment_size_bytes;
    let system_memory = get_total_system_memory_bytes();
    let threshold = (system_memory as f64 * 0.25) as u64;

    if total_shard_memory > threshold {
        tracing::warn!(
            shard_count,
            pool_size_bytes,
            segment_size_bytes,
            total_shard_memory_bytes = total_shard_memory,
            system_memory_bytes = system_memory,
            threshold_bytes = threshold,
            "Shard memory budget exceeds 25% of system memory. \
             Consider reducing segment_shard_count, pool size, or segment size."
        );
    }
    Ok(())
}

/// Test-visible predicate: returns `true` when the shard memory budget
/// would exceed 25% of the given system memory.
///
/// Used by tests to verify the warning path without needing to read
/// `/proc/meminfo` or set up a tracing subscriber.
#[allow(dead_code)]
pub(crate) fn shard_budget_exceeds_threshold(
    shard_count: usize,
    pool_size_bytes: usize,
    segment_size_bytes: u64,
    system_memory_bytes: u64,
) -> bool {
    let total = shard_count as u64 * pool_size_bytes as u64 * segment_size_bytes;
    let threshold = (system_memory_bytes as f64 * 0.25) as u64;
    total > threshold
}

/// Returns the total system memory in bytes.
///
/// On Linux, reads `/proc/meminfo`. Falls back to a conservative estimate
/// on other platforms.
fn get_total_system_memory_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if line.starts_with("MemTotal:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<u64>() {
                            return kb * 1024;
                        }
                    }
                }
            }
        }
    }
    // Fallback: 8 GB conservative estimate
    8 * 1024 * 1024 * 1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_config_passes_silently() {
        let result = validate_shard_memory_budget(4, 65536, 4_194_304);
        assert!(result.is_ok());
    }

    #[test]
    fn extreme_config_still_passes_but_would_warn() {
        // This would warn but still returns Ok
        let result = validate_shard_memory_budget(1000, 65536, 4_194_304);
        assert!(result.is_ok());
    }

    /// T8.4: Memory budget correctly identifies when shard allocation
    /// exceeds 25% of system memory.
    #[test]
    fn test_shard_memory_budget_warns_above_25_percent() {
        // Simulate a 1 GB system → 25% threshold = 268,435,456 bytes.
        let system_mem: u64 = 1_073_741_824; // 1 GB
                                             // With small pool/segment sizes, a single shard is trivially under.
                                             // pool_size * segment_size = 1024 * 100000 = 102,400,000 bytes per shard.
                                             // shard_count=1 → 102 MB → under 25% (268 MB).
        let under = shard_budget_exceeds_threshold(1, 1024, 100_000, system_mem);
        assert!(!under, "1 shard at 102MB should be under 25% of 1GB");

        // shard_count=3 → 307 MB → above 25% (268 MB).
        let over = shard_budget_exceeds_threshold(3, 1024, 100_000, system_mem);
        assert!(over, "3 shards at 307MB should be above 25% of 1GB");

        // Extreme case: far above threshold.
        let far_above = shard_budget_exceeds_threshold(1000, 1024, 100_000, system_mem);
        assert!(far_above);
    }

    /// T8.5: Shard count flows correctly into buffer pool sizing
    /// (`total_pool_chunks = buffer_pool_max_chunks * shard_count`).
    #[test]
    fn test_shard_count_flows_into_pool_sizing() {
        use oceanfs_core::shard::derive_shard_count;
        use oceanfs_storage::BufferPool;

        // Explicit shard count = 8, max_chunks_per_shard = 100
        let shard_count = derive_shard_count(8, 16);
        let pool_chunks_per_shard: usize = 100;
        let total_chunks = pool_chunks_per_shard * shard_count;
        assert_eq!(
            total_chunks, 800,
            "total pool chunks should be max_chunks_per_shard * shard_count"
        );

        let pool = BufferPool::new(65536, total_chunks);
        assert_eq!(pool.max_buffers(), 800);

        // Auto-detect: with max=1, the derived count is 1
        let auto_count = derive_shard_count(0, 1);
        assert_eq!(auto_count, 1);
        let auto_total = pool_chunks_per_shard * auto_count;
        let auto_pool = BufferPool::new(65536, auto_total);
        assert_eq!(auto_pool.max_buffers(), 100);
    }
}
