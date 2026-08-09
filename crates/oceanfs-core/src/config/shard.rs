//! Shard count derivation and validation utilities.
//!
//! Provides auto-detection of the optimal segment shard count
//! based on available CPU cores and configuration caps.

/// Derive the effective shard count from config.
///
/// If `config_shard_count > 0`, use it directly.
/// Otherwise, auto-detect: `min(num_cpus, config_shard_max)`.
///
/// # Examples
///
/// ```
/// use oceanfs_core::shard::derive_shard_count;
///
/// // Explicit count overrides auto-detection
/// assert_eq!(derive_shard_count(8, 16), 8);
///
/// // Auto-detect with cap (result depends on CPU count)
/// let auto = derive_shard_count(0, 1);
/// assert_eq!(auto, 1); // capped to max
/// ```
pub fn derive_shard_count(config_shard_count: usize, config_shard_max: usize) -> usize {
    if config_shard_count > 0 {
        config_shard_count
    } else {
        let num_cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        num_cpus.min(config_shard_max).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_count_overrides_auto() {
        assert_eq!(derive_shard_count(8, 16), 8);
        assert_eq!(derive_shard_count(100, 16), 100);
    }

    #[test]
    fn auto_detect_respects_max() {
        // With max=1, result must be 1
        assert_eq!(derive_shard_count(0, 1), 1);
    }

    #[test]
    fn auto_detect_uses_cpu_count() {
        let result = derive_shard_count(0, 64);
        let num_cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        // Result should be min(num_cpus, 64) but at least 1
        assert!(result >= 1);
        assert!(result <= num_cpus.min(64).max(1));
    }

    #[test]
    fn auto_detect_never_returns_zero() {
        // Even with max=0, we clamp to at least 1
        let result = derive_shard_count(0, 0);
        assert!(result >= 1);
    }
}
