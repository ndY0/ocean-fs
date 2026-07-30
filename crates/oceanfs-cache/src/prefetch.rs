//! Prefetch engine — speculative cache warming.
//!
//! After LIST or GET operations, prefetches metadata for adjacent keys
//! to warm the caches before the client requests them.

/// Configuration for the prefetch engine.
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Whether prefetching is enabled.
    pub enabled: bool,
    /// Number of objects to prefetch after a LIST.
    pub after_list: usize,
    /// Number of adjacent objects to prefetch after a GET.
    pub after_get: usize,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self { enabled: false, after_list: 16, after_get: 4 }
    }
}

/// Orchestrates speculative cache warming.
pub struct PrefetchEngine {
    config: PrefetchConfig,
}

impl PrefetchEngine {
    /// Creates a new prefetch engine.
    pub fn new(config: PrefetchConfig) -> Self {
        Self { config }
    }

    /// Returns whether prefetching is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Returns the configuration.
    pub fn config(&self) -> &PrefetchConfig {
        &self.config
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn disabled_by_default() {
        let engine = PrefetchEngine::new(PrefetchConfig::default());
        assert!(!engine.is_enabled());
    }

    #[test]
    fn can_be_enabled() {
        let config = PrefetchConfig { enabled: true, ..Default::default() };
        let engine = PrefetchEngine::new(config);
        assert!(engine.is_enabled());
    }
}
