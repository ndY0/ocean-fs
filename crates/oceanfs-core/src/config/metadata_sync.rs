//! Metadata change-journal & pull-based catch-up configuration
//! (ADR-0038, ae2 / S2).
//!
//! Controls the node-local metadata change journal and the Tier-1
//! `metadata_sync` worker. The mechanism is **off by default**: with
//! `enabled = false` no journal directory is opened, no worker runs, no
//! gRPC handler serves, and no metric series registers — behavior is
//! identical to a build without the feature.
//!
//! # Examples
//!
//! ```
//! use oceanfs_core::MetadataSyncConfig;
//!
//! let config = MetadataSyncConfig::default();
//! assert!(!config.enabled);
//! assert_eq!(config.interval_sec, 10);
//! assert_eq!(config.max_entries_per_cycle, 4096);
//! assert_eq!(config.journal_max_bytes, 256 * 1024 * 1024);
//! assert!(config.validate().is_ok());
//! ```

/// Metadata change-journal & pull-based catch-up configuration (ADR-0038).
///
/// The first seven fields are the S2 surface; the last three are the
/// S3/S4 knobs, accepted here so the configuration shape is stable but
/// **inert in S2** (they are parsed, defaulted, documented, and never
/// consulted).
///
/// # Examples
///
/// ```
/// use oceanfs_core::MetadataSyncConfig;
///
/// let mut config = MetadataSyncConfig::default();
/// assert!(!config.enabled);
///
/// // Enabling makes the whole section live; defaults validate as-is.
/// config.enabled = true;
/// assert!(config.validate().is_ok());
///
/// // Invalid combinations are rejected by name.
/// config.interval_sec = 0;
/// let error = config.validate().expect_err("zero cadence while enabled");
/// assert!(error.contains("interval_sec"), "{error}");
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct MetadataSyncConfig {
    /// Kill-switch for the metadata change journal and sync worker
    /// (default `false`). When `false` the composition root does not
    /// construct or open the journal, the store's journal handle is
    /// `None` (a branch, no I/O), no worker registers, the `FetchJournal`
    /// and `FetchMetadataRows` handlers answer `unavailable`, and no
    /// `oceanfs_metadata_*` series registers.
    pub enabled: bool,
    /// Freshness knob: cadence of the periodic Tier-1 sync cycle in
    /// seconds (default 10). Requires `1..=86400` while enabled. Peers
    /// becoming Alive trigger an additional sweep outside this cadence.
    pub interval_sec: u64,
    /// Journal entries consumed per cycle across all peers (default
    /// 4096). Bounds the work of one `metadata_sync` cycle.
    pub max_entries_per_cycle: u64,
    /// Pulled row bytes per cycle across all peers (default 4 MiB).
    /// Bounds the point-fetch volume of one cycle.
    pub max_bytes_per_cycle: u64,
    /// Maximum concurrent point-fetch (`FetchMetadataRows`) calls
    /// (default 2). Bounds catch-up fan-out.
    pub max_inflight_pulls: usize,
    /// Hard journal size cap in bytes (default 256 MiB). When the cap
    /// bites, the journal trims past the lagging peer's watermark; the
    /// lagging peer observes a gap and bootstraps (S3).
    pub journal_max_bytes: u64,
    /// Hard journal age cap in seconds (default 7 days). Files whose
    /// records are all older are trimmed even if a peer has not acked.
    pub journal_max_age_secs: u64,
    /// S4-reserved: HMAC spot-check samples per cycle (default 16).
    /// Accepted and documented, **inert in S2**.
    pub spot_check_keys_per_cycle: u32,
    /// S3-reserved: owner-only point fetch on a read miss (default
    /// `true`). Accepted and documented, **inert in S2**.
    pub read_trigger_enabled: bool,
    /// S3-reserved: key-ordered window size per bootstrap request
    /// (default 1024). Accepted and documented, **inert in S2**.
    pub bootstrap_batch_keys: u64,
}

impl Default for MetadataSyncConfig {
    fn default() -> Self {
        Self {
            enabled: default_metadata_sync_enabled(),
            interval_sec: default_metadata_sync_interval_sec(),
            max_entries_per_cycle: default_metadata_sync_max_entries_per_cycle(),
            max_bytes_per_cycle: default_metadata_sync_max_bytes_per_cycle(),
            max_inflight_pulls: default_metadata_sync_max_inflight_pulls(),
            journal_max_bytes: default_metadata_sync_journal_max_bytes(),
            journal_max_age_secs: default_metadata_sync_journal_max_age_secs(),
            spot_check_keys_per_cycle: default_metadata_sync_spot_check_keys_per_cycle(),
            read_trigger_enabled: default_metadata_sync_read_trigger_enabled(),
            bootstrap_batch_keys: default_metadata_sync_bootstrap_batch_keys(),
        }
    }
}

impl MetadataSyncConfig {
    /// Validates the metadata-sync settings.
    ///
    /// Validation applies only while `enabled` is `true`; a disabled
    /// section is inert and its values are ignored. When enabled:
    ///
    /// - `interval_sec` must be `1..=86400` (one day);
    /// - `max_entries_per_cycle` must be at least 1 and fit the wire's
    ///   `uint32`;
    /// - `max_bytes_per_cycle` must be at least 1;
    /// - `max_inflight_pulls` must be at least 1;
    /// - `journal_max_bytes` and `journal_max_age_secs` must be at least 1.
    ///
    /// The S3/S4-reserved keys are not validated beyond parsing — they do
    /// not affect behavior in S2.
    ///
    /// # Errors
    ///
    /// Returns a message naming the offending field and the accepted
    /// range.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::MetadataSyncConfig;
    ///
    /// // Disabled: inert values are accepted.
    /// let mut config = MetadataSyncConfig::default();
    /// config.interval_sec = 0;
    /// assert!(config.validate().is_ok());
    ///
    /// // Enabled: the same values are rejected.
    /// config.enabled = true;
    /// assert!(config.validate().is_err());
    ///
    /// config.interval_sec = 10;
    /// config.journal_max_bytes = 0;
    /// let error = config.validate().expect_err("zero cap while enabled");
    /// assert!(error.contains("journal_max_bytes"), "{error}");
    /// ```
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.interval_sec == 0 || self.interval_sec > METADATA_SYNC_MAX_INTERVAL_SEC {
            return Err(format!(
                "metadata_sync.interval_sec: expected 1..={METADATA_SYNC_MAX_INTERVAL_SEC} \
                 seconds while enabled, got {}",
                self.interval_sec
            ));
        }
        if self.max_entries_per_cycle == 0 || self.max_entries_per_cycle > u64::from(u32::MAX) {
            return Err(format!(
                "metadata_sync.max_entries_per_cycle: expected 1..={} while enabled, got {}",
                u32::MAX,
                self.max_entries_per_cycle
            ));
        }
        if self.max_bytes_per_cycle == 0 {
            return Err(
                "metadata_sync.max_bytes_per_cycle: expected at least 1 byte while enabled, got 0"
                    .into(),
            );
        }
        if self.max_inflight_pulls == 0 {
            return Err("metadata_sync.max_inflight_pulls: expected at least 1 while enabled, \
                 got 0"
                .into());
        }
        if self.journal_max_bytes == 0 {
            return Err(
                "metadata_sync.journal_max_bytes: expected at least 1 byte while enabled, got 0"
                    .into(),
            );
        }
        if self.journal_max_age_secs == 0 {
            return Err(
                "metadata_sync.journal_max_age_secs: expected at least 1 second while enabled, \
                 got 0"
                    .into(),
            );
        }
        Ok(())
    }
}

/// Upper bound accepted for `interval_sec` while enabled (one day).
/// # Examples
///
/// ```ignore
/// assert!(MetadataSyncConfig::default().interval_sec <= METADATA_SYNC_MAX_INTERVAL_SEC);
/// ```
pub const METADATA_SYNC_MAX_INTERVAL_SEC: u64 = 86_400;

/// Default kill-switch state: off.
/// # Examples
///
/// ```ignore
/// let value = default_metadata_sync_enabled();
/// ```
pub fn default_metadata_sync_enabled() -> bool {
    false
}

/// Default sync cycle cadence: every 10 seconds.
pub fn default_metadata_sync_interval_sec() -> u64 {
    10
}

/// Default per-cycle entry budget: 4096.
pub fn default_metadata_sync_max_entries_per_cycle() -> u64 {
    4096
}

/// Default per-cycle pulled-byte budget: 4 MiB.
/// # Examples
///
/// ```ignore
/// let value = default_metadata_sync_max_bytes_per_cycle();
/// ```
pub fn default_metadata_sync_max_bytes_per_cycle() -> u64 {
    4 * 1024 * 1024
}

/// Default bound on concurrent point fetches: 2.
pub fn default_metadata_sync_max_inflight_pulls() -> usize {
    2
}

/// Default hard journal cap: 256 MiB.
pub fn default_metadata_sync_journal_max_bytes() -> u64 {
    256 * 1024 * 1024
}

/// Default hard journal age cap: 7 days.
/// # Examples
///
/// ```ignore
/// let value = default_metadata_sync_journal_max_age_secs();
/// ```
pub fn default_metadata_sync_journal_max_age_secs() -> u64 {
    7 * 24 * 60 * 60
}

/// S4-reserved default: 16 spot-check samples per cycle (inert in S2).
pub fn default_metadata_sync_spot_check_keys_per_cycle() -> u32 {
    16
}

/// S3-reserved default: owner-only read-miss point fetch enabled
/// (inert in S2).
/// # Examples
///
/// ```ignore
/// let value = default_metadata_sync_read_trigger_enabled();
/// ```
pub fn default_metadata_sync_read_trigger_enabled() -> bool {
    true
}

/// S3-reserved default: 1024 keys per bootstrap window (inert in S2).
pub fn default_metadata_sync_bootstrap_batch_keys() -> u64 {
    1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_adr_0038_d8() {
        let config = MetadataSyncConfig::default();
        assert!(!config.enabled, "kill-switch defaults off");
        assert_eq!(config.interval_sec, 10);
        assert_eq!(config.max_entries_per_cycle, 4096);
        assert_eq!(config.max_bytes_per_cycle, 4_194_304);
        assert_eq!(config.max_inflight_pulls, 2);
        assert_eq!(config.journal_max_bytes, 268_435_456);
        assert_eq!(config.journal_max_age_secs, 604_800);
        assert_eq!(config.spot_check_keys_per_cycle, 16);
        assert!(config.read_trigger_enabled);
        assert_eq!(config.bootstrap_batch_keys, 1024);
    }

    #[test]
    fn toml_defaults_and_explicit_values_parse() {
        #[derive(serde::Deserialize)]
        struct Wrapper {
            #[serde(default)]
            metadata_sync: MetadataSyncConfig,
        }

        let absent: Wrapper = toml::from_str("").expect("empty table parses");
        assert!(!absent.metadata_sync.enabled, "serde default is off");
        assert_eq!(absent.metadata_sync.interval_sec, 10);

        let explicit: Wrapper =
            toml::from_str("[metadata_sync]\nenabled = true\ninterval_sec = 3\n")
                .expect("explicit values parse");
        assert!(explicit.metadata_sync.enabled);
        assert_eq!(explicit.metadata_sync.interval_sec, 3);
    }

    #[test]
    fn disabled_section_accepts_inert_values() {
        let mut config = MetadataSyncConfig::default();
        config.interval_sec = 0;
        config.max_entries_per_cycle = 0;
        config.max_bytes_per_cycle = 0;
        config.max_inflight_pulls = 0;
        config.journal_max_bytes = 0;
        config.journal_max_age_secs = 0;
        assert!(config.validate().is_ok(), "a disabled section is inert");
    }

    #[test]
    fn enabled_section_rejects_each_invalid_bound() {
        let mut config = MetadataSyncConfig::default();
        config.enabled = true;

        for (label, apply) in [
            ("interval_sec", (|c: &mut MetadataSyncConfig| c.interval_sec = 0) as fn(&mut _)),
            ("interval_sec", |c| c.interval_sec = 86_401),
            ("max_entries_per_cycle", |c| c.max_entries_per_cycle = 0),
            ("max_entries_per_cycle", |c| c.max_entries_per_cycle = u64::from(u32::MAX) + 1),
            ("max_bytes_per_cycle", |c| c.max_bytes_per_cycle = 0),
            ("max_inflight_pulls", |c| c.max_inflight_pulls = 0),
            ("journal_max_bytes", |c| c.journal_max_bytes = 0),
            ("journal_max_age_secs", |c| c.journal_max_age_secs = 0),
        ] {
            let mut candidate = config.clone();
            apply(&mut candidate);
            let error = candidate.validate().expect_err(&format!("{label} must be rejected"));
            assert!(error.contains(label), "expected {label} in: {error}");
        }
    }

    #[test]
    fn enabled_section_accepts_bounds_and_max_interval() {
        let mut config = MetadataSyncConfig::default();
        config.enabled = true;
        config.interval_sec = 1;
        config.interval_sec = METADATA_SYNC_MAX_INTERVAL_SEC;
        config.max_entries_per_cycle = u64::from(u32::MAX);
        assert!(config.validate().is_ok());
    }
}
