//! Role-pinned path resolution (ADR-0029 §D8).
//!
//! When storage pools are configured, the node's non-segment data paths
//! move off `data_dir` onto their role-pinned pool roots: metadata store →
//! `metadata` pool, data WAL → `wal` pool, event WAL → `wal` pool +
//! `event-wal` subdir, hint WAL → `hints` pool. In legacy mode (no pools)
//! every path resolves exactly as before — byte-for-byte.
//!
//! Resolution is a boot-time operation (perf guidelines 3.4/7.1: nothing
//! here runs on the write path).

use std::path::{Path, PathBuf};

use oceanfs_core::PoolRole;
use oceanfs_storage::{PoolRegistry, PoolStatus};

/// Resolved role-pinned directories for the node's non-segment data paths.
///
/// # Examples
///
/// ```
/// use oceanfs_node::pool_paths::PoolPaths;
///
/// let paths = PoolPaths {
///     metadata: "/mnt/meta".into(),
///     wal: "/mnt/journal".into(),
///     event_wal: "/mnt/journal/event-wal".into(),
///     hints: "/mnt/hints".into(),
/// };
/// assert!(paths.metadata.is_absolute());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolPaths {
    /// Directory for the metadata store (RocksDB).
    pub metadata: PathBuf,
    /// Directory for the data WAL.
    pub wal: PathBuf,
    /// Directory for the segment event WAL (rides the wal pool root).
    pub event_wal: PathBuf,
    /// Directory for the hinted-handoff WAL.
    pub hints: PathBuf,
}

/// Resolves the role-pinned data paths from the pool registry.
///
/// Precedence rule (pinned): each role resolves to its `Healthy` pinned
/// pool root when the registry has one; the legacy fallback (`data_dir`
/// arithmetic) is used only when the registry has no pool of that role —
/// i.e. legacy mode (no pools configured) or a topology that did not pin
/// the role. When a pinned pool exists but is not `Healthy` (Degraded
/// startup policy — Phase A bridge), the role falls back to its legacy
/// path with a prominent WARN: real degraded semantics arrive in Phase B.
///
/// - `metadata` → metadata pool root, else `data_dir/metadata`;
/// - `wal` → wal pool root, else `data_dir/wal`;
/// - `event-wal` → wal pool root + `event-wal` (the event log rides the
///   journal device, ADR-0024), else `data_dir/event-wal`;
/// - `hints` → hints pool root, else `hint_wal_dir` override, else
///   `data_dir/hints` (the legacy `hint_wal_dir` override is honored only
///   when no hints pool is pinned).
pub(crate) fn pool_paths(
    registry: &PoolRegistry,
    data_dir: &Path,
    hint_wal_dir: &Option<PathBuf>,
) -> PoolPaths {
    PoolPaths {
        metadata: resolve_pinned(registry, PoolRole::Metadata, data_dir.join("metadata")),
        wal: resolve_pinned(registry, PoolRole::Wal, data_dir.join("wal")),
        // The event log rides the journal device (ADR-0024): the pinned
        // wal pool root + `event-wal` subdir; legacy keeps
        // `data_dir/event-wal` untouched (no extra join).
        event_wal: match pinned_root(registry, PoolRole::Wal) {
            Some(root) => root.join("event-wal"),
            None => data_dir.join("event-wal"),
        },
        hints: resolve_pinned(
            registry,
            PoolRole::Hints,
            hint_wal_dir.clone().unwrap_or_else(|| data_dir.join("hints")),
        ),
    }
}

/// The `Healthy` pinned pool root for a role, if any (warns when a pool of
/// the role exists but is Degraded — Phase A bridge).
fn pinned_root(registry: &PoolRegistry, role: PoolRole) -> Option<PathBuf> {
    // A pool of the role exists but is not Healthy → the Degraded-policy
    // fallback path: keep the node bootable, but say so loudly (Phase A
    // bridge; Phase B turns this into real degraded semantics).
    if let Some(pool) = registry.pool_by_role(role) {
        if pool.status() != PoolStatus::Healthy {
            tracing::warn!(
                pool = %pool.name(),
                role = %role.as_str(),
                "role pool is not Healthy; falling back to the legacy path \
                 (Degraded startup policy — Phase A bridge)"
            );
        }
    }
    registry
        .pool_by_role(role)
        .filter(|pool| pool.status() == PoolStatus::Healthy)
        .map(|pool| pool.root().to_path_buf())
}

/// Resolves one role's path: the pinned pool root when a `Healthy` pool of
/// that role exists, else `fallback`.
fn resolve_pinned(registry: &PoolRegistry, role: PoolRole, fallback: PathBuf) -> PathBuf {
    pinned_root(registry, role).unwrap_or(fallback)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    // NOTE: the storage-pool definition type is exported from the core
    // facade as `StoragePoolConfig` (f1 naming deviation — `PoolConfig`
    // already names the active-segment-pool config).
    use oceanfs_core::{MissingRootPolicy, PoolTech, StorageConfig, StoragePoolConfig};
    use oceanfs_storage::PoolStatus;

    use super::*;

    fn pool(name: &str, role: PoolRole, root: &Path) -> StoragePoolConfig {
        StoragePoolConfig {
            name: name.to_string(),
            role,
            root: root.to_path_buf(),
            weight: None,
            tech: PoolTech::Auto,
            health: Default::default(),
        }
    }

    /// A registry with pinned metadata/wal/hints pools (sibling roots).
    fn pinned_registry(
        tmp: &tempfile::TempDir,
        roles: &[(PoolRole, &str)],
    ) -> (PoolRegistry, Vec<(PoolRole, PathBuf)>) {
        let data_dir = tmp.path().join("data");
        let mut roots = Vec::new();
        let storage = StorageConfig {
            pools: roles
                .iter()
                .enumerate()
                .map(|(index, (role, name))| {
                    let root = tmp.path().join(format!("{}-{index}", role.as_str()));
                    roots.push((*role, root.clone()));
                    pool(name, *role, &root)
                })
                .collect(),
            missing_root_policy: MissingRootPolicy::Degraded,
        };
        let registry = PoolRegistry::from_config(&storage, &data_dir).unwrap();
        (registry, roots)
    }

    fn root_for(roots: &[(PoolRole, PathBuf)], role: PoolRole) -> PathBuf {
        roots
            .iter()
            .find(|(candidate, _)| *candidate == role)
            .map(|(_, root)| root.clone())
            .expect("role root")
    }

    /// Legacy mode: resolution equals today's paths byte-for-byte.
    #[test]
    fn legacy_mode_resolves_exactly_as_before() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let registry = PoolRegistry::from_config(&StorageConfig::default(), &data_dir).unwrap();

        let paths = pool_paths(&registry, &data_dir, &None);
        assert_eq!(paths.metadata, data_dir.join("metadata"));
        assert_eq!(paths.wal, data_dir.join("wal"));
        assert_eq!(paths.event_wal, data_dir.join("event-wal"));
        assert_eq!(paths.hints, data_dir.join("hints"));

        // The legacy hint_wal_dir override is honored when no hints pool
        // is pinned.
        let custom_hints = tmp.path().join("custom-hints");
        let paths = pool_paths(&registry, &data_dir, &Some(custom_hints.clone()));
        assert_eq!(paths.hints, custom_hints);
    }

    /// Explicit mode: every pinned role resolves to its pool root.
    #[test]
    fn explicit_mode_resolves_to_pool_roots() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let (registry, roots) = pinned_registry(
            &tmp,
            &[
                (PoolRole::Data, "pool-a"),
                (PoolRole::Wal, "journal"),
                (PoolRole::Metadata, "meta"),
                (PoolRole::Hints, "hints"),
            ],
        );
        let paths = pool_paths(&registry, &data_dir, &None);

        assert_eq!(paths.metadata, root_for(&roots, PoolRole::Metadata));
        assert_eq!(paths.wal, root_for(&roots, PoolRole::Wal));
        // The event log rides the journal device, under the wal pool root.
        assert_eq!(paths.event_wal, root_for(&roots, PoolRole::Wal).join("event-wal"));
        assert_eq!(paths.hints, root_for(&roots, PoolRole::Hints));

        // The legacy hint_wal_dir override is ignored when a hints pool is
        // pinned — the pool topology is the authoritative layout.
        let custom_hints = tmp.path().join("custom-hints");
        let paths = pool_paths(&registry, &data_dir, &Some(custom_hints));
        assert_eq!(paths.hints, root_for(&roots, PoolRole::Hints));
    }

    /// Pool mode without a metadata pool: the role falls back to legacy.
    #[test]
    fn pool_mode_without_role_pool_falls_back_to_legacy() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let (registry, roots) =
            pinned_registry(&tmp, &[(PoolRole::Data, "pool-a"), (PoolRole::Wal, "journal")]);
        let paths = pool_paths(&registry, &data_dir, &None);

        // Wal pinned, metadata not configured → legacy fallback.
        assert_eq!(paths.wal, root_for(&roots, PoolRole::Wal));
        assert_eq!(paths.metadata, data_dir.join("metadata"));
        assert_eq!(paths.hints, data_dir.join("hints"));
    }

    /// A Degraded pinned pool (startup policy bridge) falls back with the
    /// role's legacy path.
    #[test]
    fn degraded_pinned_pool_falls_back_to_legacy_path() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let (registry, roots) =
            pinned_registry(&tmp, &[(PoolRole::Data, "pool-a"), (PoolRole::Wal, "journal")]);
        // Mark the wal pool Degraded (as the Degraded startup policy does
        // when the root probe fails).
        registry.set_status(1, PoolStatus::Degraded);

        let paths = pool_paths(&registry, &data_dir, &None);
        assert_eq!(paths.wal, data_dir.join("wal"), "Degraded wal pool falls back");
        assert_ne!(paths.wal, root_for(&roots, PoolRole::Wal));
    }

    /// A subscriber that records WARN+ event messages for assertion.
    /// Cloneable so `with_default` can take ownership while the test keeps
    /// a handle on the recorded events.
    #[derive(Default, Clone)]
    struct RecordingSubscriber {
        events: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    impl tracing::Subscriber for RecordingSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() >= tracing::Level::WARN {
                let mut visitor = MessageVisitor { message: String::new() };
                event.record(&mut visitor);
                self.events.lock().push(visitor.message);
            }
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    /// Collects the `message` field of a tracing event.
    struct MessageVisitor {
        message: String,
    }

    impl tracing::field::Visit for MessageVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{value:?}");
            }
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                self.message = value.to_string();
            }
        }
    }

    /// The Degraded-pinned-pool fallback emits the prominent WARN.
    #[test]
    fn degraded_pinned_pool_fallback_emits_warn() {
        use tracing::subscriber::with_default;

        let subscriber = RecordingSubscriber::default();
        with_default(subscriber.clone(), || {
            let tmp = tempfile::tempdir().unwrap();
            let data_dir = tmp.path().join("data");
            let (registry, _roots) =
                pinned_registry(&tmp, &[(PoolRole::Data, "pool-a"), (PoolRole::Wal, "journal")]);
            registry.set_status(1, PoolStatus::Degraded);

            let paths = pool_paths(&registry, &data_dir, &None);
            assert_eq!(paths.wal, data_dir.join("wal"));
        });

        let events = subscriber.events.lock();
        assert!(
            events.iter().any(|message| message.contains("role pool is not Healthy")),
            "the Degraded fallback must WARN, got: {events:?}"
        );
    }

    /// Legacy mode and pool-mode-without-role fallbacks are silent (the
    /// WARN is reserved for an existing-but-Degraded pinned pool).
    #[test]
    fn legacy_fallback_is_silent() {
        use tracing::subscriber::with_default;

        let subscriber = RecordingSubscriber::default();
        with_default(subscriber.clone(), || {
            let tmp = tempfile::tempdir().unwrap();
            let data_dir = tmp.path().join("data");
            let registry = PoolRegistry::from_config(&StorageConfig::default(), &data_dir).unwrap();
            let _ = pool_paths(&registry, &data_dir, &None);

            // Pool mode without a metadata pool: also silent.
            let (registry, _roots) =
                pinned_registry(&tmp, &[(PoolRole::Data, "pool-a"), (PoolRole::Wal, "journal")]);
            let _ = pool_paths(&registry, &data_dir, &None);
        });

        let events = subscriber.events.lock();
        assert!(events.is_empty(), "legacy / no-role fallbacks must not WARN, got: {events:?}");
    }
}
