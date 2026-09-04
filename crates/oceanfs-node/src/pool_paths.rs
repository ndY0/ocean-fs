//! Role-pinned path resolution (ADR-0029 §D8, pools-only — ADR-0031).
//!
//! The node's non-segment data paths live on their role-pinned pool
//! roots: metadata store → `metadata` pool, data WAL → `wal` pool, event
//! WAL → `wal` pool + `event-wal` subdir, hint WAL → `hints` pool.
//! Pools are mandatory since f1 and every pinned role exists by
//! construction, so resolution is total: a Degraded pool still owns its
//! root (Phase B semantics arrive later) and the legacy `data_dir`
//! fallback arms are deleted (ADR-0031 D2).
//!
//! Resolution is a boot-time operation (perf guidelines 3.4/7.1: nothing
//! here runs on the write path).

use std::path::PathBuf;

use oceanfs_core::PoolRole;
use oceanfs_storage::PoolRegistry;

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
/// Pools are mandatory (f1 boot enforcement, ADR-0031 D1): a pool of
/// every pinned role exists, so each path resolves to that role's pool
/// root unconditionally — health status is ignored (a Degraded pool
/// still owns its root; real degraded semantics arrive in Phase B) and
/// there is no legacy `data_dir` / `hint_wal_dir` fallback (ADR-0031
/// D2).
///
/// - `metadata` → metadata pool root;
/// - `wal` → wal pool root;
/// - `event-wal` → wal pool root + `event-wal` (the event log rides the
///   journal device, ADR-0024);
/// - `hints` → hints pool root.
pub(crate) fn pool_paths(registry: &PoolRegistry) -> PoolPaths {
    // Pools are mandatory (f1 boot enforcement, ADR-0031 D1): a pool of
    // every pinned role exists on any validated config. Each role below
    // is resolved through `pool_by_role` and matched explicitly — the
    // `None` arm is unreachable by construction and must never silently
    // invent a fallback path (ADR-0031 D2).
    fn role_root(registry: &PoolRegistry, role: PoolRole) -> PathBuf {
        match registry.pool_by_role(role) {
            Some(pool) => pool.root().to_path_buf(),
            None => unreachable!(
                "no {} pool: f1 boot enforcement requires every pinned role \
                 (validate refuses role-incomplete [storage.pools])",
                role.as_str()
            ),
        }
    }
    let metadata = role_root(registry, PoolRole::Metadata);
    let wal = role_root(registry, PoolRole::Wal);
    PoolPaths {
        metadata,
        wal: wal.clone(),
        // The event log rides the journal device (ADR-0024): the pinned
        // wal pool root + `event-wal` subdir.
        event_wal: wal.join("event-wal"),
        hints: role_root(registry, PoolRole::Hints),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{path::Path, sync::Arc};

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

    /// Pools-only: every pinned role resolves to its pool root — the
    /// legacy `data_dir`/`hint_wal_dir` fallbacks are gone (ADR-0031 D2).
    #[test]
    fn four_role_registry_resolves_exact_pool_roots() {
        let tmp = tempfile::tempdir().unwrap();
        let (registry, roots) = pinned_registry(
            &tmp,
            &[
                (PoolRole::Data, "pool-a"),
                (PoolRole::Wal, "journal"),
                (PoolRole::Metadata, "meta"),
                (PoolRole::Hints, "hints"),
            ],
        );
        let paths = pool_paths(&registry);

        assert_eq!(paths.metadata, root_for(&roots, PoolRole::Metadata));
        assert_eq!(paths.wal, root_for(&roots, PoolRole::Wal));
        // The event log rides the journal device, under the wal pool root.
        assert_eq!(paths.event_wal, root_for(&roots, PoolRole::Wal).join("event-wal"));
        assert_eq!(paths.hints, root_for(&roots, PoolRole::Hints));
        // No role dir resolves under a `data_dir` anymore.
        let data_dir = tmp.path().join("data");
        assert!(!paths.metadata.starts_with(&data_dir));
        assert!(!paths.wal.starts_with(&data_dir));
        assert!(!paths.hints.starts_with(&data_dir));
    }

    /// A Degraded pinned pool still owns its root: health status is
    /// ignored by resolution (Phase B semantics arrive later) and no
    /// WARN is emitted — the Degraded→legacy bridge is gone
    /// (ADR-0031 D2).
    #[test]
    fn degraded_pool_resolves_to_its_own_root_without_warn() {
        use tracing::subscriber::with_default;

        let subscriber = RecordingSubscriber::default();
        with_default(subscriber.clone(), || {
            let tmp = tempfile::tempdir().unwrap();
            let (registry, roots) = pinned_registry(
                &tmp,
                &[
                    (PoolRole::Data, "pool-a"),
                    (PoolRole::Wal, "journal"),
                    (PoolRole::Metadata, "meta"),
                    (PoolRole::Hints, "hints"),
                ],
            );
            // Mark the wal pool Degraded (as the Degraded startup policy
            // does when the root probe fails).
            registry.set_status(1, PoolStatus::Degraded);

            let paths = pool_paths(&registry);
            assert_eq!(paths.wal, root_for(&roots, PoolRole::Wal));
            assert_eq!(paths.event_wal, root_for(&roots, PoolRole::Wal).join("event-wal"));
        });

        let events = subscriber.events.lock();
        assert!(events.is_empty(), "Degraded pool resolution must not WARN, got: {events:?}");
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
}
