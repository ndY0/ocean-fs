//! Peer-side cached routing state (ADR-0029 §D5) — a hint, never a
//! dependency.
//!
//! The [`ManifestCache`] holds the last-known `NodeManifest` per peer,
//! populated from the gossip plane (f6's membership events) and
//! consulted by the read path (replica selection for a GET) and the
//! write path (replica target selection for a PUT). It implements
//! [`oceanfs_server::RoutingHint`], the trait the server's coordinators
//! consume.
//!
//! Phase A: every manifest is Healthy and `write_degraded` is always
//! false, so the filters never exclude — the cache is observationally
//! neutral. f5 (2026-09-11): Degraded data pools are a **fallback tier**,
//! never a hard exclusion — reads and writes prefer Healthy candidates
//! and fall back to Degraded ones before failing.

use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
use oceanfs_core::{Counter, LabelSet, MetricRegistrar, NodeId};
use oceanfs_membership::manifest::NodeManifest;
use oceanfs_server::{CandidateClass, FallbackPath, RoutingHint};

/// The per-peer cached routing state (ADR-0029 §D5).
///
/// An `ArcSwap`-backed map of `node_id → Arc<NodeManifest>`: lock-free
/// reads on the hot path (perf 2.4), wholesale-replaced on gossip-driven
/// updates (never mutated in place — perf 7.2: no lock in the
/// read/write path). The map is small (one entry per peer, 5–20 at
/// scale), so a full-map clone per update is negligible (updates are
/// rare: one per manifest change).
///
/// Staleness policy: versioned by the entry's own version (ADR-0028);
/// a stale-but-present manifest beats absent (D5) — the cache keeps the
/// last-known manifest until a fresher one replaces it or the node is
/// removed.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use oceanfs_core::NodeId;
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
/// use oceanfs_node::routing_cache::ManifestCache;
///
/// let cache = ManifestCache::new();
/// let manifest = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2)],
/// );
/// cache.update(NodeId::new("peer"), Arc::new(manifest));
///
/// assert!(cache.get(&NodeId::new("peer")).is_some());
/// assert!(cache.get(&NodeId::new("unknown")).is_none());
/// cache.remove(&NodeId::new("peer"));
/// assert!(cache.get(&NodeId::new("peer")).is_none());
/// ```
pub struct ManifestCache {
    /// node_id → last-known manifest. Replaced wholesale on update
    /// (perf 2.4: lock-free `load()` for readers, `store()` for the
    /// rare writer).
    manifests: ArcSwap<HashMap<NodeId, Arc<NodeManifest>>>,
    /// `oceanfs_routing_cache_misses_total` — a `get` with no entry.
    cache_misses: Counter,
    /// `oceanfs_routing_failover_total` — error-driven fallback to the
    /// next replica (the cache was a hint; the I/O error was the truth).
    failover_total: Counter,
    /// `oceanfs_routing_manifest_skips_total{path="read"}` — a read
    /// candidate excluded because its manifest reported zero Healthy
    /// data pools (g6).
    read_skips: Counter,
    /// `oceanfs_routing_manifest_skips_total{path="write"}` — a write
    /// target hard-excluded because its manifest reported
    /// `write_degraded`, `node_unavailable`, or no writable data pool.
    write_skips: Counter,
    /// `oceanfs_routing_degraded_fallbacks_total{path="read"}` — a
    /// Degraded read candidate was ordered for an attempt (f5).
    degraded_fallbacks_read: Counter,
    /// `oceanfs_routing_degraded_fallbacks_total{path="write"}` — a
    /// Degraded write target was ordered for an attempt (f5).
    degraded_fallbacks_write: Counter,
}

impl ManifestCache {
    /// Creates an empty cache with its routing metrics.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_node::routing_cache::ManifestCache;
    ///
    /// let cache = ManifestCache::new();
    /// assert_eq!(cache.len(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            manifests: ArcSwap::from_pointee(HashMap::new()),
            cache_misses: Counter::new(
                "oceanfs_routing_cache_misses_total".into(),
                "Routing cache lookups with no cached manifest".into(),
                LabelSet::empty(),
            ),
            failover_total: Counter::new(
                "oceanfs_routing_failover_total".into(),
                "Error-driven fallbacks to the next replica".into(),
                LabelSet::empty(),
            ),
            read_skips: Counter::new(
                "oceanfs_routing_manifest_skips_total".into(),
                "Read candidates hard-excluded by their manifest (unavailable / no usable data pool)"
                    .into(),
                LabelSet::new(&[("path", "read")]),
            ),
            write_skips: Counter::new(
                "oceanfs_routing_manifest_skips_total".into(),
                "Write targets hard-excluded by their manifest (write_degraded / unavailable / no writable pool)"
                    .into(),
                LabelSet::new(&[("path", "write")]),
            ),
            degraded_fallbacks_read: Counter::new(
                "oceanfs_routing_degraded_fallbacks_total".into(),
                "Degraded read candidates ordered for an attempt (fallback tier)".into(),
                LabelSet::new(&[("path", "read")]),
            ),
            degraded_fallbacks_write: Counter::new(
                "oceanfs_routing_degraded_fallbacks_total".into(),
                "Degraded write targets ordered for an attempt (fallback tier)".into(),
                LabelSet::new(&[("path", "write")]),
            ),
        }
    }

    /// Returns the last-known manifest for `node_id`.
    ///
    /// A miss (unknown peer) increments the `cache_misses_total`
    /// counter — the caller treats it as "no pool info" and proceeds
    /// (the error path is the guarantee).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_core::NodeId;
    /// use oceanfs_membership::manifest::NodeManifest;
    /// use oceanfs_node::routing_cache::ManifestCache;
    ///
    /// let cache = ManifestCache::new();
    /// let manifest = NodeManifest::from_pools(1, &[]);
    /// cache.update(NodeId::new("peer"), Arc::new(manifest));
    /// assert_eq!(cache.get(&NodeId::new("peer")).unwrap().incarnation(), 1);
    /// ```
    pub fn get(&self, node_id: &NodeId) -> Option<Arc<NodeManifest>> {
        let result = self.manifests.load().get(node_id).cloned();
        if result.is_none() {
            self.cache_misses.inc();
        }
        result
    }

    /// Replaces the cache entry for `node_id` with a newer manifest.
    ///
    /// Called on version-bumped membership entries (f6's `manifest_of`
    /// read): the map is replaced wholesale, so readers never observe a
    /// partially-updated view (perf 2.4).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_core::NodeId;
    /// use oceanfs_membership::manifest::NodeManifest;
    /// use oceanfs_node::routing_cache::ManifestCache;
    ///
    /// let cache = ManifestCache::new();
    /// let old = NodeManifest::from_pools(1, &[]);
    /// let newer = NodeManifest::from_pools(2, &[]);
    /// cache.update(NodeId::new("peer"), Arc::new(old));
    /// cache.update(NodeId::new("peer"), Arc::new(newer.clone()));
    /// // The version-bumped update replaced the entry wholesale.
    /// assert_eq!(cache.get(&NodeId::new("peer")).unwrap(), Arc::new(newer));
    /// ```
    pub fn update(&self, node_id: NodeId, manifest: Arc<NodeManifest>) {
        let mut map = (**self.manifests.load()).clone();
        map.insert(node_id, manifest);
        self.manifests.store(Arc::new(map));
    }

    /// Evicts a node's manifest (Dead/Left members).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use oceanfs_core::NodeId;
    /// use oceanfs_membership::manifest::NodeManifest;
    /// use oceanfs_node::routing_cache::ManifestCache;
    ///
    /// let cache = ManifestCache::new();
    /// cache.update(NodeId::new("peer"), Arc::new(NodeManifest::from_pools(1, &[])));
    /// cache.remove(&NodeId::new("peer"));
    /// assert!(cache.get(&NodeId::new("peer")).is_none());
    /// ```
    pub fn remove(&self, node_id: &NodeId) {
        let mut map = (**self.manifests.load()).clone();
        if map.remove(node_id).is_some() {
            self.manifests.store(Arc::new(map));
        }
    }

    /// The number of cached manifests (test/summary helper).
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_node::routing_cache::ManifestCache;
    ///
    /// let cache = ManifestCache::new();
    /// assert_eq!(cache.len(), 0);
    /// ```
    pub fn len(&self) -> usize {
        self.manifests.load().len()
    }

    /// Whether the cache is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_node::routing_cache::ManifestCache;
    ///
    /// let cache = ManifestCache::new();
    /// assert!(cache.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Registers the cache's routing metrics with a registrar.
    ///
    /// # Examples
    ///
    /// ```
    /// use oceanfs_core::{MetricRegistrar, Counter, Gauge, Histogram};
    /// use oceanfs_node::routing_cache::ManifestCache;
    ///
    /// struct Registrar;
    /// impl MetricRegistrar for Registrar {
    ///     fn register_counter(&self, _: Counter) {}
    ///     fn register_gauge(&self, _: Gauge) {}
    ///     fn register_histogram(&self, _: std::sync::Arc<Histogram>) {}
    /// }
    ///
    /// let cache = ManifestCache::new();
    /// cache.register_metrics(&Registrar);
    /// ```
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.cache_misses.clone());
        registrar.register_counter(self.failover_total.clone());
        registrar.register_counter(self.read_skips.clone());
        registrar.register_counter(self.write_skips.clone());
        registrar.register_counter(self.degraded_fallbacks_read.clone());
        registrar.register_counter(self.degraded_fallbacks_write.clone());
    }
}

impl Default for ManifestCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Number of `data`-role pools reporting `Healthy` in a manifest.
///
/// The read/write exclusion filters: a node with zero Healthy data
/// pools cannot serve segment reads (or accept new data) — every data
/// pool is Degraded/Dead.
///
/// # Examples
///
/// ```
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
/// use oceanfs_node::routing_cache::healthy_data_pools;
///
/// let healthy = NodeManifest::from_pools(
///     1,
///     &[
///         PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2),
///         PoolManifest::new(1, "data", "healthy", false, 1 << 40, 1),
///         PoolManifest::new(2, "wal", "healthy", false, 1 << 30, 1),
///     ],
/// );
/// assert_eq!(healthy_data_pools(&healthy), 2);
///
/// let all_dead = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "dead", false, 0, 2)],
/// );
/// assert_eq!(healthy_data_pools(&all_dead), 0);
/// ```
pub fn healthy_data_pools(manifest: &NodeManifest) -> usize {
    manifest.pools().iter().filter(|p| p.role() == "data" && p.status() == "healthy").count()
}

/// Whether a manifest reports the node as `write_degraded` (a role
/// consequence flag, ADR-0029 §D3 — Phase A: always `false`).
///
/// # Examples
///
/// ```
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
/// use oceanfs_node::routing_cache::is_write_degraded;
///
/// let ok = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2)],
/// );
/// assert!(!is_write_degraded(&ok));
///
/// let degraded = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "healthy", true, 1 << 40, 2)],
/// );
/// assert!(is_write_degraded(&degraded));
/// ```
pub fn is_write_degraded(manifest: &NodeManifest) -> bool {
    manifest.pools().iter().any(|p| p.write_degraded())
}

/// Whether a node manifest reports the node as able to accept NEW
/// writes (g6, ADR-0029 §D5): NOT `node_unavailable` (g8: a metadata-dead
/// node cannot persist new object rows), not `write_degraded`, AND at
/// least one Healthy data pool. This is the shared write-path filter —
/// the same predicate the peer routing hint applies when selecting
/// replica targets (a manifest miss stays eligible; the I/O error path
/// is the guarantee).
///
/// # Examples
///
/// ```
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
/// use oceanfs_node::routing_cache::can_accept_writes;
///
/// let ok = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2)],
/// );
/// assert!(can_accept_writes(&ok));
///
/// let degraded = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "healthy", true, 1 << 40, 2)],
/// );
/// assert!(!can_accept_writes(&degraded));
///
/// let no_pool = NodeManifest::from_pools(1, &[]);
/// assert!(!can_accept_writes(&no_pool));
///
/// // g8: a metadata-dead node reports healthy data pools but cannot
/// // persist object rows — it is NOT a write target.
/// let unavailable = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2)],
/// )
/// .with_node_unavailable(true);
/// assert!(!can_accept_writes(&unavailable));
/// ```
pub fn can_accept_writes(manifest: &NodeManifest) -> bool {
    !manifest.node_unavailable() && !is_write_degraded(manifest) && healthy_data_pools(manifest) > 0
}

/// Classifies a manifest for the **read** path (f5, ADR-0029 §D5).
///
/// - `Excluded`: `node_unavailable`, or no usable data pool at all
///   (zero data pools / every data pool `dead`).
/// - `Fallback`: no Healthy data pool remains, but at least one data
///   pool is still readable (Degraded, draining, or an unknown status).
/// - `Preferred`: at least one Healthy data pool.
///
/// Degraded is a preference, never an exclusion: reads still serve from
/// a Degraded pool, so a Degraded candidate is attempted after every
/// Preferred one instead of being dropped.
///
/// # Examples
///
/// ```
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
/// use oceanfs_node::routing_cache::manifest_read_class;
/// use oceanfs_server::CandidateClass;
///
/// let degraded = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "degraded", false, 1 << 40, 2)],
/// );
/// assert_eq!(manifest_read_class(&degraded), CandidateClass::Fallback);
///
/// let all_dead = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "dead", false, 0, 2)],
/// );
/// assert_eq!(manifest_read_class(&all_dead), CandidateClass::Excluded);
/// ```
pub fn manifest_read_class(manifest: &NodeManifest) -> CandidateClass {
    if manifest.node_unavailable() {
        return CandidateClass::Excluded;
    }
    let mut any_usable = false;
    let mut any_healthy = false;
    for pool in manifest.pools() {
        if pool.role() != "data" {
            continue;
        }
        match pool.status() {
            "healthy" => {
                any_healthy = true;
                any_usable = true;
            }
            // A Dead pool holds nothing to serve; Degraded/draining/unknown
            // statuses are still readable (ADR-0029 §D5 read preference).
            "dead" => {}
            _ => any_usable = true,
        }
    }
    if any_healthy {
        CandidateClass::Preferred
    } else if any_usable {
        CandidateClass::Fallback
    } else {
        CandidateClass::Excluded
    }
}

/// Classifies a manifest for the **write** path (f5, ADR-0029 §D5).
///
/// - `Excluded`: `node_unavailable`, `write_degraded` (Dead WAL — role
///   consequence), or no writable data pool (zero data pools / all
///   `dead`/`draining`).
/// - `Fallback`: no Healthy data pool remains, but at least one data
///   pool is Degraded (still accepts replication; over-committing is a
///   lesser problem than refusing to write).
/// - `Preferred`: at least one Healthy, non-draining data pool.
///
/// # Examples
///
/// ```
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
/// use oceanfs_node::routing_cache::manifest_write_class;
/// use oceanfs_server::CandidateClass;
///
/// let degraded = NodeManifest::from_pools(
///     1,
///     &[PoolManifest::new(0, "data", "degraded", false, 1 << 40, 2)],
/// );
/// assert_eq!(manifest_write_class(&degraded), CandidateClass::Fallback);
///
/// // A Dead WAL (`write_degraded`) is a hard write exclusion.
/// let wal_dead = NodeManifest::from_pools(
///     1,
///     &[
///         PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2),
///         PoolManifest::new(1, "wal", "healthy", true, 1 << 30, 1),
///     ],
/// );
/// assert_eq!(manifest_write_class(&wal_dead), CandidateClass::Excluded);
/// ```
pub fn manifest_write_class(manifest: &NodeManifest) -> CandidateClass {
    if manifest.node_unavailable() || is_write_degraded(manifest) {
        return CandidateClass::Excluded;
    }
    let mut any_writable = false;
    let mut any_healthy = false;
    for pool in manifest.pools() {
        if pool.role() != "data" {
            continue;
        }
        match pool.status() {
            // Draining pools take no new segment placements (ADR-0036 D6).
            "healthy" if !pool.write_degraded() => {
                any_healthy = true;
                any_writable = true;
            }
            "degraded" => any_writable = true,
            _ => {}
        }
    }
    if any_healthy {
        CandidateClass::Preferred
    } else if any_writable {
        CandidateClass::Fallback
    } else {
        CandidateClass::Excluded
    }
}

impl RoutingHint for ManifestCache {
    fn exclude_read_candidate(&self, node_id: &NodeId) -> bool {
        let excluded = match self.get(node_id) {
            Some(manifest) => manifest_read_class(&manifest) == CandidateClass::Excluded,
            // Unknown peer = no pool info: stay eligible; the
            // error-driven fallback is the guarantee (ADR-0029 §D5).
            None => false,
        };
        if excluded {
            self.read_skips.inc();
        }
        excluded
    }

    fn exclude_write_target(&self, node_id: &NodeId) -> bool {
        let excluded = match self.get(node_id) {
            Some(manifest) => manifest_write_class(&manifest) == CandidateClass::Excluded,
            // Unknown peer stays eligible; write failures become
            // hinted-handoff debt.
            None => false,
        };
        if excluded {
            self.write_skips.inc();
        }
        excluded
    }

    fn on_failover(&self) {
        self.failover_total.inc();
    }

    fn read_candidate_class(&self, node_id: &NodeId) -> CandidateClass {
        match self.get(node_id) {
            Some(manifest) => manifest_read_class(&manifest),
            // Unknown peers are attempted as preferred; the error path
            // decides (the manifest is only a hint).
            None => CandidateClass::Preferred,
        }
    }

    fn write_target_class(&self, node_id: &NodeId) -> CandidateClass {
        match self.get(node_id) {
            Some(manifest) => manifest_write_class(&manifest),
            None => CandidateClass::Preferred,
        }
    }

    fn on_degraded_fallback(&self, path: FallbackPath) {
        match path {
            FallbackPath::Read => self.degraded_fallbacks_read.inc(),
            FallbackPath::Write => self.degraded_fallbacks_write.inc(),
            // `FallbackPath` is non-exhaustive: unknown future paths are
            // ignored rather than counted against the wrong series.
            _ => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_membership::manifest::PoolManifest;

    use super::*;

    fn data_manifest(status: &str, write_degraded: bool, data_pools: usize) -> NodeManifest {
        let mut pools = Vec::with_capacity(data_pools + 1);
        for id in 0..data_pools as u32 {
            pools.push(PoolManifest::new(id, "data", status, write_degraded, 1 << 40, 1));
        }
        pools.push(PoolManifest::new(data_pools as u32, "wal", "healthy", false, 1 << 30, 1));
        NodeManifest::from_pools(1, &pools)
    }

    #[test]
    fn get_update_remove_basics() {
        let cache = ManifestCache::new();
        let id = NodeId::new("peer");
        assert!(cache.get(&id).is_none());

        cache.update(id.clone(), Arc::new(data_manifest("healthy", false, 1)));
        assert!(cache.get(&id).is_some());
        assert_eq!(cache.len(), 1);

        cache.remove(&id);
        assert!(cache.get(&id).is_none());
        assert!(cache.is_empty());
    }

    /// A version-bumped update replaces the entry wholesale — the new
    /// manifest wins; the map stays consistent.
    #[test]
    fn version_bumped_update_replaces_wholesale() {
        let cache = ManifestCache::new();
        let id = NodeId::new("peer");
        let old = data_manifest("healthy", false, 2);
        let newer = data_manifest("healthy", false, 1);
        cache.update(id.clone(), Arc::new(old));
        cache.update(id.clone(), Arc::new(newer.clone()));

        let seen = cache.get(&id).expect("entry present");
        assert_eq!(seen.incarnation(), newer.incarnation());
        assert_eq!(healthy_data_pools(&seen), 1, "the newer manifest replaced the old");
        assert_eq!(cache.len(), 1, "no ghost entries after replacement");
    }

    /// Stale-but-present beats absent: `get` returns the last-known
    /// manifest until a newer update or a remove.
    #[test]
    fn stale_but_present_returns_last_manifest() {
        let cache = ManifestCache::new();
        let id = NodeId::new("peer");
        let first = data_manifest("healthy", false, 1);
        cache.update(id.clone(), Arc::new(first.clone()));

        // No newer gossip has arrived — the last-known manifest is
        // still served (D5: stale-but-present beats absent).
        assert_eq!(cache.get(&id).unwrap().incarnation(), first.incarnation());
        assert_eq!(healthy_data_pools(cache.get(&id).unwrap().as_ref()), 1);
    }

    /// Cache miss metric: a get with no entry increments the counter.
    #[test]
    fn cache_miss_increments_metric() {
        let cache = ManifestCache::new();
        assert!(cache.get(&NodeId::new("unknown")).is_none());
        assert!(cache.get(&NodeId::new("unknown")).is_none());
        assert_eq!(cache.cache_misses.get(), 2);
    }

    /// Read filter: a candidate whose manifest reports zero Healthy
    /// data pools is excluded; ≥1 Healthy pool stays eligible; a cache
    /// miss (unknown) is NOT excluded (fallback only).
    #[test]
    fn read_filter_excludes_all_dead_keeps_healthy_and_unknown() {
        let cache = ManifestCache::new();
        let all_dead = NodeId::new("all-dead");
        let some_healthy = NodeId::new("some-healthy");
        let unknown = NodeId::new("unknown");

        cache.update(all_dead.clone(), Arc::new(data_manifest("dead", false, 2)));
        cache.update(some_healthy.clone(), Arc::new(data_manifest("healthy", false, 1)));

        // The trait-level filter is what the coordinators consult.
        assert!(
            cache.exclude_read_candidate(&all_dead),
            "zero Healthy data pools must exclude a read candidate"
        );
        assert!(
            !cache.exclude_read_candidate(&some_healthy),
            "≥1 Healthy data pool must stay eligible"
        );
        assert!(
            !cache.exclude_read_candidate(&unknown),
            "a cache miss must not exclude — the error path decides"
        );
    }

    /// Write filter: a `write_degraded` node and a no-Healthy-pools
    /// node are excluded; a healthy node and an unknown node stay
    /// eligible.
    #[test]
    fn write_filter_excludes_degraded_and_all_dead() {
        let cache = ManifestCache::new();
        let degraded = NodeId::new("degraded");
        let all_dead = NodeId::new("all-dead");
        let healthy = NodeId::new("healthy");
        let unknown = NodeId::new("unknown");

        cache.update(degraded.clone(), Arc::new(data_manifest("healthy", true, 2)));
        cache.update(all_dead.clone(), Arc::new(data_manifest("dead", false, 1)));
        cache.update(healthy.clone(), Arc::new(data_manifest("healthy", false, 1)));

        assert!(cache.exclude_write_target(&degraded), "write_degraded must be excluded");
        assert!(
            cache.exclude_write_target(&all_dead),
            "zero Healthy data pools must be excluded as a write target"
        );
        assert!(!cache.exclude_write_target(&healthy), "a healthy node stays a target");
        assert!(!cache.exclude_write_target(&unknown), "an unknown node stays a target");
    }

    /// g8: `node_unavailable` is a HARD routing exclusion — a node whose
    /// manifest sets the flag is excluded as BOTH a read candidate and a
    /// write target even when it reports healthy data pools (its local
    /// metadata index is gone; it cannot serve object reads or persist
    /// new rows).
    #[test]
    fn unavailable_flag_excludes_read_and_write_despite_healthy_data() {
        let cache = ManifestCache::new();
        let unavailable = NodeId::new("meta-dead");
        let available = NodeId::new("available");

        let mut manifest = data_manifest("healthy", false, 2);
        manifest = manifest.with_node_unavailable(true);
        cache.update(unavailable.clone(), Arc::new(manifest.clone()));
        cache.update(available.clone(), Arc::new(data_manifest("healthy", false, 2)));

        assert!(
            cache.exclude_read_candidate(&unavailable),
            "node_unavailable must exclude a read candidate"
        );
        assert!(
            cache.exclude_write_target(&unavailable),
            "node_unavailable must exclude a write target"
        );
        assert!(!can_accept_writes(&manifest), "unavailable cannot accept writes");

        // The flag is independent of per-pool health: clearing it makes
        // the SAME healthy-data manifest eligible again.
        assert!(
            !cache.exclude_read_candidate(&available),
            "healthy + available stays a read candidate"
        );
        assert!(
            !cache.exclude_write_target(&available),
            "healthy + available stays a write target"
        );
    }

    /// Failover metric: `on_failover` increments the failover counter.
    #[test]
    fn failover_increments_metric() {
        let cache = ManifestCache::new();
        cache.on_failover();
        cache.on_failover();
        assert_eq!(cache.failover_total.get(), 2);
    }

    /// g6: each manifest-driven skip increments the per-path
    /// `oceanfs_routing_manifest_skips_total{path}` counter — reads and
    /// writes counted separately, healthy/unknown candidates not counted.
    #[test]
    fn manifest_skips_count_per_path() {
        let cache = ManifestCache::new();
        let degraded = NodeId::new("degraded");
        let all_dead = NodeId::new("all-dead");
        let healthy = NodeId::new("healthy");

        cache.update(degraded.clone(), Arc::new(data_manifest("healthy", true, 2)));
        cache.update(all_dead.clone(), Arc::new(data_manifest("dead", false, 1)));
        cache.update(healthy.clone(), Arc::new(data_manifest("healthy", false, 1)));

        // A write skip (write_degraded target) and a read skip (zero
        // Healthy data pools) + one healthy (not counted).
        assert!(cache.exclude_write_target(&degraded), "write_degraded excluded");
        assert!(cache.exclude_read_candidate(&all_dead), "all-dead read candidate excluded");
        assert!(!cache.exclude_write_target(&healthy), "healthy stays a target");

        assert_eq!(cache.read_skips.get(), 1, "one read skip counted");
        assert_eq!(cache.write_skips.get(), 1, "one write skip counted");

        // Repeated skips accumulate.
        assert!(cache.exclude_write_target(&degraded), "still excluded");
        assert_eq!(cache.write_skips.get(), 2, "second write skip counted");
    }

    #[test]
    fn healthy_data_pools_counts_only_data_and_healthy() {
        // Degraded/Dead data pools and non-data roles do not count.
        let mixed = NodeManifest::from_pools(
            1,
            &[
                PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2),
                PoolManifest::new(1, "data", "degraded", false, 1 << 40, 2),
                PoolManifest::new(2, "data", "dead", false, 1 << 40, 2),
                PoolManifest::new(3, "wal", "healthy", false, 1 << 30, 1),
            ],
        );
        assert_eq!(healthy_data_pools(&mixed), 1);
    }

    #[test]
    fn is_write_degraded_flags_any_pool() {
        let flagged = NodeManifest::from_pools(
            1,
            &[
                PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2),
                PoolManifest::new(1, "wal", "healthy", true, 1 << 30, 1),
            ],
        );
        assert!(is_write_degraded(&flagged));

        let clean = data_manifest("healthy", false, 1);
        assert!(!is_write_degraded(&clean));
    }

    /// d1 (ADR-0036 D6): a `"draining"` data pool is not counted as a
    /// healthy data pool — a node whose data pools all drain reports zero
    /// healthy pools and is excluded as a write target through the
    /// existing manifest-derived gates.
    #[test]
    fn draining_data_pools_do_not_count_as_healthy() {
        let all_draining = data_manifest("draining", false, 2);
        assert_eq!(healthy_data_pools(&all_draining), 0, "draining is not healthy");
        assert!(
            !can_accept_writes(&all_draining),
            "a node whose data pools all drain cannot accept new writes"
        );

        // A mixed manifest counts only the healthy pool.
        let mixed = NodeManifest::from_pools(
            1,
            &[
                PoolManifest::new(0, "data", "draining", false, 1 << 40, 1),
                PoolManifest::new(1, "data", "healthy", false, 1 << 40, 1),
            ],
        );
        assert_eq!(healthy_data_pools(&mixed), 1);
    }

    /// f5: a Degraded data pool is a Fallback for BOTH paths — it is
    /// neither Preferred nor Excluded.
    #[test]
    fn degraded_data_pool_is_fallback_not_excluded() {
        let degraded = data_manifest("degraded", false, 1);
        assert_eq!(manifest_read_class(&degraded), CandidateClass::Fallback);
        assert_eq!(manifest_write_class(&degraded), CandidateClass::Fallback);

        let cache = ManifestCache::new();
        let id = NodeId::new("peer");
        cache.update(id.clone(), Arc::new(degraded));
        assert_eq!(cache.read_candidate_class(&id), CandidateClass::Fallback);
        assert_eq!(cache.write_target_class(&id), CandidateClass::Fallback);
        assert!(!cache.exclude_read_candidate(&id), "Degraded is not a hard read exclusion");
        assert!(!cache.exclude_write_target(&id), "Degraded is not a hard write exclusion");
    }

    /// f5: an all-Dead data pool set stays a hard exclusion for both
    /// paths (nothing to serve, nothing to write).
    #[test]
    fn all_dead_data_pools_are_excluded_for_read_and_write() {
        let all_dead = data_manifest("dead", false, 2);
        assert_eq!(manifest_read_class(&all_dead), CandidateClass::Excluded);
        assert_eq!(manifest_write_class(&all_dead), CandidateClass::Excluded);
    }

    /// f5: a node with no data pools at all is a hard exclusion; a
    /// Healthy pool makes it Preferred even if a sibling pool is Degraded.
    #[test]
    fn mixed_healthy_and_degraded_is_preferred() {
        let mixed = NodeManifest::from_pools(
            1,
            &[
                PoolManifest::new(0, "data", "degraded", false, 1 << 40, 2),
                PoolManifest::new(1, "data", "healthy", false, 1 << 40, 2),
            ],
        );
        assert_eq!(manifest_read_class(&mixed), CandidateClass::Preferred);
        assert_eq!(manifest_write_class(&mixed), CandidateClass::Preferred);

        let no_data = NodeManifest::from_pools(
            1,
            &[PoolManifest::new(0, "wal", "healthy", false, 1 << 30, 1)],
        );
        assert_eq!(manifest_read_class(&no_data), CandidateClass::Excluded);
        assert_eq!(manifest_write_class(&no_data), CandidateClass::Excluded);
    }

    /// f5: draining data pools still serve reads (fallback) but take no
    /// new writes (excluded) — ADR-0036 D6.
    #[test]
    fn draining_is_read_fallback_but_write_excluded() {
        let draining = data_manifest("draining", false, 1);
        assert_eq!(manifest_read_class(&draining), CandidateClass::Fallback);
        assert_eq!(manifest_write_class(&draining), CandidateClass::Excluded);
    }

    /// f5: the Degraded-fallback metric counts per path exactly once per
    /// `on_degraded_fallback` call.
    #[test]
    fn degraded_fallback_metric_counts_per_path() {
        let cache = ManifestCache::new();
        cache.on_degraded_fallback(FallbackPath::Read);
        cache.on_degraded_fallback(FallbackPath::Read);
        cache.on_degraded_fallback(FallbackPath::Write);
        assert_eq!(cache.degraded_fallbacks_read.get(), 2);
        assert_eq!(cache.degraded_fallbacks_write.get(), 1);
    }
}
