//! The node's storage-pool manifest (ADR-0029 D2) — wire types.
//!
//! The manifest is the node's declaration of its storage pools: one
//! [`PoolManifest`] per configured pool (id, role, status, write-degraded
//! flag, free capacity, weight), wrapped in a [`NodeManifest`] that ties
//! the declaration to the SWIM incarnation (a restart re-declares).
//!
//! Membership treats the manifest as an **opaque attached attribute**:
//! the authority-class merge (ADR-0028 D3) never interprets its
//! contents. A pool change bumps the owning entry's `version` (the
//! per-(node, origin) clock), which is all the dissemination layer needs
//! to forward the new manifest — the incarnation never changes for a
//! pool change, only for a restart.
//!
//! Role and status are carried as strings for forward compatibility
//! (the feature doc pins the wire fields as strings); the f2 enums'
//! constants (`PoolRole::as_str`, the `Healthy` status) are encoded here
//! by the node when it builds the manifest from its `PoolRegistry`.

use oceanfs_core::proto;

/// One storage pool's view inside a node's manifest (ADR-0029 D2).
///
/// # Examples
///
/// ```
/// use oceanfs_membership::manifest::PoolManifest;
///
/// let pool = PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2);
/// assert_eq!(pool.id(), 0);
/// assert_eq!(pool.role(), "data");
/// assert_eq!(pool.status(), "healthy");
/// assert!(!pool.write_degraded());
/// assert_eq!(pool.capacity_free_bytes(), 1 << 40);
/// assert_eq!(pool.weight(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PoolManifest {
    /// Stable pool id — the topology config order (f2), 0-based.
    id: u32,
    /// Pool purpose constant: `"data" | "wal" | "metadata" | "hints"`.
    role: String,
    /// Pool health constant: `"healthy" | "degraded" | "dead"` (Phase A
    /// always `"healthy"`; transitions land with Phase B's health
    /// monitor).
    status: String,
    /// Role-consequence flag (ADR-0029 D3); Phase A: always `false`.
    write_degraded: bool,
    /// Free bytes on the pool root at manifest build time — the
    /// capacity-aware placement signal f7's routing cache reads.
    capacity_free_bytes: u64,
    /// Placement weight (explicit config value or capacity-derived).
    weight: u32,
}

impl PoolManifest {
    /// Creates a pool manifest from fully-resolved values.
    ///
    /// The role and status strings use the f2 constants (`"data"`,
    /// `"healthy"`, …) so the wire format never forces a redesign.
    pub fn new(
        id: u32,
        role: impl Into<String>,
        status: impl Into<String>,
        write_degraded: bool,
        capacity_free_bytes: u64,
        weight: u32,
    ) -> Self {
        Self {
            id,
            role: role.into(),
            status: status.into(),
            write_degraded,
            capacity_free_bytes,
            weight,
        }
    }

    /// The stable pool id (topology config order).
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The pool purpose constant (`"data" | "wal" | "metadata" | "hints"`).
    pub fn role(&self) -> &str {
        &self.role
    }

    /// The pool health constant (`"healthy" | "degraded" | "dead"`).
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Whether the pool's role consequence flags writes as degraded.
    pub fn write_degraded(&self) -> bool {
        self.write_degraded
    }

    /// Free bytes on the pool root at manifest build time.
    pub fn capacity_free_bytes(&self) -> u64 {
        self.capacity_free_bytes
    }

    /// The placement weight.
    pub fn weight(&self) -> u32 {
        self.weight
    }

    /// Encodes into the protobuf wire form.
    pub(crate) fn to_proto(&self) -> proto::membership::PoolManifest {
        proto::membership::PoolManifest {
            id: self.id,
            role: self.role.clone(),
            status: self.status.clone(),
            write_degraded: self.write_degraded,
            capacity_free_bytes: self.capacity_free_bytes,
            weight: self.weight,
        }
    }

    /// Decodes from the protobuf wire form.
    pub(crate) fn from_proto(value: &proto::membership::PoolManifest) -> Self {
        Self {
            id: value.id,
            role: value.role.clone(),
            status: value.status.clone(),
            write_degraded: value.write_degraded,
            capacity_free_bytes: value.capacity_free_bytes,
            weight: value.weight,
        }
    }
}

/// The node's storage-pool manifest (ADR-0029 D2).
///
/// One [`PoolManifest`] per configured pool. Versioned by the owning
/// membership entry: a pool change bumps the entry's `version`
/// (ADR-0028 D3), never the `incarnation` — the incarnation changes only
/// on restart, which re-declares the manifest wholesale.
///
/// # Examples
///
/// ```
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
///
/// let pools = vec![PoolManifest::new(0, "data", "healthy", false, 1 << 40, 2)];
/// let manifest = NodeManifest::from_pools(7, &pools);
/// assert_eq!(manifest.incarnation(), 7);
/// assert_eq!(manifest.pools().len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeManifest {
    /// The SWIM incarnation this manifest was declared with — a restart
    /// re-declares with the bumped incarnation (ADR-0022 D1).
    incarnation: u64,
    /// One entry per configured pool, in topology config order.
    pools: Vec<PoolManifest>,
}

impl NodeManifest {
    /// Builds a manifest from the node's pool list (perf rule 1.3: the
    /// pool vector is pre-sized — the count is known up front).
    ///
    /// `incarnation` is the announcement incarnation the node joined
    /// with (spec §13.1, ADR-0022 D1): the same value that rides the
    /// membership entry, so peers can tie the manifest to the restart
    /// it was declared with.
    pub fn from_pools(incarnation: u64, pools: &[PoolManifest]) -> Self {
        let mut owned = Vec::with_capacity(pools.len());
        owned.extend_from_slice(pools);
        Self { incarnation, pools: owned }
    }

    /// The SWIM incarnation this manifest was declared with.
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// The node's pools, in topology config order.
    pub fn pools(&self) -> &[PoolManifest] {
        &self.pools
    }

    /// Encodes into the protobuf wire form.
    pub(crate) fn to_proto(&self) -> proto::membership::NodeManifest {
        proto::membership::NodeManifest {
            incarnation: self.incarnation,
            pools: self.pools.iter().map(PoolManifest::to_proto).collect(),
        }
    }

    /// Decodes from the protobuf wire form.
    pub(crate) fn from_proto(value: &proto::membership::NodeManifest) -> Self {
        Self {
            incarnation: value.incarnation,
            pools: value.pools.iter().map(PoolManifest::from_proto).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_manifest_round_trips_through_proto() {
        let pool = PoolManifest::new(3, "data", "healthy", false, 42 << 20, 4);
        let decoded = PoolManifest::from_proto(&pool.to_proto());
        assert_eq!(decoded, pool);
    }

    #[test]
    fn node_manifest_round_trips_through_proto() {
        let pools = vec![
            PoolManifest::new(0, "data", "healthy", false, 1 << 30, 2),
            PoolManifest::new(1, "wal", "healthy", false, 1 << 20, 1),
            PoolManifest::new(2, "metadata", "healthy", false, 1 << 20, 1),
            PoolManifest::new(3, "hints", "healthy", false, 1 << 20, 1),
        ];
        let manifest = NodeManifest::from_pools(9, &pools);
        let decoded = NodeManifest::from_proto(&manifest.to_proto());
        assert_eq!(decoded, manifest);
        assert_eq!(decoded.pools().len(), 4);
    }

    #[test]
    fn node_manifest_from_pools_pre_sizes_and_orders_pools() {
        let pools = vec![
            PoolManifest::new(1, "wal", "healthy", false, 0, 1),
            PoolManifest::new(0, "data", "healthy", false, 0, 3),
        ];
        let manifest = NodeManifest::from_pools(2, &pools);
        // Config order is preserved: the manifest mirrors the registry.
        assert_eq!(manifest.pools()[0].id(), 1);
        assert_eq!(manifest.pools()[1].id(), 0);
    }

    #[test]
    fn empty_pools_manifest_is_valid() {
        let manifest = NodeManifest::from_pools(1, &[]);
        assert_eq!(manifest.incarnation(), 1);
        assert!(manifest.pools().is_empty());
    }
}
