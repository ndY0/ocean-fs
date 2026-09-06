//! Manifest-aware peer/partition selection (ADR-0033).
//!
//! Concrete implementations of the `oceanfs-durability` injection seams
//! (`oceanfs_durability::peer_selection::{PeerSelector, PartitionPlanner}`):
//!
//! - [`ManifestPeerSelector`] — filters a segment's `storage_locations`
//!   holders down to the nodes eligible to act as a Merkle-comparison
//!   peer (anti-entropy, f2).
//! - [`ManifestPartitionPlanner`] — assigns each segment to one holder
//!   that will scrub it (distributed scrub, f3).
//!
//! Both filter holders against the membership view (state `Alive |
//! Suspect`) and the gossiped [`NodeManifest`] (at least one Healthy data
//! pool and not `write_degraded` — the g6 predicate
//! [`can_accept_writes`]). A
//! holder whose manifest is missing/stale stays eligible: manifests are a
//! hint, the I/O error path is the truth (ADR-0029 §D5).
//!
//! This is the same ADR-0005 "trait in the consuming crate, impl in the
//! composition root" pattern as
//! [`ManifestRepairTargetSelector`](crate::repair::ManifestRepairTargetSelector)
//! (ADR-0030).

use std::{collections::HashMap, sync::Arc};

use oceanfs_core::{NodeId, NodeState, SegmentId, SegmentMetadata};
use oceanfs_durability::{
    peer_selection::{PartitionPlanner, PeerSelector},
    SegmentPartition,
};
use oceanfs_membership::{manifest::NodeManifest, Membership};

use crate::routing_cache::can_accept_writes;

/// Per-node membership snapshot (state + manifest) for one planning call.
///
/// Built once per `eligible_holders` / `plan_partitions` invocation via
/// [`Membership::nodes_full`] so a cycle observes one consistent view and
/// does not take a read lock per holder.
type MembershipView = HashMap<NodeId, (NodeState, Option<Arc<NodeManifest>>)>;

/// Snapshot the membership view (ADR-0028: state, incarnation, both
/// addresses, attribution, manifest — we keep only state + manifest).
fn membership_view(membership: &Membership) -> MembershipView {
    membership
        .nodes_full()
        .into_iter()
        .map(|(id, state, _incarnation, _addr, _membership_addr, _version, _origin, manifest)| {
            (id, (state, manifest))
        })
        .collect()
}

/// Whether a membership entry is an eligible comparison/scrub holder
/// (ADR-0033 D1, ADR-0029 §D5): state `Alive | Suspect` and — when a
/// manifest is known — at least one Healthy data pool and not
/// `write_degraded`. A missing/stale manifest stays eligible.
fn entry_is_eligible(state: NodeState, manifest: Option<&NodeManifest>) -> bool {
    if !matches!(state, NodeState::Alive | NodeState::Suspect) {
        return false;
    }
    match manifest {
        Some(manifest) => can_accept_writes(manifest),
        // No manifest yet = no evidence of unhealth; stay eligible and
        // let the I/O error path be the truth (ADR-0029 §D5).
        None => true,
    }
}

/// [`PeerSelector`] over the membership + manifest view (ADR-0033 D3).
///
/// Filters a segment's `storage_locations` holders to the nodes eligible
/// as Merkle-comparison peers: not this node, membership
/// `Alive | Suspect`, and — when a manifest is known — at least one
/// Healthy data pool. The result is always a subset of `holders`; no
/// peer is ever invented.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use oceanfs_core::{
///     GossipConfig, Incarnation, NodeId, NodeState, RingConfig, SegmentId,
/// };
/// use oceanfs_durability::peer_selection::PeerSelector;
/// use oceanfs_membership::Membership;
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
/// use oceanfs_node::peer_selection::ManifestPeerSelector;
/// use oceanfs_routing::{Ring, RingCache};
///
/// let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
/// let membership = Arc::new(Membership::new(
///     NodeId::new("self-node"),
///     "127.0.0.1:9100".parse().unwrap(),
///     "127.0.0.1:9101".parse().unwrap(),
///     GossipConfig::default(),
///     ring,
/// ));
/// membership.upsert_node(
///     NodeId::new("healthy"),
///     NodeState::Alive,
///     Incarnation::new(1),
///     Some("127.0.0.1:9200".parse().unwrap()),
/// );
/// membership.upsert_node(
///     NodeId::new("dead"),
///     NodeState::Dead,
///     Incarnation::new(1),
///     Some("127.0.0.1:9201".parse().unwrap()),
/// );
/// membership.set_peer_manifest(
///     NodeId::new("healthy"),
///     NodeManifest::from_pools(
///         1,
///         &[PoolManifest::new(0, "data", "healthy", false, 1 << 40, 1)],
///     ),
/// );
///
/// let selector = ManifestPeerSelector::new(membership, NodeId::new("self-node"));
/// let eligible = selector.eligible_holders(
///     &SegmentId::new(),
///     &[NodeId::new("healthy"), NodeId::new("dead"), NodeId::new("self-node")],
/// );
/// assert_eq!(eligible, vec![NodeId::new("healthy")]);
/// ```
pub struct ManifestPeerSelector {
    membership: Arc<Membership>,
    self_id: NodeId,
}

impl ManifestPeerSelector {
    /// Creates the selector over the node's membership/manifest view.
    pub fn new(membership: Arc<Membership>, self_id: NodeId) -> Self {
        Self { membership, self_id }
    }
}

impl PeerSelector for ManifestPeerSelector {
    fn eligible_holders(&self, _segment_id: &SegmentId, holders: &[NodeId]) -> Vec<NodeId> {
        let view = membership_view(&self.membership);
        holders
            .iter()
            .filter(|id| {
                if **id == self.self_id {
                    return false;
                }
                match view.get(*id) {
                    Some((state, manifest)) => entry_is_eligible(*state, manifest.as_deref()),
                    // Not in the membership view: not alive from this
                    // node's perspective — cannot be a comparison peer.
                    None => false,
                }
            })
            .cloned()
            .collect()
    }
}

/// [`PartitionPlanner`] over the membership + manifest view (ADR-0033
/// D3, scrub half).
///
/// Assigns each segment of the coordinator's inventory to exactly one of
/// its eligible holders — never to a non-holder. Self is always an
/// eligible scrubber of a segment it holds (a node can always scan its
/// own local copy). A segment with no eligible remote holder and no self
/// membership entry (a local-only segment) lands in the self partition.
/// Assignment is deterministic: the inventory is sorted by segment id and
/// each segment is spread across its (sorted) eligible holders.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use oceanfs_core::{
///     GossipConfig, Incarnation, NodeId, NodeState, RingConfig, SegmentId,
///     SegmentMetadata, SizeTier,
/// };
/// use oceanfs_durability::peer_selection::PartitionPlanner;
/// use oceanfs_membership::Membership;
/// use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
/// use oceanfs_node::peer_selection::ManifestPartitionPlanner;
/// use oceanfs_routing::{Ring, RingCache};
///
/// let ring = Arc::new(RingCache::new(Ring::new(RingConfig::default())));
/// let membership = Arc::new(Membership::new(
///     NodeId::new("self-node"),
///     "127.0.0.1:9100".parse().unwrap(),
///     "127.0.0.1:9101".parse().unwrap(),
///     GossipConfig::default(),
///     ring,
/// ));
/// for (i, id) in ["holder-a", "holder-b"].iter().enumerate() {
///     membership.upsert_node(
///         NodeId::new(*id),
///         NodeState::Alive,
///         Incarnation::new(1),
///         Some(format!("127.0.0.1:{}", 9300 + i as u16).parse().unwrap()),
///     );
///     membership.set_peer_manifest(
///         NodeId::new(*id),
///         NodeManifest::from_pools(
///             1,
///             &[PoolManifest::new(0, "data", "healthy", false, 1 << 40, 1)],
///         ),
///     );
/// }
///
/// let planner = ManifestPartitionPlanner::new(membership, NodeId::new("self-node"));
/// let segments = (0..4)
///     .map(|i| SegmentMetadata {
///         pool_id: 0,
///         total_bytes: 0,
///         segment_id: SegmentId::new(),
///         ec_k: 4,
///         ec_m: 2,
///         size_tier: SizeTier::Standard,
///         merkle_root: None,
///         storage_locations: smallvec::smallvec![
///             NodeId::new("self-node"),
///             NodeId::new("holder-a"),
///             NodeId::new("holder-b"),
///         ],
///         sealed_at: Some(1_700_000_000_000 + i as i64),
///     })
///     .collect::<Vec<_>>();
/// let partitions = planner.plan_partitions(&segments, &NodeId::new("self-node"));
/// // Every planned partition names a holder of every segment it carries.
/// for partition in &partitions {
///     for seg in segments.iter().filter(|s| {
///         partition.segment_ids.contains(&s.segment_id)
///     }) {
///         assert!(seg.storage_locations.iter().any(|h| h == &partition.node_id));
///     }
/// }
/// ```
pub struct ManifestPartitionPlanner {
    membership: Arc<Membership>,
    self_id: NodeId,
}

impl ManifestPartitionPlanner {
    /// Creates the planner over the node's membership/manifest view.
    pub fn new(membership: Arc<Membership>, self_id: NodeId) -> Self {
        Self { membership, self_id }
    }
}

impl PartitionPlanner for ManifestPartitionPlanner {
    fn plan_partitions(
        &self,
        segments: &[SegmentMetadata],
        self_id: &NodeId,
    ) -> Vec<SegmentPartition> {
        debug_assert_eq!(
            self_id, &self.self_id,
            "planner constructed for {} but asked to plan for {}",
            self.self_id, self_id
        );
        let view = membership_view(&self.membership);
        // Sort by segment id so identical inventories plan identically.
        let mut sorted: Vec<&SegmentMetadata> = segments.iter().collect();
        sorted.sort_by_key(|seg| seg.segment_id);

        let mut self_segment_ids: Vec<SegmentId> = Vec::new();
        let mut remote: HashMap<NodeId, Vec<SegmentId>> = HashMap::new();

        for (index, seg) in sorted.iter().enumerate() {
            // Self is always an eligible scrubber of its own copy; remote
            // holders are eligible when alive + manifest-healthy.
            let mut eligible: Vec<NodeId> = seg
                .storage_locations
                .iter()
                .filter(|holder| {
                    **holder == self.self_id
                        || match view.get(*holder) {
                            Some((state, manifest)) => {
                                entry_is_eligible(*state, manifest.as_deref())
                            }
                            None => false,
                        }
                })
                .cloned()
                .collect();
            eligible.sort();

            match eligible.as_slice() {
                [] => {
                    // Local-only segment (no eligible remote holder and
                    // self is not recorded as a holder): keep it local.
                    self_segment_ids.push(seg.segment_id);
                }
                chosen => {
                    // Deterministic spread across eligible holders so one
                    // cycle does not pile every segment on one node.
                    let target = &chosen[index % chosen.len()];
                    if *target == self.self_id {
                        self_segment_ids.push(seg.segment_id);
                    } else {
                        remote.entry(target.clone()).or_default().push(seg.segment_id);
                    }
                }
            }
        }

        let mut partitions: Vec<SegmentPartition> = remote
            .into_iter()
            .map(|(node_id, segment_ids)| SegmentPartition { node_id, segment_ids })
            .collect();
        // Deterministic output order for the caller (tests, future
        // dispatch): remote partitions sorted by node id, then self.
        partitions.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        if !self_segment_ids.is_empty() {
            partitions.push(SegmentPartition {
                node_id: self.self_id.clone(),
                segment_ids: self_segment_ids,
            });
        }
        partitions
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::{GossipConfig, Incarnation, RingConfig};
    use oceanfs_membership::manifest::{NodeManifest, PoolManifest};
    use oceanfs_routing::{Ring, RingCache};

    use super::*;

    fn make_membership(self_id: &str) -> Arc<Membership> {
        let ring = Arc::new(RingCache::new(Ring::new(RingConfig {
            vnodes_per_node: 8,
            replication_factor: 3,
        })));
        Arc::new(Membership::new(
            NodeId::new(self_id),
            "127.0.0.1:9100".parse().unwrap(),
            "127.0.0.1:9101".parse().unwrap(),
            GossipConfig::default(),
            ring,
        ))
    }

    fn upsert(membership: &Membership, id: &str, state: NodeState) {
        membership.upsert_node(
            NodeId::new(id),
            state,
            Incarnation::new(1),
            Some("127.0.0.1:9200".parse().unwrap()),
        );
    }

    fn healthy_manifest() -> NodeManifest {
        NodeManifest::from_pools(1, &[PoolManifest::new(0, "data", "healthy", false, 1 << 40, 1)])
    }

    fn degraded_manifest() -> NodeManifest {
        NodeManifest::from_pools(1, &[PoolManifest::new(0, "data", "healthy", true, 1 << 40, 1)])
    }

    fn no_pool_manifest() -> NodeManifest {
        NodeManifest::from_pools(1, &[])
    }

    fn segment(holders: &[&str]) -> SegmentMetadata {
        SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: oceanfs_core::SizeTier::Standard,
            merkle_root: None,
            storage_locations: holders.iter().map(|h| NodeId::new(*h)).collect(),
            sealed_at: Some(1_700_000_000_000),
        }
    }

    // -------------------------------------------------------------------
    // ManifestPeerSelector
    // -------------------------------------------------------------------

    /// f1 DoD integration case: two holders, one healthy and one Dead —
    /// only the healthy holder is returned.
    #[test]
    fn peer_selector_returns_only_the_eligible_holder() {
        let membership = make_membership("n1");
        upsert(&membership, "n2", NodeState::Alive);
        upsert(&membership, "n3", NodeState::Dead);
        membership.set_peer_manifest(NodeId::new("n2"), healthy_manifest());
        membership.set_peer_manifest(NodeId::new("n3"), healthy_manifest());

        let selector = ManifestPeerSelector::new(membership, NodeId::new("n1"));
        let eligible =
            selector.eligible_holders(&SegmentId::new(), &[NodeId::new("n2"), NodeId::new("n3")]);
        assert_eq!(eligible, vec![NodeId::new("n2")]);
    }

    /// Self is never a comparison peer.
    #[test]
    fn peer_selector_excludes_self() {
        let membership = make_membership("n1");
        upsert(&membership, "n1", NodeState::Alive);
        membership.set_peer_manifest(NodeId::new("n1"), healthy_manifest());

        let selector = ManifestPeerSelector::new(membership, NodeId::new("n1"));
        let eligible = selector.eligible_holders(&SegmentId::new(), &[NodeId::new("n1")]);
        assert!(eligible.is_empty());
    }

    /// A manifest reporting zero Healthy data pools excludes the holder.
    #[test]
    fn peer_selector_excludes_zero_healthy_data_pools() {
        let membership = make_membership("n1");
        upsert(&membership, "n2", NodeState::Alive);
        membership.set_peer_manifest(NodeId::new("n2"), no_pool_manifest());

        let selector = ManifestPeerSelector::new(membership, NodeId::new("n1"));
        let eligible = selector.eligible_holders(&SegmentId::new(), &[NodeId::new("n2")]);
        assert!(eligible.is_empty());
    }

    /// A `write_degraded` holder is excluded.
    #[test]
    fn peer_selector_excludes_write_degraded() {
        let membership = make_membership("n1");
        upsert(&membership, "n2", NodeState::Alive);
        membership.set_peer_manifest(NodeId::new("n2"), degraded_manifest());

        let selector = ManifestPeerSelector::new(membership, NodeId::new("n1"));
        let eligible = selector.eligible_holders(&SegmentId::new(), &[NodeId::new("n2")]);
        assert!(eligible.is_empty());
    }

    /// A holder whose manifest is missing/stale stays eligible
    /// (ADR-0029 §D5 — the I/O error path is the truth).
    #[test]
    fn peer_selector_keeps_missing_manifest_eligible() {
        let membership = make_membership("n1");
        upsert(&membership, "n2", NodeState::Alive);

        let selector = ManifestPeerSelector::new(membership, NodeId::new("n1"));
        let eligible = selector.eligible_holders(&SegmentId::new(), &[NodeId::new("n2")]);
        assert_eq!(eligible, vec![NodeId::new("n2")]);
    }

    /// Suspect holders remain eligible comparison peers (still servable).
    #[test]
    fn peer_selector_keeps_suspect_eligible() {
        let membership = make_membership("n1");
        upsert(&membership, "n2", NodeState::Suspect);

        let selector = ManifestPeerSelector::new(membership, NodeId::new("n1"));
        let eligible = selector.eligible_holders(&SegmentId::new(), &[NodeId::new("n2")]);
        assert_eq!(eligible, vec![NodeId::new("n2")]);
    }

    /// A holder absent from the membership view is not a peer.
    #[test]
    fn peer_selector_excludes_unknown_holders() {
        let membership = make_membership("n1");
        let selector = ManifestPeerSelector::new(membership, NodeId::new("n1"));
        let eligible = selector.eligible_holders(&SegmentId::new(), &[NodeId::new("unknown")]);
        assert!(eligible.is_empty());
    }

    // -------------------------------------------------------------------
    // ManifestPartitionPlanner
    // -------------------------------------------------------------------

    /// Every returned partition's segment_ids are all ∈ that node's
    /// storage_locations; no partition lists a non-holder.
    #[test]
    fn planner_never_assigns_a_non_holder() {
        let membership = make_membership("self-node");
        upsert(&membership, "holder-a", NodeState::Alive);
        upsert(&membership, "holder-b", NodeState::Alive);
        membership.set_peer_manifest(NodeId::new("holder-a"), healthy_manifest());
        membership.set_peer_manifest(NodeId::new("holder-b"), healthy_manifest());

        let planner = ManifestPartitionPlanner::new(membership, NodeId::new("self-node"));
        let segments = vec![
            segment(&["holder-a"]),
            segment(&["holder-b"]),
            segment(&["holder-a", "holder-b"]),
            segment(&["self-node", "holder-a"]),
        ];
        let partitions = planner.plan_partitions(&segments, &NodeId::new("self-node"));
        assert!(!partitions.is_empty());

        let mut all_segment_ids: Vec<SegmentId> = segments.iter().map(|s| s.segment_id).collect();
        for partition in &partitions {
            for seg_id in &partition.segment_ids {
                let seg = segments
                    .iter()
                    .find(|s| &s.segment_id == seg_id)
                    .expect("partition references a planned segment");
                assert!(
                    seg.storage_locations.iter().any(|h| h == &partition.node_id)
                        || (partition.node_id.as_str() == "self-node"
                            && seg.storage_locations.is_empty()),
                    "partition node {} does not hold segment {}",
                    partition.node_id,
                    seg_id
                );
                // remove from the seen set to detect duplicates
                let idx = all_segment_ids.iter().position(|s| s == seg_id).unwrap();
                all_segment_ids.swap_remove(idx);
            }
        }
        assert!(all_segment_ids.is_empty(), "every segment appears in exactly one partition");
    }

    /// A local-only segment (storage_locations == {self}) lands in the
    /// self partition.
    #[test]
    fn planner_local_only_lands_in_self_partition() {
        let membership = make_membership("self-node");
        upsert(&membership, "holder-a", NodeState::Alive);
        membership.set_peer_manifest(NodeId::new("holder-a"), healthy_manifest());

        let planner = ManifestPartitionPlanner::new(membership, NodeId::new("self-node"));
        let segments =
            vec![segment(&["self-node", "holder-a"]), segment(&["self-node"]), segment(&[])];
        let partitions = planner.plan_partitions(&segments, &NodeId::new("self-node"));

        let self_partition = partitions
            .iter()
            .find(|p| p.node_id.as_str() == "self-node")
            .expect("a self partition must exist for local-only segments");
        let local_only = &segments[1];
        let empty_locations = &segments[2];
        assert!(self_partition.segment_ids.contains(&local_only.segment_id));
        assert!(self_partition.segment_ids.contains(&empty_locations.segment_id));
    }

    /// Planning is deterministic: identical inputs produce identical
    /// partitions.
    #[test]
    fn planner_is_deterministic() {
        let membership = make_membership("self-node");
        upsert(&membership, "holder-a", NodeState::Alive);
        upsert(&membership, "holder-b", NodeState::Alive);
        membership.set_peer_manifest(NodeId::new("holder-a"), healthy_manifest());
        membership.set_peer_manifest(NodeId::new("holder-b"), healthy_manifest());

        let planner = ManifestPartitionPlanner::new(membership, NodeId::new("self-node"));
        let segments = vec![
            segment(&["self-node", "holder-a", "holder-b"]),
            segment(&["self-node", "holder-a", "holder-b"]),
            segment(&["self-node", "holder-a", "holder-b"]),
            segment(&["self-node", "holder-a", "holder-b"]),
            segment(&["self-node"]),
        ];
        let first = planner.plan_partitions(&segments, &NodeId::new("self-node"));
        let second = planner.plan_partitions(&segments, &NodeId::new("self-node"));
        assert_eq!(first.len(), second.len());
        for (a, b) in first.iter().zip(second.iter()) {
            assert_eq!(a.node_id, b.node_id);
            assert_eq!(a.segment_ids, b.segment_ids);
        }
    }
    /// A dead holder is never a scrub target; its segments stay local or
    /// go to a live holder.
    #[test]
    fn planner_excludes_dead_holders() {
        let membership = make_membership("self-node");
        upsert(&membership, "alive", NodeState::Alive);
        upsert(&membership, "dead", NodeState::Dead);
        membership.set_peer_manifest(NodeId::new("alive"), healthy_manifest());
        membership.set_peer_manifest(NodeId::new("dead"), healthy_manifest());

        let planner = ManifestPartitionPlanner::new(membership, NodeId::new("self-node"));
        let segments = vec![segment(&["dead"]), segment(&["self-node", "dead"])];
        let partitions = planner.plan_partitions(&segments, &NodeId::new("self-node"));
        assert!(
            partitions.iter().all(|p| p.node_id.as_str() != "dead"),
            "a dead holder must never appear as a partition node"
        );
        // The dead-only segment has no live holder → self (local-only).
        let self_partition = partitions
            .iter()
            .find(|p| p.node_id.as_str() == "self-node")
            .expect("self partition exists for the local-only segment");
        assert!(self_partition.segment_ids.contains(&segments[0].segment_id));
    }

    // -------------------------------------------------------------------
    // f3 scrub partition semantics (ADR-0033 D1, scrub half)
    // -------------------------------------------------------------------

    /// f3 planner correctness: an inventory whose storage_locations span
    /// {self, A, B} plus local-only segments plans partitions where every
    /// segment in a peer's partition lists that peer, the local-only
    /// segment lands in the self partition, no segment appears in more
    /// than one partition, and no partition contains a non-holder.
    #[test]
    fn planner_scrub_partition_distribution_is_holder_sound() {
        let membership = make_membership("self-node");
        upsert(&membership, "holder-a", NodeState::Alive);
        upsert(&membership, "holder-b", NodeState::Alive);
        membership.set_peer_manifest(NodeId::new("holder-a"), healthy_manifest());
        membership.set_peer_manifest(NodeId::new("holder-b"), healthy_manifest());

        let planner = ManifestPartitionPlanner::new(membership, NodeId::new("self-node"));
        let segments = vec![
            segment(&["holder-a"]),
            segment(&["holder-b"]),
            segment(&["self-node", "holder-a", "holder-b"]),
            segment(&["self-node"]),
            segment(&[]),
        ];
        let partitions = planner.plan_partitions(&segments, &NodeId::new("self-node"));

        // Every planned segment appears in exactly one partition.
        let mut seen: Vec<SegmentId> = segments.iter().map(|s| s.segment_id).collect();
        for partition in &partitions {
            for seg_id in &partition.segment_ids {
                let seg = segments
                    .iter()
                    .find(|s| &s.segment_id == seg_id)
                    .expect("partition references a planned segment");
                let holds = seg.storage_locations.iter().any(|h| h == &partition.node_id);
                let local_only_in_self = partition.node_id.as_str() == "self-node"
                    && (seg.storage_locations.is_empty()
                        || (seg.storage_locations.len() == 1
                            && seg.storage_locations[0].as_str() == "self-node"));
                assert!(
                    holds || local_only_in_self,
                    "partition node {} does not hold segment {}",
                    partition.node_id,
                    seg_id
                );
                let pos = seen.iter().position(|s| s == seg_id).expect("duplicate plan");
                seen.swap_remove(pos);
            }
        }
        assert!(seen.is_empty(), "every segment appears in exactly one partition");

        // Local-only segments (self-only holder, or no locations at all)
        // land in the self partition.
        let self_partition = partitions
            .iter()
            .find(|p| p.node_id.as_str() == "self-node")
            .expect("a self partition exists");
        assert!(self_partition.segment_ids.contains(&segments[3].segment_id));
        assert!(self_partition.segment_ids.contains(&segments[4].segment_id));
    }
}
