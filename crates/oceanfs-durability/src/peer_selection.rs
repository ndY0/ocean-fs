//! Holder-aware peer/partition selection seams (ADR-0033 D3).
//!
//! Anti-entropy and scrub both consume a segment's *replica set*
//! (`SegmentMetadata::storage_locations`) as the entry point of a cycle,
//! never "all alive nodes". The two traits here are that seam: they say
//! *which of a segment's holders* may act as a Merkle-comparison peer
//! ([`PeerSelector`]) and *which holder scrubs each segment*
//! ([`PartitionPlanner`]).
//!
//! Per ADR-0005 / ADR-0009 the traits live in the **consuming** crate
//! (`oceanfs-durability`) and the concrete implementations live in
//! `oceanfs-node` (which owns membership + the gossiped `NodeManifest`
//! filters). This crate never interprets manifests; it only defines the
//! contract the node layer implements.
//!
//! ## Shape precedent
//!
//! Mirrors `crate::repair::RepairTargetSelector` (ADR-0030) — injected at
//! the composition root, consumed by the durability worker.

use oceanfs_core::{NodeId, SegmentId, SegmentMetadata};

use crate::scrub::SegmentPartition;

/// Selects which of a segment's `storage_locations` holders may act as a
/// Merkle-comparison peer (ADR-0033 D1/D3).
///
/// Injected from the node layer. The durable crate never interprets
/// manifests; the node implementation filters holders against membership
/// state + the gossiped `NodeManifest` (g6 predicate). A holder whose
/// manifest is missing/stale stays eligible — the I/O error path is the
/// truth (ADR-0029 §D5).
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, SegmentId};
/// use oceanfs_durability::peer_selection::PeerSelector;
///
/// /// Test selector: excludes the holder nicknamed "dead".
/// struct HealthyHoldersOnly;
///
/// impl PeerSelector for HealthyHoldersOnly {
///     fn eligible_holders(&self, _segment_id: &SegmentId, holders: &[NodeId]) -> Vec<NodeId> {
///         holders
///             .iter()
///             .filter(|id| id.as_str() != "dead")
///             .cloned()
///             .collect()
///     }
/// }
///
/// let selector = HealthyHoldersOnly;
/// let holders = [NodeId::new("n1"), NodeId::new("dead"), NodeId::new("n2")];
/// let eligible = selector.eligible_holders(&SegmentId::new(), &holders);
/// assert_eq!(eligible, vec![NodeId::new("n1"), NodeId::new("n2")]);
/// ```
pub trait PeerSelector: Send + Sync {
    /// Returns the subset of `holders` (a segment's `storage_locations`)
    /// eligible as comparison peers: not this node, membership
    /// `Alive | Suspect`, and — when a manifest is known — reporting at
    /// least one Healthy data pool. A holder whose manifest is
    /// missing/stale stays eligible (ADR-0029 §D5).
    fn eligible_holders(&self, segment_id: &SegmentId, holders: &[NodeId]) -> Vec<NodeId>;
}

/// Builds the per-node scrub work assignment (ADR-0033 D1 scrub half).
///
/// Injected from the node layer alongside [`PeerSelector`]. The concrete
/// planner assigns each segment to one of its eligible holders and never
/// assigns a segment to a node that does not hold it. A segment whose
/// only eligible holder is `self_id` (a local-only segment) lands in the
/// self partition.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{NodeId, SegmentId, SegmentMetadata, SizeTier};
/// use oceanfs_durability::peer_selection::PartitionPlanner;
/// use oceanfs_durability::SegmentPartition;
///
/// /// Test planner: assigns every segment to its first holder.
/// struct FirstHolder;
///
/// impl PartitionPlanner for FirstHolder {
///     fn plan_partitions(
///         &self,
///         segments: &[SegmentMetadata],
///         self_id: &NodeId,
///     ) -> Vec<SegmentPartition> {
///         let mut by_holder: Vec<SegmentPartition> = Vec::new();
///         for seg in segments {
///             let target = seg
///                 .storage_locations
///                 .first()
///                 .cloned()
///                 .unwrap_or_else(|| self_id.clone());
///             match by_holder.iter_mut().find(|p| p.node_id == target) {
///                 Some(p) => p.segment_ids.push(seg.segment_id),
///                 None => by_holder.push(SegmentPartition {
///                     node_id: target,
///                     segment_ids: vec![seg.segment_id],
///                 }),
///             }
///         }
///         by_holder
///     }
/// }
///
/// let planner = FirstHolder;
/// let self_id = NodeId::new("self-node");
/// let segments = vec![SegmentMetadata {
///     pool_id: 0,
///     total_bytes: 0,
///     segment_id: SegmentId::new(),
///     ec_k: 4,
///     ec_m: 2,
///     size_tier: SizeTier::Standard,
///     merkle_root: None,
///     storage_locations: smallvec::smallvec![NodeId::new("holder-a")],
///     sealed_at: None,
/// }];
/// let partitions = planner.plan_partitions(&segments, &self_id);
/// assert_eq!(partitions.len(), 1);
/// assert_eq!(partitions[0].node_id, NodeId::new("holder-a"));
/// assert_eq!(partitions[0].segment_ids.len(), 1);
/// ```
pub trait PartitionPlanner: Send + Sync {
    /// Plans a scrub assignment over the coordinator's sealed-segment
    /// inventory (`segments`). Each returned [`SegmentPartition`] lists,
    /// for its `node_id`, only segments that node holds; a segment whose
    /// only eligible holder is `self_id` lands in the self partition.
    fn plan_partitions(
        &self,
        segments: &[SegmentMetadata],
        self_id: &NodeId,
    ) -> Vec<SegmentPartition>;
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{NodeId, SegmentId, SegmentMetadata, SizeTier};

    use super::*;

    fn holder_only(holder: &str) -> SegmentMetadata {
        SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::smallvec![NodeId::new(holder)],
            sealed_at: None,
        }
    }

    fn local_only() -> SegmentMetadata {
        SegmentMetadata {
            pool_id: 0,
            total_bytes: 0,
            segment_id: SegmentId::new(),
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: None,
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: None,
        }
    }

    #[test]
    fn peer_selector_contract_is_object_safe_and_callable() {
        // The trait surface a node implementation must satisfy: Send +
        // Sync + the single eligibility method. A minimal double proves
        // the seam (the eligibility *rules* live in the node crate).
        struct EverythingEligible;
        impl PeerSelector for EverythingEligible {
            fn eligible_holders(&self, _segment_id: &SegmentId, holders: &[NodeId]) -> Vec<NodeId> {
                holders.to_vec()
            }
        }

        let selector = EverythingEligible;
        let holders = [NodeId::new("a"), NodeId::new("b")];
        assert_eq!(
            selector.eligible_holders(&SegmentId::new(), &holders),
            vec![NodeId::new("a"), NodeId::new("b")]
        );
    }

    #[test]
    fn partition_planner_never_invents_holders() {
        // A planner backed by a test double: each segment is assigned to
        // one of its storage_locations holders. Verify the planner seam
        // passes holder sets through unmodified (the no-non-holder
        // invariant is the node impl's job; this proves the contract
        // shape).
        let segments = vec![holder_only("n1"), holder_only("n2"), local_only()];
        let self_id = NodeId::new("self-node");

        struct OwnFirst;
        impl PartitionPlanner for OwnFirst {
            fn plan_partitions(
                &self,
                segments: &[SegmentMetadata],
                self_id: &NodeId,
            ) -> Vec<SegmentPartition> {
                segments
                    .iter()
                    .map(|seg| {
                        let node_id =
                            seg.storage_locations.first().cloned().unwrap_or(self_id.clone());
                        SegmentPartition { node_id, segment_ids: vec![seg.segment_id] }
                    })
                    .collect()
            }
        }

        let partitions = OwnFirst.plan_partitions(&segments, &self_id);
        assert_eq!(partitions.len(), 3);
        // The local-only segment (no storage_locations) fell to self.
        assert!(partitions.iter().any(|p| p.node_id == self_id));
        // Every non-self partition names one of the segment's holders.
        for p in &partitions {
            let seg = segments
                .iter()
                .find(|s| s.segment_id == p.segment_ids[0])
                .expect("partition refers to a planned segment");
            if p.node_id != self_id {
                assert!(
                    seg.storage_locations.iter().any(|h| h == &p.node_id),
                    "partition must not list a non-holder"
                );
            }
        }
    }
}
