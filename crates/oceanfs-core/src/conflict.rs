//! Conflict resolution for distributed object writes.
//!
//! When reads compare data from multiple replicas, HLC timestamps may
//! differ for the same object. This module defines the pluggable
//! [`ConflictResolver`] trait and a default [`LwwResolver`] that
//! implements Last-Write-Wins semantics.
//!
//! ## Resolution Strategy
//!
//! - [`LwwResolver`]: newer HLC wins; equal HLCs tie-break by node id
//!   (the lexicographically greater node id wins, per spec §7.6).
//! - Custom resolvers can be plugged in per-bucket via the trait.

use crate::{Hlc, NodeId};

/// The outcome of a conflict resolution between two versions.
///
/// # Examples
///
/// ```
/// use oceanfs_core::Resolution;
///
/// let r = Resolution::AcceptLocal;
/// assert!(r.is_local_accepted());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Resolution {
    /// The local version should be kept.
    AcceptLocal,
    /// The remote version should replace the local version.
    AcceptRemote,
    /// The versions should be merged (reserved for future CRDT support).
    Merge,
}

impl Resolution {
    /// Returns `true` if the local version wins.
    pub fn is_local_accepted(&self) -> bool {
        matches!(self, Self::AcceptLocal)
    }

    /// Returns `true` if the remote version wins.
    pub fn is_remote_accepted(&self) -> bool {
        matches!(self, Self::AcceptRemote)
    }
}

/// A pluggable conflict resolver for object versions.
///
/// Implementations determine which version to keep when two replicas
/// have different data for the same object key.
///
/// All implementations must be `Send + Sync` safe for concurrent use,
/// and `'static` for storage as a trait object in bucket config.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{ConflictResolver, Hlc, NodeId, Resolution};
///
/// struct CustomResolver;
/// impl ConflictResolver for CustomResolver {
///     fn resolve(
///         &self,
///         _local: &Hlc,
///         _remote: &Hlc,
///         _local_node: &NodeId,
///         _remote_node: &NodeId,
///     ) -> Resolution {
///         Resolution::AcceptLocal
///     }
/// }
/// ```
pub trait ConflictResolver: Send + Sync + 'static {
    /// Resolves a conflict between two object versions.
    ///
    /// `local` is the HLC of the version on *this* node.
    /// `remote` is the HLC of the version received from a replica.
    /// `local_node` / `remote_node` identify the nodes that hold each
    /// version; resolvers may use them as a deterministic tie-break
    /// when the HLCs are equal (spec §7.6).
    fn resolve(
        &self,
        local: &Hlc,
        remote: &Hlc,
        local_node: &NodeId,
        remote_node: &NodeId,
    ) -> Resolution;
}

/// Default Last-Write-Wins conflict resolver.
///
/// - Newer HLC wins (higher wall time, then higher logical counter).
/// - If HLCs are equal, the **lexicographically greater node id** wins:
///   `AcceptRemote` when `remote_node.as_str() > local_node.as_str()`,
///   `AcceptLocal` otherwise (spec §7.6: "tie-break by node_id").
///   Two nodes may mint identical HLCs (same millisecond, logical 0)
///   for *different* data; this deterministic tie-break makes the
///   resolution identical on every node.
///
/// This is the default resolver for all buckets unless overridden.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{ConflictResolver, Hlc, LwwResolver, NodeId, Resolution};
///
/// let resolver = LwwResolver;
/// let local = Hlc::new(1000, 0);
/// let remote = Hlc::new(2000, 0);
/// let local_node = NodeId::new("node-a");
/// let remote_node = NodeId::new("node-b");
///
/// let result = resolver.resolve(&local, &remote, &local_node, &remote_node);
/// assert!(result.is_remote_accepted());
/// ```
#[derive(Debug, Clone, Copy)]
pub struct LwwResolver;

impl ConflictResolver for LwwResolver {
    fn resolve(
        &self,
        local: &Hlc,
        remote: &Hlc,
        local_node: &NodeId,
        remote_node: &NodeId,
    ) -> Resolution {
        match remote.cmp(local) {
            std::cmp::Ordering::Greater => Resolution::AcceptRemote,
            std::cmp::Ordering::Less => Resolution::AcceptLocal,
            std::cmp::Ordering::Equal => {
                // Equal HLCs — deterministic tie-break by node id:
                // the lexicographically greater node id wins, so the
                // resolution is identical on every node regardless of
                // which side is "local".
                if remote_node.as_str() > local_node.as_str() {
                    Resolution::AcceptRemote
                } else {
                    Resolution::AcceptLocal
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Resolution --

    #[test]
    fn resolution_accept_local_is_local_accepted() {
        assert!(Resolution::AcceptLocal.is_local_accepted());
        assert!(!Resolution::AcceptLocal.is_remote_accepted());
    }

    #[test]
    fn resolution_accept_remote_is_remote_accepted() {
        assert!(Resolution::AcceptRemote.is_remote_accepted());
        assert!(!Resolution::AcceptRemote.is_local_accepted());
    }

    // -- LwwResolver --

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    #[test]
    fn lww_resolver_newer_remote_wins() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 5);
        let remote = Hlc::new(2000, 0);
        let result = resolver.resolve(&local, &remote, &node("n1"), &node("n2"));
        assert_eq!(result, Resolution::AcceptRemote);
    }

    #[test]
    fn lww_resolver_older_remote_loses() {
        let resolver = LwwResolver;
        let local = Hlc::new(2000, 0);
        let remote = Hlc::new(1000, 9);
        let result = resolver.resolve(&local, &remote, &node("n1"), &node("n2"));
        assert_eq!(result, Resolution::AcceptLocal);
    }

    #[test]
    fn lww_resolver_same_wall_higher_logical_wins() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 5);
        let remote = Hlc::new(1000, 9);
        let result = resolver.resolve(&local, &remote, &node("n1"), &node("n2"));
        assert_eq!(result, Resolution::AcceptRemote);
    }

    #[test]
    fn lww_resolver_equal_hlc_accept_local_when_local_id_greater() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 5);
        let remote = Hlc::new(1000, 5);
        // Local node id is lexicographically greater → local wins.
        let result = resolver.resolve(&local, &remote, &node("node-z"), &node("node-a"));
        assert_eq!(result, Resolution::AcceptLocal);
    }

    #[test]
    fn lww_resolver_equal_hlc_higher_node_id_wins() {
        // G7: equal HLCs tie-break by node id — the lexicographically
        // greater node id wins, from either perspective.
        let resolver = LwwResolver;
        let a = Hlc::new(1000, 5);
        let b = Hlc::new(1000, 5);

        // From node-a's perspective, node-z (remote, greater) wins.
        let from_a = resolver.resolve(&a, &b, &node("node-a"), &node("node-z"));
        assert_eq!(from_a, Resolution::AcceptRemote, "node-z must win from node-a's view");
        // From node-z's perspective, node-a (remote, lesser) loses.
        let from_z = resolver.resolve(&b, &a, &node("node-z"), &node("node-a"));
        assert_eq!(from_z, Resolution::AcceptLocal, "node-a must lose from node-z's view");
    }

    #[test]
    fn lww_resolver_equal_hlc_same_node_id_accepts_local() {
        let resolver = LwwResolver;
        let h = Hlc::new(1000, 5);
        let result = resolver.resolve(&h, &h, &node("node-a"), &node("node-a"));
        assert_eq!(result, Resolution::AcceptLocal);
    }
}
