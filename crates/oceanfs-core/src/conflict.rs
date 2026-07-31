//! Conflict resolution for distributed object writes.
//!
//! When reads compare data from multiple replicas, HLC timestamps may
//! differ for the same object. This module defines the pluggable
//! [`ConflictResolver`] trait and a default [`LwwResolver`] that
//! implements Last-Write-Wins semantics.
//!
//! ## Resolution Strategy
//!
//! - [`LwwResolver`]: newer HLC wins; tie-break by `node_id`.
//! - Custom resolvers can be plugged in per-bucket via the trait.

use crate::Hlc;

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
///     fn resolve(&self, _local: &Hlc, _remote: &Hlc) -> Resolution {
///         Resolution::AcceptLocal
///     }
/// }
/// ```
pub trait ConflictResolver: Send + Sync + 'static {
    /// Resolves a conflict between two object versions.
    ///
    /// `local` is the HLC of the version on *this* node.
    /// `remote` is the HLC of the version received from a replica.
    fn resolve(&self, local: &Hlc, remote: &Hlc) -> Resolution;
}

/// Default Last-Write-Wins conflict resolver.
///
/// - Newer HLC wins (higher wall time, then higher logical counter).
/// - If HLCs are equal, the local version is kept (deterministic tie-break).
///
/// This is the default resolver for all buckets unless overridden.
///
/// # Examples
///
/// ```
/// use oceanfs_core::{ConflictResolver, Hlc, LwwResolver, Resolution};
///
/// let resolver = LwwResolver;
/// let local = Hlc::new(1000, 0);
/// let remote = Hlc::new(2000, 0);
///
/// let result = resolver.resolve(&local, &remote);
/// assert!(result.is_remote_accepted());
/// ```
#[derive(Debug, Clone, Copy)]
pub struct LwwResolver;

impl ConflictResolver for LwwResolver {
    fn resolve(&self, local: &Hlc, remote: &Hlc) -> Resolution {
        match remote.cmp(local) {
            std::cmp::Ordering::Greater => Resolution::AcceptRemote,
            std::cmp::Ordering::Less => Resolution::AcceptLocal,
            std::cmp::Ordering::Equal => {
                // Equal HLCs — accept local by default (deterministic tie-break).
                Resolution::AcceptLocal
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

    #[test]
    fn lww_resolver_newer_remote_wins() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 5);
        let remote = Hlc::new(2000, 0);
        let result = resolver.resolve(&local, &remote);
        assert_eq!(result, Resolution::AcceptRemote);
    }

    #[test]
    fn lww_resolver_older_remote_loses() {
        let resolver = LwwResolver;
        let local = Hlc::new(2000, 0);
        let remote = Hlc::new(1000, 9);
        let result = resolver.resolve(&local, &remote);
        assert_eq!(result, Resolution::AcceptLocal);
    }

    #[test]
    fn lww_resolver_same_wall_higher_logical_wins() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 5);
        let remote = Hlc::new(1000, 9);
        let result = resolver.resolve(&local, &remote);
        assert_eq!(result, Resolution::AcceptRemote);
    }

    #[test]
    fn lww_resolver_equal_hlcs_accept_local() {
        let resolver = LwwResolver;
        let local = Hlc::new(1000, 5);
        let remote = Hlc::new(1000, 5);
        let result = resolver.resolve(&local, &remote);
        assert_eq!(result, Resolution::AcceptLocal);
    }
}
