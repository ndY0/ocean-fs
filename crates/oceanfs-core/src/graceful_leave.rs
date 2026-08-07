//! Graceful leave handler trait.
//!
//! Defines the interface for WAL handoff and segment shard streaming
//! during cluster leave. Implementations live in `oceanfs-node` which
//! owns the storage components (`WalWriter`, `BlobStore`, connection pool).

use async_trait::async_trait;

use crate::{NodeId, Result};

/// Handler for data transfer operations during graceful cluster leave.
///
/// Called by `Membership` when a node is gracefully leaving the cluster.
/// The handler is responsible for sealing and transferring WAL data and
/// segment shards to the ring successor so no data is lost when the node
/// departs.
///
/// # Examples
///
/// ```ignore
/// use async_trait::async_trait;
/// use oceanfs_core::{GracefulLeaveHandler, NodeId};
/// use std::sync::Arc;
///
/// struct MyLeaveHandler;
///
/// #[async_trait]
/// impl GracefulLeaveHandler for MyLeaveHandler {
///     async fn handoff_wal_to(&self, _successor: &NodeId) -> oceanfs_core::Result<()> {
///         Ok(())
///     }
///     async fn transfer_segment_shards_to(
///         &self, _successor: &NodeId,
///     ) -> oceanfs_core::Result<usize> {
///         Ok(0)
///     }
/// }
/// ```
#[async_trait]
pub trait GracefulLeaveHandler: Send + Sync {
    /// Seals the active write-ahead log, flushes it to disk, and
    /// transfers pending entries to the given successor node.
    ///
    /// Called after the LEAVING announcement so the successor
    /// can accept the handoff.
    async fn handoff_wal_to(&self, successor: &NodeId) -> Result<()>;

    /// Enumerates segment shards owned by this node and streams
    /// them to the given successor node.
    ///
    /// Returns the number of segments successfully transferred.
    async fn transfer_segment_shards_to(&self, successor: &NodeId) -> Result<usize>;
}
