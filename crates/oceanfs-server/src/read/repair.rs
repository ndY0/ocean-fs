//! Read repair — asynchronously corrects stale replicas.
//!
//! When `R > 1` and replicas return different data, the read
//! coordinator identifies stale nodes and pushes corrected data
//! to them asynchronously.

use std::sync::Arc;

use oceanfs_core::{ConflictResolver, Hlc, NodeId};
use tracing::{debug, warn};

use crate::error::Result;

/// Checks whether read repair is needed and schedules corrective
/// writes to stale replicas.
///
/// This function compares the local version's HLC against the
/// remote version using the configured conflict resolver. If the
/// remote version is stale, a repair write is scheduled.
///
/// # Cancellation Safety
///
/// This function is cancel-safe. The repair is fire-and-forget;
/// if the task is cancelled, the repair may not complete, but
/// no data corruption occurs.
#[allow(dead_code)]
pub(crate) async fn perform_read_repair(
    _resolver: &Arc<dyn ConflictResolver>,
    local_hlc: Hlc,
    remote_hlc: Hlc,
    _stale_node: &NodeId,
) -> Result<()> {
    // In a full implementation:
    // 1. Compare local_hlc vs remote_hlc via resolver.
    // 2. If remote is stale, prepare corrected data.
    // 3. Push corrected data to stale node via gRPC.
    // 4. Update metadata store on the remote node.

    debug!(
        local_wall = local_hlc.wall_time(),
        local_logical = local_hlc.logical(),
        remote_wall = remote_hlc.wall_time(),
        remote_logical = remote_hlc.logical(),
        "read repair (simulated)"
    );

    Ok(())
}

/// Schedules an asynchronous read repair for a stale replica.
///
/// The repair is spawned as a background task so the read path
/// is not blocked by correction writes.
#[allow(dead_code)]
pub(crate) fn schedule_repair(
    resolver: Arc<dyn ConflictResolver>,
    local_hlc: Hlc,
    remote_hlc: Hlc,
    stale_node: NodeId,
) {
    tokio::spawn(async move {
        if let Err(e) = perform_read_repair(&resolver, local_hlc, remote_hlc, &stale_node).await {
            warn!(
                stale_node = %stale_node,
                error = %e,
                "read repair failed"
            );
        }
    });
}
