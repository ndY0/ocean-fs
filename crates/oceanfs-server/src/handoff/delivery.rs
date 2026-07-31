//! Hint delivery for hinted handoff.
//!
//! Delivers buffered hints to nodes that have returned to the
//! cluster after being temporarily unreachable.

use oceanfs_core::NodeId;
use tracing::debug;

use crate::hinted_handoff::HintRecord;

/// Delivers a single hint to a returned node.
#[allow(dead_code)]
pub(crate) async fn deliver_hint(
    _node: &NodeId,
    _hint: &HintRecord,
) -> std::result::Result<(), String> {
    // In a full gRPC implementation, this would stream hint data to the node.
    debug!("hint delivery (simulated)");
    Ok(())
}
