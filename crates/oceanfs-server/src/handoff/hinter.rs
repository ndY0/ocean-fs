//! Hint storage for hinted handoff.

use oceanfs_core::NodeId;

use crate::hinted_handoff::HintRecord;

/// Creates a hint record for an unreachable node.
#[allow(dead_code)]
pub(crate) fn create_hint(
    intended_for: NodeId,
    segment_id: oceanfs_core::SegmentId,
    offset: u64,
    length: u32,
    timestamp: oceanfs_core::Hlc,
) -> HintRecord {
    HintRecord {
        intended_for,
        segment_id,
        offset,
        length,
        timestamp,
    }
}
