//! Hinted handoff — buffers writes for unreachable nodes.

use oceanfs_core::{NodeId, WriteAck};

/// Stores a hinted write for a temporarily unreachable node.
#[derive(Debug, Clone)]
pub struct HintedHandoff {
    _hints: Vec<HintRecord>,
}

/// A buffered write intended for a specific node.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HintRecord {
    /// The node this write was intended for.
    pub intended_for: NodeId,
    /// The write acknowledgment data.
    pub ack: WriteAck,
}

impl HintedHandoff {
    /// Creates a new empty hinted handoff buffer.
    pub fn new() -> Self {
        Self { _hints: Vec::new() }
    }

    /// Returns the number of pending hints.
    pub fn pending_count(&self) -> usize {
        self._hints.len()
    }
}

impl Default for HintedHandoff {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn new_handoff_is_empty() {
        let hh = HintedHandoff::new();
        assert_eq!(hh.pending_count(), 0);
    }
}
