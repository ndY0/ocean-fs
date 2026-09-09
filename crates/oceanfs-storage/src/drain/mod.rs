//! Drain workers (Phase C, ADR-0036).
//!
//! The mover half of the drain model lives behind storage-owned
//! primitives: the registry drain state (d1, `pool::drain`), the durable
//! relocation primitive (d2, `segment::relocate`), and this module's
//! workers that orchestrate them. d3 ships the intra-node sibling-pool
//! mover ([`intra_node::IntraNodeDrain`]); d4 adds the cluster mover on
//! top of the same state.

pub mod intra_node;

pub use intra_node::{DrainCycleStats, IntraNodeDrain, IntraNodeDrainConfig};
