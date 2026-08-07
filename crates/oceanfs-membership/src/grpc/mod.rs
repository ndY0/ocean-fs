//! gRPC service implementations for membership operations.
//!
//! ## Services
//!
//! - [`gossip_service::GossipGrpcService`] — GossipPush / GossipPull
//! - `probe_service::ProbeHandler` — SWIM Probe (internal)

pub mod gossip_service;
pub mod probe_service;
