//! Cluster membership and failure detection.
//!
//! Implements SWIM-based failure detection with configurable suspicion
//! and failure timeouts, plus a push-pull gossip protocol for
//! disseminating membership state across the cluster.

#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_imports,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    missing_docs
)]
// Internal infrastructure types are constructed by future integration code.
#![allow(dead_code)]

mod error;
mod failure_detector;
mod gossip;
mod graceful_leave;
pub mod grpc;
mod membership;

pub use error::{Error, Result};
pub use graceful_leave::GracefulLeaveHandler;
pub use membership::{Membership, MembershipEvent};
