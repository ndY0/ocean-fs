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
// async_trait generates #[must_use] on methods returning Result,
// which is redundant (Result is already #[must_use]). This lint fires
// in nightly-2026-08-10+ clippy and is denied via workspace RUSTFLAGS.
#![allow(clippy::double_must_use)]
// Internal infrastructure types are constructed by future integration code.
#![allow(dead_code)]

mod error;
mod failure_detector;
mod gossip;
mod graceful_leave;
pub mod grpc;
mod membership;
pub mod plane;

pub use error::{Error, Result};
pub use graceful_leave::GracefulLeaveHandler;
pub use membership::{Membership, MembershipEvent};
