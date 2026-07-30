//! S3-compatible HTTP server and request coordinators.

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

mod error;
mod hinted_handoff;
mod read_coordinator;
mod router;
mod write_coordinator;

pub use error::{Error, Result};
pub use hinted_handoff::HintedHandoff;
pub use read_coordinator::{ReadCoordinator, ReadRequest, ReadResult};
pub use router::Router;
pub use write_coordinator::{WriteCoordinator, WriteRequest};
