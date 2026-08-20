//! Read coordinator sub-modules.
//!
//! Contains multi-chunk assembly, parallel shard fetch,
//! read repair logic, and the read coordinator.
//!
//! The legacy `repair` module (`perform_read_repair`/`schedule_repair`)
//! was deleted during hlc-causality-closure G7: it was dead code,
//! superseded by `ReadCoordinator::run_read_repair`.

pub mod assembly;
pub mod coordinator;
pub use coordinator::ReadCoordinatorHintObjectReader;
pub(crate) mod fetch;

#[allow(unused_imports)]
pub(crate) use fetch::fetch_chunks;
