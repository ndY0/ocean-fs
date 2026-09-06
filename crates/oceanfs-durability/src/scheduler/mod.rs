//! Durability scheduler (ADR-0017 as amended 2026-09-06).
//!
//! Two components:
//!
//! - [`budget::DurabilityBudget`] — the two-tier admission budget. Tier-0
//!   ("repair": heal ops, re-replication pulls/writes, inbound hint apply)
//!   is **never gated behind Tier-1** ("housekeeping": GC/orphan/scrub/AE
//!   scheduled cycles); within a tier admission is FIFO-fair. Tier separation
//!   is admission-level only — no device-level io-class arbitration.
//! - [`engine::DurabilityScheduler`] — the Tier-1 interval-cycle engine:
//!   one loop per registered [`task::DurabilityTask`], skip/overrun
//!   accounting, per-cycle timeout, error tolerance, and shutdown; every
//!   cycle runs under a Tier-1 permit from the shared budget.
//!
//! ## Tier membership and scan shape
//!
//! The four registered tasks run full-space passes (`keyspace_fraction() ==
//! 1.0`); their cycle cost (ADR-0034 accounting substrate) is:
//!
//! | Task | Cycle pass today | Why not sharded |
//! |---|---|---|
//! | GC | accounting liveness over the registry + aged dead-chunk records | liveness is attributeable only at full-registry granularity; no `MetadataStore` range-scan API |
//! | Orphan reaper | byte-accounting fully-dead detection over the registry | same — a fraction would multiply full passes per unit time |
//! | Scrub | verify every Sealed segment against its stored Merkle root | partitions by alive nodes (H5), not keyspace fraction |
//! | AE | continuous root exchange / full cycle reads + divergence repair | ADR-0015 incremental-tree model; not keyspace-sharded |
//!
//! Sharding GC/orphan would multiply whole passes per unit time because the
//! `MetadataStore` API has no range-scan method; the `keyspace_fraction`
//! rotation cursor ships inert and each Tier-1 adaptor rejects any
//! [`task::KeyspaceWindow::Shard`] window loudly. Heal, re-replication,
//! reconciliation, and hint delivery are NOT [`task::DurabilityTask`]s: they
//! are queue/event-driven, and heal/re-replication/hint-apply participate in
//! the budget as Tier-0 clients instead.

pub mod adaptors;
pub mod budget;
pub mod engine;
pub mod task;

pub use adaptors::{AeTask, GcTask, OrphanTask, ScrubTask};
pub use budget::{DurabilityBudget, DurabilityPermit, DurabilityTier};
pub use engine::DurabilityScheduler;
pub use task::{DurabilityTask, KeyspaceWindow};
