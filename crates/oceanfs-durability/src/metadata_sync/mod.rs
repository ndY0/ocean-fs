//! Metadata change journal + pull-based catch-up (ADR-0038; ae2 / S2).
//!
//! This module is the **change edge** of index-plane convergence:
//!
//! - [`WatermarkStore`] — durable per-peer `consumed`/`acked` positions;
//! - [`MetadataSync`] — the Tier-1 worker that pulls journals, filters to
//!   currently co-owned keys, point-fetches current state, and applies it
//!   through the store's HLC-LWW guards;
//! - [`MetadataSyncService`] — the enabled-only server side for
//!   `FetchJournal` / `FetchMetadataRows`.
//!
//! Everything here is constructed only when `[metadata_sync] enabled =
//! true`; a disabled node builds none of it and serves `unavailable`.

mod service;
mod watermark;
mod worker;

pub use service::MetadataSyncService;
pub use watermark::{PeerWatermark, WatermarkStore};
pub use worker::{BootstrapEnqueuer, BootstrapReason, MetadataSync, MetadataSyncMetrics};
