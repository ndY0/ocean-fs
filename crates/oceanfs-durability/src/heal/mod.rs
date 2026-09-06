//! EC Heal Dispatch — background pipeline for repairing corrupt segment shards.
//!
//! ## Architecture
//!
//! The heal module has two main components:
//!
//! - [`HealQueue`]: A bounded `tokio::sync::mpsc` channel that buffers
//!   [`HealRequest`] items submitted by Scrub and Anti-Entropy on corruption
//!   detection. Backpressure is enforced: when the queue is full, producers
//!   receive an error (perf rule 2.6).
//!
//! - [`HealWorker`]: A background task that drains the heal queue, fetches
//!   `k` healthy shards from peer nodes via gRPC, calls
//!   [`oceanfs_ec::Decoder::decode`] to reconstruct corrupt or missing shards,
//!   writes the repaired data back, and updates metadata. Each heal op
//!   acquires a Tier-0 (repair) permit from the shared `DurabilityBudget`
//!   (ADR-0017 amendment) — the single node-wide repair gate.
//!
//! ## Data Flow
//!
//! ```text
//! Scrub / AntiEntropy → enqueue_heal() → HealQueue → HealWorker
//!                                                        ↓
//!                                              FetchShard (gRPC streaming)
//!                                                        ↓
//!                                              Decoder::decode() (EC repair)
//!                                                        ↓
//!                                              Write repaired shard
//!                                              Update metadata
//! ```
//!
//! ## LOCK ORDER
//!
//! No multi-lock acquisitions occur in this module. All state access is
//! through atomic counters or single-task ownership.
//!
//! # Examples
//!
//! ```ignore
//! use oceanfs_durability::heal::{HealQueue, HealWorker, HealConfig};
//!
//! let config = HealConfig::default();
//! let queue = HealQueue::new(config.queue_capacity());
//!
//! // enqueue a heal request from scrub/anti-entropy
//! queue.sender().enqueue(HealRequest {
//!     segment_id: my_segment,
//!     corrupt_shard_indices: vec![2],
//!     retry_count: 0,
//! }).await?;
//! ```

mod queue;
mod worker;

/// Re-export core types for ergonomic use within heal module callers.
pub use oceanfs_core::{HealConfig, HealRequest, HealStats};
pub use queue::{enqueue_heal, init_global_queue, HealQueue, HealQueueSender};
pub use worker::HealWorker;
