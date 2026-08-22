//! OceanFS node configuration.
//!
//! Configuration is loaded from `oceanfs.toml` at startup. This module
//! defines the root config struct and its sub-components. Per-bucket
//! policy overrides are defined in `oceanfs-server` (Phase 5).

mod accel;
mod auth;
mod compression;
mod event_wal;
mod lifecycle;
mod metadata;
mod node;
mod ring;
pub mod shard;
mod storage;
mod wal;

pub use accel::AccelConfig;
pub use auth::AuthConfig;
pub use compression::CompressionConfig;
pub use event_wal::EventWalConfig;
pub use lifecycle::LifecycleConfig;
pub use metadata::MetadataConfig;
pub use node::{AntiEntropyConfig, NodeConfig};
pub use ring::RingConfig;
// The storage-pool definition type is named `PoolConfig` in this module, but
// the crate facade already exports `types::config::PoolConfig` (the active
// segment pool). The storage one is re-exported as `StoragePoolConfig` to
// keep both reachable without ambiguity.
pub use storage::PoolConfig as StoragePoolConfig;
pub use storage::{MissingRootPolicy, PoolHealthConfig, PoolRole, PoolTech, StorageConfig};
pub use wal::WalConfig;
