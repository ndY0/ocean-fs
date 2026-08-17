//! OceanFS node configuration.
//!
//! Configuration is loaded from `oceanfs.toml` at startup. This module
//! defines the root config struct and its sub-components. Per-bucket
//! policy overrides are defined in `oceanfs-server` (Phase 5).

mod accel;
mod auth;
mod compression;
mod lifecycle;
mod metadata;
mod node;
mod ring;
pub mod shard;
mod wal;

pub use accel::AccelConfig;
pub use auth::AuthConfig;
pub use compression::CompressionConfig;
pub use lifecycle::LifecycleConfig;
pub use metadata::MetadataConfig;
pub use node::{AntiEntropyConfig, NodeConfig};
pub use ring::RingConfig;
pub use wal::WalConfig;
