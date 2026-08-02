//! gRPC service implementations for node-to-node communication.
//!
//! Each module implements a tonic service trait generated from the
//! protobuf service definitions in `oceanfs-network`.
//!
//! ## Services
//!
//! - [`segment_service::SegmentGrpcService`] — AppendSegment / FetchShard
//! - [`healing_service::HealingGrpcService`] — HintedHandoff / MerkleExchange / FetchShard / PushRepairedShard
//! - [`cache_service::CacheGrpcService`] — CacheInvalidate
//! - [`scrub_service::ScrubGrpcService`] — AssignPartition / ReportPartitionResult

pub mod cache_service;
pub mod healing_service;
pub mod scrub_service;
pub mod segment_service;
