//! gRPC service implementations for node-to-node communication.
//!
//! Each module implements a tonic service trait generated from the
//! protobuf service definitions.
//!
//! ## Services
//!
//! - [`segment_service::SegmentGrpcService`] — AppendSegment / FetchShard
//! - [`cache_service::CacheGrpcService`] — CacheInvalidate

pub mod cache_service;
#[cfg(feature = "storage")]
pub mod segment_service;
