//! RPC client abstraction for testability.
//!
//! The [`RpcClient`] trait provides a marker interface for service-specific
//! gRPC client stubs. Each crate that provides RPC services implements this
//! trait on its generated client type.

use std::fmt::Debug;

/// Marker trait for gRPC client stubs.
///
/// Implemented by generated tonic client types. Allows service-specific
/// clients to be passed through generic connection pool interfaces.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_network::RpcClient;
///
/// #[derive(Clone)]
/// struct MyServiceClient {
///     inner: tonic::transport::Channel,
/// }
///
/// impl RpcClient for MyServiceClient {}
/// ```
pub trait RpcClient: Clone + Debug + Send + Sync + 'static {}
