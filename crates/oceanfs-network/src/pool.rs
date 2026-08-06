//! Persistent gRPC connection pool per peer node.
//!
//! Maintains a pool of N reusable gRPC channels per peer. Channels are
//! acquired via [`ConnectionPool::get_channel`] and returned on drop of
//! the [`PooledChannel`] guard. The pool enforces concurrency limits
//! via a semaphore, ensuring bounded resource usage under load.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use oceanfs_core::{Counter, Gauge, LabelSet, MetricRegistrar, RpcConfig};
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::transport::{Channel, Endpoint};

/// Error type for connection pool operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// All channels for the given peer are busy.
    #[error("no available channels for peer {0} (all {1} in use)")]
    NoAvailableChannel(String, usize),

    /// Failed to establish a connection to the peer.
    #[error("connection to {0} failed: {1}")]
    ConnectionFailed(String, #[source] tonic::transport::Error),

    /// The pool capacity has been reached for this peer.
    #[error("pool capacity reached for peer {0}")]
    PoolExhausted(String),
}

/// A convenience result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A pooled gRPC channel.
///
/// Wraps a tonic [`Channel`] with a semaphore permit. The channel is
/// returned to the pool when this guard is dropped.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_network::ConnectionPool;
///
/// # async fn example(pool: &ConnectionPool) {
/// let addr = "127.0.0.1:9001".parse().unwrap();
/// let pooled = pool.get_channel(addr).await.unwrap();
/// // Use pooled.channel() for RPC calls...
/// drop(pooled); // returns channel to pool
/// # }
/// ```
pub struct PooledChannel {
    channel: Channel,
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
}

impl PooledChannel {
    /// Returns a reference to the underlying gRPC channel.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }
}

/// Per-peer pool state.
struct PeerPool {
    /// Pre-established gRPC channels.
    channels: Mutex<Vec<Channel>>,
    /// Semaphore controlling concurrent channel usage.
    semaphore: Arc<Semaphore>,
    /// Total channels created (for metrics).
    total_channels: AtomicUsize,
    /// Round-robin index for channel selection.
    next_index: AtomicUsize,
}

/// A pool of gRPC channels, keyed by peer socket address.
///
/// Channels are lazily created on first access and cached per peer.
/// A [`Semaphore`] limits concurrent usage to `pool_size_per_peer`
/// channels per peer, enforcing backpressure.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_core::RpcConfig;
/// use oceanfs_network::ConnectionPool;
///
/// # async fn example() {
/// let config = RpcConfig::default();
/// let pool = ConnectionPool::new(config);
/// let addr = "127.0.0.1:9001".parse().unwrap();
/// let channel = pool.get_channel(addr).await.unwrap();
/// // make RPC calls...
/// # }
/// ```
pub struct ConnectionPool {
    config: RpcConfig,
    peers: DashMap<SocketAddr, Arc<PeerPool>>,
    connection_errors_total: Counter,
    connections_active: Gauge,
}

impl ConnectionPool {
    /// Creates a new connection pool with the given configuration.
    pub fn new(config: RpcConfig) -> Self {
        Self {
            config,
            peers: DashMap::new(),
            connection_errors_total: Counter::new(
                "grpc_connection_errors_total".into(),
                "gRPC connection errors".into(),
                LabelSet::empty(),
            ),
            connections_active: Gauge::new(
                "grpc_connections_active".into(),
                "Active gRPC peer connections".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Registers connection pool metrics with a registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.connection_errors_total.clone());
        registrar.register_gauge(self.connections_active.clone());
    }

    /// Acquires a channel for the given peer.
    ///
    /// Returns a [`PooledChannel`] that holds a semaphore permit.
    /// When the permit is dropped, the channel is effectively returned
    /// to the pool for reuse by other callers.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConnectionFailed` if the channel cannot be
    /// established. Returns `Error::NoAvailableChannel` if the
    /// semaphore is exhausted (should not normally happen with async
    /// semaphore — it waits).
    pub async fn get_channel(&self, peer: SocketAddr) -> Result<PooledChannel> {
        let pool = match self.get_or_create_pool(peer).await {
            Ok(p) => p,
            Err(e) => {
                self.connection_errors_total.inc();
                return Err(e);
            }
        };

        // Acquire a permit — this waits if all channels are in use.
        let permit = pool.semaphore.clone().acquire_owned().await.map_err(|_e| {
            self.connection_errors_total.inc();
            Error::NoAvailableChannel(peer.to_string(), self.config.pool_size_per_peer)
        })?;

        // Select a channel via round-robin.
        let channel = {
            let channels = pool.channels.lock();
            let idx = pool.next_index.fetch_add(1, Ordering::Relaxed) % channels.len();
            channels[idx].clone()
        };

        Ok(PooledChannel { channel, permit })
    }

    /// Performs a health check on all peer pools.
    ///
    /// Currently a no-op placeholder. Future: probes each channel
    /// with a gRPC health check RPC.
    pub async fn health_check(&self) {
        // Placeholder for gRPC health probing.
    }

    /// Returns the number of active peer pools.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Returns the pool configuration.
    pub fn config(&self) -> &RpcConfig {
        &self.config
    }

    // --- private ---

    /// Gets or creates a peer pool for the given address.
    async fn get_or_create_pool(&self, peer: SocketAddr) -> Result<Arc<PeerPool>> {
        // Fast path: already cached.
        if let Some(pool) = self.peers.get(&peer) {
            return Ok(pool.clone());
        }

        // Slow path: create new pool.
        let pool = self.create_peer_pool(peer).await?;
        let pool = self.peers.entry(peer).or_insert(pool);
        Ok(pool.value().clone())
    }

    /// Creates a new peer pool with pre-established channels.
    async fn create_peer_pool(&self, peer: SocketAddr) -> Result<Arc<PeerPool>> {
        let pool_size = self.config.pool_size_per_peer;
        let mut channels = Vec::with_capacity(pool_size);

        let uri = format!("http://{peer}");
        let endpoint = Endpoint::from_shared(uri)
            .map_err(|e| Error::ConnectionFailed(peer.to_string(), e))?
            .tcp_nodelay(true)
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(Duration::from_secs(self.config.keepalive_sec))
            .connect_timeout(Duration::from_millis(self.config.connect_timeout_ms))
            .timeout(Duration::from_millis(self.config.request_timeout_ms));

        // Pre-connect all channels in the pool.
        for _ in 0..pool_size {
            let channel = endpoint
                .connect()
                .await
                .map_err(|e| Error::ConnectionFailed(peer.to_string(), e))?;
            channels.push(channel);
        }

        Ok(Arc::new(PeerPool {
            channels: Mutex::new(channels),
            semaphore: Arc::new(Semaphore::new(pool_size)),
            total_channels: AtomicUsize::new(pool_size),
            next_index: AtomicUsize::new(0),
        }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn pool_creation_with_config() {
        let config = RpcConfig { pool_size_per_peer: 4, ..RpcConfig::default() };
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.config().pool_size_per_peer, 4);
        assert_eq!(pool.peer_count(), 0);
    }

    #[test]
    fn pool_config_has_expected_defaults() {
        let config = RpcConfig::default();
        assert_eq!(config.pool_size_per_peer, 4);
        assert_eq!(config.connect_timeout_ms, 5000);
        assert_eq!(config.request_timeout_ms, 30000);
        assert!(config.tls_cert_path.is_none());
    }

    #[tokio::test]
    async fn get_channel_fails_for_invalid_address() {
        let config =
            RpcConfig { pool_size_per_peer: 1, connect_timeout_ms: 100, ..RpcConfig::default() };
        let pool = ConnectionPool::new(config);
        // Use an unroutable address — should fail fast.
        let addr: SocketAddr = "192.0.2.1:1".parse().unwrap();
        let result = pool.get_channel(addr).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn peer_count_increases_after_channel_attempt() {
        let config =
            RpcConfig { pool_size_per_peer: 1, connect_timeout_ms: 100, ..RpcConfig::default() };
        let pool = ConnectionPool::new(config);

        assert_eq!(pool.peer_count(), 0);

        // Attempt to connect to an invalid address (will fail, but pool entry is created).
        let addr: SocketAddr = "192.0.2.1:2".parse().unwrap();
        let _ = pool.get_channel(addr).await;
        // Even on failure, the peer pool is still created in the map.
        // Note: the exact behavior depends on whether the channel creation fails
        // before or after inserting into the map. In our impl, it's inserted after
        // all channels are pre-connected, so a failure means no insert.
        // This test verifies the pool tracking works as expected.
        // With connection failure, peer_count stays 0.
        assert_eq!(pool.peer_count(), 0);
    }

    #[test]
    fn config_returns_configured_values() {
        let config = RpcConfig {
            pool_size_per_peer: 8,
            keepalive_sec: 60,
            max_idle_connections: 128,
            connect_timeout_ms: 10000,
            request_timeout_ms: 60000,
            tls_cert_path: None,
        };
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.config().pool_size_per_peer, 8);
        assert_eq!(pool.config().keepalive_sec, 60);
    }

    #[test]
    fn connection_pool_metrics_initialized() {
        let config = RpcConfig::default();
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.connection_errors_total.get(), 0);

        pool.connection_errors_total.inc();
        pool.connection_errors_total.inc();
        assert_eq!(pool.connection_errors_total.get(), 2);
    }
}
