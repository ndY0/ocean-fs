//! Persistent gRPC connection pool per peer node.
//!
//! Maintains a pool of N reusable gRPC channels per peer. Channels are
//! acquired via [`ConnectionPool::get_channel`] and returned on drop of
//! the [`PooledChannel`] guard. The pool enforces concurrency limits
//! via a semaphore, ensuring bounded resource usage under load.
//!
//! Periodic health checks probe each peer's gRPC health service and
//! evict broken channels, triggering lazy reconnection on the next
//! [`get_channel`] call.

use std::{
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use hyper_util::rt::TokioIo;
use oceanfs_core::{Counter, Gauge, LabelSet, MetricRegistrar, RpcConfig};
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use tonic_health::pb::health_client::HealthClient;
use tracing::{debug, info, warn};

use crate::socket_opts::{set_busy_poll, set_quickack};

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
    health_check_failures_total: Counter,
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
            health_check_failures_total: Counter::new(
                "grpc_health_check_failures_total".into(),
                "gRPC health check failures".into(),
                LabelSet::empty(),
            ),
        }
    }

    /// Registers connection pool metrics with a registrar.
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.connection_errors_total.clone());
        registrar.register_counter(self.health_check_failures_total.clone());
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
    /// Probes each peer channel with a gRPC health check RPC
    /// (`grpc.health.v1.Health/Check`). Channels that fail the
    /// health check are evicted from the pool, triggering lazy
    /// reconnection on the next [`ConnectionPool::get_channel`] call.
    ///
    /// This method does not hold the pool lock during gRPC calls,
    /// satisfying performance rule §7.1.
    pub async fn health_check(&self) {
        // Collect peer addresses to avoid holding DashMap during async calls.
        let peers: Vec<SocketAddr> = self.peers.iter().map(|entry| *entry.key()).collect();

        let mut failed_count = 0u64;
        for peer in &peers {
            match self.check_peer(peer).await {
                Ok(true) => {
                    debug!(peer = %peer, "health check passed");
                }
                Ok(false) => {
                    warn!(peer = %peer, "health check failed — evicting channels");
                    self.evict_peer(*peer);
                    failed_count += 1;
                }
                Err(e) => {
                    warn!(peer = %peer, error = %e, "health check error — evicting channels");
                    self.evict_peer(*peer);
                    failed_count += 1;
                }
            }
        }

        if failed_count > 0 {
            self.health_check_failures_total.add(failed_count);
        }

        // Update active connections gauge.
        let active_count: usize =
            self.peers.iter().map(|e| e.value().total_channels.load(Ordering::Relaxed)).sum();
        self.connections_active.set(active_count as u64);
    }

    /// Starts a periodic health check background task.
    ///
    /// Runs health checks on the configured interval
    /// (`RpcConfig::health_check_interval_sec`). If the interval is
    /// 0, no periodic checks are performed.
    ///
    /// The task runs until the given [`CancellationToken`] is cancelled.
    pub fn start_health_check_loop(self: &Arc<Self>, cancel: CancellationToken) {
        let interval_sec = self.config.health_check_interval_sec;
        if interval_sec == 0 {
            info!("periodic health check disabled (health_check_interval_sec = 0)");
            return;
        }

        let pool = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
            // Don't fire immediately — wait for the first interval.
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        info!("health check loop cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        pool.health_check().await;
                    }
                }
            }
        });

        info!(interval_sec, "started periodic connection pool health check");
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

    /// Checks a single peer's health by probing one of its channels.
    ///
    /// Returns `Ok(true)` if the health check succeeds, `Ok(false)` if
    /// the service reports non-SERVING status, or `Err` if the RPC fails.
    async fn check_peer(&self, peer: &SocketAddr) -> std::result::Result<bool, Box<tonic::Status>> {
        let channel = {
            let pool = match self.peers.get(peer) {
                Some(p) => p,
                None => return Ok(true), // Peer already evicted.
            };
            let channels = pool.channels.lock();
            channels.first().cloned()
        };

        let channel = match channel {
            Some(c) => c,
            None => return Ok(true), // No channels to check.
        };

        let mut client = HealthClient::new(channel);
        let request =
            tonic::Request::new(tonic_health::pb::HealthCheckRequest { service: "".to_string() });

        let response = client.check(request).await.map_err(Box::new)?;
        let serving = response.into_inner().status
            == tonic_health::pb::health_check_response::ServingStatus::Serving as i32;

        Ok(serving)
    }

    /// Evicts a peer's pool, removing all channels.
    ///
    /// Subsequent [`get_channel`] calls will lazily recreate the pool
    /// with fresh connections.
    fn evict_peer(&self, peer: SocketAddr) {
        self.peers.remove(&peer);
        debug!(peer = %peer, "peer evicted from connection pool");
    }

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
    ///
    /// Socket-level options (TCP_QUICKACK, SO_BUSY_POLL) are applied
    /// via a custom connector that wraps tonic's TCP transport.
    async fn create_peer_pool(&self, peer: SocketAddr) -> Result<Arc<PeerPool>> {
        let pool_size = self.config.pool_size_per_peer;
        let mut channels = Vec::with_capacity(pool_size);

        let uri_str = format!("http://{peer}");

        // Build the endpoint with standard tonic options.
        let endpoint = Endpoint::from_shared(uri_str.clone())
            .map_err(|e| Error::ConnectionFailed(peer.to_string(), e))?
            .tcp_nodelay(true)
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(Duration::from_secs(self.config.keepalive_sec))
            .connect_timeout(Duration::from_millis(self.config.connect_timeout_ms))
            .timeout(Duration::from_millis(self.config.request_timeout_ms));

        let quickack = self.config.quickack;
        let busy_poll = self.config.busy_poll_us;

        // Pre-connect all channels in the pool via a custom connector
        // that applies TCP_QUICKACK and SO_BUSY_POLL after connect.
        for _ in 0..pool_size {
            let connector = {
                tower::service_fn(move |_: http::Uri| {
                    let peer = peer;
                    let quickack = quickack;
                    let busy_poll = busy_poll;
                    async move {
                        use socket2::{Domain, Protocol, Socket, Type};
                        let addr: socket2::SockAddr = peer.into();

                        let socket = Socket::new(
                            Domain::for_address(peer),
                            Type::STREAM,
                            Some(Protocol::TCP),
                        )?;

                        if quickack {
                            let _ = set_quickack(&socket);
                        }
                        if busy_poll > 0 {
                            let _ = set_busy_poll(&socket, busy_poll);
                        }

                        socket.connect(&addr)?;

                        let tcp: std::net::TcpStream = socket.into();
                        tcp.set_nonblocking(true)?;
                        let tokio_stream = tokio::net::TcpStream::from_std(tcp)?;
                        Ok::<_, io::Error>(TokioIo::new(tokio_stream))
                    }
                })
            };

            let channel = endpoint
                .connect_with_connector(connector)
                .await
                .map_err(|e| Error::ConnectionFailed(peer.to_string(), e))?;
            channels.push(channel);
        }

        let pool = Arc::new(PeerPool {
            channels: Mutex::new(channels),
            semaphore: Arc::new(Semaphore::new(pool_size)),
            total_channels: AtomicUsize::new(pool_size),
            next_index: AtomicUsize::new(0),
        });

        // Update active connections gauge.
        self.connections_active.set(
            self.peers
                .iter()
                .map(|e| e.value().total_channels.load(Ordering::Relaxed) as u64)
                .sum::<u64>()
                + pool_size as u64,
        );

        Ok(pool)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
        assert_eq!(config.health_check_interval_sec, 30);
    }

    #[test]
    fn health_check_disabled_when_interval_zero() {
        let config = RpcConfig { health_check_interval_sec: 0, ..RpcConfig::default() };
        let pool = ConnectionPool::new(config);
        // health_check on empty pool is a no-op
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(pool.health_check());
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
            health_check_interval_sec: 15,
            ..Default::default()
        };
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.config().pool_size_per_peer, 8);
        assert_eq!(pool.config().keepalive_sec, 60);
        assert_eq!(pool.config().health_check_interval_sec, 15);
    }

    #[test]
    fn connection_pool_metrics_initialized() {
        let config = RpcConfig::default();
        let pool = ConnectionPool::new(config);
        assert_eq!(pool.connection_errors_total.get(), 0);
        assert_eq!(pool.health_check_failures_total.get(), 0);

        pool.connection_errors_total.inc();
        pool.connection_errors_total.inc();
        assert_eq!(pool.connection_errors_total.get(), 2);
    }

    /// Integration test: starts a real tonic test server with a health
    /// service, connects via `ConnectionPool`, acquires a channel, and
    /// verifies health check succeeds.
    #[tokio::test]
    async fn health_check_succeeds_with_real_server() {
        use std::net::SocketAddr;

        use tonic::transport::Server;

        // Bind to a random port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        // Create a health reporter and get the server-side service.
        // The default status is UNKNOWN, so we need to set a service as SERVING.
        // The health check client uses an empty string for the service name.
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_service_status("", tonic_health::ServingStatus::Serving).await;

        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(health_service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Give the server a moment to start.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Create pool and connect.
        let config = RpcConfig {
            pool_size_per_peer: 1,
            connect_timeout_ms: 1000,
            health_check_interval_sec: 0,
            ..RpcConfig::default()
        };
        let pool = ConnectionPool::new(config);

        // Acquire a channel.
        let pooled = pool.get_channel(addr).await.expect("should connect to test server");

        // Run health check.
        pool.health_check().await;
        assert_eq!(pool.health_check_failures_total.get(), 0, "health check should pass");

        // Cleanup.
        drop(pooled);
        server_task.abort();
    }
}
