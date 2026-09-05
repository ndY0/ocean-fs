//! Data-plane bundle (c4 — planes split).
//!
//! Owns the node's outward *data* surface: the shared data-plane
//! connection pool (client side of every data RPC) and the HTTP + gRPC
//! listener binds that expose the c3 server module's router and
//! data-plane services (segment/healing/cache/scrub). The membership
//! plane lives in `modules/membership.rs` (ADR-0028 D1: the two planes
//! are isolated by design — dedicated listener, dedicated pool).
//!
//! What stays in `Node::start()` is the ordering: this module's `serve`
//! must run after the server module built the router/services, and the
//! membership plane's `start_plane_and_join` must follow `serve` (peers
//! probe and deliver hinted handoffs to our gRPC listener immediately
//! after the join announcement).

use std::{net::SocketAddr, sync::Arc};

use oceanfs_core::{NodeConfig, RpcConfig};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The data-plane transport bundle (c4).
///
/// `pool` is the shared data-plane [`ConnectionPool`] every data RPC
/// client uses (constructed here per the §5 move); `grpc_addr` is the
/// strictly-parsed data-plane bind address (the §15 bind and the node's
/// advertised data address — review #64: no silent default addresses).
pub(crate) struct DataPlaneModule {
    /// The shared data-plane connection pool.
    pub(crate) pool: Arc<oceanfs_network::ConnectionPool>,
    /// Strictly-parsed data-plane gRPC bind address.
    pub(crate) grpc_addr: SocketAddr,
    /// HTTP bind address (bound by string, as the original §14 did).
    http_listen_addr: String,
    /// Perf 4.3 socket options applied to accepted data-plane connections.
    quickack: bool,
    busy_poll: u32,
}

/// The bound data-plane surface — returned by [`DataPlaneModule::serve`]
/// and consumed by `Node::start()` for the node fields and the
/// background gRPC handle.
pub(crate) struct BoundDataPlane {
    /// The bound HTTP server socket address.
    pub(crate) server_addr: SocketAddr,
    /// The data-plane gRPC bind address.
    pub(crate) grpc_addr: SocketAddr,
    /// HTTP graceful-shutdown token.
    pub(crate) http_shutdown: CancellationToken,
    /// gRPC graceful-shutdown token.
    pub(crate) grpc_shutdown: CancellationToken,
    /// The data-plane gRPC serve task handle (held by
    /// `BackgroundTasks` for shutdown).
    pub(crate) grpc_server_handle: JoinHandle<()>,
}

impl DataPlaneModule {
    /// Builds the data-plane transport bundle.
    ///
    /// Owns the construction previously inline in `Node::start()` §5
    /// (the data-plane pool + socket options). The binds themselves
    /// happen later, in [`Self::serve`], once the server module has
    /// produced the router + services.
    ///
    /// # Errors
    ///
    /// Returns an error when `grpc_listen_addr` does not parse (review
    /// #64 — no silent default network addresses).
    pub(crate) fn build(config: &NodeConfig) -> Result<Self, String> {
        // Strict parse of the data-plane bind address (the §15 bind and
        // the membership announce derivation consume the same value).
        let grpc_addr: SocketAddr = config
            .grpc_listen_addr
            .parse()
            .map_err(|e| format!("invalid grpc_listen_addr: {e}"))?;
        // [review][config][high]
        // no rpc config from config is operational, only the default values are used. rpc should be configurable
        // [end]
        let rpc_config = RpcConfig::default();
        let quickack = rpc_config.quickack;
        let busy_poll = rpc_config.busy_poll_us;
        let pool = Arc::new(oceanfs_network::ConnectionPool::new(rpc_config));
        Ok(DataPlaneModule {
            pool,
            grpc_addr,
            http_listen_addr: config.listen_addr.clone(),
            quickack,
            busy_poll,
        })
    }

    /// Binds and serves the data plane (§14/§15).
    ///
    /// Consumes the server module's axum `router` (HTTP) and the four
    /// tonic-wrapped data-plane services (`grpc`, decode caps already
    /// applied by the c3 module) and exposes them on the configured
    /// listeners. The HTTP bind failure is a hard startup error (as in
    /// the inline §14); the gRPC listener failure is logged and the
    /// serve task returns (as in the inline §15 — the task holds the
    /// listener logic). The membership plane's `start_plane_and_join`
    /// must be called AFTER this method returns: peers probe our gRPC
    /// listener the moment the join announcement lands.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP listener cannot bind.
    pub(crate) async fn serve(
        &self,
        router: axum::Router,
        grpc: crate::modules::server::DataPlaneServices,
    ) -> Result<BoundDataPlane, String> {
        // ---- HTTP bind (§14) ----
        let http_listener = tokio::net::TcpListener::bind(&self.http_listen_addr)
            .await
            .map_err(|e| format!("failed to bind HTTP server on {}: {e}", self.http_listen_addr))?;
        let server_addr = http_listener
            .local_addr()
            .map_err(|e| format!("failed to resolve HTTP listener address: {e}"))?;

        let http_shutdown = CancellationToken::new();
        let http_shutdown_signal = http_shutdown.clone();

        tokio::spawn(async move {
            if let Err(e) = axum::serve(http_listener, router.into_make_service())
                .with_graceful_shutdown(http_shutdown_signal.cancelled_owned())
                .await
            {
                tracing::error!("HTTP server error: {e}");
            }
        });

        // ---- Data-plane gRPC bind (§15) ----
        // The services were constructed (and decode-capped) by the c3
        // server module; the tonic router assembly lives at the bind.
        let grpc_router = tonic::transport::Server::builder()
            .add_service(grpc.segment)
            .add_service(grpc.healing)
            .add_service(grpc.cache)
            .add_service(grpc.scrub);

        // Create gRPC shutdown token before spawning so it can be used
        // by both the gRPC server and BackgroundTasks.
        let grpc_shutdown = CancellationToken::new();
        let _grpc_shutdown_signal = grpc_shutdown.clone();

        let grpc_addr = self.grpc_addr;
        let quickack = self.quickack;
        let busy_poll = self.busy_poll;
        let grpc_server_handle = tokio::spawn(async move {
            use std::os::unix::io::AsRawFd;

            use tokio_stream::StreamExt;

            let listener = match oceanfs_network::create_reuseport_listener(grpc_addr) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("gRPC listener creation failed for {grpc_addr}: {e}");
                    return;
                }
            };

            let stream =
                tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |conn| {
                    if let Ok(ref stream) = conn {
                        oceanfs_network::apply_opts_to_fd(stream.as_raw_fd(), quickack, busy_poll);
                    }
                    conn
                });

            if let Err(e) = grpc_router.serve_with_incoming(stream).await {
                tracing::error!("gRPC server error: {e}");
            }
        });

        Ok(BoundDataPlane {
            server_addr,
            grpc_addr,
            http_shutdown,
            grpc_shutdown,
            grpc_server_handle,
        })
    }
}
