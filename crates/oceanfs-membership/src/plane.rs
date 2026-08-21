//! The membership plane (ADR-0028 D1): dedicated transport resources for
//! the gossip + SWIM probe protocol, isolated from the data plane.
//!
//! The membership protocol runs on its own listener, connection pool, and
//! announced address so that probe latency is never coupled to the data
//! plane's behavior (16 MiB replica streams, hinted-handoff batches,
//! healing transfers). The fleet churn campaign measured gossip push
//! (the only liveness signal at the time) at 28 ms p50 / 195 ms p99 while
//! sharing the data-plane pool — the shared semaphore queued pings behind
//! in-flight streams.

use std::net::SocketAddr;

use oceanfs_core::RpcConfig;
use oceanfs_network::ConnectionPool;

/// Default membership plane port, offset from the data-plane gRPC port.
pub const DEFAULT_MEMBERSHIP_PORT: u16 = 9002;

/// Derives the announced membership address from the listen address.
///
/// A listen address of `0.0.0.0:9002` binds all interfaces but is not
/// reachable by peers (they would dial themselves — the phase-3 fleet
/// failure class fixed for the gRPC address in `sut-deploy.sh`). When
/// `advertise_ip` is given, the port of `listen` is kept and the IP is
/// replaced. An explicit (non-any) IP in `listen` is returned as-is.
///
/// # Examples
///
/// ```
/// use oceanfs_membership::plane::membership_address;
///
/// let addr = membership_address("0.0.0.0:9002", Some("10.0.0.2"));
/// assert_eq!(addr.to_string(), "10.0.0.2:9002");
///
/// let explicit = membership_address("10.0.0.3:9002", None);
/// assert_eq!(explicit.to_string(), "10.0.0.3:9002");
/// ```
pub fn membership_address(listen: &str, advertise_ip: Option<&str>) -> SocketAddr {
    let parsed: SocketAddr = listen
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], DEFAULT_MEMBERSHIP_PORT)));
    match advertise_ip {
        Some(ip) if parsed.ip().is_unspecified() => match ip.parse::<std::net::IpAddr>() {
            Ok(ip) => SocketAddr::new(ip, parsed.port()),
            Err(_) => parsed,
        },
        _ => parsed,
    }
}

/// Builds the membership plane's dedicated connection pool.
///
/// The pool is deliberately small (2 channels per peer): the plane's
/// traffic is tiny RPCs (probes, deltas), and probes additionally use a
/// fresh channel with a hard per-call deadline rather than waiting on
/// this semaphore (the detector feature wires that). The connect timeout
/// is derived from `ping_timeout_ms` so a down peer fails the probe
/// budget, not the TCP handshake budget.
///
/// # Examples
///
/// ```
/// use oceanfs_membership::plane::membership_pool;
///
/// let pool = membership_pool(1000, None);
/// assert_eq!(pool.config().pool_size_per_peer, 2);
/// ```
pub fn membership_pool(
    ping_timeout_ms: u64,
    tls_cert_path: Option<std::path::PathBuf>,
) -> std::sync::Arc<ConnectionPool> {
    let config = RpcConfig {
        pool_size_per_peer: 2,
        // A down peer must fail the ping budget (connect + RPC deadline),
        // not stall past it.
        connect_timeout_ms: ping_timeout_ms,
        request_timeout_ms: ping_timeout_ms,
        // Health checks are the data plane's job; probes are their own
        // health signal.
        health_check_interval_sec: 0,
        tls_cert_path,
        ..RpcConfig::default()
    };
    std::sync::Arc::new(ConnectionPool::new(config))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn any_listen_with_advertise_ip_substitutes_ip() {
        let addr = membership_address("0.0.0.0:9002", Some("10.0.0.2"));
        assert_eq!(addr.to_string(), "10.0.0.2:9002");
    }

    #[test]
    fn any_listen_without_advertise_ip_keeps_any() {
        let addr = membership_address("0.0.0.0:9002", None);
        assert_eq!(addr.to_string(), "0.0.0.0:9002");
    }

    #[test]
    fn explicit_ip_listen_is_returned_as_is() {
        let addr = membership_address("10.0.0.3:9002", Some("10.0.0.2"));
        assert_eq!(addr.to_string(), "10.0.0.3:9002");
    }

    #[test]
    fn bad_listen_falls_back_to_default_port() {
        let addr = membership_address("not-an-addr", None);
        assert_eq!(addr.port(), DEFAULT_MEMBERSHIP_PORT);
    }

    #[test]
    fn pool_uses_probe_derived_timeouts() {
        let pool = membership_pool(1500, None);
        let cfg = pool.config();
        assert_eq!(cfg.pool_size_per_peer, 2);
        assert_eq!(cfg.connect_timeout_ms, 1500);
        assert_eq!(cfg.request_timeout_ms, 1500);
    }
}
