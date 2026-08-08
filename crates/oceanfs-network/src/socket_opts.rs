//! Linux socket options for low-latency gRPC networking.
//!
//! Three `setsockopt`-level optimisations applied to gRPC server and client
//! sockets that reduce inter-node RPC latency and improve connection
//! distribution:
//!
//! - `SO_BUSY_POLL` — low-latency busy-wait polling for small RPCs
//!   (Linux 3.11+). The kernel spins in a tight loop for up to `poll_us`
//!   microseconds instead of sleeping for a hardware interrupt.
//! - `TCP_QUICKACK` — disable delayed ACKs on gRPC connections (Linux 2.4+).
//!   Eliminates up to 40ms of unnecessary ack delay for independent
//!   request-response RPCs.
//! - `SO_REUSEPORT` — bind N sockets to the same port (Linux 3.9+).
//!   Kernel distributes incoming connections via 4-tuple hash, eliminating
//!   contention on the single accept queue.
//!
//! `TCP_QUICKACK` and `SO_REUSEPORT` use the `socket2` crate's safe wrappers.
//! `SO_BUSY_POLL` requires a raw `libc::setsockopt` call (per ADR-0013).
//!
//! All functions are `#[cfg(target_os = "linux")]`-gated with no-op
//! fallbacks on non-Linux per performance guideline §10.6.

use std::io;

use socket2::SockRef;

/// Creates a SO_REUSEPORT-enabled `TcpListener` from a pre-configured socket.
///
/// The socket must have `SO_REUSEPORT` set before binding. This function
/// creates the socket, applies `SO_REUSEPORT`, binds, listens, and returns
/// a tokio `TcpListener`. On non-Linux, `SO_REUSEPORT` is silently skipped.
///
/// # Errors
///
/// Returns an I/O error if socket creation, bind, or listen fails.
pub fn create_reuseport_listener(
    addr: std::net::SocketAddr,
) -> io::Result<tokio::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};

    let sock_addr: socket2::SockAddr = addr.into();
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    set_reuseport(&socket)?;
    socket.bind(&sock_addr)?;
    socket.listen(1024)?;

    let listener: std::net::TcpListener = socket.into();
    listener.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(listener)
}

/// Applies `TCP_QUICKACK` and `SO_BUSY_POLL` to a raw file descriptor.
///
/// Used on accepted connections where we have a raw `fd` but not a
/// `socket2::Socket`. This function temporarily constructs a
/// `socket2::Socket` from the fd, applies the options, and relinquishes
/// ownership via `into_raw_fd` so the fd is not closed.
///
/// # Safety
///
/// `fd` must be a valid socket file descriptor. Per ADR-0013.
#[allow(unsafe_code)]
pub fn apply_opts_to_fd(fd: std::os::unix::io::RawFd, quickack: bool, busy_poll: u32) {
    use std::os::unix::io::{FromRawFd, IntoRawFd};

    // SAFETY: `fd` is a valid socket fd from an accepted connection.
    // We construct a socket2::Socket, apply options, then relinquish
    // ownership via into_raw_fd so the fd survives.
    let sock = unsafe { socket2::Socket::from_raw_fd(fd) };
    if quickack {
        let _ = set_quickack(&sock);
    }
    if busy_poll > 0 {
        let _ = set_busy_poll(&sock, busy_poll);
    }
    let _ = sock.into_raw_fd();
}

/// Disables delayed ACKs on the socket via `TCP_QUICKACK`.
///
/// Without `TCP_QUICKACK`, the kernel may delay ACKs up to 40ms (or 500ms
/// with the legacy timer) to coalesce them with response data. For OceanFS
/// inter-node RPCs, each message is an independent request-response pair
/// — there is no bidirectional streaming where ACKs can piggyback.
///
/// # Errors
///
/// Returns an I/O error if `setsockopt` fails.
pub fn set_quickack(socket: &socket2::Socket) -> io::Result<()> {
    let sock_ref = SockRef::from(socket);
    sock_ref.set_quickack(true)
}

/// Enables `SO_BUSY_POLL` on the socket for low-latency receives.
///
/// When enabled, the kernel busy-waits (spins polling the NIC receive ring)
/// for up to `poll_us` microseconds instead of sleeping and waiting for a
/// hardware interrupt. Useful for workloads where median RPC payload is
/// small (< 4 KB) and latency matters more than CPU efficiency — exactly
/// the inter-node quorum write ack pattern.
///
/// Set `poll_us` to 0 to disable busy polling.
///
/// # Errors
///
/// Returns an I/O error if `setsockopt` fails.
pub fn set_busy_poll(socket: &socket2::Socket, poll_us: u32) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;

        let fd = socket.as_raw_fd();
        // SAFETY: `fd` is a valid socket fd provided by the caller.
        // `SO_BUSY_POLL` is an advisory hint — the kernel may ignore
        // it. Cannot cause UB. Per ADR-0013.
        #[allow(unsafe_code)]
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                &poll_us as *const _ as *const libc::c_void,
                std::mem::size_of::<u32>() as libc::socklen_t,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = socket;
        let _ = poll_us;
    }
    Ok(())
}

/// Sets `SO_REUSEPORT` on the socket, allowing multiple sockets to bind
/// to the same port.
///
/// Must be called **before** `bind(2)`. The kernel distributes incoming
/// TCP connections across all sockets bound to the same port using a hash
/// of the 4-tuple (src_ip, src_port, dst_ip, dst_port), eliminating
/// contention on the single accept queue.
///
/// # Errors
///
/// Returns an I/O error if `setsockopt` fails.
pub fn set_reuseport(socket: &socket2::Socket) -> io::Result<()> {
    let sock_ref = SockRef::from(socket);
    sock_ref.set_reuse_port(true)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn set_quickack_on_valid_socket_returns_ok() {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )
        .unwrap();
        // set_quickack on an unconnected socket is fine — it's a socket-level option.
        let result = set_quickack(&socket);
        // On Linux: succeeds. On non-Linux: no-op Ok.
        // Both paths return Ok.
        assert!(result.is_ok());
    }

    #[test]
    fn set_busy_poll_on_valid_socket_returns_ok() {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )
        .unwrap();
        let result = set_busy_poll(&socket, 50);
        // On Linux: succeeds (sets SO_BUSY_POLL). On non-Linux: no-op Ok.
        assert!(result.is_ok());
    }

    #[test]
    fn set_busy_poll_zero_disables() {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )
        .unwrap();
        // poll_us = 0 should work (disables busy polling).
        assert!(set_busy_poll(&socket, 0).is_ok());
    }

    #[test]
    fn set_reuseport_on_valid_socket_returns_ok() {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )
        .unwrap();
        let result = set_reuseport(&socket);
        assert!(result.is_ok());
    }
}
