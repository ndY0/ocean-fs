//! Remote-target mode — connect to already-running OceanFS processes.
//!
//! Per ADR-0019, cloud-based test phases (Phase 2+) run the harness on a
//! dedicated Harness VM and connect to OceanFS processes already running
//! on a separate SUT VM. The harness does **not** spawn processes in this
//! mode; it reaches the SUT over HTTP at `TARGET_HOST=<host>:9000`.
//!
//! This module provides:
//!
//! - [`RemoteNode`]: a single remote OceanFS endpoint (base URL + client).
//! - [`RemoteCluster`]: a [`LoadTarget`] implementation over one or more
//!   `RemoteNode`s, so the load generator and manifest verification run
//!   unchanged against remote targets.
//! - SSH crash control ([`RemoteCluster::kill_and_restart_via_ssh`]):
//!   the SUT process is managed by systemd on the SUT VM; the harness
//!   SIGKILLs and restarts the unit over SSH so the WAL crash-recovery
//!   phase of the load test works in remote mode too.
//!
//! ## Environment contract
//!
//! | Variable | Purpose |
//! |---|---|
//! | `TARGET_HOST` | Comma-separated `host:port` list of remote OceanFS endpoints (Phase 2: exactly one). |
//! | `TARGET_HOST_SSH` | SSH target for crash control, e.g. `root@10.0.0.5` or a `~/.ssh/config` alias like `oceanfs-sut`. When unset, remote crash-recovery is skipped (local quick mode always covers it). |
//! | `TARGET_SERVICE` | systemd unit name managing the SUT OceanFS process (default `oceanfs`). The unit must **not** auto-restart (`Restart=no`), otherwise the SIGKILL→restart sequencing is meaningless. |

use std::{net::SocketAddr, time::Duration};

use crate::harness::{Error, LoadTarget};

/// Builds a shared reqwest client for remote endpoints (30s timeout,
/// connection pool reused across requests).
fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client should build with default TLS")
}

/// A single remote OceanFS endpoint reached over HTTP.
///
/// Unlike [`NodeProcess`](crate::harness::NodeProcess) this handle owns
/// no child process: it only knows where the SUT listens and reuses one
/// reqwest client for every request.
#[derive(Debug)]
pub struct RemoteNode {
    /// Base URL, e.g. `http://10.0.0.5:9000`.
    base_url: String,
    /// HTTP client (connection pool reused across requests).
    client: reqwest::Client,
}

impl RemoteNode {
    /// Creates a handle for the given `host:port` endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the host:port pair does not parse as a socket
    /// address (e.g. `TARGET_HOST=not-an-address`).
    pub fn new(host_port: &str) -> Result<Self, Error> {
        let addr: SocketAddr = host_port
            .parse()
            .map_err(|e| Error::ClusterError(format!("invalid TARGET_HOST {host_port:?}: {e}")))?;
        Ok(Self { base_url: format!("http://{addr}"), client: build_client() })
    }

    /// Returns the base URL of this endpoint (`http://host:port`).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// HTTP GET to the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn get(&self, path: &str) -> Result<reqwest::Response, Error> {
        Ok(self.client.get(format!("{}{}", self.base_url, path)).send().await?)
    }

    /// HTTP PUT to the given path with a body.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn put(&self, path: &str, body: &[u8]) -> Result<reqwest::Response, Error> {
        Ok(self.client.put(format!("{}{}", self.base_url, path)).body(body.to_vec()).send().await?)
    }

    /// HTTP DELETE to the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn delete(&self, path: &str) -> Result<reqwest::Response, Error> {
        Ok(self.client.delete(format!("{}{}", self.base_url, path)).send().await?)
    }

    /// HTTP HEAD to the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn head(&self, path: &str) -> Result<reqwest::Response, Error> {
        Ok(self.client.head(format!("{}{}", self.base_url, path)).send().await?)
    }

    /// HTTP POST to the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn post(&self, path: &str) -> Result<reqwest::Response, Error> {
        Ok(self.client.post(format!("{}{}", self.base_url, path)).send().await?)
    }
}

/// A remote cluster: one or more already-running OceanFS endpoints.
///
/// Implements [`LoadTarget`] so the entire load generator and manifest
/// verification pipeline runs unchanged in remote-target mode. The
/// local-spawn path (`Cluster`) is preserved for CI.
#[derive(Debug)]
pub struct RemoteCluster {
    /// Remote endpoints, one per node.
    nodes: Vec<RemoteNode>,
    /// Shared HTTP client.
    client: reqwest::Client,
}

impl RemoteCluster {
    /// Connects to the endpoints listed in `TARGET_HOST`.
    ///
    /// Accepts a single `host:port` or a comma-separated list
    /// (`TARGET_HOSTS` style, for Phase 3+ multi-node remote targets).
    ///
    /// # Errors
    ///
    /// Returns an error if any endpoint fails to parse.
    pub fn connect(target_host: &str) -> Result<Self, Error> {
        let client = build_client();
        let mut hosts: Vec<&str> = target_host
            .split(',')
            .map(|host_port| host_port.trim())
            .filter(|h| !h.is_empty())
            .collect();
        if hosts.is_empty() {
            return Err(Error::ClusterError("TARGET_HOST is empty".into()));
        }
        let nodes = hosts.drain(..).map(RemoteNode::new).collect::<Result<Vec<_>, _>>()?;
        Ok(Self { nodes, client })
    }

    /// Returns the number of remote endpoints.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if there are no remote endpoints.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns the base URL of node `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds.
    pub fn base_url(&self, i: usize) -> &str {
        self.nodes[i].base_url()
    }

    /// Polls `/admin/health` on node 0 until it returns 2xx or the
    /// timeout elapses.
    ///
    /// # Errors
    ///
    /// Returns [`Error::HealthTimeout`] when the node never becomes
    /// healthy.
    pub async fn wait_for_health(&self, timeout: Duration) -> Result<(), Error> {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > timeout {
                return Err(Error::HealthTimeout(timeout));
            }
            match self.nodes[0].get("/admin/health").await {
                Ok(resp) if resp.status().is_success() => {
                    let _ = resp.bytes().await;
                    return Ok(());
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
    }

    /// Waits until `/admin/health` stops returning 2xx (the node is down)
    /// or the timeout elapses. Returns `true` if the node went down.
    async fn wait_health_down(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > timeout {
                return false;
            }
            match self.nodes[0].get("/admin/health").await {
                Ok(resp) if resp.status().is_success() => {}
                _ => return true,
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// SIGKILLs and restarts node 0's OceanFS service over SSH.
    ///
    /// Used by the Phase 2 crash-recovery phase in remote-target mode:
    /// `ssh <ssh_target> systemctl kill -s KILL <service>`, wait for the
    /// node to go down, then `systemctl restart <service>` and wait for
    /// health. The data directory persists on the SUT VM, so WAL replay
    /// exercises the same recovery path as local SIGKILL.
    ///
    /// The systemd unit must be configured with `Restart=no`, otherwise
    /// the service may come back before the harness observes it down.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Ssh`] if any SSH command fails, the node never
    /// goes down, or it never comes back healthy.
    pub async fn kill_and_restart_via_ssh(
        &self,
        ssh_target: &str,
        service: &str,
    ) -> Result<(), Error> {
        // 1. SIGKILL the service's main process.
        run_ssh(&[
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            ssh_target,
            "systemctl",
            "kill",
            "-s",
            "KILL",
            service,
        ])
        .await?;

        // 2. Wait for the node to go down.
        if !self.wait_health_down(Duration::from_secs(30)).await {
            return Err(Error::Ssh(format!(
                "node {service} did not go down within 30s of SIGKILL (is the unit Restart=no?)"
            )));
        }
        eprintln!("remote: node down after SIGKILL, restarting {service} via ssh");

        // 3. Give systemd a moment to settle the unit state and the OS
        //    to release the listen socket.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 4. Restart the service.
        run_ssh(&[
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            ssh_target,
            "systemctl",
            "restart",
            service,
        ])
        .await?;

        // 5. Wait for health.
        self.wait_for_health(Duration::from_secs(60)).await
    }
}

impl LoadTarget for RemoteCluster {
    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn node_addr(&self, i: usize) -> SocketAddr {
        // Re-derive the socket address from the base URL's host:port.
        let url = self.nodes[i].base_url();
        let host_port = url.strip_prefix("http://").unwrap_or(url);
        host_port.parse().expect("validated at construction")
    }

    fn client(&self) -> &reqwest::Client {
        &self.client
    }

    async fn get(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.nodes[i].get(path).await
    }

    async fn put(&self, i: usize, path: &str, body: &[u8]) -> Result<reqwest::Response, Error> {
        self.nodes[i].put(path, body).await
    }

    async fn delete(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.nodes[i].delete(path).await
    }

    async fn head(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.nodes[i].head(path).await
    }

    async fn post(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.nodes[i].post(path).await
    }
}

/// Runs an `ssh` command to completion and checks its exit status.
///
/// Runs on the blocking pool — `ssh` is a synchronous child process.
///
/// # Errors
///
/// Returns [`Error::Ssh`] when ssh cannot be spawned, exits non-zero, or
/// the blocking task panics.
async fn run_ssh(args: &[&str]) -> Result<(), Error> {
    // Own the arguments so the blocking closure is `'static`.
    let args: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    let args_display = format!("{args:?}");
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("ssh")
            .args(&args)
            .status()
            .map_err(|e| Error::Ssh(format!("failed to spawn ssh {args:?}: {e}")))
    })
    .await
    .map_err(|e| Error::Ssh(format!("ssh task join failed: {e}")))??;
    if !status.success() {
        return Err(Error::Ssh(format!("ssh {args_display} exited with {status}")));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_node_parses_host_port() {
        let node = RemoteNode::new("127.0.0.1:9000").expect("valid endpoint");
        assert_eq!(node.base_url(), "http://127.0.0.1:9000");
    }

    #[test]
    fn remote_node_rejects_invalid_host_port() {
        let err = RemoteNode::new("not-an-address").expect_err("invalid endpoint must fail");
        assert!(err.to_string().contains("invalid TARGET_HOST"));
    }

    #[test]
    fn remote_cluster_connects_single_host() {
        let cluster = RemoteCluster::connect("10.0.0.5:9000").expect("connect");
        assert_eq!(cluster.len(), 1);
        assert_eq!(cluster.base_url(0), "http://10.0.0.5:9000");
        assert_eq!(cluster.node_addr(0), "10.0.0.5:9000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn remote_cluster_connects_multiple_hosts() {
        let cluster = RemoteCluster::connect("10.0.0.5:9000, 10.0.0.6:9001").expect("connect");
        assert_eq!(cluster.len(), 2);
        assert_eq!(cluster.base_url(1), "http://10.0.0.6:9001");
    }

    #[test]
    fn remote_cluster_rejects_empty_host_list() {
        let err = RemoteCluster::connect("").expect_err("empty host list must fail");
        assert!(err.to_string().contains("TARGET_HOST is empty"));
    }
}
