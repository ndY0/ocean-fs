//! Test harness for spawning and controlling an OceanFS node process.
//!
//! Provides [`NodeProcess`], a handle that spawns the OceanFS release
//! binary as a child process, waits for it to become healthy, and exposes
//! HTTP helpers (`get`, `put`, `delete`) plus lifecycle control (`kill`,
//! `shutdown`).
//!
//! ## Port Assignment
//!
//! Each node process binds to unique OS-assigned ephemeral ports. Before
//! spawning, we bind temporary sockets to `127.0.0.1:0`, record the
//! assigned ports, drop the sockets, and pass the ports to the binary via
//! TOML config. This avoids port conflicts when tests run in parallel.
//!
//! ## Config Templates
//!
//! The harness provides config helpers for each test scenario (standard,
//! short-GC, short-AE, prefetch-enabled, etc.). These generate valid TOML
//! strings with the appropriate settings and ports.

use std::{
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during harness operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to bind a temporary socket for port discovery.
    #[error("port discovery failed: {0}")]
    PortDiscovery(std::io::Error),
    /// Failed to write the TOML config file.
    #[error("config write failed: {0}")]
    ConfigWrite(std::io::Error),
    /// Failed to spawn the OceanFS binary.
    #[error("spawn failed: {0}")]
    Spawn(std::io::Error),
    /// The node process exited prematurely.
    #[error("process exited prematurely with status: {0}")]
    PrematureExit(String),
    /// Health endpoint did not become ready within the timeout.
    #[error("health check timeout after {0:?}")]
    HealthTimeout(Duration),
    /// HTTP request to the node failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    /// A cluster node returned an unexpected response.
    #[error("cluster node error: {0}")]
    ClusterError(String),
}

// ---------------------------------------------------------------------------
// NodeProcess
// ---------------------------------------------------------------------------

/// A handle to a running OceanFS node process.
///
/// Owns the child process, its temp data directory, and the HTTP/gRPC
/// addresses. Dropping this struct does **not** kill the process — call
/// [`shutdown`](NodeProcess::shutdown) or [`kill`](NodeProcess::kill)
/// explicitly.
///
/// For WAL crash-recovery tests, use [`spawn_with_data_dir`](NodeProcess::spawn_with_data_dir)
/// to provide a persistent data directory that survives process kill/restart cycles.
///
/// # Examples
///
/// ```no_run
/// use e2e::harness::{config_standard, NodeProcess};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let node = NodeProcess::spawn(&config_standard()).await?;
/// let resp = node.get("/admin/health").await?;
/// assert_eq!(resp.status(), 200);
/// node.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct NodeProcess {
    /// The spawned child process.
    child: Child,
    /// HTTP API address.
    http_addr: SocketAddr,
    /// gRPC API address.
    grpc_addr: SocketAddr,
    /// Temporary data directory (cleaned up on drop of `_temp_dir`).
    /// Path is accessible even after `_temp_dir` is consumed.
    data_dir: PathBuf,
    /// RAII guard for temp directory cleanup. `None` when using a
    /// custom (user-managed) data directory.
    _temp_dir: Option<TempDir>,
    /// Path to the config file (inside `data_dir`).
    _config_path: PathBuf,
    /// HTTP client (connection pool reused across requests).
    client: reqwest::Client,
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        // Kill the child process if it's still running so that panics
        // and early returns don't leave orphaned processes behind.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl NodeProcess {
    /// Spawns an OceanFS node with the given TOML configuration string.
    ///
    /// Creates a temporary data directory that is cleaned up when the
    /// `NodeProcess` is dropped or shut down.
    ///
    /// The configuration string must include at least `listen_addr` and
    /// `grpc_listen_addr`. The `{http_port}` and `{grpc_port}` placeholders
    /// in the config string are replaced with automatically discovered
    /// ephemeral ports.
    ///
    /// # Errors
    ///
    /// Returns an error if the binary cannot be found, the process fails
    /// to start, or the health endpoint does not respond within the
    /// timeout (30 seconds).
    pub async fn spawn(config_toml: &str) -> Result<Self, Error> {
        let temp_dir = TempDir::new().map_err(Error::ConfigWrite)?;
        let data_dir = temp_dir.path().to_path_buf();
        Self::spawn_inner(config_toml, data_dir, Some(temp_dir), false).await
    }

    /// Spawns an OceanFS node using a specific (possibly pre-existing)
    /// data directory.
    ///
    /// The data directory is **not** cleaned up automatically — the caller
    /// is responsible for cleanup. This is used for WAL crash-recovery
    /// tests where the same data directory must survive across process
    /// kill/restart cycles.
    ///
    /// # Errors
    ///
    /// Returns an error if the binary cannot be found, the process fails
    /// to start, or the health endpoint does not respond within the
    /// timeout (30 seconds).
    pub async fn spawn_with_data_dir(config_toml: &str, data_dir: &Path) -> Result<Self, Error> {
        Self::spawn_inner(config_toml, data_dir.to_path_buf(), None, true).await
    }

    /// Internal spawn logic shared by `spawn` and `spawn_with_data_dir`.
    ///
    /// `create_dir` controls whether the data directory is created if it
    /// doesn't exist (true for custom dirs, false for temp dirs since
    /// TempDir already created them).
    async fn spawn_inner(
        config_toml: &str,
        data_dir: PathBuf,
        temp_dir: Option<TempDir>,
        create_dir: bool,
    ) -> Result<Self, Error> {
        // ---- 1. Discover available ports ----
        let http_listener = TcpListener::bind("127.0.0.1:0").map_err(Error::PortDiscovery)?;
        let http_addr = http_listener.local_addr().map_err(Error::PortDiscovery)?;
        drop(http_listener);

        let grpc_listener = TcpListener::bind("127.0.0.1:0").map_err(Error::PortDiscovery)?;
        let grpc_addr = grpc_listener.local_addr().map_err(Error::PortDiscovery)?;
        drop(grpc_listener);

        // ---- 2. Ensure data directory exists ----
        if create_dir {
            std::fs::create_dir_all(&data_dir).map_err(Error::ConfigWrite)?;
        }

        // ---- 3. Build config with resolved ports ----
        let resolved_config = config_toml
            .replace("{http_port}", &http_addr.port().to_string())
            .replace("{grpc_port}", &grpc_addr.port().to_string());

        let full_config = format!(
            "data_dir = \"{data_dir_path}\"\n{resolved_config}",
            data_dir_path = data_dir.display(),
            resolved_config = resolved_config
        );

        let config_path = data_dir.join("oceanfs.toml");
        std::fs::write(&config_path, &full_config).map_err(Error::ConfigWrite)?;

        // ---- 4. Find the binary ----
        let bin_path = resolve_binary_path();

        // ---- 5. Spawn the process ----
        let child = Command::new(&bin_path)
            .arg("--config")
            .arg(&config_path)
            .arg("--log-level")
            .arg("error")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(Error::Spawn)?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client should build with default TLS");

        let mut node = NodeProcess {
            child,
            http_addr,
            grpc_addr,
            data_dir,
            _temp_dir: temp_dir,
            _config_path: config_path,
            client,
        };

        // ---- 6. Wait for health endpoint ----
        node.wait_for_health(Duration::from_secs(30)).await?;

        Ok(node)
    }

    /// Returns the data directory path.
    ///
    /// Useful for WAL recovery tests that need to respawn with the
    /// same data directory after killing the process.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Returns the HTTP API address (e.g., `127.0.0.1:9000`).
    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    /// Returns the gRPC API address (e.g., `127.0.0.1:9001`).
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    // ------------------------------------------------------------------
    // HTTP helpers
    // ------------------------------------------------------------------

    /// Performs an HTTP GET request to the given path on the node.
    ///
    /// The path should start with `/`, e.g., `/admin/health` or
    /// `/my-bucket/my-key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails (connection refused,
    /// timeout, etc.).
    pub async fn get(&self, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.http_addr, path);
        Ok(self.client.get(&url).send().await?)
    }

    /// Performs an HTTP PUT request to the given path with a body.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn put(&self, path: &str, body: &[u8]) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.http_addr, path);
        Ok(self.client.put(&url).body(body.to_vec()).send().await?)
    }

    /// Performs an HTTP DELETE request to the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn delete(&self, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.http_addr, path);
        Ok(self.client.delete(&url).send().await?)
    }

    /// Performs an HTTP POST request to the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn post(&self, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.http_addr, path);
        Ok(self.client.post(&url).send().await?)
    }

    /// Sends a HEAD request to the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn head(&self, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.http_addr, path);
        Ok(self.client.head(&url).send().await?)
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Sends SIGKILL to the child process (hard kill for crash recovery
    /// tests).
    ///
    /// After calling this, the process is dead. Use `respawn` or create
    /// a new `NodeProcess` to start again.
    pub fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill()
    }

    /// Sends SIGTERM to the child process for graceful shutdown, then
    /// waits for the process to exit.
    ///
    /// On Unix, sends SIGTERM via the `kill` command. On non-Unix,
    /// falls back to SIGKILL.
    ///
    /// # Errors
    ///
    /// Returns an error if the signal cannot be sent or the process
    /// doesn't exit within the timeout.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        // Send SIGTERM for graceful shutdown.
        let pid = self.child.id();
        #[cfg(unix)]
        {
            // Use the `kill` command to send SIGTERM.
            let _ = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
        }

        // Wait for process to exit with timeout.
        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();
        loop {
            match self.child.try_wait() {
                Ok(Some(_status)) => return Ok(()),
                Ok(None) => {
                    if start.elapsed() > timeout {
                        // Force kill if still running.
                        let _ = self.child.kill();
                        let _ = self.child.wait();
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(Error::Spawn(e)),
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    /// Polls `/admin/health` until it returns 200 or the timeout expires.
    async fn wait_for_health(&mut self, timeout: Duration) -> Result<(), Error> {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > timeout {
                // Check if process is still alive.
                match self.child.try_wait() {
                    Ok(Some(status)) => {
                        return Err(Error::PrematureExit(format!("{status:?}")));
                    }
                    Ok(None) => {} // still running
                    Err(e) => return Err(Error::Spawn(e)),
                }
                return Err(Error::HealthTimeout(timeout));
            }

            match self.get("/admin/health").await {
                Ok(resp) if resp.status().is_success() => {
                    let _ = resp.bytes().await;
                    return Ok(());
                }
                Ok(_) => {
                    // Got a response but not 200; keep waiting.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(_) => {
                    // Connection refused or similar; keep waiting.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Binary resolution
// ---------------------------------------------------------------------------

/// Resolves the path to the OceanFS release binary.
///
/// Checks in order:
/// 1. `OCEANFS_BIN` environment variable
/// 2. `target/release/oceanfs` relative to workspace root
/// 3. `target/debug/oceanfs` relative to workspace root
fn resolve_binary_path() -> PathBuf {
    if let Ok(path) = std::env::var("OCEANFS_BIN") {
        let p = PathBuf::from(&path);
        if p.exists() {
            return p;
        }
    }

    // Find workspace root by walking up from the current exe or cwd.
    let candidates =
        [PathBuf::from("target/release/oceanfs"), PathBuf::from("target/debug/oceanfs")];

    for candidate in &candidates {
        if candidate.exists() {
            return candidate.clone();
        }
    }

    // Last resort: try to find it relative to the manifest dir.
    if let Ok(manifest) = std::env::var("CARGO_MANIFEST_DIR") {
        let workspace_root = Path::new(&manifest).parent().unwrap_or(Path::new("."));
        for candidate in &candidates {
            let full = workspace_root.join(candidate);
            if full.exists() {
                return full;
            }
        }
    }

    // Fallback to just "oceanfs" (hope it's on PATH).
    PathBuf::from("oceanfs")
}

// ---------------------------------------------------------------------------
// Config templates
// ---------------------------------------------------------------------------

/// Standard node configuration for general smoke tests.
///
/// Uses OS-assigned ephemeral ports (`{http_port}`, `{grpc_port}`)
/// which are replaced at spawn time. All caches and durability features
/// are enabled with default intervals.
pub fn config_standard() -> String {
    r#"
node_id = "e2e-standard"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
log_level = "error"
prefetch_enabled = false
"#
    .to_string()
}

/// Node configuration with prefetch enabled.
///
/// Enables the prefetch engine so that LIST/GET triggers cache warming.
pub fn config_prefetch_enabled() -> String {
    r#"
node_id = "e2e-prefetch"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
log_level = "error"
prefetch_enabled = true
"#
    .to_string()
}

/// Configuration with shortened GC intervals for testing garbage
/// collection.
///
/// Uses the configurable `gc_interval_sec` and `tombstone_ttl_sec`
/// fields introduced in commit ddc87ad. GC runs every 10 seconds,
/// tombstones expire after 5 seconds.
pub fn config_short_gc() -> String {
    r#"
node_id = "e2e-gc"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
log_level = "error"
prefetch_enabled = false
gc_interval_sec = 10
tombstone_ttl_sec = 5
"#
    .to_string()
}

/// Configuration with shortened anti-entropy intervals.
///
/// Uses the configurable `ae_interval_sec` field introduced in
/// commit ddc87ad. AE runs every 10 seconds.
pub fn config_short_ae() -> String {
    r#"
node_id = "e2e-ae"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
log_level = "error"
prefetch_enabled = false
ae_interval_sec = 10
"#
    .to_string()
}

// ---------------------------------------------------------------------------
// Cluster config templates
// ---------------------------------------------------------------------------

/// Standard 3-node cluster configuration: W=2, R=2, N=3, replication_factor=3.
///
/// Each node gets a unique `node_id` and `{http_port}`/`{grpc_port}` placeholders
/// are replaced at spawn time. The `{seed_node}` placeholder is replaced with
/// node-0's gRPC address for nodes 1+.
pub fn config_3node_w2_r2() -> String {
    r#"
node_id = "e2e-c3n-0"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
log_level = "error"
prefetch_enabled = false
write_quorum = 2
read_quorum = 2
replication_factor = 3
"#
    .to_string()
}

/// Configuration with shortened gossip interval (1s) for fast convergence tests.
///
/// Gossip push happens every second instead of the default 30s, allowing
/// convergence assertions within reasonable test timeouts.
pub fn config_fast_gossip() -> String {
    r#"
node_id = "e2e-gossip-0"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
log_level = "error"
prefetch_enabled = false

[gossip]
interval_ms = 100
"#
    .to_string()
}

/// Configuration with shortened failure detection intervals for SWIM tests.
///
/// Suspicion timeout is 2s, failure timeout is 5s, allowing failure
/// detection assertions within reasonable test timeouts.
pub fn config_fast_swim() -> String {
    r#"
node_id = "e2e-swim-0"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
log_level = "error"
prefetch_enabled = false

[gossip]
interval_ms = 50
suspicion_timeout_ms = 2000
failure_timeout_ms = 5000
"#
    .to_string()
}

/// Configuration with shortened anti-entropy interval (10s) for Merkle
/// exchange tests.
///
/// Anti-entropy runs every 10 seconds instead of the default 300s.
pub fn config_fast_ae() -> String {
    r#"
node_id = "e2e-ae-0"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
log_level = "error"
prefetch_enabled = false
ae_interval_sec = 10
"#
    .to_string()
}

// ---------------------------------------------------------------------------
// Helper functions for tests
// ---------------------------------------------------------------------------

/// Generates a vector of random bytes of the given length.
///
/// Uses a simple PRNG seeded from entropy. Suitable for test data
/// generation; not cryptographically secure.
pub fn random_bytes(len: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.gen()).collect()
}

/// Polls a condition function every `interval` until it returns `true`
/// or `timeout` elapses.
///
/// Returns `true` if the condition succeeded, `false` on timeout.
pub async fn poll_until<F, Fut>(interval: Duration, timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    loop {
        if condition().await {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Reads the body of an HTTP response as bytes.
pub async fn response_bytes(resp: reqwest::Response) -> Vec<u8> {
    resp.bytes().await.map(|b| b.to_vec()).unwrap_or_default()
}

/// Reads the body of an HTTP response as a string.
pub async fn response_text(resp: reqwest::Response) -> String {
    resp.text().await.unwrap_or_default()
}

/// Parses an HTTP response body as JSON.
pub async fn response_json<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, serde_json::Error> {
    let text = resp.text().await.unwrap_or_default();
    serde_json::from_str(&text)
}

// ---------------------------------------------------------------------------
// Cluster harness
// ---------------------------------------------------------------------------

/// A managed cluster of N OceanFS nodes running as child processes.
///
/// All nodes share a common temporary directory root; each node gets
/// its own subdirectory (`node-0/`, `node-1/`, etc.). The first node
/// starts without seed nodes; subsequent nodes use `nodes[0]`'s gRPC
/// address as their seed for cluster discovery.
///
/// # Examples
///
/// ```no_run
/// use e2e::harness::{config_3node_w2_r2, Cluster};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await?;
/// cluster.wait_for_convergence(3).await?;
/// let resp = cluster.get(0, "/admin/cluster").await?;
/// assert_eq!(resp.status(), 200);
/// cluster.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct Cluster {
    /// All node processes in this cluster.
    nodes: Vec<Option<NodeProcess>>,
    /// Shared temporary directory root (cleaned on drop).
    _temp_dir: TempDir,
    /// Base config template string.
    base_config: String,
    /// HTTP client for admin polling.
    #[allow(dead_code)]
    client: reqwest::Client,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // Kill any remaining node processes so that panics don't leave
        // orphaned oceanfs instances consuming CPU and ports.
        for node in self.nodes.iter_mut().flatten() {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
    }
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("node_count", &self.nodes.len())
            .field("alive_count", &self.alive_count())
            .finish_non_exhaustive()
    }
}

impl Cluster {
    /// Spawns `count` nodes. The first node starts without seed nodes;
    /// subsequent nodes use `nodes[0]`'s gRPC address as their seed.
    ///
    /// Each node gets a unique `node_id` (by replacing `-0` with `-{i}`
    /// in the config template) and its own data subdirectory under a
    /// shared temporary directory root.
    ///
    /// # Errors
    ///
    /// Returns an error if any node fails to spawn or become healthy.
    pub async fn spawn(count: usize, base_config: &str) -> Result<Self, Error> {
        assert!(count > 0, "Cluster must have at least 1 node");

        let _temp_dir = TempDir::new().map_err(Error::ConfigWrite)?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client should build with default TLS");

        let mut nodes: Vec<Option<NodeProcess>> = Vec::with_capacity(count);

        // Spawn node 0 first (no seeds).
        let node0_config = build_node_config(base_config, 0, "");
        let node0_dir = _temp_dir.path().join("node-0");
        let node0 = NodeProcess::spawn_with_data_dir(&node0_config, &node0_dir).await?;
        let seed_addr = format!("127.0.0.1:{}", node0.grpc_addr().port());
        nodes.push(Some(node0));

        // Spawn remaining nodes with node 0 as seed.
        for i in 1..count {
            let node_config = build_node_config(base_config, i, &seed_addr);
            let node_dir = _temp_dir.path().join(format!("node-{i}"));
            let node = NodeProcess::spawn_with_data_dir(&node_config, &node_dir).await?;
            nodes.push(Some(node));
        }

        Ok(Self { nodes, _temp_dir, base_config: base_config.to_string(), client })
    }

    /// Returns a reference to node `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds or if the node has been killed
    /// without being restarted.
    pub fn node(&self, i: usize) -> &NodeProcess {
        self.nodes[i].as_ref().expect("node has been killed and not restarted")
    }

    /// Returns the number of nodes in the cluster (including killed ones).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the cluster has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// HTTP GET from node `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds or the node is killed.
    pub async fn get(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.node(i).get(path).await
    }

    /// HTTP PUT to node `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds or the node is killed.
    pub async fn put(&self, i: usize, path: &str, body: &[u8]) -> Result<reqwest::Response, Error> {
        self.node(i).put(path, body).await
    }

    /// HTTP DELETE from node `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds or the node is killed.
    pub async fn delete(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.node(i).delete(path).await
    }

    /// Kill node `i` with SIGKILL (hard crash for failure tests).
    ///
    /// After calling this, `node(i)` will panic until `restart(i)` is called.
    /// The node's data directory is preserved for restart.
    pub fn kill(&mut self, i: usize) -> std::io::Result<()> {
        let mut node = self.nodes[i].take().expect("node already killed");
        let result = node.kill();
        // Drop the NodeProcess handle so the OS releases the process.
        drop(node);
        result
    }

    /// Restart a previously killed node `i` with its original data directory.
    ///
    /// The node is respawned with the same config (and thus same ports,
    /// though the OS may assign different ports if the original ones were
    /// released). For cluster rejoin tests, the seed is still configured.
    pub async fn restart(&mut self, i: usize) -> Result<(), Error> {
        let seed = if i == 0 {
            String::new()
        } else {
            self.nodes[0]
                .as_ref()
                .map(|n| format!("127.0.0.1:{}", n.grpc_addr().port()))
                .unwrap_or_default()
        };

        let node_dir = self._temp_dir.path().join(format!("node-{i}"));
        let node_config = build_node_config(&self.base_config, i, &seed);
        let node = NodeProcess::spawn_with_data_dir(&node_config, &node_dir).await?;
        self.nodes[i] = Some(node);

        Ok(())
    }

    /// Wait until all nodes agree on `expected_nodes` cluster size.
    ///
    /// Polls `GET /admin/cluster` on every alive node every 500ms.
    /// Times out after 30s. Returns `Ok(())` when all nodes report
    /// exactly `expected_nodes` members.
    pub async fn wait_for_convergence(&self, expected_nodes: usize) -> Result<(), Error> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(30);
        let poll_interval = Duration::from_millis(500);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::HealthTimeout(timeout));
            }

            let mut all_converged = true;
            for (i, node_opt) in self.nodes.iter().enumerate() {
                if let Some(node) = node_opt {
                    match self.get_cluster_node_count(node).await {
                        Ok(count) if count == expected_nodes => {}
                        Ok(count) => {
                            eprintln!(
                                "  cluster: node {i} reports {count} nodes (expected {expected_nodes})"
                            );
                            all_converged = false;
                        }
                        Err(_) => {
                            all_converged = false;
                        }
                    }
                }
            }

            if all_converged && self.alive_count() > 0 {
                return Ok(());
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Returns the number of alive nodes (not killed).
    fn alive_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_some()).count()
    }

    /// Queries `/admin/cluster` on a node and returns the number of nodes
    /// in the cluster view.
    async fn get_cluster_node_count(&self, node: &NodeProcess) -> Result<usize, Error> {
        let resp = node.get("/admin/cluster").await?;
        if !resp.status().is_success() {
            return Err(Error::ClusterError(format!(
                "cluster endpoint returned {}",
                resp.status()
            )));
        }

        let body: serde_json::Value = response_json(resp)
            .await
            .map_err(|e| Error::ClusterError(format!("failed to parse cluster JSON: {e}")))?;

        Ok(body["nodes"].as_array().map(|a| a.len()).unwrap_or(0))
    }

    /// Shut down all nodes gracefully (SIGTERM), waiting for each to exit.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        for i in 0..self.nodes.len() {
            if let Some(node) = self.nodes[i].take() {
                let _ = node.shutdown().await;
            }
        }
        Ok(())
    }
}

/// Extract the config name prefix from a config template.
///
/// Looks for patterns like `node_id = "e2e-{name}-0"` and returns `{name}`.
fn extract_config_name(config: &str) -> String {
    config
        .lines()
        .find(|l| l.contains("node_id"))
        .and_then(|l| {
            let start = l.find('"')? + 1;
            let end = l.rfind('"')?;
            Some(l[start..end].to_string())
        })
        .unwrap_or_else(|| "cluster".to_string())
}

/// Builds the TOML config for a specific cluster node.
///
/// Replaces the `node_id` in the template with a cluster-specific ID
/// like `e2e-cluster-{i}-{original_suffix}`. If `seed` is non-empty,
/// adds it as `seed_nodes`.
fn build_node_config(base_config: &str, index: usize, seed: &str) -> String {
    let old_node_id = extract_config_name(base_config);
    let new_node_id = format!("e2e-cluster-{index}");

    // Replace the node_id line.
    let config = base_config.replacen(
        &format!("node_id = \"{old_node_id}\""),
        &format!("node_id = \"{new_node_id}\""),
        1,
    );

    // Add seed_nodes if needed.
    if seed.is_empty() {
        config
    } else if config.contains("seed_nodes") {
        // Replace placeholder or add to existing seed_nodes value.
        config.replace("{seed_node}", seed)
    } else {
        format!("{}seed_nodes = [\"{}\"]\n", config, seed)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_standard_contains_placeholders() {
        let cfg = config_standard();
        assert!(cfg.contains("{http_port}"));
        assert!(cfg.contains("{grpc_port}"));
    }

    #[test]
    fn config_prefetch_enabled_has_flag() {
        let cfg = config_prefetch_enabled();
        assert!(cfg.contains("prefetch_enabled = true"));
    }

    #[test]
    fn random_bytes_produces_correct_length() {
        let data = random_bytes(100);
        assert_eq!(data.len(), 100);
    }

    #[test]
    fn random_bytes_is_not_all_zeros() {
        // Extremely unlikely to be all zeros for 1KB.
        let data = random_bytes(1024);
        assert!(data.iter().any(|b| *b != 0));
    }

    #[tokio::test]
    async fn poll_until_immediate_success() {
        let result =
            poll_until(Duration::from_millis(10), Duration::from_secs(1), || async { true }).await;
        assert!(result);
    }

    #[tokio::test]
    async fn poll_until_timeout() {
        let result =
            poll_until(Duration::from_millis(10), Duration::from_millis(50), || async { false })
                .await;
        assert!(!result);
    }

    #[test]
    fn resolve_binary_path_finds_release() {
        // The release binary should exist since we just built it.
        let path = resolve_binary_path();
        assert!(path.exists(), "binary not found at {:?}", path);
    }
}
