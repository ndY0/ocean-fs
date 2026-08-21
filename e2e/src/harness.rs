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
//!
//! ## Binary Verification
//!
//! `resolve_binary_path` prefers `target/release/oceanfs` over
//! `target/debug/oceanfs`. Before spawning, the resolved binary is
//! checked against the newest source file under `crates/` (recursive
//! `*.rs`, plus the workspace `Cargo.toml` / `Cargo.lock` / `build.rs`):
//! an older binary is a silent false-failure risk (a stale release
//! binary once caused an entire forensics round), so the harness
//! panics with a clear message instead of testing the stale binary.
//! A binary pinned via `OCEANFS_BIN` is never staleness-checked — that
//! is the operator's responsibility.
//!
//! ## Log Capture
//!
//! Node stdout+stderr are captured **by default** into
//! `e2e/target/e2e-logs` (anchored to the e2e crate root via
//! `CARGO_MANIFEST_DIR`, not the process cwd — this is the single
//! documented convention). Files accumulate across runs; each spawn
//! appends to a fresh uuid-named file so parallel tests never collide.
//! Set `E2E_CAPTURE_NODE_LOGS=0` (or `false`) to opt out. The default
//! node log level is `info`; override via `E2E_NODE_LOG_LEVEL` or
//! per-spawn [`NodeOptions`]. Use [`NodeProcess::grep_logs`] and
//! [`Cluster::any_node_logs_contain`] to assert on captured logs.

use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime},
};

use parking_lot::RwLock;
use tempfile::TempDir;

/// Name of the port-preservation file written into each node's data directory.
///
/// On first spawn, the harness writes the HTTP and gRPC ports to this file so
/// that a subsequent restart (crash recovery) can reuse the same ports. Without
/// port preservation, the restarted node binds to new ephemeral ports and
/// cannot rejoin the cluster because peers still have the old address in their
/// gossip state.
const PORTS_FILE_NAME: &str = "ports.toml";

/// Binds a random ephemeral port on localhost.
fn bind_random_port() -> Result<SocketAddr, Error> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(Error::PortDiscovery)?;
    let addr = listener.local_addr().map_err(Error::PortDiscovery)?;
    drop(listener);
    Ok(addr)
}

/// Saves the assigned HTTP, gRPC, and membership-plane ports to a TOML
/// file so a subsequent restart can reuse the same ports.
fn save_ports(
    ports_file: &Path,
    http_port: u16,
    grpc_port: u16,
    membership_port: u16,
) -> Result<(), Error> {
    let content = format!(
        "http_port = {http_port}\ngrpc_port = {grpc_port}\nmembership_port = {membership_port}\n"
    );
    fs::write(ports_file, content).map_err(Error::ConfigWrite)?;
    Ok(())
}

/// Attempts to restore previously-saved ports from the port file.
///
/// Returns `Some((http, grpc, membership))` on success, or `None` if the
/// file is missing or corrupt.
fn restore_ports(ports_file: &Path) -> Option<(u16, u16, u16)> {
    let content = fs::read_to_string(ports_file).ok()?;
    let http = content
        .lines()
        .find(|l| l.starts_with("http_port"))
        .and_then(|l| l.split('=').nth(1))
        .and_then(|v| v.trim().parse::<u16>().ok())?;
    let grpc = content
        .lines()
        .find(|l| l.starts_with("grpc_port"))
        .and_then(|l| l.split('=').nth(1))
        .and_then(|v| v.trim().parse::<u16>().ok())?;
    let membership = content
        .lines()
        .find(|l| l.starts_with("membership_port"))
        .and_then(|l| l.split('=').nth(1))
        .and_then(|v| v.trim().parse::<u16>().ok())
        .unwrap_or(grpc.saturating_add(1));
    Some((http, grpc, membership))
}

/// Acquires the HTTP, gRPC, and membership-plane ports for a node.
///
/// On first spawn (`ports_file` does not exist) three random ephemeral
/// ports are bound, then saved. On restart (`ports_file` exists) the
/// saved ports are reused if available; if they have been taken by
/// another process the function falls back to random ports and
/// overwrites the file.
fn bind_ports(ports_file: &Path) -> Result<(SocketAddr, SocketAddr, SocketAddr), Error> {
    // Try to restore ports from a previous run.
    if let Some((http_port, grpc_port, membership_port)) = restore_ports(ports_file) {
        // Try to bind to the saved HTTP port.
        let http_addr = match TcpListener::bind(format!("127.0.0.1:{http_port}"))
            .map_err(Error::PortDiscovery)
        {
            Ok(listener) => {
                let addr = listener.local_addr().map_err(Error::PortDiscovery)?;
                drop(listener);
                addr
            }
            Err(_) => {
                // Port was taken since last run — fall back to random.
                bind_random_port()?
            }
        };

        // Try to bind to the saved gRPC port.
        let grpc_addr = if http_addr.port() == http_port {
            // HTTP port was restored successfully; try gRPC port too.
            match TcpListener::bind(format!("127.0.0.1:{grpc_port}")).map_err(Error::PortDiscovery)
            {
                Ok(listener) => {
                    let addr = listener.local_addr().map_err(Error::PortDiscovery)?;
                    drop(listener);
                    addr
                }
                Err(_) => {
                    // gRPC port was taken — fall back to random.
                    bind_random_port()?
                }
            }
        } else {
            // HTTP port changed — gRPC port is probably also gone.
            bind_random_port()?
        };

        // Membership-plane port: independent of the others (the plane
        // is a separate listener, ADR-0028 D1). Restored if free,
        // otherwise random.
        let membership_addr = match TcpListener::bind(format!("127.0.0.1:{membership_port}"))
            .map_err(Error::PortDiscovery)
        {
            Ok(listener) => {
                let addr = listener.local_addr().map_err(Error::PortDiscovery)?;
                drop(listener);
                addr
            }
            Err(_) => bind_random_port()?,
        };

        // Always save the actual ports used (may differ from saved values).
        save_ports(ports_file, http_addr.port(), grpc_addr.port(), membership_addr.port())?;
        return Ok((http_addr, grpc_addr, membership_addr));
    }

    // First spawn: bind random ports and save them.
    let http_addr = bind_random_port()?;
    let grpc_addr = bind_random_port()?;
    let membership_addr = bind_random_port()?;
    save_ports(ports_file, http_addr.port(), grpc_addr.port(), membership_addr.port())?;
    Ok((http_addr, grpc_addr, membership_addr))
}

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
    /// The resolved OceanFS binary is older than the newest source file
    /// under `crates/` — e2e would silently test an outdated binary.
    #[error("stale binary {bin_path} (mtime {bin_mtime:?}) is older than the newest source file {source_path} (mtime {source_mtime:?}); {hint}")]
    StaleBinary {
        /// Path of the resolved binary.
        bin_path: PathBuf,
        /// Modification time of the resolved binary.
        bin_mtime: SystemTime,
        /// Newest source file under `crates/`.
        source_path: PathBuf,
        /// Modification time of the newest source file.
        source_mtime: SystemTime,
        /// Actionable remediation advice, tailored to the newest source.
        hint: String,
    },
    /// Failed to read a captured node log file.
    #[error("failed to read captured log {0}: {1}")]
    LogRead(PathBuf, #[source] std::io::Error),
    /// An SSH command (remote crash control) failed.
    #[error("ssh command failed: {0}")]
    Ssh(String),
}

// ---------------------------------------------------------------------------
// NodeOptions
// ---------------------------------------------------------------------------

/// Per-spawn overrides for node process configuration.
///
/// `None` fields fall back to environment defaults:
/// - `log_level`: `E2E_NODE_LOG_LEVEL`, then `"info"`
/// - `capture_logs`: `E2E_CAPTURE_NODE_LOGS` (`0`/`false` opt-out), then `true`
///
/// # Examples
///
/// ```
/// use e2e::harness::NodeOptions;
///
/// // Defaults: env-based log level ("info"), capture on.
/// let options = NodeOptions::default();
/// // Per-test override: capture at debug level.
/// let debug = NodeOptions::default().with_log_level("debug");
/// ```
#[derive(Debug, Clone, Default)]
pub struct NodeOptions {
    log_level: Option<String>,
    capture_logs: Option<bool>,
}

impl NodeOptions {
    /// Returns options with all defaults (env-based).
    ///
    /// Equivalent to [`NodeOptions::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the node `--log-level` override (e.g. `"debug"`).
    ///
    /// Takes precedence over the `E2E_NODE_LOG_LEVEL` environment
    /// variable. The harness default is `"info"`.
    pub fn with_log_level(mut self, level: impl Into<String>) -> Self {
        self.log_level = Some(level.into());
        self
    }

    /// Sets whether node stdout+stderr are captured into
    /// `e2e/target/e2e-logs`.
    ///
    /// Takes precedence over the `E2E_CAPTURE_NODE_LOGS` environment
    /// variable. The default is `true` (capture on).
    pub fn with_capture(mut self, capture: bool) -> Self {
        self.capture_logs = Some(capture);
        self
    }
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
    /// Log files written for this node process (uuid-named, appended
    /// into `e2e/target/e2e-logs`). Empty when capture was disabled.
    log_files: Vec<PathBuf>,
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
        Self::spawn_with_options(config_toml, &NodeOptions::default()).await
    }

    /// Spawns an OceanFS node with the given configuration and per-spawn
    /// overrides (log level, log capture).
    ///
    /// See [`NodeProcess::spawn`] for the general contract; [`NodeOptions`]
    /// lets a test force a log level (e.g. `"debug"`) or opt out of log
    /// capture without touching the process environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the binary cannot be found, the process fails
    /// to start, or the health endpoint does not respond within the
    /// timeout (30 seconds).
    pub async fn spawn_with_options(
        config_toml: &str,
        options: &NodeOptions,
    ) -> Result<Self, Error> {
        let temp_dir = TempDir::new().map_err(Error::ConfigWrite)?;
        let data_dir = temp_dir.path().to_path_buf();
        Self::spawn_inner(config_toml, data_dir, Some(temp_dir), false, options).await
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
        Self::spawn_with_data_dir_and_options(config_toml, data_dir, &NodeOptions::default()).await
    }

    /// Spawns an OceanFS node using a specific data directory and
    /// per-spawn overrides.
    ///
    /// See [`NodeProcess::spawn_with_data_dir`] for the general contract
    /// and [`NodeOptions`] for the overrides.
    ///
    /// # Errors
    ///
    /// Returns an error if the binary cannot be found, the process fails
    /// to start, or the health endpoint does not respond within the
    /// timeout (30 seconds).
    pub async fn spawn_with_data_dir_and_options(
        config_toml: &str,
        data_dir: &Path,
        options: &NodeOptions,
    ) -> Result<Self, Error> {
        Self::spawn_inner(config_toml, data_dir.to_path_buf(), None, true, options).await
    }

    /// Internal spawn logic shared by all spawn entry points.
    ///
    /// `create_dir` controls whether the data directory is created if it
    /// doesn't exist (true for custom dirs, false for temp dirs since
    /// TempDir already created them).
    ///
    /// # Panics
    ///
    /// Panics when the resolved binary (not pinned via `OCEANFS_BIN`) is
    /// older than the newest source file under `crates/` — silently
    /// testing a stale binary has produced false failure signatures in
    /// the past, so the harness refuses to spawn instead.
    async fn spawn_inner(
        config_toml: &str,
        data_dir: PathBuf,
        temp_dir: Option<TempDir>,
        create_dir: bool,
        options: &NodeOptions,
    ) -> Result<Self, Error> {
        // ---- 1. Ensure data directory exists ----
        if create_dir {
            std::fs::create_dir_all(&data_dir).map_err(Error::ConfigWrite)?;
        }

        // ---- 2. Discover / restore ports ----
        let ports_file = data_dir.join(PORTS_FILE_NAME);
        let (http_addr, grpc_addr, membership_addr) = bind_ports(&ports_file)?;

        // ---- 3. Build config with resolved ports ----
        let resolved_config = config_toml
            .replace("{http_port}", &http_addr.port().to_string())
            .replace("{grpc_port}", &grpc_addr.port().to_string())
            .replace("{membership_port}", &membership_addr.port().to_string());

        let full_config = format!(
            "data_dir = \"{data_dir_path}\"\n{resolved_config}",
            data_dir_path = data_dir.display(),
            resolved_config = resolved_config
        );

        let config_path = data_dir.join("oceanfs.toml");
        std::fs::write(&config_path, &full_config).map_err(Error::ConfigWrite)?;

        // ---- 4. Find the binary ----
        let resolved = resolve_binary_path();
        // Staleness gate: silently testing an outdated binary produced
        // false failure signatures during gap-closure. Binaries pinned
        // via `OCEANFS_BIN` are the operator's responsibility and are
        // never staleness-checked (documented on `resolve_binary_path`).
        if !resolved.is_operator_pinned() {
            if let Err(stale) = check_binary_freshness(resolved.path(), &workspace_root()) {
                panic!("refusing to run e2e tests against a stale binary:\n{stale}");
            }
        }
        let bin_path = resolved.path().to_path_buf();

        // ---- 5. Spawn the process ----
        // Node logs are captured by default into
        // `e2e/target/e2e-logs` (anchored to the e2e crate root — see
        // `log_dir`) so failure signatures survive temp-dir cleanup and
        // remain analyzable after the run. `E2E_CAPTURE_NODE_LOGS=0`
        // (or `false`) opts out. The default log level is "info";
        // `E2E_NODE_LOG_LEVEL` overrides it, and per-spawn
        // `NodeOptions` take precedence over both.
        let log_level = resolve_log_level(
            options.log_level.as_deref(),
            std::env::var("E2E_NODE_LOG_LEVEL").ok().as_deref(),
        );
        let capture_logs = resolve_capture(
            options.capture_logs,
            std::env::var("E2E_CAPTURE_NODE_LOGS").ok().as_deref(),
        );

        let mut cmd = Command::new(&bin_path);
        cmd.arg("--config").arg(&config_path).arg("--log-level").arg(&log_level);

        let mut log_files = Vec::new();
        let child = if capture_logs {
            let log_dir = log_dir();
            let _ = std::fs::create_dir_all(&log_dir);
            let parent = data_dir
                .parent()
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let base =
                data_dir.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            let log_path = log_dir.join(format!("{parent}-{base}-{}.log", uuid::Uuid::now_v7()));
            let log_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .map_err(Error::Spawn)?;
            log_files.push(log_path);
            cmd.stdout(Stdio::from(log_file.try_clone().map_err(Error::Spawn)?))
                .stderr(Stdio::from(log_file))
                .spawn()
                .map_err(Error::Spawn)?
        } else {
            cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn().map_err(Error::Spawn)?
        };

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
            log_files,
        };

        // ---- 6. Wait for health endpoint ----
        // 60s: a restart under heavy load (the churn test's hint-debt
        // replay + drain) can take longer than 30s to become healthy —
        // a tight wait produced one flaky churn-restart failure.
        node.wait_for_health(Duration::from_secs(60)).await?;

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

    /// Returns the paths of the log files written for this node process.
    ///
    /// Files live under `e2e/target/e2e-logs` (see module docs). The
    /// slice is empty when log capture was disabled for this spawn
    /// (`E2E_CAPTURE_NODE_LOGS=0` or `NodeOptions::with_capture(false)`).
    pub fn captured_logs(&self) -> &[PathBuf] {
        &self.log_files
    }

    /// Greps this node's captured logs for `pattern` (substring match).
    ///
    /// Returns the matching lines, each prefixed with the log file it
    /// came from. An empty result means no match (or no captured logs).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::harness::{config_standard, NodeProcess};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let node = NodeProcess::spawn(&config_standard()).await?;
    /// let matches = node.grep_logs("seal queue full")?;
    /// assert!(matches.is_empty(), "no seal-pressure signatures expected");
    /// node.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if a captured log file cannot be read.
    pub fn grep_logs(&self, pattern: &str) -> Result<Vec<String>, Error> {
        grep_logs_in_files(&self.log_files, pattern)
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
// Binary resolution & staleness verification
// ---------------------------------------------------------------------------

/// The absolute path of the e2e crate root, captured at compile time.
///
/// Used to anchor both the workspace root (for the binary staleness
/// check) and the log directory, so the harness behaves identically
/// regardless of the process cwd.
const E2E_CRATE_ROOT: &str = env!("CARGO_MANIFEST_DIR");

/// Returns the workspace root: the parent of the e2e crate directory.
fn workspace_root() -> PathBuf {
    Path::new(E2E_CRATE_ROOT).parent().unwrap_or(Path::new(".")).to_path_buf()
}

/// The result of [`resolve_binary_path`].
pub(crate) struct ResolvedBinary {
    path: PathBuf,
    operator_pinned: bool,
}

impl ResolvedBinary {
    /// The resolved binary path.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Whether the path was pinned by the operator via `OCEANFS_BIN`.
    ///
    /// Pinned binaries are exists-checked but never staleness-checked.
    pub(crate) fn is_operator_pinned(&self) -> bool {
        self.operator_pinned
    }
}

/// Resolves the path to the OceanFS binary.
///
/// Checks in order:
/// 1. `OCEANFS_BIN` environment variable — the operator pins a specific
///    binary. The path must exist (otherwise resolution falls through);
///    it is **never** staleness-checked: verifying that a pinned binary
///    matches the sources is the operator's responsibility.
/// 2. `target/release/oceanfs` relative to workspace root
/// 3. `target/debug/oceanfs` relative to workspace root
/// 4. PATH fallback (bare `oceanfs`).
fn resolve_binary_path() -> ResolvedBinary {
    if let Ok(path) = std::env::var("OCEANFS_BIN") {
        let p = PathBuf::from(&path);
        if p.exists() {
            return ResolvedBinary { path: p, operator_pinned: true };
        }
    }

    // Find workspace root by walking up from the current exe or cwd.
    let workspace = workspace_root();
    let candidates =
        [workspace.join("target/release/oceanfs"), workspace.join("target/debug/oceanfs")];

    for candidate in &candidates {
        if candidate.exists() {
            return ResolvedBinary { path: candidate.clone(), operator_pinned: false };
        }
    }

    // Fallback to just "oceanfs" (hope it's on PATH).
    ResolvedBinary { path: PathBuf::from("oceanfs"), operator_pinned: false }
}

/// Returns the newest source file under the workspace and its mtime.
///
/// Scans `crates/` recursively for `*.rs` files (which includes
/// `build.rs`), plus the workspace `Cargo.toml`, `Cargo.lock`, and
/// root `build.rs`. Returns `None` when no comparable source exists
/// (missing `crates/` tree, unreadable metadata).
fn newest_source_mtime(workspace: &Path) -> Option<(PathBuf, SystemTime)> {
    let mut newest: Option<(PathBuf, SystemTime)> = None;

    // Consider a candidate file, keeping the one with the newest mtime.
    let mut consider = |path: &Path| {
        if let Ok(metadata) = fs::metadata(path) {
            if let Ok(mtime) = metadata.modified() {
                if newest.as_ref().map_or(true, |(_, t)| mtime > *t) {
                    newest = Some((path.to_path_buf(), mtime));
                }
            }
        }
    };

    // Recursively walk `crates/` for `.rs` sources.
    let crates_dir = workspace.join("crates");
    if crates_dir.is_dir() {
        let mut stack = vec![crates_dir];
        while let Some(dir) = stack.pop() {
            if let Ok(entries) = fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        stack.push(path);
                    } else if path.extension().is_some_and(|e| e == std::ffi::OsStr::new("rs")) {
                        consider(&path);
                    }
                }
            }
        }
    }

    // Workspace-level manifest files drive builds too.
    consider(&workspace.join("Cargo.toml"));
    consider(&workspace.join("Cargo.lock"));
    consider(&workspace.join("build.rs"));

    newest
}

/// Verifies that the resolved binary is not older than the newest
/// source file under `crates/`.
///
/// The check passes when the binary is at least as new as every source
/// file (recursive `*.rs` under `crates/`, workspace `Cargo.toml`,
/// `Cargo.lock`, root `build.rs`). A binary that cannot be inspected
/// (missing file, unreadable metadata) also passes — a missing binary
/// fails later at spawn time, and a missing `crates/` tree leaves
/// nothing to compare against.
///
/// # Errors
///
/// Returns [`Error::StaleBinary`] when the binary is strictly older
/// than the newest source file.
fn check_binary_freshness(bin_path: &Path, workspace: &Path) -> Result<(), Error> {
    let bin_mtime = match fs::metadata(bin_path).and_then(|m| m.modified()) {
        Ok(mtime) => mtime,
        Err(_) => return Ok(()),
    };
    let Some((source_path, source_mtime)) = newest_source_mtime(workspace) else {
        return Ok(());
    };
    if bin_mtime < source_mtime {
        // Tailor the remediation to the kind of file that went stale:
        // a workspace-manifest change (Cargo.toml/Cargo.lock/build.rs)
        // may not affect the binary at all (e.g. an e2e-only dev-dependency
        // bump), in which case `cargo build --release` no-ops and cargo
        // does not relink — the operator must then explicitly accept the
        // binary. A `crates/` source change always requires a rebuild.
        let hint = if source_path.starts_with(workspace.join("crates")) {
            "run `cargo build --release`, or pin a known-good binary via `OCEANFS_BIN`".to_string()
        } else {
            "the newest file is a workspace manifest — run `cargo build --release`; \
             if it completes without relinking the binary (the manifest change only \
             affects unrelated crates), the binary is still valid: `touch` the binary \
             or pin it via `OCEANFS_BIN` to acknowledge"
                .to_string()
        };
        return Err(Error::StaleBinary {
            bin_path: bin_path.to_path_buf(),
            bin_mtime,
            source_path,
            source_mtime,
            hint,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Log capture
// ---------------------------------------------------------------------------

/// The directory where captured node logs are written.
///
/// Convention: `e2e/target/e2e-logs`, anchored to the e2e crate root
/// (`CARGO_MANIFEST_DIR`) — **not** the process cwd. When tests run
/// under `cargo test -p e2e` the cwd already equals the crate root, so
/// the legacy `target/e2e-logs` relative path landed here too; anchoring
/// makes the location explicit and cwd-independent.
///
/// Logs accumulate across runs (append-only); each node spawn appends
/// to a fresh uuid-named file, so parallel tests never collide.
fn log_dir() -> PathBuf {
    Path::new(E2E_CRATE_ROOT).join("target").join("e2e-logs")
}

/// Resolves the node log level from per-spawn options and environment.
///
/// Precedence: explicit options > `E2E_NODE_LOG_LEVEL` > `"info"`.
fn resolve_log_level(options_level: Option<&str>, env_level: Option<&str>) -> String {
    options_level.or(env_level).unwrap_or("info").to_string()
}

/// Resolves whether node logs are captured.
///
/// Precedence: explicit options > `E2E_CAPTURE_NODE_LOGS`
/// (`0`/`false` opt-out) > `true` (capture by default).
fn resolve_capture(options_capture: Option<bool>, env_capture: Option<&str>) -> bool {
    match options_capture {
        Some(capture) => capture,
        None => !env_capture.is_some_and(|v| v == "0" || v.eq_ignore_ascii_case("false")),
    }
}

/// Greps the given log files for `pattern` (substring match).
///
/// Returns matching lines, each prefixed with the file it came from.
/// Non-existent or unreadable files are skipped.
///
/// # Errors
///
/// Returns an error when a log file exists but cannot be read.
fn grep_logs_in_files(log_files: &[PathBuf], pattern: &str) -> Result<Vec<String>, Error> {
    let mut matches = Vec::new();
    for path in log_files {
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(Error::LogRead(path.clone(), e)),
        };
        for line in content.lines() {
            if line.contains(pattern) {
                matches.push(format!("{}: {}", path.display(), line));
            }
        }
    }
    Ok(matches)
}

// ---------------------------------------------------------------------------
// Config templates
// ---------------------------------------------------------------------------

/// Standard node configuration for general smoke tests.
///
/// Uses OS-assigned ephemeral ports (`{http_port}`, `{grpc_port}`)
/// which are replaced at spawn time. All caches and durability features
/// are enabled with default intervals.
///
/// `max_body_size` is raised to 16 MiB (from the 2 MiB production
/// default) to match the load generator's `BlobSizeDist` MULTI_MAX.
/// The production default stays at 2 MiB — this config only *permits*
/// the larger tiered-blob PUTs that the load tests send.
pub fn config_standard() -> String {
    r#"
node_id = "e2e-standard"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
membership_listen_addr = "127.0.0.1:{membership_port}"
log_level = "error"
prefetch_enabled = false
max_body_size = 16777216   # 16 MiB — matches BlobSizeDist MULTI_MAX
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
membership_listen_addr = "127.0.0.1:{membership_port}"
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
membership_listen_addr = "127.0.0.1:{membership_port}"
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
membership_listen_addr = "127.0.0.1:{membership_port}"
log_level = "error"
prefetch_enabled = false
ae_interval_sec = 10
"#
    .to_string()
}

/// Configuration with a shortened scrub interval.
///
/// Scrub runs every 60 seconds instead of the multi-hour production
/// default so that a sustained-load test observes scrub cycles within
/// its runtime. Uses the configurable `scrub_interval_sec` field.
pub fn config_short_scrub() -> String {
    r#"
node_id = "e2e-scrub"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
membership_listen_addr = "127.0.0.1:{membership_port}"
log_level = "error"
prefetch_enabled = false
scrub_interval_sec = 60
"#
    .to_string()
}

/// Phase 2 sustained-load configuration: shortened background intervals,
/// a 16 MiB body limit, and an L1 cache covering every tier.
///
/// Combines every interval the Phase 2 test shortens — GC (10s cycle,
/// 5s tombstone TTL), anti-entropy (10s), scrub (60s) — with the
/// `max_body_size` raised to 16 MiB so the tiered load generator's
/// multi-tier blobs (up to `MULTI_MAX`) are accepted. The L1 object
/// cache's `object_cache_max_blob_size` is raised from the 1 MiB
/// production default to 16 MiB so **every** tier participates in the
/// L1 cache: the Phase 2 cache-hit-rate assertion (>50%) measures the
/// cache pipeline, not the size-threshold tuning (the production
/// default excludes standard >1 MiB and all multi-tier blobs, which
/// caps hit rates near 30% under this workload). Gossip is left at its
/// default with no seed nodes: the single-node Phase 2 topology has no
/// peers, so the gossip loop is inert.
pub fn config_sustained() -> String {
    r#"
node_id = "e2e-sustained"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
membership_listen_addr = "127.0.0.1:{membership_port}"
log_level = "error"
prefetch_enabled = false
max_body_size = 16777216   # 16 MiB — matches BlobSizeDist MULTI_MAX
gc_interval_sec = 10
tombstone_ttl_sec = 5
ae_interval_sec = 10
scrub_interval_sec = 60
object_cache_size_bytes = 268435456    # 256 MiB — holds the hot-key working set; keeps RSS < 2× baseline
object_cache_max_blob_size = 16777216   # 16 MiB — every tier is L1-cacheable
object_cache_ttl_ms = 0                # no TTL expiry — hit-rate invariant measures cache health, not expiry churn
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
membership_listen_addr = "127.0.0.1:{membership_port}"
log_level = "warn"
prefetch_enabled = false
write_quorum = 2
read_quorum = 2
replication_factor = 3

[gossip]
interval_ms = 100
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
membership_listen_addr = "127.0.0.1:{membership_port}"
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
membership_listen_addr = "127.0.0.1:{membership_port}"
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
membership_listen_addr = "127.0.0.1:{membership_port}"
log_level = "error"
prefetch_enabled = false
ae_interval_sec = 10
"#
    .to_string()
}

/// Configuration for the Phase 3 cluster-churn test (local-spawn mode).
///
/// Mirrors the fleet profile that `sut-deploy.sh --cluster` writes on
/// the SUT VMs (ADR-0026): 3-node quorum semantics (write_quorum=2,
/// read_quorum=2, replication_factor=3), fast gossip (500ms), fast SWIM
/// (3s suspicion / 8s failure — the two-VM profile of the Phase 3
/// feature doc), shortened AE/GC/scrub intervals, and zero L1 cache TTL
/// (so cross-node cache invalidation is observable within the run).
pub fn config_cluster_churn() -> String {
    r#"
node_id = "e2e-churn-0"
listen_addr = "127.0.0.1:{http_port}"
grpc_listen_addr = "127.0.0.1:{grpc_port}"
membership_listen_addr = "127.0.0.1:{membership_port}"
log_level = "error"
prefetch_enabled = false
write_quorum = 2
read_quorum = 2
replication_factor = 3
object_cache_ttl_ms = 0
gc_interval_sec = 10
tombstone_ttl_sec = 5
ae_interval_sec = 10
scrub_interval_sec = 60

[gossip]
interval_ms = 500
# The failure detector must tolerate the load-test machine's CPU
# contention (SWIM pings timing out under load produced FALSE
# suspect/dead markings during the settle — a live node marked dead by
# a peer, observed 2026-08-20). The timings are looser than the
# fast-swim test profile: convergence is exercised by the churn cycles
# themselves, not by tight suspicion windows.
suspicion_timeout_ms = 6000
failure_timeout_ms = 15000
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
///
/// Fills the buffer in bulk via [`rand::RngCore::fill_bytes`] instead of
/// per-byte `gen()` calls. Measured on this machine: ~18 MB/s in a debug
/// build and ~930 MB/s in release (vs ~4 MB/s / ~244 MB/s for the
/// per-byte version) — a ~4.5× speedup in both profiles.
pub fn random_bytes(len: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut buf = vec![0u8; len];
    rng.fill_bytes(&mut buf);
    buf
}

/// Realistically-compressible payload (~2x): half incompressible
/// random data, half compressible text.
///
/// The default load uses [`random_bytes`] — incompressible, so the
/// node's don't-shrink guard stores everything raw and the
/// decompression path is never exercised. A pure repeated block would
/// compress ~12,000x (a 4-16 MiB object becomes ~341 bytes stored), so
/// standard segments need ~12,000 chunks to fill and the seal-time EC
/// encode fires about once every three minutes — the dashboards look
/// dead and the phase-2 CPU-bound premise evaporates. Mixing random
/// data with text gives zstd a realistic 2x ratio: segments fill at
/// the designed rate and the encode/decompress paths are observable.
pub fn compressible_bytes(len: usize) -> Vec<u8> {
    use rand::RngCore;
    const BLOCK: &[u8] = b"The quick brown fox jumps over the lazy dog. 0123456789 ";
    const SECTION: usize = 2048;
    let mut buf = Vec::with_capacity(len);
    let mut rng = rand::thread_rng();
    let mut section = 0usize;
    while buf.len() < len {
        if section % 2 == 0 {
            // Compressible text section (repeated sentence).
            while buf.len() < len && buf.len() % SECTION < SECTION - BLOCK.len() {
                buf.extend_from_slice(BLOCK);
            }
        } else {
            // Incompressible random section.
            let take = (len - buf.len()).min(SECTION);
            let mut r = vec![0u8; take];
            rng.fill_bytes(&mut r);
            buf.extend_from_slice(&r);
        }
        section += 1;
    }
    buf
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

// ---------------------------------------------------------------------------
// Harness self-monitoring
// ---------------------------------------------------------------------------

/// Reads the harness process's resident memory from `/proc/self/statm`.
///
/// Per ADR-0019 Decision 4 the harness records its own RSS and FD count
/// into the [`LoadReport`](crate::load::LoadReport) as **metadata** (not
/// assertions), so borderline results can be attributed when the harness
/// is co-located with the SUT (`--single-vm` mode).
///
/// # Errors
///
/// Returns an error if `/proc/self/statm` cannot be read or parsed.
pub fn read_self_memory_bytes() -> Result<u64, std::io::Error> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    // Format: size resident shared text lib data dt (in pages).
    let parts: Vec<&str> = statm.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected statm format",
        ));
    }
    let resident_pages: u64 =
        parts[1].parse().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(resident_pages * 4096)
}

/// Counts the harness process's open file descriptors from `/proc/self/fd`.
///
/// # Errors
///
/// Returns an error if the directory cannot be read.
pub fn read_self_open_fds() -> Result<u64, std::io::Error> {
    let entries = std::fs::read_dir("/proc/self/fd")?;
    Ok(entries.count() as u64)
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
    /// All node processes in this cluster (interior-mutability for churn).
    nodes: RwLock<Vec<Option<NodeProcess>>>,
    /// Shared temporary directory root (cleaned on drop).
    _temp_dir: TempDir,
    /// Base config template string.
    base_config: String,
    /// HTTP client for admin polling.
    client: reqwest::Client,
    /// Per-spawn options applied to every node (and to restarts).
    options: NodeOptions,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in self.nodes.get_mut().iter_mut().flatten() {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
    }
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nodes = self.nodes.read();
        f.debug_struct("Cluster")
            .field("node_count", &nodes.len())
            .field("alive_count", &nodes.iter().filter(|n| n.is_some()).count())
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
        Self::spawn_with_options(count, base_config, &NodeOptions::default()).await
    }

    /// Spawns `count` nodes with per-spawn options applied to every node
    /// (and to later restarts via [`Cluster::restart`]).
    ///
    /// See [`Cluster::spawn`] for the general contract and [`NodeOptions`]
    /// for the overrides.
    ///
    /// # Errors
    ///
    /// Returns an error if any node fails to spawn or become healthy.
    pub async fn spawn_with_options(
        count: usize,
        base_config: &str,
        options: &NodeOptions,
    ) -> Result<Self, Error> {
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
        let node0 =
            NodeProcess::spawn_with_data_dir_and_options(&node0_config, &node0_dir, options)
                .await?;
        let seed_addr = format!("127.0.0.1:{}", node0.grpc_addr().port());
        nodes.push(Some(node0));

        // Spawn remaining nodes with node 0 as seed.
        for i in 1..count {
            let node_config = build_node_config(base_config, i, &seed_addr);
            let node_dir = _temp_dir.path().join(format!("node-{i}"));
            let node =
                NodeProcess::spawn_with_data_dir_and_options(&node_config, &node_dir, options)
                    .await?;
            nodes.push(Some(node));
        }

        Ok(Self {
            nodes: RwLock::new(nodes),
            _temp_dir,
            base_config: base_config.to_string(),
            client,
            options: options.clone(),
        })
    }

    /// Returns a reference to node `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds or if the node has been killed
    /// without being restarted.
    ///
    /// The returned guard holds a read lock on the nodes vector.
    /// It implements `Deref<Target = NodeProcess>` so all `NodeProcess`
    /// methods are available. The lock is released when the guard drops.
    pub fn node(&self, i: usize) -> parking_lot::MappedRwLockReadGuard<'_, NodeProcess> {
        parking_lot::RwLockReadGuard::map(self.nodes.read(), |nodes| {
            nodes[i].as_ref().expect("node has been killed and not restarted")
        })
    }

    /// Returns the HTTP address of node `i` without holding the lock
    /// across an await point. Prefer this over `node(i).http_addr()` in
    /// async contexts.
    pub fn node_http_addr(&self, i: usize) -> SocketAddr {
        let nodes = self.nodes.read();
        nodes[i].as_ref().expect("node killed").http_addr()
    }

    /// Checked variant of [`node_http_addr`](Self::node_http_addr): an
    /// error instead of a panic when node `i` is currently killed (churn
    /// tests address nodes that the scheduler has SIGKILLed mid-run —
    /// workers must observe a transport error, not abort the test).
    fn checked_http_addr(&self, i: usize) -> Result<SocketAddr, Error> {
        let nodes = self.nodes.read();
        match nodes.get(i) {
            Some(Some(node)) => Ok(node.http_addr()),
            _ => Err(Error::ClusterError(format!("node {i} is killed or out of bounds"))),
        }
    }

    /// Returns the number of nodes in the cluster (including killed ones).
    pub fn len(&self) -> usize {
        self.nodes.read().len()
    }

    /// Returns `true` if the cluster has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.read().is_empty()
    }

    /// HTTP GET from node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if `i` is out of bounds or the node is currently
    /// killed (churn tests) — the request fails like a transport error
    /// instead of panicking.
    pub async fn get(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.checked_http_addr(i)?, path);
        Ok(self.client.get(&url).send().await?)
    }

    /// HTTP PUT to node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if `i` is out of bounds or the node is currently
    /// killed (churn tests).
    pub async fn put(&self, i: usize, path: &str, body: &[u8]) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.checked_http_addr(i)?, path);
        Ok(self.client.put(&url).body(body.to_vec()).send().await?)
    }

    /// HTTP DELETE from node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if `i` is out of bounds or the node is currently
    /// killed (churn tests).
    pub async fn delete(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.checked_http_addr(i)?, path);
        Ok(self.client.delete(&url).send().await?)
    }

    /// HTTP HEAD from node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if `i` is out of bounds or the node is currently
    /// killed (churn tests).
    pub async fn head(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.checked_http_addr(i)?, path);
        Ok(self.client.head(&url).send().await?)
    }

    /// HTTP POST to node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if `i` is out of bounds or the node is currently
    /// killed (churn tests).
    pub async fn post(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("http://{}{}", self.checked_http_addr(i)?, path);
        Ok(self.client.post(&url).send().await?)
    }

    /// Kill node `i` with SIGKILL (hard crash for failure tests).
    ///
    /// After calling this, `node(i)` will panic until `restart(i)` is called.
    /// The node's data directory is preserved for restart.
    pub fn kill(&self, i: usize) -> std::io::Result<()> {
        let mut node = self.nodes.write()[i].take().expect("node already killed");
        let result = node.kill();
        drop(node);
        result
    }

    /// Restart a previously killed node `i` with its original data directory.
    ///
    /// The node is respawned with the same config (and thus same ports,
    /// though the OS may assign different ports if the original ones were
    /// released). For cluster rejoin tests, the seed is still configured.
    ///
    /// A brief delay before restart lets the OS release TCP sockets from
    /// the killed process (TIME_WAIT can hold ports for up to 60s, but
    /// localhost sockets typically clean up faster).
    pub async fn restart(&self, i: usize) -> Result<(), Error> {
        // Let the OS release the killed process's ports.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let seed = {
            let nodes = self.nodes.read();
            if i == 0 {
                String::new()
            } else {
                // Prefer the live node-0 gRPC port; fall back to node 0's
                // PRESERVED port from its ports.toml. The churn scheduler
                // can kill node 0 in the same tick as this restart (kill
                // phase runs before the restart phase) — an empty seed
                // would make the restarted node seedless, and with a
                // failed fallback pull it would start as a permanently
                // isolated singleton (observed as churn-run
                // convergence=false).
                match nodes[0].as_ref() {
                    Some(node0) => format!("127.0.0.1:{}", node0.grpc_addr().port()),
                    None => {
                        let ports_file = self._temp_dir.path().join("node-0").join(PORTS_FILE_NAME);
                        restore_ports(&ports_file)
                            .map(|(_, grpc_port, _)| format!("127.0.0.1:{grpc_port}"))
                            .unwrap_or_default()
                    }
                }
            }
        };

        let node_dir = self._temp_dir.path().join(format!("node-{i}"));
        let node_config = build_node_config(&self.base_config, i, &seed);
        let node =
            NodeProcess::spawn_with_data_dir_and_options(&node_config, &node_dir, &self.options)
                .await?;
        self.nodes.write()[i] = Some(node);

        Ok(())
    }

    /// Returns `true` when any alive node's captured logs contain
    /// `pattern` (substring match).
    ///
    /// Nodes whose log files are missing or unreadable are skipped.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::harness::{config_3node_w2_r2, Cluster};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = Cluster::spawn(3, &config_3node_w2_r2()).await?;
    /// let dirty = cluster.any_node_logs_contain("seal queue full");
    /// assert!(!dirty, "no seal-pressure signatures expected");
    /// cluster.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn any_node_logs_contain(&self, pattern: &str) -> bool {
        // Snapshot the log file paths first so file I/O happens outside
        // the nodes read lock (lock hold times stay minimal).
        let log_files: Vec<PathBuf> = {
            let nodes = self.nodes.read();
            nodes.iter().flatten().flat_map(|node| node.captured_logs().iter().cloned()).collect()
        };
        log_files
            .iter()
            .any(|path| fs::read_to_string(path).is_ok_and(|content| content.contains(pattern)))
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

            // Collect node addresses without holding the lock across await.
            let node_addrs: Vec<SocketAddr> = {
                let nodes = self.nodes.read();
                nodes.iter().filter_map(|n| n.as_ref().map(|np| np.http_addr())).collect()
            };

            let mut all_converged = true;
            for &addr in &node_addrs {
                match self.get_cluster_node_count(addr).await {
                    Ok(count) if count == expected_nodes => {}
                    Ok(count) => {
                        eprintln!(
                            "  cluster: {addr} reports {count} nodes (expected {expected_nodes})"
                        );
                        all_converged = false;
                    }
                    Err(_) => {
                        all_converged = false;
                    }
                }
            }

            if all_converged && !node_addrs.is_empty() {
                return Ok(());
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Returns the number of alive nodes (not killed).
    #[allow(dead_code)]
    pub(crate) fn alive_count(&self) -> usize {
        self.nodes.read().iter().filter(|n| n.is_some()).count()
    }

    /// Queries `/admin/cluster` on a node and returns the number of nodes
    /// in the cluster view.
    async fn get_cluster_node_count(&self, addr: SocketAddr) -> Result<usize, Error> {
        let url = format!("http://{addr}/admin/cluster");
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(Error::ClusterError(format!(
                "cluster endpoint returned {}",
                resp.status()
            )));
        }

        let body: serde_json::Value = response_json(resp)
            .await
            .map_err(|e| Error::ClusterError(format!("failed to parse cluster JSON: {e}")))?;

        // Count ALIVE entries only (ADR-0027 Decision 1): dead nodes are
        // RETAINED in the table (state=Dead), so counting raw entries
        // would report "3 nodes" for 2 alive + 1 retained dead —
        // convergence would pass while a node is still down.
        Ok(body["nodes"]
            .as_array()
            .map(|a| a.iter().filter(|n| n["state"] == "Alive").count())
            .unwrap_or(0))
    }

    /// Shut down all nodes gracefully (SIGTERM), waiting for each to exit.
    pub async fn shutdown(self) -> Result<(), Error> {
        let len = self.nodes.read().len();
        for i in 0..len {
            let node = self.nodes.write()[i].take();
            if let Some(node) = node {
                let _ = node.shutdown().await;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// LoadTarget
// ---------------------------------------------------------------------------

/// A target the load harness can issue S3-style HTTP operations against.
///
/// Implemented by [`Cluster`] (spawned local processes) and by
/// `RemoteCluster` (already-running OceanFS processes reached over the
/// network, per ADR-0019 remote-target mode). The load generator
/// ([`Worker`](crate::load::Worker), [`Manifest`](crate::load::Manifest))
/// is generic over this trait, so the same scenario runs against either
/// topology.
///
/// The HTTP methods return explicitly `+ Send` futures so workers can be
/// spawned on the multi-threaded tokio runtime (stable Rust's `async fn`
/// in traits does not imply `Send`).
pub trait LoadTarget: Send + Sync + 'static {
    /// Returns the number of nodes in the target.
    fn len(&self) -> usize;

    /// Returns `true` if the target has no nodes.
    fn is_empty(&self) -> bool;

    /// Returns the HTTP address of node `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of bounds or (for `Cluster`) the node has
    /// been killed without being restarted.
    fn node_addr(&self, i: usize) -> SocketAddr;

    /// Returns the shared HTTP client used by every request to this target.
    fn client(&self) -> &reqwest::Client;

    /// HTTP GET to node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    fn get<'a>(
        &'a self,
        i: usize,
        path: &'a str,
    ) -> impl std::future::Future<Output = Result<reqwest::Response, Error>> + Send + 'a;

    /// HTTP PUT to node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    fn put<'a>(
        &'a self,
        i: usize,
        path: &'a str,
        body: &'a [u8],
    ) -> impl std::future::Future<Output = Result<reqwest::Response, Error>> + Send + 'a;

    /// HTTP DELETE to node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    fn delete<'a>(
        &'a self,
        i: usize,
        path: &'a str,
    ) -> impl std::future::Future<Output = Result<reqwest::Response, Error>> + Send + 'a;

    /// HTTP HEAD to node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    fn head<'a>(
        &'a self,
        i: usize,
        path: &'a str,
    ) -> impl std::future::Future<Output = Result<reqwest::Response, Error>> + Send + 'a;

    /// HTTP POST to node `i`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    fn post<'a>(
        &'a self,
        i: usize,
        path: &'a str,
    ) -> impl std::future::Future<Output = Result<reqwest::Response, Error>> + Send + 'a;
}

impl LoadTarget for Cluster {
    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn node_addr(&self, i: usize) -> SocketAddr {
        self.node_http_addr(i)
    }

    fn client(&self) -> &reqwest::Client {
        &self.client
    }

    async fn get(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.get(i, path).await
    }

    async fn put(&self, i: usize, path: &str, body: &[u8]) -> Result<reqwest::Response, Error> {
        self.put(i, path, body).await
    }

    async fn delete(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.delete(i, path).await
    }

    async fn head(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.head(i, path).await
    }

    async fn post(&self, i: usize, path: &str) -> Result<reqwest::Response, Error> {
        self.post(i, path).await
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

    // Add seed_nodes under [gossip] if needed.
    if seed.is_empty() {
        config
    } else if config.contains("[gossip]") {
        // Append seed_nodes inside the existing [gossip] section.
        format!("{config}seed_nodes = [\"{seed}\"]\n")
    } else {
        // Add a [gossip] section with seed_nodes.
        format!("{config}\n[gossip]\nseed_nodes = [\"{seed}\"]\n")
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
    fn config_standard_allows_16mib_bodies() {
        // The tiered load distribution sends multi-tier blobs up to
        // 16 MiB; the test config must accept them (production default
        // remains 2 MiB — this is a test-harness-only override).
        let cfg = config_standard();
        assert!(cfg.contains("max_body_size = 16777216"));
    }

    #[test]
    fn config_prefetch_enabled_has_flag() {
        let cfg = config_prefetch_enabled();
        assert!(cfg.contains("prefetch_enabled = true"));
    }

    #[test]
    fn config_short_scrub_sets_60_second_interval() {
        let cfg = config_short_scrub();
        assert!(cfg.contains("scrub_interval_sec = 60"));
    }

    #[test]
    fn config_sustained_sets_all_short_intervals() {
        let cfg = config_sustained();
        assert!(cfg.contains("gc_interval_sec = 10"));
        assert!(cfg.contains("tombstone_ttl_sec = 5"));
        assert!(cfg.contains("ae_interval_sec = 10"));
        assert!(cfg.contains("scrub_interval_sec = 60"));
    }

    #[test]
    fn config_sustained_allows_16mib_bodies() {
        // The tiered load distribution sends multi-tier blobs up to
        // 16 MiB; the Phase 2 config must accept them.
        let cfg = config_sustained();
        assert!(cfg.contains("max_body_size = 16777216"));
    }

    #[test]
    fn config_sustained_has_port_placeholders() {
        let cfg = config_sustained();
        assert!(cfg.contains("{http_port}"));
        assert!(cfg.contains("{grpc_port}"));
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
        let resolved = resolve_binary_path();
        assert!(resolved.path().exists(), "binary not found at {:?}", resolved.path());
        // The pinned flag must mirror whether OCEANFS_BIN points at an
        // existing binary in this environment.
        let pinned = std::env::var("OCEANFS_BIN").is_ok_and(|p| PathBuf::from(p).exists());
        assert_eq!(resolved.is_operator_pinned(), pinned);
    }

    // ── Port Preservation (§4.7) ─────

    #[test]
    fn save_and_restore_ports_roundtrip() {
        let dir = TempDir::new().expect("temp dir");
        let ports_file = dir.path().join(PORTS_FILE_NAME);

        // Save known ports (http, grpc, membership).
        save_ports(&ports_file, 12345, 12346, 12347).expect("save");
        assert!(ports_file.exists(), "port file must exist after save");

        // Restore.
        let (http, grpc, membership) = restore_ports(&ports_file).expect("restore");
        assert_eq!(http, 12345);
        assert_eq!(grpc, 12346);
        assert_eq!(membership, 12347);
    }

    /// ADR-0028 D1: a port file written before the membership plane
    /// existed (no membership_port line) must still restore, deriving
    /// the membership port from the gRPC port.
    #[test]
    fn restore_ports_without_membership_line_falls_back() {
        let dir = TempDir::new().expect("temp dir");
        let ports_file = dir.path().join(PORTS_FILE_NAME);
        fs::write(&ports_file, "http_port = 20001\ngrpc_port = 20002\n").expect("write");

        let (http, grpc, membership) = restore_ports(&ports_file).expect("restore");
        assert_eq!(http, 20001);
        assert_eq!(grpc, 20002);
        assert_eq!(membership, 20003, "membership port falls back to grpc + 1");
    }

    #[test]
    fn restore_ports_returns_none_when_file_missing() {
        let dir = TempDir::new().expect("temp dir");
        let ports_file = dir.path().join(PORTS_FILE_NAME);
        assert!(restore_ports(&ports_file).is_none());
    }

    #[test]
    fn bind_ports_first_spawn_creates_port_file() {
        let dir = TempDir::new().expect("temp dir");
        let ports_file = dir.path().join(PORTS_FILE_NAME);

        assert!(!ports_file.exists(), "no port file before first spawn");
        let (http, grpc, membership) = bind_ports(&ports_file).expect("bind");
        assert!(ports_file.exists(), "port file must exist after first spawn");
        assert_ne!(http.port(), 0, "HTTP port must be non-zero");
        assert_ne!(grpc.port(), 0, "gRPC port must be non-zero");
        assert_ne!(membership.port(), 0, "membership port must be non-zero");
        assert_ne!(grpc.port(), membership.port(), "membership plane port must differ from gRPC");
    }

    #[test]
    fn bind_ports_restart_reuses_saved_ports() {
        let dir = TempDir::new().expect("temp dir");
        let ports_file = dir.path().join(PORTS_FILE_NAME);

        // First spawn: write ports.
        let (first_http, first_grpc, first_membership) =
            bind_ports(&ports_file).expect("first bind");
        assert_ne!(first_http.port(), first_grpc.port(), "ports must differ");
        assert_ne!(first_grpc.port(), first_membership.port(), "planes must differ");

        // Restart: should reuse the same ports.
        let (second_http, second_grpc, second_membership) =
            bind_ports(&ports_file).expect("second bind");
        assert_eq!(
            first_http.port(),
            second_http.port(),
            "HTTP port must be preserved across restart"
        );
        assert_eq!(
            first_grpc.port(),
            second_grpc.port(),
            "gRPC port must be preserved across restart"
        );
        assert_eq!(
            first_membership.port(),
            second_membership.port(),
            "membership port must be preserved across restart"
        );
    }

    #[test]
    fn bind_ports_restart_falls_back_when_port_taken() {
        let dir = TempDir::new().expect("temp dir");
        let ports_file = dir.path().join(PORTS_FILE_NAME);

        // First spawn.
        let (first_http, first_grpc, _first_membership) =
            bind_ports(&ports_file).expect("first bind");

        // Hold the HTTP port so the restart cannot bind to it.
        let _holder =
            TcpListener::bind(format!("127.0.0.1:{}", first_http.port())).expect("hold HTTP port");

        // Restart: HTTP port is taken — falls back to random, but
        // must still succeed.
        let (second_http, second_grpc, _second_membership) =
            bind_ports(&ports_file).expect("second bind with fallback");
        assert_ne!(
            second_http.port(),
            first_http.port(),
            "HTTP port must change when saved port is taken"
        );
        // gRPC port should also change (one port changed → both reassigned).
        assert_ne!(
            second_grpc.port(),
            first_grpc.port(),
            "gRPC port must also change when HTTP port is taken"
        );
        drop(_holder);
    }

    /// Verifies that `bind_ports` succeeds even when the parent
    /// directory does not exist (the caller must create it first).
    /// This test guards against the regression where `save_ports`
    /// was called before `create_dir_all`.
    #[test]
    fn bind_ports_first_spawn_with_nonexistent_dir() {
        let dir = TempDir::new().expect("temp dir");
        // Construct a path under a non-existent subdirectory.
        let subdir = dir.path().join("node-0");
        assert!(!subdir.exists(), "subdirectory must not exist before test");

        // Create the directory BEFORE binding (mimics the fix).
        std::fs::create_dir_all(&subdir).expect("create subdir");

        let ports_file = subdir.join(PORTS_FILE_NAME);
        let result = bind_ports(&ports_file);
        assert!(result.is_ok(), "bind_ports must succeed when parent dir exists");
        assert!(ports_file.exists(), "port file must be created");
    }

    /// Verifies that `restore_ports` returns `None` when the port
    /// file contains garbled content.
    #[test]
    fn restore_ports_returns_none_when_file_garbled() {
        let dir = TempDir::new().expect("temp dir");
        let ports_file = dir.path().join(PORTS_FILE_NAME);
        std::fs::write(&ports_file, "not a valid port file at all").expect("write garbled file");
        assert!(restore_ports(&ports_file).is_none(), "garbled file must return None");
    }

    // ── Binary staleness verification (§A) ─────────────────────

    /// Creates a fake workspace: `crates/` tree, `Cargo.toml`,
    /// `Cargo.lock`, and a fake binary, with controllable mtimes.
    struct FakeWorkspace {
        dir: TempDir,
        bin_path: PathBuf,
    }

    impl FakeWorkspace {
        fn new() -> Self {
            let dir = TempDir::new().expect("temp dir");
            std::fs::create_dir_all(dir.path().join("crates/oceanfs-core/src"))
                .expect("create crates tree");
            std::fs::write(dir.path().join("Cargo.toml"), "[workspace]\n")
                .expect("write Cargo.toml");
            std::fs::write(dir.path().join("Cargo.lock"), "# lock\n").expect("write Cargo.lock");
            let bin_path = dir.path().join("target/release/oceanfs");
            std::fs::create_dir_all(bin_path.parent().expect("bin parent"))
                .expect("create target dir");
            std::fs::write(&bin_path, "fake binary").expect("write binary");
            // Pin the manifest files to a fixed OLD time so tests can
            // control which file is "newest" via explicit mtimes below.
            let old = filetime::FileTime::from_unix_time(50, 0);
            filetime::set_file_mtime(dir.path().join("Cargo.toml"), old).expect("pin Cargo.toml");
            filetime::set_file_mtime(dir.path().join("Cargo.lock"), old).expect("pin Cargo.lock");
            Self { dir, bin_path }
        }

        /// Writes a source file and pins both it and the binary to
        /// explicit mtimes. `bin_mtime`/`source_mtime` use `1.0`-based
        /// seconds offsets from a fixed epoch for deterministic ordering.
        fn write_source_with_mtimes(&self, rel: &str, bin_mtime: f64, source_mtime: f64) {
            let source = self.dir.path().join(rel);
            std::fs::write(&source, "fn main() {}\n").expect("write source");
            filetime::set_file_mtime(
                &source,
                filetime::FileTime::from_unix_time(source_mtime as i64, 0),
            )
            .expect("set source mtime");
            filetime::set_file_mtime(
                &self.bin_path,
                filetime::FileTime::from_unix_time(bin_mtime as i64, 0),
            )
            .expect("set binary mtime");
        }

        fn workspace_root(&self) -> PathBuf {
            self.dir.path().to_path_buf()
        }
    }

    #[test]
    fn check_binary_freshness_when_stale_returns_error() {
        let ws = FakeWorkspace::new();
        // Binary (100.0) older than the newest source (200.0) → stale.
        ws.write_source_with_mtimes("crates/oceanfs-core/src/lib.rs", 100.0, 200.0);

        let err = check_binary_freshness(&ws.bin_path, &ws.workspace_root())
            .expect_err("stale binary must fail the freshness check");
        match err {
            Error::StaleBinary { bin_path, source_path, .. } => {
                assert_eq!(bin_path, ws.bin_path);
                assert_eq!(
                    source_path,
                    ws.dir.path().join("crates/oceanfs-core/src/lib.rs"),
                    "the newest source file must be named in the error"
                );
            }
            other => panic!("expected StaleBinary, got {other:?}"),
        }
    }

    #[test]
    fn check_binary_freshness_when_stale_error_message_names_fix() {
        let ws = FakeWorkspace::new();
        ws.write_source_with_mtimes("crates/oceanfs-core/src/lib.rs", 100.0, 200.0);

        let err = check_binary_freshness(&ws.bin_path, &ws.workspace_root())
            .expect_err("stale binary must fail");
        let message = err.to_string();
        assert!(
            message.contains("cargo build --release"),
            "message must suggest the fix: {message}"
        );
        assert!(
            message.contains("OCEANFS_BIN"),
            "message must suggest the pinned-binary escape hatch: {message}"
        );
    }

    #[test]
    fn check_binary_freshness_when_lockfile_only_newer_suggests_touch() {
        // A Cargo.lock change that does not affect the binary (e.g. an
        // e2e-only dev-dependency bump) must not leave the operator at a
        // dead end: `cargo build --release` may no-op, so the message
        // must explain the touch/OCEANFS_BIN acknowledgement.
        let ws = FakeWorkspace::new();
        ws.write_source_with_mtimes("crates/oceanfs-core/src/lib.rs", 100.0, 100.0);
        let lock = ws.dir.path().join("Cargo.lock");
        filetime::set_file_mtime(&lock, filetime::FileTime::from_unix_time(200, 0))
            .expect("make the lockfile newer than the binary");

        let err = check_binary_freshness(&ws.bin_path, &ws.workspace_root())
            .expect_err("lockfile newer than the binary must fail");
        let message = err.to_string();
        assert!(
            message.contains("workspace manifest"),
            "must explain the manifest case: {message}"
        );
        assert!(message.contains("`touch`"), "must suggest touching the binary: {message}");
        assert!(
            message.contains("OCEANFS_BIN"),
            "must suggest the pinned-binary escape hatch: {message}"
        );
    }

    #[test]
    fn check_binary_freshness_when_fresh_passes() {
        let ws = FakeWorkspace::new();
        // Binary (200.0) newer than every source (100.0) → fresh.
        ws.write_source_with_mtimes("crates/oceanfs-core/src/lib.rs", 200.0, 100.0);

        check_binary_freshness(&ws.bin_path, &ws.workspace_root())
            .expect("fresh binary must pass the freshness check");
    }

    #[test]
    fn check_binary_freshness_when_equal_mtime_passes() {
        // Same-tick builds (coarse filesystems) must not be flagged.
        let ws = FakeWorkspace::new();
        ws.write_source_with_mtimes("crates/oceanfs-core/src/lib.rs", 100.0, 100.0);

        check_binary_freshness(&ws.bin_path, &ws.workspace_root())
            .expect("binary with equal mtime must pass (>= comparison)");
    }

    #[test]
    fn check_binary_freshness_skips_missing_binary() {
        // A missing binary fails at spawn time, not at the freshness
        // check (which has nothing to compare).
        let ws = FakeWorkspace::new();
        ws.write_source_with_mtimes("crates/oceanfs-core/src/lib.rs", 100.0, 200.0);
        let missing = ws.dir.path().join("target/release/does-not-exist");

        check_binary_freshness(&missing, &ws.workspace_root())
            .expect("missing binary must not fail the freshness check");
    }

    #[test]
    fn newest_source_mtime_finds_deepest_newest_source() {
        let dir = TempDir::new().expect("temp dir");
        std::fs::create_dir_all(dir.path().join("crates/oceanfs-core/src/deep")).expect("mkdir");
        let older = dir.path().join("crates/oceanfs-core/src/lib.rs");
        let newest = dir.path().join("crates/oceanfs-core/src/deep/inner.rs");
        std::fs::write(&older, "a").expect("write older");
        std::fs::write(&newest, "b").expect("write newest");
        let epoch = SystemTime::UNIX_EPOCH;
        filetime::set_file_mtime(&older, filetime::FileTime::from_unix_time(100, 0)).expect("mt");
        filetime::set_file_mtime(&newest, filetime::FileTime::from_unix_time(300, 0)).expect("mt");

        let (found, mtime) =
            newest_source_mtime(dir.path()).expect("must find a source file in the fake workspace");
        assert_eq!(found, newest);
        let secs = mtime.duration_since(epoch).expect("after epoch").as_secs();
        assert_eq!(secs, 300);
    }

    #[test]
    fn newest_source_mtime_considers_workspace_manifest_files() {
        // A stale binary with an untouched crates/ tree but a newer
        // Cargo.lock must still be flagged (dependency changes matter).
        let dir = TempDir::new().expect("temp dir");
        std::fs::create_dir_all(dir.path().join("crates/oceanfs-core/src")).expect("mkdir");
        std::fs::write(dir.path().join("crates/oceanfs-core/src/lib.rs"), "a").expect("write rs");
        std::fs::write(dir.path().join("Cargo.lock"), "# lock\n").expect("write lock");
        filetime::set_file_mtime(
            dir.path().join("crates/oceanfs-core/src/lib.rs"),
            filetime::FileTime::from_unix_time(100, 0),
        )
        .expect("mt rs");
        filetime::set_file_mtime(
            dir.path().join("Cargo.lock"),
            filetime::FileTime::from_unix_time(400, 0),
        )
        .expect("mt lock");

        let (found, _) = newest_source_mtime(dir.path()).expect("must find a source");
        assert_eq!(found, dir.path().join("Cargo.lock"));
    }

    #[test]
    fn newest_source_mtime_returns_none_without_crates_tree() {
        let dir = TempDir::new().expect("temp dir");
        // No crates/, no manifest files — nothing to compare against.
        assert!(newest_source_mtime(dir.path()).is_none());
    }

    // ── Log capture (§B) ────────────────────────────────────────

    #[test]
    fn log_dir_is_anchored_to_e2e_crate_root() {
        let dir = log_dir();
        assert!(
            dir.starts_with(E2E_CRATE_ROOT),
            "log dir must be anchored to the e2e crate root, got {dir:?}"
        );
        assert!(dir.ends_with(Path::new("target/e2e-logs")));
    }

    #[test]
    fn resolve_log_level_precedence_options_env_default() {
        // Explicit options win over env, which wins over "info".
        assert_eq!(resolve_log_level(Some("debug"), Some("warn")), "debug");
        assert_eq!(resolve_log_level(None, Some("warn")), "warn");
        assert_eq!(resolve_log_level(None, None), "info");
        assert_eq!(resolve_log_level(Some("trace"), None), "trace");
    }

    #[test]
    fn resolve_capture_precedence_options_env_default() {
        // Explicit options win; env opt-out is "0"/"false"; default on.
        assert!(!resolve_capture(Some(false), Some("1")));
        assert!(resolve_capture(Some(true), Some("0")));
        assert!(!resolve_capture(None, Some("0")));
        assert!(!resolve_capture(None, Some("false")));
        assert!(resolve_capture(None, Some("1")));
        assert!(resolve_capture(None, Some("true")));
        assert!(resolve_capture(None, None));
    }

    #[test]
    fn grep_logs_in_files_finds_matching_lines() {
        let dir = TempDir::new().expect("temp dir");
        let log = dir.path().join("node.log");
        std::fs::write(
            &log,
            "info: worker started\nwarn: seal queue full; seal deferred\nerror: other\n",
        )
        .expect("write fixture log");

        let matches = grep_logs_in_files(std::slice::from_ref(&log), "seal queue full")
            .expect("grep must succeed");
        assert_eq!(matches.len(), 1);
        assert!(matches[0].contains("seal queue full; seal deferred"));

        let none =
            grep_logs_in_files(std::slice::from_ref(&log), "BadDigest").expect("grep must succeed");
        assert!(none.is_empty(), "pattern not present must yield no matches");
    }

    #[test]
    fn grep_logs_in_files_skips_missing_files() {
        let dir = TempDir::new().expect("temp dir");
        let missing = dir.path().join("does-not-exist.log");
        let found = grep_logs_in_files(&[missing], "anything").expect("grep must succeed");
        assert!(found.is_empty());
    }

    #[test]
    fn grep_logs_in_files_returns_error_on_unreadable_file() {
        // A directory cannot be read as a log file — deterministic
        // error regardless of process privileges (no chmod needed).
        let dir = TempDir::new().expect("temp dir");
        let log = dir.path().join("not-a-file");
        std::fs::create_dir_all(&log).expect("mkdir");
        let err = grep_logs_in_files(&[log], "x").expect_err("a directory must error");
        assert!(matches!(err, Error::LogRead(..)));
    }
}
