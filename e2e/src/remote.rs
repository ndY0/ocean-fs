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
//! - SSH command execution ([`RemoteCluster::ssh_exec`] and
//!   [`RemoteCluster::ssh_exec_on`]) returning [`SshOutput`] — the black-box
//!   substrate for the fleet fault injectors (`load::fleet_degrade`).
//! - JSON POST ([`RemoteNode::post_json`] / [`RemoteCluster::post_json`]) for
//!   the admin surface that requires a body (`POST /admin/pools`, drain
//!   `{"mode": …}`).
//!
//! ## Environment contract
//!
//! | Variable | Purpose |
//! |---|---|
//! | `TARGET_HOST` | Comma-separated `host:port` list of remote OceanFS endpoints (Phase 2: exactly one; Phase 3: the fleet, one per node). |
//! | `TARGET_HOST_SSH` | Comma-separated SSH targets for crash control and fault injection, one per node, e.g. `root@10.0.0.2,root@10.0.0.3` or `~/.ssh/config` aliases. A single target (Phase 2) or a `~/.ssh/config` alias like `oceanfs-sut` also works. When unset, remote crash-recovery/injection is skipped (local quick mode always covers crash recovery). |
//! | `TARGET_SERVICE` | systemd unit name managing the SUT OceanFS process (default `oceanfs`). The unit must **not** auto-restart (`Restart=no`), otherwise the SIGKILL→restart sequencing is meaningless. |
//!
//! ## SSH command safety
//!
//! [`RemoteCluster::ssh_exec_on`] takes a command string. Callers must build
//! that string from **typed parameters** (a volume id, a mount path, a
//! segment id) and shell-quote every interpolated value — never pass an
//! arbitrary string that originated in a report, record, or environment
//! variable. The fleet injectors in
//! [`load::fleet_degrade`](crate::load::fleet_degrade) follow this rule.

use std::{net::SocketAddr, time::Duration};

use serde_json::Value;

use crate::harness::{Error, LoadTarget};

/// Builds a shared reqwest client for remote endpoints (30s timeout,
/// connection pool reused across requests).
fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client should build with default TLS")
}

/// Builds a JSON POST request without sending it.
///
/// Kept separate from [`RemoteNode::post_json`] so the request shape
/// (method, content type, body) is unit-testable without a receiver
/// server. The body is serialized explicitly because reqwest is built
/// without its `json` feature.
fn post_json_request(
    client: &reqwest::Client,
    url: String,
    body: &Value,
) -> Result<reqwest::Request, Error> {
    let payload = serde_json::to_vec(body)
        .map_err(|e| Error::ClusterError(format!("failed to serialize JSON body: {e}")))?;
    Ok(client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(payload)
        .build()?)
}

/// Captured result of a single SSH command run on a remote node's VM.
///
/// A non-zero `status` is **not** an error: the command ran and failed.
/// [`Error::Ssh`] is reserved for ssh itself failing to run or being
/// misconfigured.
///
/// # Examples
///
/// ```
/// use e2e::remote::SshOutput;
///
/// let out = SshOutput {
///     status: 0,
///     stdout: "sdb\n".to_string(),
///     stderr: String::new(),
/// };
/// assert!(out.success());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshOutput {
    /// Process exit status (`-1` when killed by a signal).
    pub status: i32,
    /// Captured standard output (lossy UTF-8).
    pub stdout: String,
    /// Captured standard error (lossy UTF-8).
    pub stderr: String,
}

impl SshOutput {
    /// Returns `true` when the command exited with status 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::remote::SshOutput;
    ///
    /// let out = SshOutput { status: 1, stdout: String::new(), stderr: "boom".into() };
    /// assert!(!out.success());
    /// ```
    pub fn success(&self) -> bool {
        self.status == 0
    }
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

    /// HTTP POST with a JSON body (`Content-Type: application/json`).
    ///
    /// The admin surface requires a body for `POST /admin/pools` (a
    /// `StoragePoolConfig`) and accepts one for drain
    /// (`{"mode":"cluster"|"intra-node"}`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use e2e::remote::RemoteNode;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let node = RemoteNode::new("10.0.0.2:9000")?;
    /// let resp = node
    ///     .post_json("/admin/pools", &serde_json::json!({ "name": "spare" }))
    ///     .await?;
    /// assert!(resp.status().is_success());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request cannot be built or HTTP fails.
    pub async fn post_json(&self, path: &str, body: &Value) -> Result<reqwest::Response, Error> {
        let request = post_json_request(&self.client, format!("{}{}", self.base_url, path), body)?;
        Ok(self.client.execute(request).await?)
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
    /// Per-node SSH targets (from `TARGET_HOST_SSH`), aligned by index with
    /// `nodes`. Empty entries mean "no SSH configured for that node".
    ssh_targets: Vec<String>,
}

impl RemoteCluster {
    /// Connects to the endpoints listed in `TARGET_HOST`.
    ///
    /// Accepts a single `host:port` or a comma-separated list
    /// (`TARGET_HOSTS` style, for Phase 3+ multi-node remote targets).
    /// Per-node SSH targets are read from `TARGET_HOST_SSH` (comma-separated,
    /// aligned by index); unset means SSH operations are unavailable and
    /// callers must skip them explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error if any endpoint fails to parse or an SSH target is
    /// malformed.
    pub fn connect(target_host: &str) -> Result<Self, Error> {
        Self::connect_with_ssh_targets(target_host, ssh_targets_from_env())
    }

    /// Connects to the endpoints listed in `target_host` with explicit
    /// per-node SSH targets.
    ///
    /// Used by tests and by callers that resolve SSH targets themselves
    /// (e.g. from the provisioning record) instead of `TARGET_HOST_SSH`.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::remote::RemoteCluster;
    ///
    /// let cluster = RemoteCluster::connect_with_ssh_targets(
    ///     "10.0.0.2:9000",
    ///     vec!["root@10.0.0.2".to_string()],
    /// )
    /// .expect("connect");
    /// assert_eq!(cluster.ssh_target_for(0), Some("root@10.0.0.2"));
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if any endpoint fails to parse or any SSH target is
    /// malformed (conservative character set, never option-shaped).
    pub fn connect_with_ssh_targets(
        target_host: &str,
        ssh_targets: Vec<String>,
    ) -> Result<Self, Error> {
        let client = build_client();
        let mut hosts: Vec<&str> = target_host
            .split(',')
            .map(|host_port| host_port.trim())
            .filter(|h| !h.is_empty())
            .collect();
        if hosts.is_empty() {
            return Err(Error::ClusterError("TARGET_HOST is empty".into()));
        }
        for target in &ssh_targets {
            validate_ssh_target(target)?;
        }
        let nodes = hosts.drain(..).map(RemoteNode::new).collect::<Result<Vec<_>, _>>()?;
        Ok(Self { nodes, client, ssh_targets })
    }

    /// Returns the number of remote endpoints.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if there are no remote endpoints.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns the configured per-node SSH targets, aligned by index with
    /// the remote endpoints (possibly shorter or empty when unset).
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::remote::RemoteCluster;
    ///
    /// let cluster = RemoteCluster::connect_with_ssh_targets(
    ///     "10.0.0.2:9000,10.0.0.3:9000",
    ///     vec!["root@10.0.0.2".to_string(), "root@10.0.0.3".to_string()],
    /// )
    /// .expect("connect");
    /// assert_eq!(cluster.ssh_targets().len(), 2);
    /// ```
    pub fn ssh_targets(&self) -> &[String] {
        &self.ssh_targets
    }

    /// Returns the SSH target for node `i`, or `None` when SSH is not
    /// configured for that node.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::remote::RemoteCluster;
    ///
    /// let cluster = RemoteCluster::connect_with_ssh_targets(
    ///     "10.0.0.2:9000",
    ///     vec!["root@10.0.0.2".to_string()],
    /// )
    /// .expect("connect");
    /// assert_eq!(cluster.ssh_target_for(0), Some("root@10.0.0.2"));
    /// assert_eq!(cluster.ssh_target_for(1), None);
    /// ```
    pub fn ssh_target_for(&self, i: usize) -> Option<&str> {
        self.ssh_targets.get(i).map(String::as_str)
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

    /// Waits until `/admin/health` on node `node_idx` stops returning 2xx
    /// (the node is down) or the timeout elapses. Returns `true` if the
    /// node went down.
    async fn wait_node_health_down(&self, node_idx: usize, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > timeout {
                return false;
            }
            match self.nodes[node_idx].get("/admin/health").await {
                Ok(resp) if resp.status().is_success() => {}
                _ => return true,
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// SIGKILLs and restarts node `node_idx`'s OceanFS service over SSH.
    ///
    /// Used by the Phase 3 churn scheduler in remote-target mode: the
    /// node's VM is reached at `ssh_target` (the i-th `TARGET_HOST_SSH`
    /// entry), `systemctl kill -s KILL <service>`, wait for THAT node to
    /// go down, then `systemctl restart <service>` and wait for its health
    /// to return. The data directory persists on the node's VM, so WAL
    /// replay exercises the same recovery path as local SIGKILL.
    ///
    /// The systemd unit must be configured with `Restart=no`, otherwise
    /// the service may come back before the harness observes it down.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Ssh`] if any SSH command fails, the node never
    /// goes down, or it never comes back healthy.
    pub async fn kill_and_restart_node_via_ssh(
        &self,
        node_idx: usize,
        ssh_target: &str,
        service: &str,
    ) -> Result<(), Error> {
        // 1. SIGKILL the service's main process. `--kill-who=main` is
        // required: plain `systemctl kill` also signals the unit's
        // auxiliary/control processes, which do not exist for our
        // Type=simple unit and make systemd fail with "Failed to send
        // signal SIGKILL to auxiliary processes: Invalid argument" even
        // though the main process was killed.
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
            "--kill-who=main",
            service,
        ])
        .await?;

        // 2. Wait for the node to go down.
        if !self.wait_node_health_down(node_idx, Duration::from_secs(30)).await {
            return Err(Error::Ssh(format!(
                "node {node_idx} ({service}) did not go down within 30s of SIGKILL (is the unit Restart=no?)"
            )));
        }
        eprintln!("remote: node {node_idx} down after SIGKILL, restarting {service} via ssh");

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

        // 5. Wait for THIS node's health.
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > Duration::from_secs(60) {
                return Err(Error::HealthTimeout(Duration::from_secs(60)));
            }
            match self.nodes[node_idx].get("/admin/health").await {
                Ok(resp) if resp.status().is_success() => {
                    let _ = resp.bytes().await;
                    return Ok(());
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
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
        self.kill_and_restart_node_via_ssh(0, ssh_target, service).await
    }

    /// HTTP POST with a JSON body to node `i`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use e2e::remote::RemoteCluster;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000")?;
    /// let resp = cluster
    ///     .post_json(0, "/admin/pools/0/drain", &serde_json::json!({ "mode": "cluster" }))
    ///     .await?;
    /// assert!(resp.status().is_success());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if node `i` is out of bounds or HTTP fails.
    pub async fn post_json(
        &self,
        i: usize,
        path: &str,
        body: &Value,
    ) -> Result<reqwest::Response, Error> {
        self.nodes
            .get(i)
            .ok_or_else(|| Error::ClusterError(format!("node {i} out of bounds")))?
            .post_json(path, body)
            .await
    }

    /// Runs a single shell command on node `node_idx`'s VM over SSH and
    /// returns its captured output.
    ///
    /// The command runs on the blocking pool (`ssh` is a synchronous child
    /// process) and is passed to ssh as a single argument; the **caller**
    /// is responsible for building it from typed parameters and quoting
    /// interpolated values (see the module-level SSH safety note).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use e2e::remote::RemoteCluster;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect_with_ssh_targets(
    ///     "10.0.0.2:9000",
    ///     vec!["root@10.0.0.2".to_string()],
    /// )?;
    /// let out = cluster.ssh_exec(0, "mountpoint -q /mnt/oceanfs-data").await?;
    /// assert!(out.success());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Ssh`] when no SSH target is configured for the node
    /// or ssh itself cannot run.
    pub async fn ssh_exec(&self, node_idx: usize, command: &str) -> Result<SshOutput, Error> {
        let target = self.ssh_target_for(node_idx).ok_or_else(|| {
            Error::Ssh(format!(
                "TARGET_HOST_SSH has no entry for node {node_idx} ({} targets configured)",
                self.ssh_targets.len()
            ))
        })?;
        self.ssh_exec_on(target, command).await
    }

    /// Runs a single shell command against an explicit SSH target and
    /// returns its captured output.
    ///
    /// Used when the per-node target comes from the provisioning record
    /// rather than `TARGET_HOST_SSH`. Same quoting contract as
    /// [`RemoteCluster::ssh_exec`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use e2e::remote::RemoteCluster;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000")?;
    /// let out = cluster.ssh_exec_on("root@10.0.0.2", "uname -s").await?;
    /// assert_eq!(out.stdout.trim(), "Linux");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Ssh`] when the target is malformed or ssh cannot run.
    pub async fn ssh_exec_on(&self, ssh_target: &str, command: &str) -> Result<SshOutput, Error> {
        validate_ssh_target(ssh_target)?;
        run_ssh_output(&["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_target, command])
            .await
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
    let display = format!("{args:?}");
    let output = run_ssh_output(args).await?;
    if !output.success() {
        return Err(Error::Ssh(format!("ssh {display} exited with status {}", output.status)));
    }
    Ok(())
}

/// Runs an `ssh` command to completion and captures its output.
///
/// Runs on the blocking pool — `ssh` is a synchronous child process.
/// Host-key checking is disabled: the SUT is a disposable test VM reached
/// over the internal network, and the harness may be re-provisioned at any
/// time.
///
/// # Errors
///
/// Returns [`Error::Ssh`] when ssh cannot be spawned or the blocking task
/// panics. A non-zero remote exit status is returned in the output, not as
/// an error.
async fn run_ssh_output(args: &[&str]) -> Result<SshOutput, Error> {
    let mut full_args: Vec<&str> =
        vec!["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"];
    full_args.extend_from_slice(args);
    // Own the arguments so the blocking closure is `'static`.
    let args: Vec<String> = full_args.iter().map(|s| (*s).to_string()).collect();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("ssh")
            .args(&args)
            .output()
            .map_err(|e| Error::Ssh(format!("failed to spawn ssh {args:?}: {e}")))
    })
    .await
    .map_err(|e| Error::Ssh(format!("ssh task join failed: {e}")))??;
    Ok(SshOutput {
        status: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

/// Reads the comma-separated `TARGET_HOST_SSH` list into per-node targets.
fn ssh_targets_from_env() -> Vec<String> {
    std::env::var("TARGET_HOST_SSH")
        .ok()
        .map(|list| {
            list.split(',').map(str::trim).filter(|t| !t.is_empty()).map(str::to_string).collect()
        })
        .unwrap_or_default()
}

/// Validates an SSH target before it is handed to the `ssh` argv.
///
/// SSH targets come from `TARGET_HOST_SSH` or the provisioning record —
/// both are operator-controlled, but a target is passed to `ssh` as an
/// **argument**, so a value starting with `-` could be parsed as an ssh
/// option (e.g. `-oProxyCommand=…`). Restrict targets to
/// `[A-Za-z0-9._@:\[\]-]`, at most 255 chars, and never a leading `-`.
fn validate_ssh_target(target: &str) -> Result<(), Error> {
    let ok = !target.is_empty()
        && target.len() <= 255
        && !target.starts_with('-')
        && target.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '@' | '.' | '_' | '-' | ':' | '[' | ']')
        });
    if ok {
        Ok(())
    } else {
        Err(Error::Ssh(format!("invalid SSH target {target:?}")))
    }
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

    #[test]
    fn post_json_request_sets_method_content_type_and_body() {
        let client = build_client();
        let request = post_json_request(
            &client,
            "http://10.0.0.2:9000/admin/pools".to_string(),
            &serde_json::json!({ "name": "spare" }),
        )
        .expect("build request");
        assert_eq!(request.method(), reqwest::Method::POST);
        assert_eq!(request.url().as_str(), "http://10.0.0.2:9000/admin/pools");
        let content_type =
            request.headers().get(reqwest::header::CONTENT_TYPE).expect("content-type set");
        assert!(content_type.to_str().unwrap().starts_with("application/json"));
        let body = request.body().and_then(reqwest::Body::as_bytes).expect("in-memory body");
        assert_eq!(body, br#"{"name":"spare"}"#);
    }

    #[test]
    fn validate_ssh_target_accepts_hosts_and_aliases() {
        for target in ["root@10.0.0.2", "oceanfs-sut", "user@[fd00::1]:22", "10.0.0.2"] {
            validate_ssh_target(target).expect("valid target");
        }
    }

    #[test]
    fn validate_ssh_target_rejects_option_injection_and_whitespace() {
        let long = "a".repeat(256);
        for target in ["-oProxyCommand=touch /tmp/pwned", "root@10.0.0.2 extra", "", long.as_str()]
        {
            assert!(validate_ssh_target(target).is_err(), "must reject {target:?}");
        }
    }

    #[test]
    fn connect_with_ssh_targets_rejects_invalid_target() {
        let err = RemoteCluster::connect_with_ssh_targets(
            "10.0.0.5:9000",
            vec!["-oProxyCommand=touch /tmp/pwned".to_string()],
        )
        .expect_err("option-shaped target must fail");
        assert!(err.to_string().contains("invalid SSH target"));
    }

    #[test]
    fn connect_with_ssh_targets_maps_per_node_targets() {
        let cluster = RemoteCluster::connect_with_ssh_targets(
            "10.0.0.5:9000,10.0.0.6:9000",
            vec!["root@10.0.0.5".to_string(), "root@10.0.0.6".to_string()],
        )
        .expect("connect");
        assert_eq!(cluster.ssh_target_for(0), Some("root@10.0.0.5"));
        assert_eq!(cluster.ssh_target_for(1), Some("root@10.0.0.6"));
        assert_eq!(cluster.ssh_target_for(2), None);
        assert_eq!(cluster.ssh_targets(), &["root@10.0.0.5", "root@10.0.0.6"]);
    }

    #[tokio::test]
    async fn ssh_exec_without_configured_target_returns_error() {
        let cluster =
            RemoteCluster::connect_with_ssh_targets("10.0.0.5:9000", Vec::new()).expect("connect");
        let err = cluster.ssh_exec(0, "true").await.expect_err("no target configured");
        assert!(err.to_string().contains("no entry for node 0"));
    }
}
