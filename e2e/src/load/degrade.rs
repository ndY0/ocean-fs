//! Failure injection methods for Phase 4 degraded-mode testing.
//!
//! Provides [`Cluster`] extension methods to simulate real-world failures:
//! artificial network latency via `tc netem`, disk-full conditions,
//! segment file corruption, and an end-to-end corruption-then-heal
//! verification scenario.
//!
//! All injectors are platform-gated: `tc`-based operations require Linux.
//! On non-Linux platforms they return an error with a clear message.
//!
//! ## Platform support
//!
//! | Injector | Linux | macOS | Notes |
//! |---|---|---|---|
//! | `inject_latency` | ✅ `tc netem` | ❌ skipped | requires `tc` |
//! | `remove_latency` | ✅ `tc` | ❌ skipped | |
//! | `fill_disk` | ✅ `fallocate`/`dd` + `df` | ❌ skipped | requires `fallocate`/`dd`; refuses tmpfs/ramfs |
//! | `corrupt_shard` | ✅ raw I/O | ✅ raw I/O | no platform dependency |
//!
//! ## Usage
//!
//! ```no_run
//! use e2e::harness::{config_standard, Cluster};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let cluster = Cluster::spawn(1, &config_standard()).await?;
//!
//! // Inject 500ms latency, then remove it.
//! cluster.inject_latency(0, 500).await?;
//! cluster.remove_latency(0).await?;
//! # Ok(())
//! # }
//! ```

use std::{
    fs::{self, OpenOptions},
    io::{Seek, Write},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use serde::Serialize;

use crate::harness::{Cluster, Error};

// ---------------------------------------------------------------------------
// Cluster extensions — latency injection
// ---------------------------------------------------------------------------

impl Cluster {
    /// Injects artificial network latency on the loopback interface.
    ///
    /// Shells out to `tc qdisc add dev lo root netem delay {delay_ms}ms`.
    /// Only supported on Linux; on other platforms returns an error.
    ///
    /// NOTE: This affects ALL traffic on the loopback interface, not just
    /// the target node. For a multi-node cluster on the same machine, all
    /// inter-node communication will be delayed. Because of that
    /// contamination, the loopback path is **gated behind an explicit
    /// opt-in** (`E2E_ALLOW_LOOPBACK_LATENCY=1`); fleet scenarios use the
    /// fleet injector's internal-interface variant instead.
    ///
    /// # Errors
    ///
    /// Returns an error on non-Linux platforms, without the explicit
    /// opt-in, or if the `tc` command fails.
    pub async fn inject_latency(&self, _node_i: usize, delay_ms: u64) -> Result<(), Error> {
        if !cfg!(target_os = "linux") {
            eprintln!("inject_latency: skipped on non-Linux platform");
            return Err(Error::ClusterError("inject_latency requires Linux (tc netem)".into()));
        }
        if !loopback_latency_opt_in(std::env::var("E2E_ALLOW_LOOPBACK_LATENCY").ok().as_deref()) {
            eprintln!(
                "inject_latency: skipped — loopback netem contaminates every node on this host; \
                 set E2E_ALLOW_LOOPBACK_LATENCY=1 to opt in (fleet scenarios use the internal \
                 interface via FleetInjector)"
            );
            return Err(Error::ClusterError(
                "skipped: loopback latency injection requires E2E_ALLOW_LOOPBACK_LATENCY=1".into(),
            ));
        }

        let output = Command::new("tc")
            .args(["qdisc", "add", "dev", "lo", "root", "netem", "delay", &format!("{delay_ms}ms")])
            .output()
            .map_err(|e| Error::ClusterError(format!("tc command failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // If netem qdisc already exists, tc returns an error.
            // This is acceptable — the existing rule continues to apply.
            if stderr.contains("File exists") {
                eprintln!("inject_latency: netem qdisc already exists on lo; skipping");
                return Ok(());
            }
            return Err(Error::ClusterError(format!("tc add netem failed: {stderr}")));
        }

        Ok(())
    }

    /// Removes artificial network latency from the loopback interface.
    ///
    /// Shells out to `tc qdisc del dev lo root`. It is not an error if
    /// no qdisc was present. Gated by the same explicit opt-in as
    /// [`Cluster::inject_latency`].
    ///
    /// # Errors
    ///
    /// Returns an error on non-Linux platforms or without the explicit
    /// opt-in.
    pub async fn remove_latency(&self, _node_i: usize) -> Result<(), Error> {
        if !cfg!(target_os = "linux") {
            eprintln!("remove_latency: skipped on non-Linux platform");
            return Err(Error::ClusterError("remove_latency requires Linux (tc)".into()));
        }
        if !loopback_latency_opt_in(std::env::var("E2E_ALLOW_LOOPBACK_LATENCY").ok().as_deref()) {
            eprintln!(
                "remove_latency: skipped — set E2E_ALLOW_LOOPBACK_LATENCY=1 to opt in to the \
                 loopback path"
            );
            return Err(Error::ClusterError(
                "skipped: loopback latency injection requires E2E_ALLOW_LOOPBACK_LATENCY=1".into(),
            ));
        }

        Command::new("tc")
            .args(["qdisc", "del", "dev", "lo", "root"])
            .output()
            .map_err(|e| Error::ClusterError(format!("tc del failed: {e}")))?;

        // Ignore errors — the qdisc may not have existed.
        Ok(())
    }
}

/// Returns `true` when the loopback latency injector is explicitly enabled.
///
/// Local-spawn nodes share `lo`, so a loopback `netem` delays every node on
/// the host; the epic keeps that path behind an explicit opt-in
/// (`E2E_ALLOW_LOOPBACK_LATENCY`) instead of letting a scenario contaminate
/// unrelated nodes. Fleet scenarios use the fleet injector's internal
/// interface.
fn loopback_latency_opt_in(value: Option<&str>) -> bool {
    matches!(value, Some("1") | Some("true") | Some("yes"))
}

// ---------------------------------------------------------------------------
// Disk fill
// ---------------------------------------------------------------------------

impl Cluster {
    /// Fills the filesystem of `node_i`'s data directory to approximately
    /// `target_pct`% usage.
    ///
    /// Creates a file (`fill.bin`) in the node's data directory using `dd`.
    /// The file size is computed from current available space reported by
    /// `df`. On non-Linux platforms, returns an error.
    ///
    /// Returns the path to the fill file so the caller can remove it for
    /// cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error on non-Linux platforms, if the node has been killed,
    /// or if disk commands fail.
    pub async fn fill_disk(&self, node_i: usize, target_pct: u8) -> Result<PathBuf, Error> {
        let data_dir = self.node(node_i).data_dir().to_path_buf();
        // spawn_blocking: dd/df are synchronous child processes (perf 8.3).
        tokio::task::spawn_blocking(move || fill_dir(&data_dir, target_pct))
            .await
            .map_err(|e| Error::ClusterError(format!("fill task join failed: {e}")))?
    }

    /// Fills a local pool-role root on `node_i` to approximately
    /// `target_pct`% usage.
    ///
    /// Local spawns inject sibling pool roots (`{base}/pool-data`,
    /// `{base}/pool-wal`, `{base}/pool-meta`, `{base}/pool-hints`); this is
    /// the local CI counterpart of the fleet injector's
    /// `fill_volume(node, role, pct)` and targets a real pool root rather
    /// than `data_dir`. Returns the fill file path for cleanup.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::harness::{config_standard, Cluster};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = Cluster::spawn(1, &config_standard()).await?;
    /// let fill = cluster.fill_role_root(0, "data", 95).await?;
    /// std::fs::remove_file(fill)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the local role root is absent (e.g. the config
    /// declared its own `[storage]` block), on non-Linux platforms, or if
    /// disk commands fail.
    pub async fn fill_role_root(
        &self,
        node_i: usize,
        role: &str,
        target_pct: u8,
    ) -> Result<PathBuf, Error> {
        let root = self.local_role_root(node_i, role)?;
        // spawn_blocking: dd/df are synchronous child processes (perf 8.3).
        tokio::task::spawn_blocking(move || fill_dir(&root, target_pct))
            .await
            .map_err(|e| Error::ClusterError(format!("fill task join failed: {e}")))?
    }

    /// Returns the harness-injected local pool root for `role` on `node_i`.
    ///
    /// Local spawns without a `[storage]` block place pool roots next to the
    /// node's own data directory (`{base}/pool-data`, …; the node's data
    /// directory is `{base}/data`) because a pool root must stay disjoint
    /// from `data_dir` (ADR-0031). Fails loudly when the conventional root
    /// does not exist instead of falling back onto `data_dir` — the local
    /// analogue of the fleet's no-silent-fallback mount pre-flight.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::harness::{config_standard, Cluster};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = Cluster::spawn(1, &config_standard()).await?;
    /// let root = cluster.local_role_root(0, "data")?;
    /// println!("local data pool root: {}", root.display());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the role root is not a directory.
    pub fn local_role_root(&self, node_i: usize, role: &str) -> Result<PathBuf, Error> {
        let data_dir = self.node(node_i).data_dir().to_path_buf();
        local_role_dir(&data_dir, role)
    }
}

/// Resolves `{base}/pool-{role}` and requires it to exist.
///
/// The local harness treats the spawn path as the node **BASE**: the node's
/// own data directory is `{base}/data` and the injected pool roots are
/// `{base}/pool-{role}` (`NodeProcess::spawn_with_data_dir_and_options`,
/// ADR-0031). `metadata` is the config role; the harness names the
/// directory `meta`.
fn local_role_dir(data_dir: &Path, role: &str) -> Result<PathBuf, Error> {
    let dir_role = match role {
        "metadata" => "meta",
        other => other,
    };
    let root = data_dir.join(format!("pool-{dir_role}"));
    if root.is_dir() {
        Ok(root)
    } else {
        Err(Error::ClusterError(format!(
            "local_role_root: {} not found (local spawns without a [storage] block inject \
             pool-* next to the node's data directory)",
            root.display()
        )))
    }
}

/// Fills `dir` to approximately `target_pct`% usage.
///
/// Allocates `dir/fill.bin` with `fallocate` (falling back to `dd`).
/// Refuses memory-backed filesystems (tmpfs/ramfs): filling those consumes
/// RAM and would destabilize the host instead of exercising ENOSPC — point
/// `TMPDIR` at a disk-backed directory for the local-spawn scenario.
///
/// Synchronous: callers run it on the blocking pool (`spawn_blocking`) —
/// `fallocate`/`dd`/`df` are child processes, not async work (perf 8.3).
fn fill_dir(dir: &Path, target_pct: u8) -> Result<PathBuf, Error> {
    if !cfg!(target_os = "linux") {
        eprintln!("fill_disk: skipped on non-Linux platform");
        return Err(Error::ClusterError("fill_disk requires Linux (fallocate/dd + df)".into()));
    }

    let fill_path = dir.join("fill.bin");

    let fstype = filesystem_type(dir)?;
    if is_memory_fs(&fstype) {
        return Err(Error::ClusterError(format!(
            "fill_disk: refusing to fill {} — it is {fstype}; set TMPDIR to a disk-backed \
             directory for the local-spawn scenario",
            dir.display()
        )));
    }

    // df's own percent denominator (`used + avail`) excludes reserved
    // blocks; `size - avail` includes them and under-fills (the f2 live
    // run landed at 16% for a 20% target before the same fix).
    let used_space = get_disk_space(dir, "used")?;
    let avail_space = get_disk_space(dir, "avail")?;
    let fill_size = fill_target_bytes(used_space, avail_space, target_pct);

    if fill_size == 0 {
        eprintln!(
            "fill_disk: already at or above {target_pct}% usage (used {used_space}, avail {avail_space})"
        );
        return Ok(fill_path);
    }

    // fallocate allocates instantly on ext4/xfs; dd is the portable fallback.
    let fallocate_ok = Command::new("fallocate")
        .arg("-l")
        .arg(fill_size.to_string())
        .arg(&fill_path)
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    if !fallocate_ok {
        let count_mb = fill_size.div_ceil(1024 * 1024);
        let output = Command::new("dd")
            .args([
                "if=/dev/zero",
                &format!("of={}", fill_path.display()),
                "bs=1M",
                &format!("count={count_mb}"),
            ])
            .output()
            .map_err(|e| Error::ClusterError(format!("dd command failed: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::ClusterError(format!("dd fill failed: {stderr}")));
        }
    }

    // Verify usage is within tolerance (±5% of target).
    let new_used = get_disk_space(dir, "used")?;
    let denominator = new_used.saturating_add(get_disk_space(dir, "avail")?);
    let actual_pct = if denominator == 0 {
        0
    } else {
        (new_used as f64 / denominator as f64 * 100.0).round() as u8
    };
    let tolerance = 5u8;
    if actual_pct < target_pct.saturating_sub(tolerance)
        || actual_pct > target_pct.saturating_add(tolerance)
    {
        eprintln!("fill_disk: usage is {actual_pct}%, target was {target_pct}% ± {tolerance}%");
    }

    Ok(fill_path)
}

/// Returns the bytes to write so `dir`'s usage reaches `target_pct`%.
///
/// Uses df's percent denominator (`used + avail`, reserved blocks excluded),
/// matching the fleet injector's `fill_command`.
fn fill_target_bytes(used: u64, avail: u64, target_pct: u8) -> u64 {
    let denominator = used.saturating_add(avail);
    let target_used = denominator.saturating_mul(target_pct as u64) / 100;
    target_used.saturating_sub(used)
}

/// Returns `true` for memory-backed filesystems that must not be filled.
fn is_memory_fs(fstype: &str) -> bool {
    matches!(fstype, "tmpfs" | "ramfs")
}

/// Returns the filesystem type of `dir` (e.g. `ext4`, `tmpfs`).
fn filesystem_type(dir: &Path) -> Result<String, Error> {
    let output = Command::new("stat")
        .args(["-f", "-c", "%T"])
        .arg(dir)
        .output()
        .map_err(|e| Error::ClusterError(format!("stat -f failed: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::ClusterError(format!("stat -f failed: {stderr}")));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Returns a disk space metric from `df` for the given directory.
///
/// `field` is one of `"size"` (total), `"used"`, or `"avail"`.
/// Forces `LC_ALL=C` to avoid locale-dependent output formatting.
fn get_disk_space(dir: &Path, field: &str) -> Result<u64, Error> {
    let output = Command::new("df")
        .env("LC_ALL", "C")
        // `--output` takes its field list with `=`; a space-separated value
        // becomes a FILE operand (`df: used: No such file or directory`).
        .arg(format!("--output={field}"))
        .arg("--block-size=1")
        .arg(dir)
        .output()
        .map_err(|e| Error::ClusterError(format!("df command failed: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::ClusterError(format!("df process failed: {stderr}")));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Skip the header line(s); parse the first numeric data line.
    for line in stdout.lines().skip(1) {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            return trimmed
                .parse::<u64>()
                .map_err(|_| Error::ClusterError(format!("failed to parse df output: {stdout}")));
        }
    }

    Err(Error::ClusterError(format!("df produced no data lines: {stdout}")))
}

// ---------------------------------------------------------------------------
// Segment corruption
// ---------------------------------------------------------------------------

impl Cluster {
    /// Corrupts a segment data file on `node_i`.
    ///
    /// Searches the node's data directory for files whose name contains
    /// `segment_id`. Prioritizes files under a `segments/` subdirectory,
    /// then falls back to a recursive search of the entire data directory.
    /// Once found, overwrites 64 random bytes at a random offset.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is killed or no file matching
    /// `segment_id` could be found.
    pub async fn corrupt_shard(&self, node_i: usize, segment_id: &str) -> Result<(), Error> {
        let node = self.node(node_i);
        let data_dir = node.data_dir().to_path_buf();

        // Find files matching the segment ID.
        let candidates = find_segment_files(&data_dir, segment_id);
        if candidates.is_empty() {
            return Err(Error::ClusterError(format!(
                "corrupt_shard: no files matching segment_id '{segment_id}' found under {}",
                data_dir.display()
            )));
        }

        // Corrupt every matching file.
        for target in &candidates {
            overwrite_random_bytes(target, 64).map_err(|e| {
                Error::ClusterError(format!("corrupt_shard failed on {}: {e}", target.display()))
            })?;
        }

        Ok(())
    }
}

/// Finds files under `data_dir` whose name contains `segment_id`.
///
/// Searches `data_dir/segments/` (priority), the entire `data_dir` tree,
/// and the harness-injected data pool root `{data_dir}/../pool-data` —
/// since ADR-0031 the segment `.dat` files live on a pool root that is a
/// sibling of `data_dir`, so a data_dir-only walk finds nothing.
/// Results from `segments/` appear first.
fn find_segment_files(data_dir: &Path, segment_id: &str) -> Vec<PathBuf> {
    let mut results = Vec::new();

    // Priority: entries directly inside data_dir/segments/.
    let segments_dir = data_dir.join("segments");
    if segments_dir.is_dir() {
        if let Ok(entries) = fs::read_dir(&segments_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if name.contains(segment_id) && path.is_file() {
                    results.push(path);
                }
            }
        }
    }

    // Search the data directory, then the data pool root (pools live on
    // sibling roots since ADR-0031; `.dat` files are at
    // `{pool-data-root}/{segment_id}.dat`).
    let mut roots = vec![data_dir.to_path_buf()];
    if let Some(base) = data_dir.parent() {
        let pool_data = base.join("pool-data");
        if pool_data.is_dir() {
            roots.push(pool_data);
        }
    }
    for root in roots {
        for path in collect_data_files(&root) {
            // Skip files we already found via segments/.
            if results.contains(&path) {
                continue;
            }
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if name.contains(segment_id) {
                results.push(path);
            }
        }
    }

    results
}

impl Cluster {
    /// End-to-end corruption-then-heal verification.
    ///
    /// 1. Writes a known blob to a healthy node, waits for replication.
    /// 2. Corrupts the segment identified by `segment_id` on `node_i`.
    /// 3. Triggers a full distributed scrub (`POST /admin/scrub`, 202; the
    ///    cycle runs asynchronously). The old `POST /admin/trigger-anti-entropy`
    ///    route never existed on the current admin surface.
    /// 4. Waits up to `timeout` for the corrupted node to return the
    ///    original blob content (i.e., healing reconstructed it from
    ///    surviving replicas).
    /// 5. Returns `Ok(())` if healed, `Err` if the timeout expires.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster has fewer than 2 nodes, the blob
    /// cannot be written, or healing does not complete within the timeout.
    pub async fn corrupt_and_verify_heal(
        &self,
        node_i: usize,
        segment_id: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        let healthy_idx = if node_i == 0 { 1 } else { 0 };
        if healthy_idx >= self.len() {
            return Err(Error::ClusterError(format!(
                "corrupt_and_verify_heal needs ≥2 nodes, have {}",
                self.len()
            )));
        }

        // Step 1: Write a known blob to a healthy node.
        let bucket = "heal-test";
        let key = format!("heal-key-{segment_id}");
        let body: Vec<u8> = (0..4096u16).map(|b| (b % 256) as u8).collect();

        // Create bucket and write the blob.
        self.put(healthy_idx, &format!("/{bucket}"), &[]).await?;
        self.put(healthy_idx, &format!("/{bucket}/{key}"), &body).await?;

        // Wait for replication to the other nodes.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify the blob is readable from the target node before corruption.
        match self.get(node_i, &format!("/{bucket}/{key}")).await {
            Ok(resp) if resp.status().is_success() => {
                let read_back = resp.bytes().await.unwrap_or_default();
                if read_back.as_ref() != body.as_slice() {
                    return Err(Error::ClusterError(
                        "pre-corruption read: body mismatch — replication may not have completed"
                            .into(),
                    ));
                }
            }
            Ok(resp) => {
                return Err(Error::ClusterError(format!(
                    "pre-corruption read returned HTTP {}",
                    resp.status()
                )));
            }
            Err(e) => {
                return Err(Error::ClusterError(format!(
                    "pre-corruption read from node {node_i} failed: {e}"
                )));
            }
        }

        // Step 2: Corrupt.
        self.corrupt_shard(node_i, segment_id).await?;

        // Step 3: Trigger a full distributed scrub (the current admin
        // surface; 202 means the cycle is running asynchronously).
        let node = self.node(node_i);
        match node.post("/admin/scrub").await {
            Ok(resp) => {
                let status = resp.status();
                let _ = resp.bytes().await;
                if !status.is_success() {
                    eprintln!("corrupt_and_verify_heal: POST /admin/scrub returned {status}");
                }
            }
            Err(e) => {
                eprintln!("corrupt_and_verify_heal: scrub trigger unavailable: {e}");
                // Proceed — the periodic scrub/AE cycle will eventually repair.
            }
        }

        // Step 4: Wait for heal by polling the corrupted node.
        // The blob should become readable again with the correct content.
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_secs(1);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::ClusterError(format!(
                    "heal verification timed out after {timeout:?} — blob {bucket}/{key} still \
                     unreadable from node {node_i}"
                )));
            }

            match self.get(node_i, &format!("/{bucket}/{key}")).await {
                Ok(resp) if resp.status().is_success() => {
                    let read_back = resp.bytes().await.unwrap_or_default();
                    if read_back.as_ref() == body.as_slice() {
                        // Heal succeeded: data reconstructed from replicas.
                        return Ok(());
                    }
                    // Body mismatch — still waiting for heal.
                }
                _ => {
                    // Node unreachable or non-200 — keep polling.
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Recursively collects all regular files under a directory.
fn collect_data_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_files_recursive(root, &mut files);
    files
}

fn collect_files_recursive(dir: &Path, files: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            } else if path.is_dir() {
                collect_files_recursive(&path, files);
            }
        }
    }
}

/// Overwrites `n` random bytes at a random offset in a file.
fn overwrite_random_bytes(path: &Path, n_bytes: usize) -> std::io::Result<()> {
    use std::io::SeekFrom;

    let metadata = fs::metadata(path)?;
    let file_size = metadata.len();
    if file_size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file is empty, cannot corrupt",
        ));
    }

    let max_offset = file_size.saturating_sub(n_bytes as u64);
    let offset = if max_offset > 0 { rand::random::<u64>() % max_offset } else { 0 };

    let random_bytes: Vec<u8> = (0..n_bytes).map(|_| rand::random::<u8>()).collect();

    let mut file = OpenOptions::new().write(true).create(false).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&random_bytes)?;
    file.sync_all()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// FailureInjectionRecord
// ---------------------------------------------------------------------------

/// A record of a single failure injection event.
///
/// These are collected during a load test and can be included in the
/// [`LoadReport`](crate::load::LoadReport) for post-hoc analysis.
#[derive(Debug, Clone, Serialize)]
pub struct FailureInjectionRecord {
    /// Unix timestamp (seconds since epoch) when the injection was applied.
    pub timestamp: f64,
    /// Human-readable injection type (e.g., `"latency"`, `"disk_fill"`).
    pub injection_type: String,
    /// The cluster node index that was targeted.
    pub node_index: usize,
    /// Whether the injection was applied successfully.
    pub success: bool,
    /// Human-readable detail (error message or confirmation).
    pub detail: String,
}

impl FailureInjectionRecord {
    /// Creates a new record.
    pub fn new(
        injection_type: impl Into<String>,
        node_index: usize,
        success: bool,
        detail: impl Into<String>,
    ) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        Self {
            timestamp,
            injection_type: injection_type.into(),
            node_index,
            success,
            detail: detail.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── corrupt_shard unit tests ─────

    #[test]
    fn overwrite_random_bytes_changes_file_content() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let file_path = dir.path().join("test_segment.dat");

        // Create a file with known content.
        let original: Vec<u8> = (0..4096u16).map(|b| (b % 256) as u8).collect();
        fs::write(&file_path, &original).expect("write");

        // Corrupt 64 bytes.
        overwrite_random_bytes(&file_path, 64).expect("corrupt");

        // Read back — content should differ.
        let corrupted = fs::read(&file_path).expect("read");
        assert_eq!(corrupted.len(), original.len(), "file size unchanged");

        // Count differing bytes.
        let diff_count = original.iter().zip(corrupted.iter()).filter(|(a, b)| a != b).count();
        // At most 64 bytes should differ (could be fewer due to overlap at offset).
        assert!(diff_count > 0, "at least some bytes should differ after corruption");
        assert!(diff_count <= 64, "at most 64 bytes should differ");
    }

    #[test]
    fn overwrite_random_bytes_on_empty_file_returns_error() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let file_path = dir.path().join("empty.dat");
        fs::write(&file_path, []).expect("write empty");

        let result = overwrite_random_bytes(&file_path, 64);
        assert!(result.is_err(), "should error on empty file");
    }

    #[test]
    fn overwrite_random_bytes_on_small_file_corrupts_from_start() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let file_path = dir.path().join("small.dat");

        // File smaller than the corruption size.
        let original: Vec<u8> = vec![0xAA; 10];
        fs::write(&file_path, &original).expect("write");

        overwrite_random_bytes(&file_path, 64).expect("corrupt");

        // File grows because write_all at offset 0 on a 10-byte file
        // extends to 64 bytes.
        let corrupted = fs::read(&file_path).expect("read");
        assert_eq!(corrupted.len(), 64, "file should grow to corruption size");
        assert_ne!(&corrupted[..10], &original[..]);
    }

    // ── collect_data_files tests ─────

    #[test]
    fn collect_data_files_finds_files_recursively() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        fs::create_dir(dir.path().join("subdir")).expect("mkdir");
        fs::write(dir.path().join("a.txt"), b"a").expect("write");
        fs::write(dir.path().join("subdir/b.txt"), b"b").expect("write");
        fs::create_dir(dir.path().join("empty_dir")).expect("mkdir");

        let files = collect_data_files(dir.path());
        assert_eq!(files.len(), 2, "should find exactly 2 files");
        let paths: Vec<String> =
            files.iter().map(|p| p.file_name().unwrap().to_string_lossy().to_string()).collect();
        assert!(paths.contains(&"a.txt".to_string()));
        assert!(paths.contains(&"b.txt".to_string()));
    }

    #[test]
    fn collect_data_files_empty_dir_returns_empty() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let files = collect_data_files(dir.path());
        assert!(files.is_empty());
    }

    // ── find_segment_files tests ─────

    #[test]
    fn find_segment_files_matches_by_segment_id_in_name() {
        let dir = tempfile::TempDir::new().expect("temp dir");

        // Create files, some matching the segment ID.
        fs::write(dir.path().join("segment_abc123.dat"), b"data").expect("write");
        fs::write(dir.path().join("segment_xyz789.dat"), b"data").expect("write");
        fs::write(dir.path().join("unrelated.log"), b"log").expect("write");

        let matches = find_segment_files(dir.path(), "abc123");
        assert_eq!(matches.len(), 1);
        assert!(matches[0].file_name().unwrap().to_string_lossy().contains("abc123"));
    }

    #[test]
    fn find_segment_files_prioritizes_segments_subdirectory() {
        let dir = tempfile::TempDir::new().expect("temp dir");

        // File in segments/ subdirectory.
        fs::create_dir(dir.path().join("segments")).expect("mkdir");
        fs::write(dir.path().join("segments/seg_abc.dat"), b"shard").expect("write");

        // File at data-dir root with same ID.
        fs::write(dir.path().join("seg_abc.dat"), b"root").expect("write");

        let matches = find_segment_files(dir.path(), "abc");
        assert_eq!(matches.len(), 2);
        // First result should be the one under segments/ (priority order).
        assert!(matches[0].to_string_lossy().contains("segments"));
    }

    #[test]
    fn find_segment_files_no_match_returns_empty() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        fs::write(dir.path().join("other_file.dat"), b"data").expect("write");

        let matches = find_segment_files(dir.path(), "nonexistent");
        assert!(matches.is_empty());
    }

    #[test]
    fn find_segment_files_finds_pool_data_sibling() {
        // Local spawn layout (ADR-0031): data_dir = {base}/data, the data
        // pool root is the sibling {base}/pool-data.
        let base = tempfile::TempDir::new().expect("temp dir");
        let data_dir = base.path().join("data");
        let pool_data = base.path().join("pool-data");
        fs::create_dir_all(&data_dir).expect("mkdir data");
        fs::create_dir_all(&pool_data).expect("mkdir pool-data");
        fs::write(pool_data.join("seg_abc.dat"), b"shard").expect("write");

        let matches = find_segment_files(&data_dir, "abc");
        assert_eq!(matches.len(), 1);
        assert!(matches[0].to_string_lossy().contains("pool-data"));
    }

    // ── local role-root resolution tests ─────

    #[test]
    fn local_role_dir_maps_metadata_to_meta_and_requires_existing_root() {
        let base = tempfile::TempDir::new().expect("temp dir");
        // The spawn path is the node BASE: `{base}/data` is the node's own
        // data directory and `{base}/pool-*` are the injected role roots
        // (`NodeProcess::spawn_with_data_dir_and_options`).
        fs::create_dir_all(base.path().join("data")).expect("mkdir data");

        // Missing role root → loud error, never a data_dir fallback.
        assert!(local_role_dir(base.path(), "data").is_err());

        fs::create_dir_all(base.path().join("pool-data")).expect("mkdir pool-data");
        fs::create_dir_all(base.path().join("pool-meta")).expect("mkdir pool-meta");
        assert_eq!(
            local_role_dir(base.path(), "data").expect("data root"),
            base.path().join("pool-data")
        );
        assert_eq!(
            local_role_dir(base.path(), "metadata").expect("metadata root"),
            base.path().join("pool-meta")
        );
    }

    // ── FailureInjectionRecord tests ─────

    #[test]
    fn failure_injection_record_new_populates_fields() {
        let record = FailureInjectionRecord::new("latency", 2, true, "500ms on node 2");
        assert_eq!(record.injection_type, "latency");
        assert_eq!(record.node_index, 2);
        assert!(record.success);
        assert!(record.detail.contains("500ms"));
        assert!(record.timestamp > 0.0);
    }

    #[test]
    fn failure_injection_record_serializes() {
        let record = FailureInjectionRecord::new("disk_fill", 0, false, "not enough space");
        let json = serde_json::to_string(&record).expect("serialize");
        assert!(json.contains("\"injection_type\":\"disk_fill\""));
        assert!(json.contains("\"success\":false"));
    }

    // ── fill_disk helper tests ─────

    #[test]
    fn loopback_latency_opt_in_requires_explicit_truthy_value() {
        assert!(loopback_latency_opt_in(Some("1")));
        assert!(loopback_latency_opt_in(Some("true")));
        assert!(loopback_latency_opt_in(Some("yes")));
        assert!(!loopback_latency_opt_in(None));
        assert!(!loopback_latency_opt_in(Some("0")));
        assert!(!loopback_latency_opt_in(Some("")));
    }

    #[test]
    fn fill_disk_requires_linux() {
        // On non-Linux, fill_disk should not work at the platform check level.
        // On Linux, it requires a real Cluster.
        const { assert!(cfg!(target_os = "linux") || !cfg!(target_os = "linux")) };
    }

    #[test]
    fn fill_target_bytes_uses_df_percent_denominator() {
        // used=300, avail=500 → denominator 800 (excludes 200 reserved);
        // 50% = 400, so 100 bytes are needed. The old `size - avail`
        // arithmetic (used=500, target=50% of 1000) computed 0.
        assert_eq!(fill_target_bytes(300, 500, 50), 100);
        assert_eq!(fill_target_bytes(400, 400, 50), 0);
        assert_eq!(fill_target_bytes(0, 1000, 95), 950);
        // Already above the target: never shrink.
        assert_eq!(fill_target_bytes(900, 100, 10), 0);
    }

    #[test]
    fn is_memory_fs_flags_tmpfs_and_ramfs_only() {
        assert!(is_memory_fs("tmpfs"));
        assert!(is_memory_fs("ramfs"));
        for fstype in ["ext4", "xfs", "btrfs", "overlay", "nfs"] {
            assert!(!is_memory_fs(fstype), "{fstype} must not be refused");
        }
    }
}
