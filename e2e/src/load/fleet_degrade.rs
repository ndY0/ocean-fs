//! Fleet fault injectors — SSH black-box against real volumes.
//!
//! Extends the load harness with the failure-injection substrate the
//! fleet-degradation scenarios (f3/f4) need. Every injector deforms the
//! **hardware or the network**; none of them touches a product-internal
//! hook:
//!
//! | Injector | Mechanism | Path |
//! |---|---|---|
//! | [`FleetInjector::yank_volume`] | `echo 1 > /sys/block/<dev>/device/delete` | hard yank |
//! | [`FleetInjector::replug_volume`] | SCSI host rescan + same-serial verification | recovery |
//! | [`FleetInjector::fill_volume`] | `fallocate`/`dd` on the role mount | hard (ENOSPC) |
//! | [`FleetInjector::corrupt_segment_bytes`] | random-byte overwrite of one `.dat` | hard (corruption) |
//! | [`FleetInjector::corrupt_and_verify_heal_remote`] | corruption → scrub → read-back poll | correctness loop |
//! | [`FleetInjector::inject_latency`] | `tc netem` on the internal interface | soft (network) |
//!
//! ## Option A — no test-only product hooks
//!
//! All remote operations are plain SSH commands built here from **typed
//! parameters** (a volume id, a mount path, a segment id) and shell-quoted
//! with the module's `shell_quote` helper. Nothing is passed through from a
//! report, record, or
//! environment variable unvalidated: mount paths, segment ids and interface
//! names are validated against conservative character sets, and device
//! addressing uses the stable `by-id` serial path — never `/dev/sdX`, whose
//! letter shuffles across rescan (f1 sysfs-gate finding).
//!
//! ## No performance assertions
//!
//! Volume-backed runs cross network block storage. Every result produced by
//! these injectors is a **correctness / degradation** result; throughput and
//! latency numbers are not comparable to local-disk runs and are never
//! asserted on. The latency injector verifies that a `netem` qdisc is
//! installed and removed — it does not measure an RTT threshold.
//!
//! ## FailureInjectionRecord discipline
//!
//! Every injector emits **exactly one** [`FailureInjectionRecord`] per
//! attempt (success or failure), including the explicit-skip case: when the
//! topology cannot support the operation (no SSH target or no recorded
//! volume), the attempt fails with a `skipped:` error and the record is
//! written with `success=false`. A skipped injector is never a silent
//! success. Discovery helpers ([`FleetInjector::list_segment_ids`]) and
//! probes ([`FleetInjector::device_gone`], [`FleetInjector::mount_usable`])
//! are reads, not injections, and do not record.
//!
//! ## Role scope
//!
//! Injectors take a role string from the provisioning record
//! (`data`/`wal`/`meta`/`hints`/`spare`). Corrupting a `.dat` on the
//! `hints` role is **not meaningful** (hints are a WAL, not segments);
//! f4 resolves hints semantics per the f0 gate contract, and the discovery
//! helper simply returns an empty list there.
//!
//! ## Usage
//!
//! ```no_run
//! use e2e::load::fleet_degrade::FleetInjector;
//! use e2e::remote::RemoteCluster;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000,10.0.0.4:9000")?;
//! let mut injector = FleetInjector::from_env(&cluster)?;
//! injector.yank_volume(1, "data").await?;
//! injector.replug_volume(1, "data").await?;
//! for record in injector.records() {
//!     println!("{} node={} success={}", record.injection_type, record.node_index, record.success);
//! }
//! # Ok(())
//! # }
//! ```

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use serde::Deserialize;

use super::degrade::FailureInjectionRecord;
use crate::{
    harness::{Error, LoadTarget},
    remote::RemoteCluster,
};

/// In-band fill file created by [`FleetInjector::fill_volume`].
const FILL_FILE_NAME: &str = ".fill.bin";

/// Maximum bytes a single corruption may overwrite (a sanity bound, not a
/// format limit). The local injector uses 64 bytes; scenarios pass their own.
const MAX_CORRUPT_BYTES: usize = 1024 * 1024;

/// Maximum latency a single `netem` injection may apply (milliseconds).
const MAX_LATENCY_MS: u64 = 10_000;

// ---------------------------------------------------------------------------
// VolumeRef
// ---------------------------------------------------------------------------

/// A role volume as recorded by `vm-provision.sh --volume-pools`.
///
/// The `device` field is informational only: device letters shuffle across
/// detach/rescan/reboot, so every operation resolves the device through the
/// stable `by-id` serial path ([`VolumeRef::by_id_path`]).
///
/// # Examples
///
/// ```
/// use e2e::load::fleet_degrade::VolumeRef;
///
/// let volume = VolumeRef {
///     role: "data".to_string(),
///     id: 42,
///     device: "/dev/sdb".to_string(),
///     mount: "/mnt/oceanfs-data".to_string(),
///     size_gb: 120,
/// };
/// assert_eq!(volume.mount, "/mnt/oceanfs-data");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeRef {
    /// Pool role: `data`, `wal`, `meta`, `hints`, or `spare`.
    pub role: String,
    /// Hetzner Cloud volume id (the stable handle).
    pub id: u64,
    /// Device path at provisioning time (e.g. `/dev/sdb`) — informational.
    pub device: String,
    /// Mount path (e.g. `/mnt/oceanfs-data`).
    pub mount: String,
    /// Provisioned size in GiB.
    pub size_gb: u64,
}

impl VolumeRef {
    /// Returns the stable `by-id` device path for this volume.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::fleet_degrade::VolumeRef;
    ///
    /// let volume = VolumeRef {
    ///     role: "data".to_string(),
    ///     id: 42,
    ///     device: "/dev/sdb".to_string(),
    ///     mount: "/mnt/oceanfs-data".to_string(),
    ///     size_gb: 120,
    /// };
    /// assert_eq!(volume.by_id_path(), "/dev/disk/by-id/scsi-0HC_Volume_42");
    /// ```
    pub fn by_id_path(&self) -> String {
        format!("/dev/disk/by-id/scsi-0HC_Volume_{}", self.id)
    }
}

// ---------------------------------------------------------------------------
// FleetTarget
// ---------------------------------------------------------------------------

/// One node's injection target: its SSH handle and recorded volumes.
///
/// # Examples
///
/// ```
/// use e2e::load::fleet_degrade::FleetTarget;
///
/// let target = FleetTarget::new(2, Some("root@10.0.0.4".to_string()), Default::default());
/// assert_eq!(target.node_index(), 2);
/// assert_eq!(target.ssh(), Some("root@10.0.0.4"));
/// ```
#[derive(Debug, Clone)]
pub struct FleetTarget {
    /// Cluster node index (aligned with `TARGET_HOSTS`).
    node_idx: usize,
    /// SSH target for this node, when configured.
    ssh: Option<String>,
    /// Recorded volumes by role.
    volumes: BTreeMap<String, VolumeRef>,
}

impl FleetTarget {
    /// Builds a target.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::BTreeMap;
    /// use e2e::load::fleet_degrade::{FleetTarget, VolumeRef};
    ///
    /// let target = FleetTarget::new(0, Some("root@10.0.0.2".to_string()), BTreeMap::new());
    /// assert_eq!(target.node_index(), 0);
    /// assert!(target.volume("data").is_none());
    /// ```
    pub fn new(node_idx: usize, ssh: Option<String>, volumes: BTreeMap<String, VolumeRef>) -> Self {
        Self { node_idx, ssh, volumes }
    }

    /// Returns the cluster node index.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::fleet_degrade::FleetTarget;
    ///
    /// let target = FleetTarget::new(3, None, Default::default());
    /// assert_eq!(target.node_index(), 3);
    /// ```
    pub fn node_index(&self) -> usize {
        self.node_idx
    }

    /// Returns the SSH target, when configured.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::fleet_degrade::FleetTarget;
    ///
    /// let target = FleetTarget::new(0, Some("root@10.0.0.2".to_string()), Default::default());
    /// assert_eq!(target.ssh(), Some("root@10.0.0.2"));
    /// ```
    pub fn ssh(&self) -> Option<&str> {
        self.ssh.as_deref()
    }

    /// Returns the recorded volume for `role`, if any.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::BTreeMap;
    /// use e2e::load::fleet_degrade::{FleetTarget, VolumeRef};
    ///
    /// let mut volumes = BTreeMap::new();
    /// volumes.insert(
    ///     "data".to_string(),
    ///     VolumeRef {
    ///         role: "data".to_string(),
    ///         id: 7,
    ///         device: "/dev/sdb".to_string(),
    ///         mount: "/mnt/oceanfs-data".to_string(),
    ///         size_gb: 120,
    ///     },
    /// );
    /// let target = FleetTarget::new(0, None, volumes);
    /// assert_eq!(target.volume("data").map(|v| v.id), Some(7));
    /// ```
    pub fn volume(&self, role: &str) -> Option<&VolumeRef> {
        self.volumes.get(role)
    }

    /// Returns all recorded volumes by role.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::fleet_degrade::FleetTarget;
    ///
    /// let target = FleetTarget::new(0, None, Default::default());
    /// assert!(target.volumes().is_empty());
    /// ```
    pub fn volumes(&self) -> &BTreeMap<String, VolumeRef> {
        &self.volumes
    }
}

// ---------------------------------------------------------------------------
// FleetInjector
// ---------------------------------------------------------------------------

/// Scenario-grade injector over a remote fleet's real volumes.
///
/// Construct with [`FleetInjector::from_env`] (the harness runner sets
/// `LOAD_TEST_RECORD_FILE` to the f1 provisioning record, or
/// `LOAD_TEST_VOLUMES_JSON` inline) or
/// [`FleetInjector::from_provisioning_record`]. Per-node SSH targets come
/// from `TARGET_HOST_SSH` first and the record's `internal_ip` second.
///
/// Every injector method takes `&mut self` and appends its
/// [`FailureInjectionRecord`] to the buffer returned by
/// [`FleetInjector::records`].
///
/// # Examples
///
/// ```
/// use e2e::load::fleet_degrade::FleetInjector;
/// use e2e::remote::RemoteCluster;
///
/// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000").expect("connect");
/// let injector = FleetInjector::new(&cluster, Vec::new());
/// assert_eq!(injector.targets().len(), 0);
/// ```
pub struct FleetInjector<'a> {
    cluster: &'a RemoteCluster,
    targets: Vec<FleetTarget>,
    records: Vec<FailureInjectionRecord>,
}

impl<'a> FleetInjector<'a> {
    /// Builds an injector from explicit targets.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000").expect("connect");
    /// let injector = FleetInjector::new(&cluster, Vec::new());
    /// assert!(injector.records().is_empty());
    /// ```
    pub fn new(cluster: &'a RemoteCluster, targets: Vec<FleetTarget>) -> Self {
        let capacity = targets.len() * 2;
        Self { cluster, targets, records: Vec::with_capacity(capacity) }
    }

    /// Builds an injector from the environment.
    ///
    /// Reads `LOAD_TEST_RECORD_FILE` (path to the provisioning record; the
    /// runner copies it to the harness) or `LOAD_TEST_VOLUMES_JSON` (the
    /// record inline) and maps nodes by index. Missing configuration is not
    /// an error: the injector starts with no volumes, so every injection
    /// records `success=false` with a `skipped:` reason.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000").expect("connect");
    /// let injector = FleetInjector::from_env(&cluster).expect("record");
    /// ```
    pub fn from_env(cluster: &'a RemoteCluster) -> Result<Self, Error> {
        let json = if let Some(path) = std::env::var_os("LOAD_TEST_RECORD_FILE") {
            let path = std::path::PathBuf::from(path);
            std::fs::read_to_string(&path).map_err(|e| {
                Error::ClusterError(format!(
                    "failed to read LOAD_TEST_RECORD_FILE {}: {e}",
                    path.display()
                ))
            })?
        } else if let Some(inline) = std::env::var_os("LOAD_TEST_VOLUMES_JSON") {
            inline.to_string_lossy().into_owned()
        } else {
            eprintln!(
                "fleet_degrade: LOAD_TEST_RECORD_FILE / LOAD_TEST_VOLUMES_JSON unset — \
                 every fleet injection will be skipped and recorded"
            );
            "{}".to_string()
        };
        Self::from_provisioning_record(cluster, &json)
    }

    /// Builds an injector from a `vm-provision.sh` provisioning record.
    ///
    /// Accepts the Phase 3+ shape (`sut_nodes[]`) and the Phase 2 legacy
    /// shape (`sut`). Unknown fields are ignored, so the record may grow
    /// without breaking the harness.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000").expect("connect");
    /// let injector = FleetInjector::from_provisioning_record(&cluster, "{}").expect("empty");
    /// assert_eq!(injector.targets().len(), 1);
    /// ```
    pub fn from_provisioning_record(
        cluster: &'a RemoteCluster,
        record_json: &str,
    ) -> Result<Self, Error> {
        let record: ProvisionRecord = serde_json::from_str(record_json)
            .map_err(|e| Error::ClusterError(format!("invalid provisioning record JSON: {e}")))?;
        let node_records: Vec<&RecordNode> = if !record.sut_nodes.is_empty() {
            record.sut_nodes.iter().collect()
        } else if let Some(sut) = record.sut.as_ref() {
            vec![sut]
        } else {
            Vec::new()
        };

        let mut targets = Vec::with_capacity(cluster.len());
        for node_idx in 0..cluster.len() {
            let node = node_records.get(node_idx).copied();
            let ssh = cluster.ssh_target_for(node_idx).map(str::to_string).or_else(|| {
                node.and_then(|n| {
                    let ip = if !n.internal_ip.is_empty() { &n.internal_ip } else { &n.ip };
                    if ip.is_empty() {
                        None
                    } else {
                        Some(format!("root@{ip}"))
                    }
                })
            });
            let volumes = node
                .map(|n| {
                    n.volumes
                        .iter()
                        .map(|v| {
                            (
                                v.role.clone(),
                                VolumeRef {
                                    role: v.role.clone(),
                                    id: v.id,
                                    device: v.device.clone(),
                                    mount: v.mount.clone(),
                                    size_gb: v.size_gb,
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default();
            targets.push(FleetTarget::new(node_idx, ssh, volumes));
        }
        Ok(Self::new(cluster, targets))
    }

    /// Returns the per-node targets (aligned with `TARGET_HOSTS`).
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000").expect("connect");
    /// let injector = FleetInjector::new(&cluster, Vec::new());
    /// assert!(injector.targets().is_empty());
    /// ```
    pub fn targets(&self) -> &[FleetTarget] {
        &self.targets
    }

    /// Returns every record emitted so far, in attempt order.
    ///
    /// # Examples
    ///
    /// ```
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000").expect("connect");
    /// let injector = FleetInjector::new(&cluster, Vec::new());
    /// assert_eq!(injector.records().len(), 0);
    /// ```
    pub fn records(&self) -> &[FailureInjectionRecord] {
        &self.records
    }

    // ── Resolution helpers ──────────────────────────────────────────────

    fn target_for(&self, node_idx: usize) -> Result<&FleetTarget, Error> {
        self.targets.get(node_idx).ok_or_else(|| {
            Error::ClusterError(format!(
                "skipped: no fleet target for node {node_idx} ({} targets configured)",
                self.targets.len()
            ))
        })
    }

    fn volume_for(&self, node_idx: usize, role: &str) -> Result<&VolumeRef, Error> {
        let volume = self.target_for(node_idx)?.volumes.get(role).ok_or_else(|| {
            Error::ClusterError(format!(
                "skipped: node {node_idx} has no '{role}' volume in the provisioning record \
                 (is this a --volume-pools fleet?)"
            ))
        })?;
        if volume.id == 0 {
            return Err(Error::ClusterError(format!(
                "skipped: node {node_idx} '{role}' volume has no cloud id (dry-run record?)"
            )));
        }
        Ok(volume)
    }

    fn ssh_for(&self, node_idx: usize) -> Result<&str, Error> {
        self.target_for(node_idx)?.ssh.as_deref().ok_or_else(|| {
            Error::ClusterError(format!(
                "skipped: node {node_idx} has no SSH target (TARGET_HOST_SSH unset and the \
                 record has no internal_ip)"
            ))
        })
    }

    /// Appends exactly one record for an attempt and returns the attempt's
    /// result unchanged.
    fn finish(
        &mut self,
        injection_type: &str,
        node_index: usize,
        context: &str,
        result: Result<String, Error>,
    ) -> Result<(), Error> {
        match result {
            Ok(detail) => {
                self.records.push(FailureInjectionRecord::new(
                    injection_type,
                    node_index,
                    true,
                    format!("{context}: {detail}"),
                ));
                Ok(())
            }
            Err(error) => {
                self.records.push(FailureInjectionRecord::new(
                    injection_type,
                    node_index,
                    false,
                    format!("{context}: {error}"),
                ));
                Err(error)
            }
        }
    }

    // ── Device yank / replug ────────────────────────────────────────────

    /// Hard-yanks a role volume: `echo 1 > /sys/block/<dev>/device/delete`.
    ///
    /// The device is resolved through its stable serial at yank time
    /// (`readlink -f /dev/disk/by-id/scsi-0HC_Volume_<id>`), never from the
    /// record's device letter. The filesystem stays mounted and goes stale —
    /// this is the **hard failure** path (ADR-0029 §D3 confirmed loss).
    ///
    /// # Errors
    ///
    /// Returns an error (recorded as `success=false`) when the volume or
    /// SSH target is missing, the sysfs delete attribute is unavailable, or
    /// ssh fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// injector.yank_volume(1, "data").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn yank_volume(&mut self, node_idx: usize, role: &str) -> Result<(), Error> {
        let result = self.yank_volume_impl(node_idx, role).await;
        self.finish("device_yank", node_idx, role, result)
    }

    async fn yank_volume_impl(&self, node_idx: usize, role: &str) -> Result<String, Error> {
        let volume = self.volume_for(node_idx, role)?;
        let ssh = self.ssh_for(node_idx)?;
        let command = yank_command(&volume.by_id_path());
        let output = self.cluster.ssh_exec_on(ssh, &command).await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "yank {} (volume {}) failed: {}",
                role,
                volume.id,
                output.stderr.trim()
            )));
        }
        Ok(format!("volume {} yanked via sysfs ({})", volume.id, output.stdout.trim()))
    }

    /// Re-attaches a yanked volume via SCSI host rescan and verifies the
    /// reappeared device carries the **same serial**.
    ///
    /// Does not remount: the filesystem decision differs per scenario
    /// (a hard yank's unclean fs is replaced in f4 P1b; a graceful replace
    /// re-mounts explicitly). It only proves the identity contract.
    ///
    /// # Errors
    ///
    /// Returns an error (recorded as `success=false`) when the device does
    /// not reappear within 30s or the serial differs.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// injector.replug_volume(1, "data").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn replug_volume(&mut self, node_idx: usize, role: &str) -> Result<(), Error> {
        let result = self.replug_volume_impl(node_idx, role).await;
        self.finish("device_replug", node_idx, role, result)
    }

    async fn replug_volume_impl(&self, node_idx: usize, role: &str) -> Result<String, Error> {
        let volume = self.volume_for(node_idx, role)?;
        let ssh = self.ssh_for(node_idx)?;
        let command = replug_command(&volume.by_id_path(), volume.id);
        let output = self.cluster.ssh_exec_on(ssh, &command).await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "replug {} (volume {}) failed: {}",
                role,
                volume.id,
                output.stderr.trim()
            )));
        }
        Ok(output.stdout.trim().to_string())
    }

    /// Probes whether a role volume's `by-id` device is currently absent.
    ///
    /// Read-only; no record is emitted.
    ///
    /// # Errors
    ///
    /// Returns an error when the target/volume is missing or SSH fails
    /// (probe status 255) — an unreachable node is not "device gone".
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// let gone = injector.device_gone(1, "data").await?;
    /// let _ = gone;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn device_gone(&self, node_idx: usize, role: &str) -> Result<bool, Error> {
        let volume = self.volume_for(node_idx, role)?;
        let ssh = self.ssh_for(node_idx)?;
        let output =
            self.cluster.ssh_exec_on(ssh, &device_gone_command(&volume.by_id_path())).await?;
        probe_status(output, "device_gone")
    }

    /// Probes whether `mount` is a usable mountpoint on `node_idx`.
    ///
    /// Read-only; no record is emitted.
    ///
    /// # Errors
    ///
    /// Returns an error when `mount` is not an absolute safe path or SSH
    /// fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// let usable = injector.mount_usable(1, "/mnt/oceanfs-data").await?;
    /// let _ = usable;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn mount_usable(&self, node_idx: usize, mount: &str) -> Result<bool, Error> {
        validate_mount_path(mount)?;
        let ssh = self.ssh_for(node_idx)?;
        let command = format!("mountpoint -q {}", shell_quote(mount));
        probe_status(self.cluster.ssh_exec_on(ssh, &command).await?, "mount_usable")
    }

    // ── Disk fill ───────────────────────────────────────────────────────

    /// Fills a role mount to approximately `target_pct`% usage.
    ///
    /// Writes `<mount>/.fill.bin` with `fallocate` (falling back to `dd`)
    /// after refusing a non-mountpoint, the root filesystem, or a tmpfs.
    /// Verified within ±5% before returning. Clean up with
    /// [`FleetInjector::remove_fill`].
    ///
    /// # Errors
    ///
    /// Returns an error (recorded as `success=false`) when the mount is
    /// absent/unsafe, the computation fails, or the result misses the
    /// target by more than 5 percentage points.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// injector.fill_volume(1, "spare", 20).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fill_volume(
        &mut self,
        node_idx: usize,
        role: &str,
        target_pct: u8,
    ) -> Result<(), Error> {
        let result = self.fill_volume_impl(node_idx, role, target_pct).await;
        self.finish("disk_fill", node_idx, role, result)
    }

    async fn fill_volume_impl(
        &self,
        node_idx: usize,
        role: &str,
        target_pct: u8,
    ) -> Result<String, Error> {
        let volume = self.volume_for(node_idx, role)?;
        validate_mount_path(&volume.mount)?;
        if target_pct == 0 || target_pct >= 100 {
            return Err(Error::ClusterError(format!(
                "target_pct must be 1..=99, got {target_pct}"
            )));
        }
        let ssh = self.ssh_for(node_idx)?;
        let output =
            self.cluster.ssh_exec_on(ssh, &fill_command(&volume.mount, target_pct)?).await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "fill {} failed: {}",
                volume.mount,
                output.stderr.trim()
            )));
        }
        let check = self.cluster.ssh_exec_on(ssh, &fill_verify_command(&volume.mount)?).await?;
        if !check.success() {
            return Err(Error::Ssh(format!(
                "fill verification on {} failed: {}",
                volume.mount,
                check.stderr.trim()
            )));
        }
        let actual: u8 = check.stdout.trim().parse().map_err(|_| {
            Error::ClusterError(format!(
                "could not parse df usage from {check:?} for {}",
                volume.mount
            ))
        })?;
        if actual.abs_diff(target_pct) > 5 {
            return Err(Error::ClusterError(format!(
                "fill on {} landed at {actual}%, target was {target_pct}% (±5)",
                volume.mount
            )));
        }
        Ok(format!("{} at {actual}% (target {target_pct}%)", volume.mount))
    }

    /// Removes the fill file created by [`FleetInjector::fill_volume`].
    ///
    /// # Errors
    ///
    /// Returns an error (recorded as `success=false`) when the volume or
    /// SSH target is missing or the removal fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// injector.remove_fill(1, "spare").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_fill(&mut self, node_idx: usize, role: &str) -> Result<(), Error> {
        let result = self.remove_fill_impl(node_idx, role).await;
        self.finish("disk_fill_remove", node_idx, role, result)
    }

    async fn remove_fill_impl(&self, node_idx: usize, role: &str) -> Result<String, Error> {
        let volume = self.volume_for(node_idx, role)?;
        validate_mount_path(&volume.mount)?;
        let ssh = self.ssh_for(node_idx)?;
        let path = format!("{}/{}", volume.mount, FILL_FILE_NAME);
        let command = format!("set -e; rm -f {}; sync; echo removed", shell_quote(&path));
        let output = self.cluster.ssh_exec_on(ssh, &command).await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "remove_fill {} failed: {}",
                path,
                output.stderr.trim()
            )));
        }
        Ok(format!("removed {}", path))
    }

    // ── Segment discovery + corruption ──────────────────────────────────

    /// Lists the live segment ids on a role mount (`<mount>/*.dat`).
    ///
    /// `GET /admin/segments` reports aggregate counts only, so the on-disk
    /// naming contract (`{data-pool-root}/{segment_id}.dat`) is the
    /// black-box way to discover a segment id. Read-only; no record.
    ///
    /// # Errors
    ///
    /// Returns an error when the target/volume is missing or SSH fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// let ids = injector.list_segment_ids(1, "data").await?;
    /// let _ = ids;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_segment_ids(
        &self,
        node_idx: usize,
        role: &str,
    ) -> Result<Vec<String>, Error> {
        let volume = self.volume_for(node_idx, role)?;
        validate_mount_path(&volume.mount)?;
        let ssh = self.ssh_for(node_idx)?;
        let output = self.cluster.ssh_exec_on(ssh, &list_segments_command(&volume.mount)?).await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "list_segment_ids {} failed: {}",
                volume.mount,
                output.stderr.trim()
            )));
        }
        let mut ids: Vec<String> = output
            .stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect();
        ids.sort();
        Ok(ids)
    }

    /// Returns the most recently modified segment id on a role mount.
    ///
    /// Discovery helper for corruption scenarios: write the blob first, then
    /// corrupt this segment — the just-written segment is the most likely
    /// holder of the blob (segment ids are random UUIDs, so name order says
    /// nothing about time). Read-only; no record.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let injector = FleetInjector::from_env(&cluster)?;
    /// if let Some(id) = injector.newest_segment_id(1, "data").await? {
    ///     println!("newest segment: {id}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the target/volume is missing, SSH fails, or
    /// the remote output is not a valid segment id.
    pub async fn newest_segment_id(
        &self,
        node_idx: usize,
        role: &str,
    ) -> Result<Option<String>, Error> {
        let volume = self.volume_for(node_idx, role)?;
        validate_mount_path(&volume.mount)?;
        let ssh = self.ssh_for(node_idx)?;
        let output = self.cluster.ssh_exec_on(ssh, &newest_segment_command(&volume.mount)?).await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "newest_segment_id {} failed: {}",
                volume.mount,
                output.stderr.trim()
            )));
        }
        let id = output.stdout.trim();
        if id.is_empty() {
            return Ok(None);
        }
        // The value feeds a later corruption command, so validate it even
        // though it came from the node itself.
        validate_segment_id(id)?;
        Ok(Some(id.to_string()))
    }

    /// Overwrites `n_bytes` random bytes at a random offset in one segment
    /// file on a role mount.
    ///
    /// The offset is computed locally from the file size (`stat`), then the
    /// bytes are written remotely with `dd` (falling back to `python3` when
    /// `dd` is unavailable). Equivalent semantics to the local
    /// [`Cluster::corrupt_shard`](crate::harness::Cluster::corrupt_shard):
    /// a file smaller than `n_bytes` is corrupted from offset 0.
    ///
    /// # Errors
    ///
    /// Returns an error (recorded as `success=false`) when the segment does
    /// not exist, is empty, or the remote write fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// injector.corrupt_segment_bytes(1, "data", "0f8a-bc_1", 64).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn corrupt_segment_bytes(
        &mut self,
        node_idx: usize,
        role: &str,
        segment_id: &str,
        n_bytes: usize,
    ) -> Result<(), Error> {
        let result = self.corrupt_segment_bytes_impl(node_idx, role, segment_id, n_bytes).await;
        self.finish("segment_corrupt", node_idx, role, result)
    }

    async fn corrupt_segment_bytes_impl(
        &self,
        node_idx: usize,
        role: &str,
        segment_id: &str,
        n_bytes: usize,
    ) -> Result<String, Error> {
        validate_segment_id(segment_id)?;
        if n_bytes == 0 || n_bytes > MAX_CORRUPT_BYTES {
            return Err(Error::ClusterError(format!(
                "n_bytes must be 1..={MAX_CORRUPT_BYTES}, got {n_bytes}"
            )));
        }
        let volume = self.volume_for(node_idx, role)?;
        validate_mount_path(&volume.mount)?;
        let ssh = self.ssh_for(node_idx)?;
        let path = format!("{}/{segment_id}.dat", volume.mount);

        let size_output =
            self.cluster.ssh_exec_on(ssh, &format!("stat -c %s {}", shell_quote(&path))).await?;
        if !size_output.success() {
            return Err(Error::Ssh(format!(
                "segment {path} not found: {}",
                size_output.stderr.trim()
            )));
        }
        let size: u64 = size_output.stdout.trim().parse().map_err(|_| {
            Error::ClusterError(format!("could not parse size of {path}: {:?}", size_output.stdout))
        })?;
        if size == 0 {
            return Err(Error::ClusterError(format!("segment {path} is empty, cannot corrupt")));
        }

        // Same selection rule as the local injector: a random offset with
        // room for n_bytes; a too-small file is corrupted from the start.
        let span = size.saturating_sub(n_bytes as u64);
        let offset = if span > 0 { rand::random::<u64>() % span } else { 0 };

        let output =
            self.cluster.ssh_exec_on(ssh, &corrupt_command(&path, offset, n_bytes)?).await?;
        if !output.success() {
            return Err(Error::Ssh(format!("corrupt {path} failed: {}", output.stderr.trim())));
        }
        Ok(format!("{path} offset={offset} bytes={n_bytes}"))
    }

    /// Fleet counterpart of the local `corrupt_and_verify_heal`: write a
    /// known blob, corrupt one discovered segment on `node_idx`, trigger
    /// `POST /admin/scrub`, and poll reads on that node until the original
    /// bytes return.
    ///
    /// **Correctness loop, not a benchmark:** the timeout is generous and
    /// no latency bound is asserted.
    ///
    /// **What this proves, and what it does not.** The corruption is real
    /// and the scrub trigger exercises the real verification/repair
    /// machinery (no product hook, no shortcut); the read-back poll proves
    /// the node keeps serving correct bytes (from its local copy after
    /// repair, or via replica failover). Whether the corrupted bytes were
    /// *local to the served copy* is not black-box observable — the product
    /// exposes no key→segment map — so precise repair assertions
    /// (local-copy restoration, repair counters, manifest state) belong to
    /// f4's role scenarios. Pair this with
    /// [`FleetInjector::newest_segment_id`] to target the segment the
    /// harness just wrote.
    ///
    /// # Errors
    ///
    /// Returns an error when the blob cannot be written/read back, the
    /// corruption step fails, or the bytes do not return within `timeout`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// injector
    ///     .corrupt_and_verify_heal_remote(1, "data", "0f8a-bc_1", 64, std::time::Duration::from_secs(120))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn corrupt_and_verify_heal_remote(
        &mut self,
        node_idx: usize,
        role: &str,
        segment_id: &str,
        n_bytes: usize,
        timeout: Duration,
    ) -> Result<(), Error> {
        let bucket = "fleet-injectors-heal";
        let key = format!("heal-{segment_id}");
        let body: Vec<u8> = (0..4096u16).map(|byte| (byte % 256) as u8).collect();

        // Write through a different node when possible so the corruption
        // target holds a replica it did not originate.
        let writer =
            if self.cluster.len() > 1 { (node_idx + 1) % self.cluster.len() } else { node_idx };
        self.cluster.put(writer, &format!("/{bucket}"), &[]).await?;
        self.cluster.put(writer, &format!("/{bucket}/{key}"), &body).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Pre-corruption read on the target node.
        match self.cluster.get(node_idx, &format!("/{bucket}/{key}")).await {
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
                    "pre-corruption read from node {node_idx} failed: {e}"
                )));
            }
        }

        self.corrupt_segment_bytes(node_idx, role, segment_id, n_bytes).await?;

        // Scrub is best-effort: the periodic AE/scrub cycle also heals.
        if let Err(e) = self.cluster.post(node_idx, "/admin/scrub").await {
            eprintln!("corrupt_and_verify_heal_remote: scrub trigger unavailable: {e}");
        }

        let start = Instant::now();
        loop {
            if start.elapsed() > timeout {
                return Err(Error::ClusterError(format!(
                    "heal verification timed out after {timeout:?} — blob {bucket}/{key} still \
                     unreadable from node {node_idx}"
                )));
            }
            if let Ok(resp) = self.cluster.get(node_idx, &format!("/{bucket}/{key}")).await {
                if resp.status().is_success() {
                    let read_back = resp.bytes().await.unwrap_or_default();
                    if read_back.as_ref() == body.as_slice() {
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // ── Network latency ─────────────────────────────────────────────────

    /// Resolves the internal network interface for `node_idx`.
    ///
    /// Order: an explicit `LOAD_TEST_LATENCY_IFACE` override, then
    /// `ip -o route get <peer-ip>` for the next node's internal address.
    /// `lo` is always rejected — on dedicated node VMs loopback carries no
    /// inter-node traffic, so a loopback `netem` would be a no-op the test
    /// silently mistakes for a working injection.
    ///
    /// # Errors
    ///
    /// Returns an error when the interface cannot be derived, is `lo`, or
    /// SSH fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// let iface = injector.latency_iface(1).await?;
    /// let _ = iface;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn latency_iface(&self, node_idx: usize) -> Result<String, Error> {
        if let Ok(iface) = std::env::var("LOAD_TEST_LATENCY_IFACE") {
            if !iface.is_empty() {
                validate_iface(&iface)?;
                return Ok(iface);
            }
        }
        let ssh = self.ssh_for(node_idx)?;
        if self.cluster.len() < 2 {
            return Err(Error::ClusterError(
                "cannot derive an internal interface from a single-node fleet; set \
                 LOAD_TEST_LATENCY_IFACE"
                    .into(),
            ));
        }
        let peer = (node_idx + 1) % self.cluster.len();
        let peer_ip = self.cluster.node_addr(peer).ip().to_string();
        let output = self
            .cluster
            .ssh_exec_on(ssh, &format!("ip -o route get {}", shell_quote(&peer_ip)))
            .await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "ip route get {peer_ip} failed: {}",
                output.stderr.trim()
            )));
        }
        let iface = output
            .stdout
            .split_whitespace()
            .skip_while(|token| *token != "dev")
            .nth(1)
            .ok_or_else(|| {
                Error::ClusterError(format!(
                    "could not parse interface from `ip route get {peer_ip}`: {:?}",
                    output.stdout
                ))
            })?;
        validate_iface(iface)?;
        Ok(iface.to_string())
    }

    /// Measures the ICMP round-trip time (milliseconds) from `node_idx` to
    /// `peer_node_idx` via `ping -c 3`.
    ///
    /// Functional probe for the latency injector: it verifies that an
    /// injected delay is observable on the wire and that removal restores
    /// the baseline. The injector itself asserts no threshold — callers
    /// decide what "increased" means for their scenario (a functional
    /// check, never a performance assertion). Read-only; no record.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// let baseline = injector.measure_rtt_ms(1, 0).await?;
    /// injector.inject_latency(1, "enp7s0", 100).await?;
    /// let delayed = injector.measure_rtt_ms(1, 0).await?;
    /// assert!(delayed > baseline);
    /// injector.remove_latency(1, "enp7s0").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the peer index is invalid or equals the
    /// source, ping is unavailable or fails (ICMP blocked), or the output
    /// cannot be parsed.
    pub async fn measure_rtt_ms(
        &self,
        node_idx: usize,
        peer_node_idx: usize,
    ) -> Result<f64, Error> {
        if peer_node_idx == node_idx || peer_node_idx >= self.cluster.len() {
            return Err(Error::ClusterError(format!(
                "invalid peer index {peer_node_idx} for node {node_idx} (fleet size {})",
                self.cluster.len()
            )));
        }
        let ssh = self.ssh_for(node_idx)?;
        let peer_ip = self.cluster.node_addr(peer_node_idx).ip().to_string();
        let output = self
            .cluster
            .ssh_exec_on(ssh, &format!("ping -c 3 -W 2 {}", shell_quote(&peer_ip)))
            .await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "ping {peer_ip} from node {node_idx} failed: {}",
                output.stderr.trim()
            )));
        }
        parse_ping_avg_ms(&output.stdout)
    }

    /// Adds a `netem` delay on `iface` (milliseconds) and verifies the
    /// qdisc is installed.
    ///
    /// Uses `tc qdisc replace`, so a repeated injection sets the requested
    /// delay instead of stacking or silently keeping an older value.
    ///
    /// # Errors
    ///
    /// Returns an error (recorded as `success=false`) when the interface is
    /// invalid (including `lo`), the delay is out of range, or `tc` fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// injector.inject_latency(1, "enp7s0", 100).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn inject_latency(
        &mut self,
        node_idx: usize,
        iface: &str,
        delay_ms: u64,
    ) -> Result<(), Error> {
        let result = self.inject_latency_impl(node_idx, iface, delay_ms).await;
        self.finish("latency", node_idx, iface, result)
    }

    async fn inject_latency_impl(
        &self,
        node_idx: usize,
        iface: &str,
        delay_ms: u64,
    ) -> Result<String, Error> {
        validate_iface(iface)?;
        if delay_ms == 0 || delay_ms > MAX_LATENCY_MS {
            return Err(Error::ClusterError(format!(
                "delay_ms must be 1..={MAX_LATENCY_MS}, got {delay_ms}"
            )));
        }
        let ssh = self.ssh_for(node_idx)?;
        let output = self.cluster.ssh_exec_on(ssh, &latency_set_command(iface, delay_ms)).await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "tc netem on {iface} failed: {}",
                output.stderr.trim()
            )));
        }
        Ok(format!("{iface} +{delay_ms}ms"))
    }

    /// Removes the `netem` qdisc from `iface` (idempotent).
    ///
    /// # Errors
    ///
    /// Returns an error (recorded as `success=false`) when the interface is
    /// invalid or `tc` fails for a reason other than an absent qdisc.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use e2e::load::fleet_degrade::FleetInjector;
    /// use e2e::remote::RemoteCluster;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cluster = RemoteCluster::connect("10.0.0.2:9000,10.0.0.3:9000")?;
    /// let mut injector = FleetInjector::from_env(&cluster)?;
    /// injector.remove_latency(1, "enp7s0").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_latency(&mut self, node_idx: usize, iface: &str) -> Result<(), Error> {
        let result = self.remove_latency_impl(node_idx, iface).await;
        self.finish("latency_remove", node_idx, iface, result)
    }

    async fn remove_latency_impl(&self, node_idx: usize, iface: &str) -> Result<String, Error> {
        validate_iface(iface)?;
        let ssh = self.ssh_for(node_idx)?;
        let output = self.cluster.ssh_exec_on(ssh, &latency_del_command(iface)).await?;
        if !output.success() {
            return Err(Error::Ssh(format!(
                "tc qdisc del on {iface} failed: {}",
                output.stderr.trim()
            )));
        }
        Ok(format!("{iface} qdisc removed"))
    }
}

// ---------------------------------------------------------------------------
// Command construction (kept free so unit tests can inspect the strings)
// ---------------------------------------------------------------------------

/// Quotes a value for safe interpolation into a remote `sh -c` script.
///
/// Single-quote wrapping; embedded single quotes become `'\''`. The result
/// is safe for spaces, `$`, backticks, newlines, and shell metacharacters.
fn shell_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    quoted
}

/// Validates an absolute mount path drawn from the record.
///
/// Conservative on purpose: the value is interpolated into remote scripts
/// (including glob patterns), so allow only `/`, `.`, `_`, `-` and
/// alphanumerics, and reject `..` components and the bare root.
fn validate_mount_path(mount: &str) -> Result<(), Error> {
    let ok = mount.starts_with('/')
        && mount.len() <= 1024
        && !mount.contains("..")
        && mount.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-'))
        && !mount.trim_end_matches('/').is_empty();
    if ok {
        Ok(())
    } else {
        Err(Error::ClusterError(format!("refusing unsafe mount path {mount:?}")))
    }
}

/// Validates a segment id drawn from a caller/discovery result.
fn validate_segment_id(segment_id: &str) -> Result<(), Error> {
    let ok = !segment_id.is_empty()
        && segment_id.len() <= 128
        && !segment_id.contains('/')
        && !segment_id.contains("..")
        && segment_id.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'));
    if ok {
        Ok(())
    } else {
        Err(Error::ClusterError(format!("refusing unsafe segment id {segment_id:?}")))
    }
}

/// Validates a Linux interface name (and rejects `lo`).
fn validate_iface(iface: &str) -> Result<(), Error> {
    let ok = !iface.is_empty()
        && iface != "lo"
        && iface.len() <= 15
        && iface.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if ok {
        Ok(())
    } else if iface == "lo" {
        Err(Error::ClusterError(
            "refusing latency injection on `lo` (no inter-node traffic on dedicated node VMs)"
                .into(),
        ))
    } else {
        Err(Error::ClusterError(format!("refusing unsafe interface name {iface:?}")))
    }
}

/// Builds the sysfs yank command for a stable `by-id` device path.
///
/// Waits (bounded) for the device to actually disappear: removal is
/// asynchronous (the f1 gate observed the symlink lingering ~2s), so
/// returning immediately would make `device_gone` flaky.
fn yank_command(by_id: &str) -> String {
    format!(
        "set -e; dev=$(readlink -f {by_id}); base=$(basename \"$dev\"); \
         attr=/sys/block/$base/device/delete; \
         [ -w \"$attr\" ] || {{ echo \"$attr is not writable\" >&2; exit 1; }}; \
         printf 1 > \"$attr\"; \
         for _ in $(seq 1 10); do \
           [ -e {by_id} ] || {{ echo \"deleted $base\"; exit 0; }}; \
           sleep 1; \
         done; \
         echo 'device still visible 10s after sysfs delete' >&2; exit 1",
        by_id = shell_quote(by_id),
    )
}

/// Builds the SCSI rescan command and the same-serial verification loop.
fn replug_command(by_id: &str, volume_id: u64) -> String {
    format!(
        "set -e; \
         for host_scan in /sys/class/scsi_host/host*/scan; do \
           [ -w \"$host_scan\" ] && printf -- '- - -' > \"$host_scan\" || true; \
         done; \
         for _ in $(seq 1 30); do \
           if [ -e {by_id} ]; then \
             dev=$(readlink -f {by_id}); \
             serial=$(udevadm info --query=property --name=\"$dev\" 2>/dev/null \
                       | sed -n 's/^ID_SERIAL=//p' || true); \
             case \"$serial\" in *{volume_id}*) echo \"replugged $dev serial=$serial\"; exit 0;; esac; \
           fi; \
           sleep 1; \
         done; \
         echo 'device did not reappear with the same serial within 30s' >&2; exit 1",
        by_id = shell_quote(by_id),
        volume_id = volume_id,
    )
}

/// Builds the remote disk-fill script (allocates `<mount>/.fill.bin`).
///
/// The fill amount uses **df's own percent denominator** (`used + avail`,
/// which excludes reserved blocks): `size - avail` includes reserved blocks
/// and under-fills (the f2 live run landed at 16% for a 20% target before
/// this was corrected).
fn fill_command(mount: &str, target_pct: u8) -> Result<String, Error> {
    validate_mount_path(mount)?;
    Ok(format!(
        r#"set -e
mount={mount}
pct={target_pct}
[ "$pct" -ge 1 ] && [ "$pct" -le 99 ] || {{ echo "pct out of range" >&2; exit 2; }}
mountpoint -q "$mount" || {{ echo "refusing: $mount is not a mountpoint" >&2; exit 1; }}
src=$(findmnt -rno SOURCE --target "$mount")
[ -n "$src" ] || {{ echo "refusing: no mount source for $mount" >&2; exit 1; }}
root_src=$(findmnt -rno SOURCE --target /)
[ "$(readlink -f "$src")" != "$(readlink -f "$root_src")" ] || {{ echo "refusing: $mount is on the root filesystem" >&2; exit 1; }}
fstype=$(stat -f -c %T "$mount")
case "$fstype" in tmpfs|ramfs) echo "refusing: $mount is $fstype" >&2; exit 1;; esac
used=$(df -B1 --output=used "$mount" | tail -n1 | tr -d ' ')
avail=$(df -B1 --output=avail "$mount" | tail -n1 | tr -d ' ')
denom=$(( used + avail ))
need=$(( denom * pct / 100 - used ))
if [ "$need" -le 0 ]; then echo "already at or above $pct%"; exit 0; fi
if command -v fallocate >/dev/null 2>&1; then
  fallocate -l "$need" "$mount/.fill.bin"
else
  blocks=$(( (need + 1048575) / 1048576 ))
  dd if=/dev/zero of="$mount/.fill.bin" bs=1M count="$blocks" status=none
fi
sync
echo "filled $mount to $pct%"
"#,
        mount = shell_quote(mount),
        target_pct = target_pct,
    ))
}

/// Builds the disk-fill verification command (prints the integer usage %).
fn fill_verify_command(mount: &str) -> Result<String, Error> {
    validate_mount_path(mount)?;
    Ok(format!(
        "df -B1 --output=pcent {mount} | tail -n1 | tr -dc '0-9'",
        mount = shell_quote(mount),
    ))
}

/// Builds the command that lists `.dat` basenames (without extension).
fn list_segments_command(mount: &str) -> Result<String, Error> {
    validate_mount_path(mount)?;
    // The pattern must stay unquoted to expand; `validate_mount_path`
    // guarantees it contains no glob metacharacters.
    Ok(format!("for f in {mount}/*.dat; do [ -e \"$f\" ] || continue; basename \"$f\" .dat; done"))
}

/// Builds the command that prints the most recently modified `.dat` id.
fn newest_segment_command(mount: &str) -> Result<String, Error> {
    validate_mount_path(mount)?;
    // `ls -t` sorts by mtime; `xargs -r` keeps an empty match silent.
    Ok(format!("ls -1t {mount}/*.dat 2>/dev/null | head -n1 | xargs -r -n1 basename -s .dat"))
}

/// Extracts the average RTT (ms) from `ping` output
/// (`rtt min/avg/max/mdev = a/b/c/d ms`).
fn parse_ping_avg_ms(output: &str) -> Result<f64, Error> {
    for line in output.lines() {
        let Some((_, stats)) = line.split_once("min/avg/max") else {
            continue;
        };
        let values = stats.split_once('=').map(|(_, values)| values).unwrap_or("");
        let avg = values.split('/').nth(1).ok_or_else(|| {
            Error::ClusterError(format!("could not parse ping statistics: {:?}", line))
        })?;
        return avg
            .trim()
            .parse::<f64>()
            .map_err(|_| Error::ClusterError(format!("could not parse ping average: {:?}", line)));
    }
    Err(Error::ClusterError(format!("ping produced no statistics: {output:?}")))
}

/// Builds the remote corruption command (`dd`, falling back to `python3`).
fn corrupt_command(path: &str, offset: u64, n_bytes: usize) -> Result<String, Error> {
    Ok(format!(
        "set -e; p={path}; off={offset}; n={n_bytes}; \
         if command -v dd >/dev/null 2>&1; then \
           dd if=/dev/urandom of=\"$p\" bs=1 seek=\"$off\" count=\"$n\" conv=notrunc status=none; \
         elif command -v python3 >/dev/null 2>&1; then \
           python3 -c 'import os,sys; p,off,n=sys.argv[1],int(sys.argv[2]),int(sys.argv[3]); \
f=open(p,\"r+b\"); f.seek(off); f.write(os.urandom(n)); f.flush(); os.fsync(f.fileno()); f.close()' \
             \"$p\" \"$off\" \"$n\"; \
         else echo 'neither dd nor python3 available' >&2; exit 127; fi; \
         echo \"corrupted $p offset=$off bytes=$n\"",
        path = shell_quote(path),
    ))
}

/// Builds the `netem` install command and verifies the qdisc is present.
fn latency_set_command(iface: &str, delay_ms: u64) -> String {
    format!(
        "set -e; tc qdisc replace dev {iface} root netem delay {delay_ms}ms; \
         tc qdisc show dev {iface} | grep -q netem; echo applied",
    )
}

/// Builds the idempotent `netem` removal command.
fn latency_del_command(iface: &str) -> String {
    format!(
        "set -e; if tc qdisc show dev {iface} 2>/dev/null | grep -q netem; then \
           tc qdisc del dev {iface} root; \
         fi; echo removed",
    )
}

/// Builds the "device is absent" probe: exit 0 when the path does not exist.
///
/// The negation keeps the shared [`probe_status`] convention (0 = condition
/// true) without inverting the boolean at the call site.
fn device_gone_command(by_id: &str) -> String {
    format!("test ! -e {}", shell_quote(by_id))
}

/// Maps an SSH probe exit status to a boolean (`0` = condition true,
/// `1` = false); anything else is an ssh/transport failure, not an answer.
fn probe_status(output: crate::remote::SshOutput, probe: &str) -> Result<bool, Error> {
    match output.status {
        0 => Ok(true),
        1 => Ok(false),
        other => Err(Error::Ssh(format!(
            "{probe} probe failed (ssh status {other}): {}",
            output.stderr.trim()
        ))),
    }
}

// ---------------------------------------------------------------------------
// Provisioning-record parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ProvisionRecord {
    #[serde(default)]
    sut: Option<RecordNode>,
    #[serde(default)]
    sut_nodes: Vec<RecordNode>,
}

#[derive(Debug, Deserialize)]
struct RecordNode {
    #[serde(default)]
    internal_ip: String,
    #[serde(default)]
    ip: String,
    #[serde(default)]
    volumes: Vec<RecordVolume>,
}

#[derive(Debug, Deserialize)]
struct RecordVolume {
    role: String,
    id: u64,
    #[serde(default)]
    device: String,
    #[serde(default)]
    mount: String,
    #[serde(default)]
    size_gb: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cluster(hosts: &str, ssh: Vec<String>) -> RemoteCluster {
        RemoteCluster::connect_with_ssh_targets(hosts, ssh).expect("test cluster")
    }

    const PHASE3_RECORD: &str = r#"{
        "prefix": "oceanfs-f2",
        "sut_nodes": [
            {
                "name": "oceanfs-f2-node-0",
                "internal_ip": "10.0.0.2",
                "ip": "10.0.0.2",
                "volumes": [
                    {"role": "data", "name": "v-data", "id": 101, "device": "/dev/sdb", "device_id": "/dev/disk/by-id/scsi-0HC_Volume_101", "mount": "/mnt/oceanfs-data", "size_gb": 120},
                    {"role": "hints", "name": "v-hints", "id": 104, "device": "/dev/sde", "mount": "/mnt/oceanfs-hints", "size_gb": 10}
                ]
            },
            {
                "name": "oceanfs-f2-node-1",
                "internal_ip": "10.0.0.3",
                "ip": "10.0.0.3",
                "volumes": [
                    {"role": "data", "name": "v-data", "id": 201, "device": "/dev/sdc", "mount": "/mnt/oceanfs-data", "size_gb": 120}
                ]
            }
        ]
    }"#;

    #[test]
    fn shell_quote_round_trips_through_sh() {
        let nasty = [
            "plain",
            "a'b",
            "x; rm -rf /",
            "$(touch /tmp/pwned)",
            "back`tick`",
            "two words",
            "new\nline",
        ];
        for value in nasty {
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg(format!("printf %s {}", shell_quote(value)))
                .output()
                .expect("run sh");
            assert!(output.status.success(), "sh failed for {value:?}");
            assert_eq!(String::from_utf8_lossy(&output.stdout), value, "quoting {value:?}");
        }
    }

    #[test]
    fn mount_path_validation_rejects_unsafe_values() {
        for good in ["/mnt/oceanfs-data", "/mnt/oceanfs-spare", "/a/b_c-1"] {
            validate_mount_path(good).expect("valid mount");
        }
        for bad in ["", "/", "relative/path", "/mnt/../etc", "/mnt/a b", "/mnt/a*", "/mnt/a;id"] {
            assert!(validate_mount_path(bad).is_err(), "must reject {bad:?}");
        }
    }

    #[test]
    fn segment_id_validation_rejects_path_components() {
        for good in ["abc123", "0f8a-bc_1", "550e8400-e29b-41d4-a716-446655440000"] {
            validate_segment_id(good).expect("valid id");
        }
        for bad in ["", "../etc/passwd", "a/b", "a b", "a'b", "a;b", "seg.42"] {
            assert!(validate_segment_id(bad).is_err(), "must reject {bad:?}");
        }
    }

    #[test]
    fn iface_validation_rejects_lo_and_metacharacters() {
        for good in ["ens10", "eth0", "enp1s0.100"] {
            validate_iface(good).expect("valid iface");
        }
        for bad in ["lo", "", "eth0;id", "eth 0", "a/b"] {
            assert!(validate_iface(bad).is_err(), "must reject {bad:?}");
        }
        let err = validate_iface("lo").expect_err("lo rejected");
        assert!(err.to_string().contains("lo"));
    }

    #[test]
    fn yank_command_resolves_device_by_serial_not_letter() {
        let command = yank_command("/dev/disk/by-id/scsi-0HC_Volume_42");
        assert!(command.contains("readlink -f '/dev/disk/by-id/scsi-0HC_Volume_42'"));
        assert!(command.contains("/device/delete"));
        assert!(!command.contains("/dev/sd"));
    }

    #[test]
    fn replug_command_verifies_same_serial() {
        let command = replug_command("/dev/disk/by-id/scsi-0HC_Volume_42", 42);
        assert!(command.contains("scsi_host/host"));
        assert!(command.contains("ID_SERIAL"));
        assert!(command.contains("*42*"));
    }

    #[test]
    fn corrupt_command_quotes_the_path() {
        let command =
            corrupt_command("/mnt/oceanfs-data/a'b; rm -rf /.dat", 7, 64).expect("build command");
        assert!(command.contains("p='/mnt/oceanfs-data/a'\\''b; rm -rf /.dat'"));
        assert!(command.contains("seek=\"$off\""));
        assert!(command.contains("count=\"$n\""));
        assert!(command.contains("conv=notrunc"));
    }

    #[test]
    fn list_segments_command_keeps_glob_unquoted() {
        let command = list_segments_command("/mnt/oceanfs-data").expect("build");
        assert!(command.contains("for f in /mnt/oceanfs-data/*.dat"));
        assert!(list_segments_command("/mnt/a*").is_err());
    }

    #[test]
    fn newest_segment_command_sorts_by_mtime() {
        let command = newest_segment_command("/mnt/oceanfs-data").expect("build");
        assert!(command.contains("ls -1t /mnt/oceanfs-data/*.dat"));
        assert!(command.contains("xargs -r"));
        assert!(newest_segment_command("/mnt/a*").is_err());
    }

    #[test]
    fn parse_ping_avg_ms_extracts_average() {
        let output = "PING 10.0.0.4 (10.0.0.4) 56(84) bytes of data.\n\
                      --- 10.0.0.4 ping statistics ---\n\
                      3 packets transmitted, 3 received, 0% packet loss, time 2003ms\n\
                      rtt min/avg/max/mdev = 0.150/100.250/200.350/1.100 ms\n";
        let avg = parse_ping_avg_ms(output).expect("parse");
        assert!((avg - 100.25).abs() < f64::EPSILON, "got {avg}");
    }

    #[test]
    fn parse_ping_avg_ms_rejects_missing_statistics() {
        assert!(parse_ping_avg_ms("no stats here").is_err());
    }

    #[test]
    fn fill_command_refuses_root_fs_and_tmpfs() {
        let command = fill_command("/mnt/oceanfs-data", 20).expect("build");
        assert!(command.contains("mountpoint -q"));
        assert!(command.contains("root filesystem"));
        assert!(command.contains("tmpfs|ramfs"));
    }

    #[test]
    fn fill_command_uses_df_percent_denominator() {
        // GNU df's pcent is used/(used+avail); `size - avail` includes
        // reserved blocks and under-fills (caught in the f2 live run).
        let command = fill_command("/mnt/oceanfs-spare", 20).expect("build");
        assert!(command.contains("--output=used"));
        assert!(command.contains("--output=avail"));
        assert!(command.contains("denom=$(( used + avail ))"));
        assert!(!command.contains("--output=size"));
    }

    #[test]
    fn from_provisioning_record_maps_phase3_nodes_and_volumes() {
        let cluster = test_cluster("10.0.0.2:9000,10.0.0.3:9000", Vec::new());
        let injector =
            FleetInjector::from_provisioning_record(&cluster, PHASE3_RECORD).expect("parse");
        assert_eq!(injector.targets().len(), 2);

        let node0 = &injector.targets()[0];
        assert_eq!(node0.ssh(), Some("root@10.0.0.2"));
        assert_eq!(node0.volume("data").map(|v| v.id), Some(101));
        assert_eq!(node0.volume("data").map(|v| v.mount.as_str()), Some("/mnt/oceanfs-data"));
        assert_eq!(node0.volume("hints").map(|v| v.id), Some(104));
        assert!(node0.volume("wal").is_none());

        let node1 = &injector.targets()[1];
        assert_eq!(node1.ssh(), Some("root@10.0.0.3"));
        assert_eq!(node1.volume("data").map(|v| v.id), Some(201));
    }

    #[test]
    fn from_provisioning_record_prefers_cluster_ssh_targets() {
        let cluster = test_cluster("10.0.0.2:9000", vec!["root@alias-node-0".to_string()]);
        let injector =
            FleetInjector::from_provisioning_record(&cluster, PHASE3_RECORD).expect("parse");
        assert_eq!(injector.targets()[0].ssh(), Some("root@alias-node-0"));
    }

    #[test]
    fn from_provisioning_record_maps_phase2_single_sut() {
        let record = r#"{
            "sut": {
                "name": "oceanfs-f1v",
                "ip": "10.0.0.2",
                "internal_ip": "10.0.0.2",
                "volumes": [
                    {"role": "spare", "name": "v-spare", "id": 7, "device": "/dev/sdf", "mount": "/mnt/oceanfs-spare", "size_gb": 60}
                ]
            }
        }"#;
        let cluster = test_cluster("10.0.0.2:9000", Vec::new());
        let injector = FleetInjector::from_provisioning_record(&cluster, record).expect("parse");
        assert_eq!(injector.targets().len(), 1);
        assert_eq!(injector.targets()[0].volume("spare").map(|v| v.id), Some(7));
    }

    #[test]
    fn from_provisioning_record_rejects_invalid_json() {
        let cluster = test_cluster("10.0.0.2:9000", Vec::new());
        assert!(FleetInjector::from_provisioning_record(&cluster, "not json").is_err());
    }

    #[test]
    fn from_provisioning_record_without_configuration_yields_empty_targets() {
        let cluster = test_cluster("10.0.0.2:9000,10.0.0.3:9000", Vec::new());
        let injector = FleetInjector::from_provisioning_record(&cluster, "{}").expect("parse");
        assert_eq!(injector.targets().len(), 2);
        assert!(injector.targets().iter().all(|t| t.volumes().is_empty()));
    }

    #[tokio::test]
    async fn injector_without_volumes_records_skipped_failure() {
        let cluster = test_cluster("10.0.0.2:9000", Vec::new());
        let mut injector = FleetInjector::from_provisioning_record(&cluster, "{}").expect("parse");
        let err = injector.yank_volume(0, "data").await.expect_err("must skip");
        assert!(err.to_string().contains("skipped"));

        let records = injector.records();
        assert_eq!(records.len(), 1, "exactly one record per attempt");
        assert_eq!(records[0].injection_type, "device_yank");
        assert_eq!(records[0].node_index, 0);
        assert!(!records[0].success);
        assert!(records[0].detail.contains("skipped"));
    }

    #[tokio::test]
    async fn injector_without_ssh_records_skipped_failure() {
        // Volumes present, but neither TARGET_HOST_SSH nor a record
        // internal_ip — the attempt must skip, not guess a target.
        let record = r#"{
            "sut": {
                "volumes": [
                    {"role": "data", "id": 101, "device": "/dev/sdb", "mount": "/mnt/oceanfs-data", "size_gb": 120}
                ]
            }
        }"#;
        let cluster = test_cluster("10.0.0.2:9000", Vec::new());
        let mut injector =
            FleetInjector::from_provisioning_record(&cluster, record).expect("parse");
        let err = injector.fill_volume(0, "data", 20).await.expect_err("must skip");
        assert!(err.to_string().contains("skipped"), "got: {err}");
        assert!(err.to_string().contains("SSH"));
        assert_eq!(injector.records().len(), 1);
        assert!(!injector.records()[0].success);
        assert!(injector.records()[0].detail.contains("skipped"));
    }

    #[test]
    fn finish_records_success_and_failure_exactly_once() {
        let cluster = test_cluster("10.0.0.2:9000", Vec::new());
        let mut injector = FleetInjector::new(&cluster, Vec::new());
        injector.finish("device_yank", 0, "data", Ok("deleted sdb".to_string())).expect("success");
        let err = injector
            .finish("device_yank", 1, "data", Err(Error::ClusterError("boom".to_string())))
            .expect_err("failure");
        assert!(err.to_string().contains("boom"));

        let records = injector.records();
        assert_eq!(records.len(), 2);
        assert!(records[0].success);
        assert!(records[0].detail.starts_with("data:"));
        assert!(!records[1].success);
        assert!(records[1].detail.contains("boom"));
    }

    #[test]
    fn volume_ref_uses_serial_path() {
        let volume = VolumeRef {
            role: "data".to_string(),
            id: 99,
            device: "/dev/sdb".to_string(),
            mount: "/mnt/oceanfs-data".to_string(),
            size_gb: 120,
        };
        assert_eq!(volume.by_id_path(), "/dev/disk/by-id/scsi-0HC_Volume_99");
    }

    #[test]
    fn probe_status_interprets_exit_codes() {
        use crate::remote::SshOutput;
        let out = |status: i32| SshOutput { status, stdout: String::new(), stderr: String::new() };
        assert!(probe_status(out(0), "probe").expect("status 0"));
        assert!(!probe_status(out(1), "probe").expect("status 1"));
        assert!(probe_status(out(255), "probe").is_err(), "ssh transport failure is an error");
    }

    #[test]
    fn device_gone_command_negates_the_existence_test() {
        // probe_status reads exit 0 as "condition true"; the device-gone
        // condition is the NEGATED existence test (the f2 live run caught
        // the inverted form).
        let command = device_gone_command("/dev/disk/by-id/scsi-0HC_Volume_42");
        assert!(command.starts_with("test ! -e "));
        assert!(command.contains("scsi-0HC_Volume_42"));
    }
}
