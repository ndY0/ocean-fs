//! Event WAL checkpoint — the event log's own GC (ADR-0024 Decision 3).
//!
//! An atomic snapshot of the folded registry in **our own format** —
//! temp file + fsync + rename + directory fsync — triggered **only** by
//! a byte threshold on the event log (`event_wal_checkpoint_bytes`,
//! default 64 MB), after which events older than the snapshot are
//! truncated. The threshold is the *only* trigger; there is no
//! time-based fallback (ADR-0024 Decision 4's rationale applies to the
//! trigger as well).
//!
//! The checkpoint bounds startup replay: fold cost is capped by the
//! threshold, not by lifetime event volume. Startup becomes: load latest
//! checkpoint (ms) → append-fold events after it → machine ready. The
//! checkpoint is the on-disk state snapshot that eventually replaces the
//! `segments` CF (ADR-0025 Decision 3) — deliberately NOT RocksDB
//! (ADR-0023 direction): plain files in our own format.
//!
//! # Snapshot format (explicit byte layout — perf 6.3 discipline)
//!
//! ```text
//! checkpoint-{file_seq:08}-{offset}:
//!   magic        [4]   = b"CHK\1"
//!   version      [1]   = 3 (2 = pre-pool: 7-field metadata, no pool_id —
//!                      refused at boot since ADR-0031 D3)
//!   covered_pos  [12]  file_seq(4 LE) + offset(8 LE) — the EventWalPos
//!                      covered by this snapshot (the fold starts after it)
//!   entry_count  [4]   LE
//!   entries      [entry_count]
//!     segment_id [16]
//!     state      [1]   0=Reserved, 1=Sealed (Deleted never appears —
//!                      a DeleteEvent makes the segment's history garbage)
//!     meta_len   [4]   LE — the serialized metadata payload length
//!                      (extension over the ADR sketch: the bincode payload
//!                      is variable-length and needs a length prefix)
//!     metadata   [meta_len]  bincode(SegmentMetadata) — the full metadata
//!                      incl. merkle_root for Sealed entries; the
//!                      metadata always carries `pool_id` (ADR-0029 f5,
//!                      the durable segment→pool mapping)
//!     data_wal_pos [12]  file_seq(4 LE) + offset(8 LE) — Sealed entries
//!                      only (retention needs it to survive checkpointing)
//!     repacked_flag [1]  0/1 — Sealed entries only (the compaction
//!                      marker, ADR-0025 Decision 4 — recovery needs it to
//!                      identify incomplete compaction units)
//!     repacked_from [16]  segment id — present iff repacked_flag = 1
//!   crc32        [4]   over all preceding bytes
//! ```
//!
//! Version 2 adds the compaction marker to Sealed entries (version 1
//! snapshots without the flag byte are rejected — none exist in the
//! field); version 3 adds `pool_id` to the metadata. A v2 (pre-pool)
//! checkpoint is refused at boot with an explicit "unsupported pre-pool
//! data directory" error (ADR-0031 D3) — never decoded, never silently
//! replaced by an older snapshot or an empty registry.

use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use oceanfs_core::{
    Counter, EventWalConfig, LabelSet, MetricRegistrar, SegmentId, SegmentMetadata,
};

use crate::{
    error::{Error, Result},
    segment::{
        event_wal::{DataWalPos, EventWal, EventWalPos},
        lifecycle::{SegmentLifecycleRegistry, SegmentState},
    },
};

/// Magic bytes at the start of every checkpoint file (4 bytes: "CHK\1").
pub(crate) const CHECKPOINT_MAGIC: [u8; 4] = [b'C', b'H', b'K', 1];

/// On-disk format version of checkpoint files.
/// Checkpoint format version. v3 (current): `SegmentMetadata` carries
/// `pool_id` (ADR-0029 f5). v2 — the pre-pool 7-field metadata without
/// `pool_id` — is refused at boot (ADR-0031 D3); the literal version
/// byte appears only in the pre-pool classifier below.
pub(crate) const CHECKPOINT_VERSION: u8 = 3;

/// Fixed header size of a checkpoint file: magic(4) + version(1) +
/// covered_pos(12) + entry_count(4) = 21.
pub(crate) const CHECKPOINT_HEADER_SIZE: usize = 21;

/// Entry envelope sizes: segment_id(16) + state(1) + meta_len(4) = 21,
/// plus data_wal_pos(12) + repacked_flag(1) [+ repacked_from(16)] for
/// Sealed entries.
const ENTRY_FIXED_SIZE: usize = 21;
const SEALED_EXTRA_SIZE: usize = 12;
const REPACKED_FLAG_SIZE: usize = 1;
const REPACKED_FROM_SIZE: usize = 16;

/// State bytes on disk.
const STATE_RESERVED: u8 = 0;
const STATE_SEALED: u8 = 1;

/// The observable result of a checkpoint write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// The `EventWalPos` covered by the snapshot — the fold starts after
    /// it.
    pub covered_pos: EventWalPos,
    /// Number of live entries snapshotted.
    pub entries: usize,
    /// Snapshot file size in bytes.
    pub bytes: u64,
}

/// The event log's checkpoint manager (ADR-0024 Decision 3).
///
/// Snapshots the folded registry to `checkpoint-{file_seq:08}-{offset}`
/// files next to the event WAL files, loads the newest valid snapshot at
/// startup, and truncates covered events. The trigger is a byte
/// threshold only ([`needs_checkpoint`](Self::needs_checkpoint)); the
/// manager caches the newest covered position so the trigger is O(1).
///
/// # Examples
///
/// ```ignore
/// // Requires a tokio runtime (the event wal is async); the unit tests
/// // in this module exercise the full cycle.
/// use oceanfs_core::EventWalConfig;
/// use oceanfs_storage::segment::event_checkpoint::EventCheckpoint;
/// use oceanfs_storage::segment::event_wal::EventWal;
///
/// # #[tokio::main]
/// # async fn main() {
/// let dir = std::env::temp_dir().join("event-checkpoint-example");
/// let wal = Arc::new(EventWal::open(dir.clone(), &EventWalConfig::default()).await.unwrap());
/// let checkpoint = EventCheckpoint::open(dir, wal).unwrap();
/// assert!(checkpoint.last_checkpoint_pos().is_none());
/// # }
/// ```
pub struct EventCheckpoint {
    /// Directory holding the `checkpoint-*` files (the event WAL dir).
    dir: PathBuf,
    /// The event log — `bytes_since` (the trigger input) and the
    /// truncation target.
    event_wal: Arc<EventWal>,
    /// The newest covered position, cached for the O(1) trigger.
    last_covered: AtomicU64,
    /// `oceanfs_event_wal_checkpoint_bytes` — bytes written by checkpoints.
    checkpoint_bytes: Counter,
    /// `oceanfs_event_wal_truncated_bytes` — bytes removed by truncation.
    truncated_bytes: Counter,
}

impl EventCheckpoint {
    /// Opens the checkpoint manager for the event WAL directory.
    ///
    /// Scans for existing `checkpoint-*` files, caches the newest covered
    /// position, and removes orphan `.tmp` files (a crash during the
    /// temp write leaves one behind — the old checkpoint plus a full
    /// fold remain the recovery path, ADR-0024 §Negative).
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be scanned.
    pub fn open(dir: PathBuf, event_wal: Arc<EventWal>) -> Result<Self> {
        let last_covered = Self::scan_newest(&dir)?;
        let checkpoint = Self {
            dir,
            event_wal,
            last_covered: AtomicU64::new(last_covered.map(|pos| pos.packed()).unwrap_or(0)),
            checkpoint_bytes: Counter::new(
                "oceanfs_event_wal_checkpoint_bytes".into(),
                "Bytes written by segment event WAL checkpoints".into(),
                LabelSet::empty(),
            ),
            truncated_bytes: Counter::new(
                "oceanfs_event_wal_truncated_bytes".into(),
                "Bytes truncated from the segment event WAL by checkpoints".into(),
                LabelSet::empty(),
            ),
        };
        Ok(checkpoint)
    }

    /// Writes a checkpoint snapshot of `registry` covering `up_to`.
    ///
    /// Atomic: `checkpoint-{pos}.tmp` → write → fsync → rename to
    /// `checkpoint-{pos}` → directory fsync. Live entries (`Reserved` /
    /// `Sealed`) are copied under the registry's short read guards
    /// (perf 7.1 — the serialization itself is lock-free); the snapshot
    /// stays O(live segments) because `Deleted` entries are already
    /// evicted (a `DeleteEvent` makes the segment's entire history
    /// garbage).
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the snapshot cannot be written or the
    /// rename/directory fsync fails; the old checkpoint (if any) is
    /// untouched.
    pub fn write_checkpoint(
        &self,
        registry: &SegmentLifecycleRegistry,
        up_to: EventWalPos,
    ) -> Result<CheckpointInfo> {
        let bytes = encode_snapshot(registry, up_to)?;
        let name = checkpoint_file_name(up_to);
        let tmp_path = self.dir.join(format!("{name}.tmp"));
        let final_path = self.dir.join(&name);

        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, &final_path)?;
        // The rename must be durable before the truncation can rely on
        // it (ADR-0024 Decision 3: temp file + rename + fsync).
        let dir_file = std::fs::File::open(&self.dir)?;
        dir_file.sync_all()?;

        let entries = u32::from_le_bytes(
            bytes[17..21]
                .try_into()
                .map_err(|_| Error::Io(std::io::Error::other("snapshot header corruption")))?,
        ) as usize;
        self.checkpoint_bytes.add(bytes.len() as u64);
        self.last_covered.store(up_to.packed(), Ordering::Release);
        Ok(CheckpointInfo { covered_pos: up_to, entries, bytes: bytes.len() as u64 })
    }

    /// Loads the newest **valid** checkpoint: the snapshot registry plus
    /// its covered position (the fold starts after it).
    ///
    /// The newest checkpoint by position is tried first; if it fails
    /// validation (checksum/version — disk corruption), the next newest
    /// is tried: every checkpoint is a complete snapshot at its covered
    /// position, so falling back is safe (startup folds the events after
    /// the older covered position instead). Orphan `.tmp` files are
    /// removed.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be scanned.
    pub fn load_checkpoint(&self) -> Result<Option<(SegmentLifecycleRegistry, EventWalPos)>> {
        self.remove_orphan_tmp()?;
        let mut candidates: Vec<EventWalPos> = Vec::new();
        let dir = std::fs::read_dir(&self.dir)?;
        for entry in dir.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(pos) = parse_checkpoint_name(&name) {
                candidates.push(pos);
            }
        }
        candidates.sort_by(|a, b| b.cmp(a)); // newest first
        for pos in candidates {
            let path = checkpoint_file_path(&self.dir, pos);
            if let Ok(raw) = std::fs::read(&path) {
                if let Some((covered, registry)) = decode_snapshot(&raw) {
                    if covered == pos {
                        return Ok(Some((registry, covered)));
                    }
                    tracing::warn!(
                        covered = ?pos,
                        "checkpoint body covered position disagrees with its name; skipping"
                    );
                } else if is_pre_pool_checkpoint(&raw) {
                    // A v2 (pre-pool) checkpoint is not corruption and
                    // not an older-format snapshot to fall back past: it
                    // proves the directory was written before pools.
                    // Boot refuses explicitly (ADR-0031 D3) — never a
                    // silent start from an older snapshot or from
                    // scratch over a pre-pool directory.
                    return Err(Error::UnsupportedPrePoolDataDir {
                        detail: format!(
                            "checkpoint {} is a pre-pool (v2) snapshot — \
                             boot onto a pre-pool data directory is refused (ADR-0031 D3)",
                            path.display(),
                        ),
                    });
                } else {
                    tracing::warn!(
                        covered = ?pos,
                        "checkpoint failed validation; falling back to an older snapshot"
                    );
                }
            }
        }
        Ok(None)
    }

    /// Truncates the event log before `pos`: deletes files fully covered
    /// by `pos` and trims the straddling file exactly at `pos.offset`.
    /// Events at/after `pos` are never touched (the fold starts at
    /// `pos`).
    ///
    /// # Errors
    ///
    /// Returns an I/O error if a file cannot be removed or trimmed.
    pub async fn truncate_before(&self, pos: EventWalPos) -> Result<()> {
        let removed = self.event_wal.truncate_before(pos).await?;
        self.truncated_bytes.add(removed);
        Ok(())
    }

    /// Returns the newest checkpoint's covered position, or `None` when
    /// no checkpoint exists yet.
    pub fn last_checkpoint_pos(&self) -> Option<EventWalPos> {
        let packed = self.last_covered.load(Ordering::Acquire);
        if packed == 0 {
            None
        } else {
            Some(EventWalPos::from_packed(packed))
        }
    }

    /// The threshold-only trigger (ADR-0024 Decision 3): `true` when the
    /// event log has grown at least `event_wal_checkpoint_bytes` since
    /// the last checkpoint. O(1) — the covered position is cached and
    /// `bytes_since` is an atomic read.
    pub fn needs_checkpoint(&self, config: &EventWalConfig) -> bool {
        let last = self.last_checkpoint_pos().unwrap_or(EventWalPos { file_seq: 0, offset: 0 });
        self.event_wal.bytes_since(last) >= config.event_wal_checkpoint_bytes
    }

    /// Registers the checkpoint metrics with a metrics registrar
    /// (`oceanfs_event_wal_checkpoint_bytes`,
    /// `oceanfs_event_wal_truncated_bytes` — perf 11.1, atomic counters).
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.checkpoint_bytes.clone());
        registrar.register_counter(self.truncated_bytes.clone());
    }

    /// Scans the directory for the newest checkpoint file by covered
    /// position.
    fn scan_newest(dir: &Path) -> Result<Option<EventWalPos>> {
        let mut newest: Option<EventWalPos> = None;
        let dir_entries = std::fs::read_dir(dir)?;
        for entry in dir_entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(pos) = parse_checkpoint_name(&name) {
                if newest.map(|n| pos > n).unwrap_or(true) {
                    newest = Some(pos);
                }
            }
        }
        Ok(newest)
    }

    /// Removes orphan `checkpoint-*.tmp` files (a crash during the temp
    /// write; the old checkpoint + full fold remain the recovery path).
    fn remove_orphan_tmp(&self) -> Result<()> {
        let dir = std::fs::read_dir(&self.dir)?;
        for entry in dir.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("checkpoint-") && name.ends_with(".tmp") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        Ok(())
    }
}

/// Checkpoint file name for a covered position
/// (`checkpoint-{file_seq:08}-{offset}`).
fn checkpoint_file_name(pos: EventWalPos) -> String {
    format!("checkpoint-{:08}-{}", pos.file_seq, pos.offset)
}

/// Parses a checkpoint file name into its covered position.
fn parse_checkpoint_name(name: &str) -> Option<EventWalPos> {
    let rest = name.strip_prefix("checkpoint-")?;
    let (seq, offset) = rest.split_once('-')?;
    Some(EventWalPos { file_seq: seq.parse::<u32>().ok()?, offset: offset.parse::<u64>().ok()? })
}

fn checkpoint_file_path(dir: &Path, pos: EventWalPos) -> PathBuf {
    dir.join(checkpoint_file_name(pos))
}

/// Serializes a registry snapshot covering `up_to` (see the module
/// docs for the format).
fn encode_snapshot(registry: &SegmentLifecycleRegistry, up_to: EventWalPos) -> Result<Vec<u8>> {
    // Collect live entries under the shard read guards (perf 7.1: the
    // copies are cheap; the serialization below is lock-free).
    type SnapshotEntry =
        (SegmentId, SegmentState, SegmentMetadata, Option<DataWalPos>, Option<SegmentId>);
    let mut entries: Vec<SnapshotEntry> = Vec::new();
    registry.for_each(|id, entry| {
        entries.push((
            id,
            entry.state,
            entry.metadata.clone(),
            entry.data_wal_pos,
            entry.repacked_from,
        ));
    });

    let mut buf =
        Vec::with_capacity(CHECKPOINT_HEADER_SIZE + entries.len() * (ENTRY_FIXED_SIZE + 128));
    buf.extend_from_slice(&CHECKPOINT_MAGIC);
    buf.push(CHECKPOINT_VERSION);
    buf.extend_from_slice(&up_to.file_seq.to_le_bytes());
    buf.extend_from_slice(&up_to.offset.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    for (id, state, meta, data_wal_pos, repacked_from) in entries {
        buf.extend_from_slice(id.as_uuid().as_bytes());
        let state_byte = match state {
            SegmentState::Reserved => STATE_RESERVED,
            SegmentState::Sealed => STATE_SEALED,
            SegmentState::Deleted => {
                // Unreachable: the registry evicts Deleted entries (a
                // DeleteEvent makes the segment's history garbage).
                return Err(Error::Io(std::io::Error::other(
                    "checkpoint snapshot: Deleted entry in live registry",
                )));
            }
        };
        buf.push(state_byte);
        let meta_bytes = bincode::serialize(&meta).map_err(|e| {
            Error::Io(std::io::Error::other(format!("checkpoint metadata serialization: {e}")))
        })?;
        buf.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&meta_bytes);
        if state == SegmentState::Sealed {
            // Retention needs the sealed position to survive
            // checkpointing; `(0, 0)` when nothing was recorded (the
            // SealEvent's sentinel).
            let pos = data_wal_pos.unwrap_or(DataWalPos { file_seq: 0, offset: 0 });
            buf.extend_from_slice(&pos.file_seq.to_le_bytes());
            buf.extend_from_slice(&pos.offset.to_le_bytes());
            // The compaction marker (ADR-0025 Decision 4): recovery
            // needs it to identify incomplete compaction units (rows
            // 7–9) after a checkpoint-covered restart.
            match repacked_from {
                Some(old) => {
                    buf.push(1);
                    buf.extend_from_slice(old.as_uuid().as_bytes());
                }
                None => buf.push(0),
            }
        }
    }

    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

// [review][codesmell][medium]
// usage of magic constants. name constantes, then use them as boundaries for readability
// [end]
/// True when a well-framed checkpoint has the **pre-pool** v2 shape:
/// the checkpoint magic + version byte 2 + a valid trailing CRC
/// (ADR-0031 D3). v2 snapshots carry 7-field metadata without `pool_id`;
/// boot refuses such directories as unsupported — never corruption,
/// never an older snapshot to fall back past.
fn is_pre_pool_checkpoint(bytes: &[u8]) -> bool {
    if bytes.len() < CHECKPOINT_HEADER_SIZE + 4 {
        return false;
    }
    if bytes[0..4] != CHECKPOINT_MAGIC {
        return false;
    }
    if bytes[4] != 2 {
        // Literal pre-pool version byte (the named const was deleted
        // with the v2 decode — ADR-0031 D3).
        return false;
    }
    let stored_crc = match bytes[bytes.len() - 4..].try_into() {
        Ok(tail) => u32::from_le_bytes(tail),
        Err(_) => return false,
    };
    stored_crc == crc32fast::hash(&bytes[..bytes.len() - 4])
}

/// Deserializes a snapshot into a fresh registry; `None` on any framing
/// error (bad magic, unsupported version, CRC mismatch, truncated
/// entries, unknown state).
///
/// Only the current v3 layout decodes. A v2 (pre-pool) checkpoint is
/// detected by the caller ([`EventCheckpoint::load_checkpoint`]) and
/// refused as an unsupported pre-pool data directory (ADR-0031 D3) —
/// it never reaches this function's decode arms.
fn decode_snapshot(bytes: &[u8]) -> Option<(EventWalPos, SegmentLifecycleRegistry)> {
    if bytes.len() < CHECKPOINT_HEADER_SIZE + 4 {
        return None;
    }
    if bytes[0..4] != CHECKPOINT_MAGIC {
        return None;
    }
    if bytes[4] != CHECKPOINT_VERSION {
        return None;
    }
    let stored_crc = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().ok()?);
    if stored_crc != crc32fast::hash(&bytes[..bytes.len() - 4]) {
        return None;
    }
    let covered = EventWalPos {
        file_seq: u32::from_le_bytes(bytes[5..9].try_into().ok()?),
        offset: u64::from_le_bytes(bytes[9..17].try_into().ok()?),
    };
    let entry_count = u32::from_le_bytes(bytes[17..21].try_into().ok()?) as usize;

    let registry = SegmentLifecycleRegistry::new(&oceanfs_core::LifecycleConfig::default());
    registry.reserve_hint(entry_count);

    let mut cursor = CHECKPOINT_HEADER_SIZE;
    for _ in 0..entry_count {
        // segment_id(16) + state(1) + meta_len(4)
        if cursor + ENTRY_FIXED_SIZE > bytes.len() - 4 {
            return None;
        }
        let segment_id = SegmentId::from_uuid_bytes(bytes[cursor..cursor + 16].try_into().ok()?);
        let state_byte = bytes[cursor + 16];
        let meta_len =
            u32::from_le_bytes(bytes[cursor + 17..cursor + 21].try_into().ok()?) as usize;
        cursor += ENTRY_FIXED_SIZE;
        if cursor + meta_len > bytes.len() - 4 {
            return None;
        }
        let meta: SegmentMetadata = match bincode::deserialize(&bytes[cursor..cursor + meta_len]) {
            Ok(meta) => meta,
            Err(_) => return None,
        };
        cursor += meta_len;
        match state_byte {
            STATE_RESERVED => {
                registry.reserve(segment_id, meta).ok()?;
            }
            STATE_SEALED => {
                if cursor + SEALED_EXTRA_SIZE + REPACKED_FLAG_SIZE > bytes.len() - 4 {
                    return None;
                }
                let data_wal_pos = DataWalPos {
                    file_seq: u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?),
                    offset: u64::from_le_bytes(bytes[cursor + 4..cursor + 12].try_into().ok()?),
                };
                cursor += SEALED_EXTRA_SIZE;
                let repacked_flag = bytes[cursor];
                cursor += REPACKED_FLAG_SIZE;
                let repacked_from = match repacked_flag {
                    0 => None,
                    1 => {
                        if cursor + REPACKED_FROM_SIZE > bytes.len() - 4 {
                            return None;
                        }
                        let old =
                            SegmentId::from_uuid_bytes(bytes[cursor..cursor + 16].try_into().ok()?);
                        cursor += REPACKED_FROM_SIZE;
                        Some(old)
                    }
                    _ => return None, // unknown repacked flag
                };
                // The seal transition requires a Reserved entry: reserve
                // with the pre-seal metadata shape first (like the
                // fold's Reserve arm), then record the position BEFORE
                // the seal (the record updates Reserved entries; the
                // seal keeps it).
                let reserved_meta = SegmentMetadata {
                    pool_id: 0,
                    segment_id,
                    ec_k: meta.ec_k,
                    ec_m: meta.ec_m,
                    size_tier: meta.size_tier,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                };
                registry.reserve(segment_id, reserved_meta).ok()?;
                registry.record_data_wal_pos(segment_id, data_wal_pos);
                registry.seal_with(segment_id, meta, repacked_from).ok()?;
            }
            _ => return None, // unknown state
        }
    }
    Some((covered, registry))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::{HashOutput, LifecycleConfig, SizeTier};

    use super::*;

    fn test_registry_with(entries: usize) -> SegmentLifecycleRegistry {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        for i in 0..entries {
            let id = SegmentId::new();
            registry
                .reserve(
                    id,
                    SegmentMetadata {
                        pool_id: 0,
                        segment_id: id,
                        ec_k: 4,
                        ec_m: 2,
                        size_tier: SizeTier::Standard,
                        merkle_root: None,
                        storage_locations: smallvec::SmallVec::new(),
                        sealed_at: None,
                    },
                )
                .unwrap();
            if i % 2 == 1 {
                registry
                    .record_data_wal_pos(id, DataWalPos { file_seq: 1, offset: 100 + i as u64 });
                registry
                    .seal(
                        id,
                        SegmentMetadata {
                            pool_id: 0,
                            segment_id: id,
                            ec_k: 4,
                            ec_m: 2,
                            size_tier: SizeTier::Standard,
                            merkle_root: Some(HashOutput::from_bytes([0xAB; 32])),
                            storage_locations: smallvec::SmallVec::new(),
                            sealed_at: Some(1_700_000_000_000),
                        },
                    )
                    .unwrap();
            }
        }
        registry
    }

    async fn test_env() -> (tempfile::TempDir, Arc<EventWal>) {
        let dir = tempfile::tempdir().unwrap();
        let config = EventWalConfig {
            event_wal_dir: dir.path().join("event-wal"),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024,
        };
        let wal = Arc::new(EventWal::open(config.event_wal_dir.clone(), &config).await.unwrap());
        (dir, wal)
    }

    // ------------------------------------------------------------------
    // Snapshot encode/decode round trip
    // ------------------------------------------------------------------

    /// ADR-0031 D3: a v2 (pre-pool) checkpoint — whose metadata blobs
    /// lack `pool_id` — no longer decodes and no longer falls back to
    /// an older snapshot or an empty registry: it proves the directory
    /// was written before pools, and boot refuses it explicitly.
    #[tokio::test]
    async fn v2_checkpoint_is_refused_not_decoded() {
        let id = SegmentId::new();

        // Craft a minimal v2 snapshot: magic + version 2 + covered pos +
        // zero entries + crc (a v2 writer emits this exact frame).
        let covered = EventWalPos { file_seq: 3, offset: 4096 };
        let mut buf = Vec::new();
        buf.extend_from_slice(&CHECKPOINT_MAGIC);
        buf.push(2); // pre-pool checkpoint version (ADR-0031 D3)
        buf.extend_from_slice(&covered.file_seq.to_le_bytes());
        buf.extend_from_slice(&covered.offset.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // entry_count = 0
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        // The decoder itself refuses the v2 shape.
        assert!(decode_snapshot(&buf).is_none(), "v2 snapshots must not decode");

        // The boot seam: load_checkpoint surfaces the explicit pre-pool
        // error instead of returning None (falling back to scratch).
        let (dir, wal) = test_env().await;
        let ckpt_dir = wal.dir().to_path_buf();
        std::fs::write(ckpt_dir.join("checkpoint-00000003-4096"), &buf).unwrap();
        let checkpoint = EventCheckpoint::open(ckpt_dir.clone(), wal.clone()).unwrap();
        let err = match checkpoint.load_checkpoint() {
            Ok(_) => panic!("v2 checkpoint must refuse load_checkpoint"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("unsupported pre-pool data directory"),
            "the error must carry the explicit pre-pool message: {err}"
        );
        let _ = (dir, id);
    }

    #[test]
    fn snapshot_round_trip_preserves_registry_and_covered_pos() {
        let registry = test_registry_with(10);
        let covered = EventWalPos { file_seq: 3, offset: 4096 };
        let bytes = encode_snapshot(&registry, covered).unwrap();
        let (loaded_covered, loaded) = decode_snapshot(&bytes).expect("snapshot decodes");

        assert_eq!(loaded_covered, covered);
        assert_eq!(loaded.len(), registry.len());
        // Every live entry round-trips with its state + metadata + pos.
        registry.for_each(|id, entry| {
            let other = loaded.get(id).expect("entry present after round trip");
            assert_eq!(other.state, entry.state);
            assert_eq!(other.metadata.segment_id, entry.metadata.segment_id);
            assert_eq!(other.metadata.merkle_root, entry.metadata.merkle_root);
            assert_eq!(other.metadata.sealed_at, entry.metadata.sealed_at);
            assert_eq!(other.data_wal_pos, entry.data_wal_pos);
        });
    }

    #[test]
    fn snapshot_round_trip_preserves_the_compaction_repacked_from_marker() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig::default());
        let id = SegmentId::new();
        let old = SegmentId::new();
        let meta = SegmentMetadata {
            pool_id: 0,
            segment_id: id,
            ec_k: 4,
            ec_m: 2,
            size_tier: SizeTier::Standard,
            merkle_root: Some(HashOutput::from_bytes([0xAB; 32])),
            storage_locations: smallvec::SmallVec::new(),
            sealed_at: Some(1_700_000_000_000),
        };
        registry
            .reserve(id, SegmentMetadata { merkle_root: None, sealed_at: None, ..meta.clone() })
            .unwrap();
        registry.record_data_wal_pos(id, DataWalPos { file_seq: 1, offset: 100 });
        registry.seal_with(id, meta, Some(old)).unwrap();

        let bytes = encode_snapshot(&registry, EventWalPos { file_seq: 3, offset: 4096 }).unwrap();
        let (_, loaded) = decode_snapshot(&bytes).expect("snapshot decodes");
        let entry = loaded.get(id).expect("entry present after round trip");
        assert_eq!(entry.state, SegmentState::Sealed);
        assert_eq!(entry.data_wal_pos, Some(DataWalPos { file_seq: 1, offset: 100 }));
        assert_eq!(
            entry.repacked_from,
            Some(old),
            "the compaction marker must survive checkpointing (rows 7-9 recovery)"
        );
    }

    #[test]
    fn snapshot_deleted_entries_are_absent() {
        let registry = SegmentLifecycleRegistry::new(&LifecycleConfig {
            lifecycle_registry_shards: 8,
            delete_grace_ms: 0,
        });
        let id = SegmentId::new();
        registry
            .reserve(
                id,
                SegmentMetadata {
                    pool_id: 0,
                    segment_id: id,
                    ec_k: 4,
                    ec_m: 2,
                    size_tier: SizeTier::Standard,
                    merkle_root: None,
                    storage_locations: smallvec::SmallVec::new(),
                    sealed_at: None,
                },
            )
            .unwrap();
        registry.delete(id).unwrap(); // grace 0 → evicted
        let bytes = encode_snapshot(&registry, EventWalPos { file_seq: 0, offset: 0 }).unwrap();
        let (_, loaded) = decode_snapshot(&bytes).expect("snapshot decodes");
        assert!(loaded.get(id).is_none(), "deleted segments must not appear in the snapshot");
        assert_eq!(loaded.len(), 0);
    }

    #[test]
    fn snapshot_rejects_checksum_and_version_mutations() {
        let registry = test_registry_with(4);
        let bytes = encode_snapshot(&registry, EventWalPos { file_seq: 1, offset: 200 }).unwrap();

        let mut bad_crc = bytes.clone();
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 0xFF;
        assert!(decode_snapshot(&bad_crc).is_none(), "CRC mutation must be rejected");

        let mut bad_version = bytes.clone();
        bad_version[4] = 99;
        let crc = crc32fast::hash(&bad_version[..bad_version.len() - 4]);
        let crc_bytes = crc.to_le_bytes();
        let crc_at = bad_version.len() - 4;
        bad_version[crc_at..].copy_from_slice(&crc_bytes);
        assert!(decode_snapshot(&bad_version).is_none(), "unknown version must be rejected");

        let mut bad_magic = bytes.clone();
        bad_magic[0] = b'X';
        assert!(decode_snapshot(&bad_magic).is_none(), "bad magic must be rejected");

        // Truncated payload must be rejected.
        assert!(decode_snapshot(&bytes[..bytes.len() - 10]).is_none());
    }

    #[test]
    fn snapshot_size_stays_linear_in_live_entries() {
        // The O(live segments) bound (ADR-0025 Decision 5): ~130 B per
        // entry at this metadata shape — 100K entries must stay well
        // under the TB-scale ~500 MB budget.
        let registry = test_registry_with(100_000);
        let bytes = encode_snapshot(&registry, EventWalPos { file_seq: 0, offset: 0 }).unwrap();
        assert!(
            bytes.len() < 25 * 1024 * 1024,
            "100K entries must stay far under the TB-scale budget, got {} bytes",
            bytes.len()
        );
        assert!(bytes.len() > 5 * 1024 * 1024, "sanity: the snapshot carries real entries");
    }

    // ------------------------------------------------------------------
    // write/load/truncate cycle
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn write_load_truncate_cycle() {
        let (_dir, wal) = test_env().await;
        let checkpoint = EventCheckpoint::open(wal.dir().to_path_buf(), wal.clone()).unwrap();
        assert!(checkpoint.last_checkpoint_pos().is_none());

        let registry = test_registry_with(5);
        let up_to = EventWalPos { file_seq: 2, offset: 1234 };
        let info = checkpoint.write_checkpoint(&registry, up_to).unwrap();
        assert_eq!(info.covered_pos, up_to);
        assert_eq!(info.entries, registry.len());
        assert_eq!(checkpoint.last_checkpoint_pos(), Some(up_to));

        let (loaded, covered) = checkpoint.load_checkpoint().unwrap().expect("checkpoint loads");
        assert_eq!(covered, up_to);
        assert_eq!(loaded.len(), registry.len());

        // Truncation of an empty event log is a no-op.
        checkpoint.truncate_before(up_to).await.unwrap();
        assert_eq!(checkpoint.last_checkpoint_pos(), Some(up_to));
    }

    #[tokio::test]
    async fn needs_checkpoint_trigger_arithmetic() {
        let (_dir, wal) = test_env().await;
        let config = EventWalConfig {
            event_wal_dir: wal.dir().to_path_buf(),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 128,
        };
        let checkpoint = EventCheckpoint::open(wal.dir().to_path_buf(), wal.clone()).unwrap();

        // Below the threshold: no trigger.
        assert!(!checkpoint.needs_checkpoint(&config));
        // Idle (no appends): still no trigger — the threshold is the
        // ONLY trigger, there is no time-based fallback.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!checkpoint.needs_checkpoint(&config), "idle time must never trigger");

        // A burst past the threshold triggers.
        for _ in 0..4 {
            wal.append(crate::segment::event_wal::SegmentEvent::Reserve(
                crate::segment::event_wal::ReserveEvent {
                    segment_id: SegmentId::new(),
                    tier: SizeTier::Standard,
                    ec_k: 4,
                    ec_m: 2,
                },
            ))
            .await
            .unwrap();
        }
        assert!(checkpoint.needs_checkpoint(&config), "bytes past the threshold must trigger");

        // After a checkpoint, the covered position moves forward and the
        // trigger resets.
        let registry = test_registry_with(1);
        let up_to = wal.latest_pos();
        checkpoint.write_checkpoint(&registry, up_to).unwrap();
        assert!(!checkpoint.needs_checkpoint(&config), "post-checkpoint trigger must reset");
    }

    #[tokio::test]
    async fn load_cleans_orphan_tmp_and_falls_back_to_older_checkpoint() {
        let (dir, wal) = test_env().await;
        let ckpt_dir = wal.dir().to_path_buf();
        let checkpoint = EventCheckpoint::open(ckpt_dir.clone(), wal.clone()).unwrap();

        // An orphan .tmp (crash during the temp write) must be cleaned.
        std::fs::write(ckpt_dir.join("checkpoint-00000000-100.tmp"), b"partial").unwrap();

        // Two checkpoints; the newest is corrupted after the fact.
        let registry = test_registry_with(3);
        checkpoint.write_checkpoint(&registry, EventWalPos { file_seq: 1, offset: 100 }).unwrap();
        checkpoint.write_checkpoint(&registry, EventWalPos { file_seq: 2, offset: 200 }).unwrap();
        let newest_path = ckpt_dir.join("checkpoint-00000002-200");
        let mut raw = std::fs::read(&newest_path).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0xFF; // corrupt the newest
        std::fs::write(&newest_path, &raw).unwrap();

        let (loaded, covered) =
            checkpoint.load_checkpoint().unwrap().expect("the older checkpoint loads");
        assert_eq!(
            covered,
            EventWalPos { file_seq: 1, offset: 100 },
            "falls back to the older snapshot"
        );
        assert_eq!(loaded.len(), registry.len());
        assert!(
            !ckpt_dir.join("checkpoint-00000000-100.tmp").exists(),
            "orphan .tmp must be cleaned at load"
        );
        let _ = dir;
    }

    // ------------------------------------------------------------------
    // Truncation boundaries (the event wal's truncate_before)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn truncate_before_deletes_covered_files_and_trims_exactly() {
        let (_dir, wal) = test_env().await;
        let config = EventWalConfig {
            event_wal_dir: wal.dir().to_path_buf(),
            event_wal_file_size_bytes: 64,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024,
        };
        // Reopen with the tiny file size so appends rotate.
        let wal = Arc::new(EventWal::open(config.event_wal_dir.clone(), &config).await.unwrap());
        let id = SegmentId::new();
        wal.append(crate::segment::event_wal::SegmentEvent::Reserve(
            crate::segment::event_wal::ReserveEvent {
                segment_id: id,
                tier: SizeTier::Standard,
                ec_k: 4,
                ec_m: 2,
            },
        ))
        .await
        .unwrap(); // file 0
        wal.append(crate::segment::event_wal::SegmentEvent::Seal(
            crate::segment::event_wal::SealEvent {
                pool_id: 0,
                segment_id: id,
                tier: SizeTier::Standard,
                ec_k: 4,
                ec_m: 2,
                merkle_root: HashOutput::from_bytes([0xAB; 32]),
                data_wal_pos: DataWalPos { file_seq: 0, offset: 0 },
                repacked_from: None,
            },
        ))
        .await
        .unwrap(); // rotates to file 1
                   // The covered boundary: the write position after the seal.
        let boundary = wal.latest_pos();
        assert_eq!(boundary, EventWalPos { file_seq: 1, offset: 84 });

        // A later append rotates the straddling file away.
        wal.append(crate::segment::event_wal::SegmentEvent::Seal(
            crate::segment::event_wal::SealEvent {
                pool_id: 0,
                segment_id: id,
                tier: SizeTier::Standard,
                ec_k: 4,
                ec_m: 2,
                merkle_root: HashOutput::from_bytes([0xCD; 32]),
                data_wal_pos: DataWalPos { file_seq: 0, offset: 0 },
                repacked_from: None,
            },
        ))
        .await
        .unwrap(); // rotates to file 2

        let removed = wal.truncate_before(boundary).await.unwrap();
        assert!(removed > 0);
        // File 0 (below the covered file) and file 1 (rotated and fully
        // covered: size 84 == boundary.offset) are deleted.
        assert!(!wal.dir().join("evl_00000000.log").exists(), "covered file deleted");
        assert!(
            !wal.dir().join("evl_00000001.log").exists(),
            "fully-covered straddling file deleted"
        );
        assert!(wal.dir().join("evl_00000002.log").exists(), "uncovered file survives");

        // The fold starts at the boundary: the surviving events are the
        // post-boundary ones (file 2).
        let events: Vec<_> = wal.read_from(boundary).collect::<crate::Result<_>>().unwrap();
        assert_eq!(events.len(), 1, "exactly the post-boundary record survives");
        assert_eq!(events[0].0, EventWalPos { file_seq: 2, offset: 0 });

        // The writer's accounting is consistent: appends continue (the
        // delete rotates: file 2 at 84 + 32 > 64) and bytes_since stays
        // exact.
        let p = wal
            .append(crate::segment::event_wal::SegmentEvent::Delete(
                crate::segment::event_wal::DeleteEvent { segment_id: id },
            ))
            .await
            .unwrap();
        assert_eq!(p, EventWalPos { file_seq: 3, offset: 0 });
        assert_eq!(wal.bytes_since(boundary), 32, "only the post-boundary delete counts");
    }

    #[tokio::test]
    async fn truncate_before_never_cuts_events_at_or_after_pos() {
        let (_dir, wal) = test_env().await;
        let config = EventWalConfig {
            event_wal_dir: wal.dir().to_path_buf(),
            event_wal_file_size_bytes: 64,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024,
        };
        // Every record exceeds the 64-byte file size → each append
        // rotates: reserve → file 0, seal → file 1, delete → file 2,
        // seal3 → file 3.
        let wal = Arc::new(EventWal::open(config.event_wal_dir.clone(), &config).await.unwrap());
        let id = SegmentId::new();
        wal.append(crate::segment::event_wal::SegmentEvent::Reserve(
            crate::segment::event_wal::ReserveEvent {
                segment_id: id,
                tier: SizeTier::Standard,
                ec_k: 4,
                ec_m: 2,
            },
        ))
        .await
        .unwrap(); // file 0
        wal.append(crate::segment::event_wal::SegmentEvent::Seal(
            crate::segment::event_wal::SealEvent {
                pool_id: 0,
                segment_id: id,
                tier: SizeTier::Standard,
                ec_k: 4,
                ec_m: 2,
                merkle_root: HashOutput::from_bytes([0xAB; 32]),
                data_wal_pos: DataWalPos { file_seq: 0, offset: 0 },
                repacked_from: None,
            },
        ))
        .await
        .unwrap(); // rotates to file 1
        let boundary = wal.latest_pos(); // (1, 84)

        // Appends land AFTER the snapshot's covered point (the async
        // checkpoint window): the delete → file 2, seal3 → file 3.
        wal.append(crate::segment::event_wal::SegmentEvent::Delete(
            crate::segment::event_wal::DeleteEvent { segment_id: id },
        ))
        .await
        .unwrap(); // file 2 [0..32)
        wal.append(crate::segment::event_wal::SegmentEvent::Seal(
            crate::segment::event_wal::SealEvent {
                pool_id: 0,
                segment_id: id,
                tier: SizeTier::Standard,
                ec_k: 4,
                ec_m: 2,
                merkle_root: HashOutput::from_bytes([0xEF; 32]),
                data_wal_pos: DataWalPos { file_seq: 0, offset: 0 },
                repacked_from: None,
            },
        ))
        .await
        .unwrap(); // file 3 [0..84)

        // CORRECT truncation at the covered boundary: file 0 and the
        // fully-covered rotated straddling file 1 are deleted; the file
        // holding the in-flight delete (file 2, beyond the boundary) is
        // kept entirely.
        wal.truncate_before(boundary).await.unwrap();
        assert!(!wal.dir().join("evl_00000000.log").exists());
        assert!(!wal.dir().join("evl_00000001.log").exists(), "the seal is fully covered");
        assert!(wal.dir().join("evl_00000002.log").exists(), "file with uncovered events is kept");
        let events: Vec<_> = wal.read_from(boundary).collect::<crate::Result<_>>().unwrap();
        assert_eq!(events.len(), 2, "the in-flight delete + the later seal survive");
        assert_eq!(events[0].0, EventWalPos { file_seq: 2, offset: 0 });

        // MUTATION CHECK (DoD): truncating PAST the covered position
        // deletes the file holding events between the covered point and
        // the mutation point — the post-restart fold (starting at the
        // real covered position) misses the delete and diverges.
        wal.truncate_before(EventWalPos { file_seq: 3, offset: 0 }).await.unwrap();
        assert!(
            !wal.dir().join("evl_00000002.log").exists(),
            "the mutation deletes the file holding the uncovered delete"
        );
        let events: Vec<_> = wal.read_from(boundary).collect::<crate::Result<_>>().unwrap();
        assert_eq!(
            events.len(),
            1,
            "the mutation loses the uncovered delete — exactly the failure the fold test catches"
        );
        assert_eq!(events[0].0, EventWalPos { file_seq: 3, offset: 0 });
    }
}
