//! Metadata change journal — append-only, co-durable with the metadata
//! store (ADR-0038 D1/D2; ae2 / S2).
//!
//! Every logical metadata mutation applied by this node appends a tiny
//! `{seq, op, key, hlc}` record to this journal **before** the row commit
//! is made visible (J1, fail-closed). Peers consume the journal through
//! durable watermarks; the journal itself carries no row bodies — a
//! consumer point-fetches the key's current state.
//!
//! ## Storage layout
//!
//! ```text
//! <metadata_pool_root>/metadata-journal/
//!   manifest              atomic temp+rename: epoch, base_seq, gap floor,
//!                         active file number, generation
//!   00000000000000000001.jrn
//!   00000000000000000002.jrn
//!   ...
//! ```
//!
//! Each segment is append-only, rotated at [`SEGMENT_TARGET_BYTES`]
//! (64 MiB). A segment starts with a fixed header
//! `{magic, version, node_id, epoch, first_seq}` and every record is
//! length-prefixed with a CRC32 trailer, so a torn tail is truncatable
//! at recovery; only an un-fsynced tail can be torn, which J1 makes safe
//! (no row was committed without its durable record).
//!
//! ## Durability: J1 and group commit
//!
//! [`MetadataJournal::append_batch`] serializes the records, then makes
//! them durable with a **group commit**: the first waiter becomes the
//! flusher, writes the pending buffer, calls `fsync`, and wakes every
//! waiter whose sequence is now durable. Concurrent appends share one
//! fsync (perf rule 3.4). A failed append/fsync poisons the journal for
//! further appends and returns an error to every waiter — the store
//! translates that into a **fail-closed write rejection** (the row is
//! never committed).
//!
//! ## Recovery
//!
//! Recovery scans segments in order and rebuilds the in-memory file index
//! and sparse read checkpoints (one per 256 records). A torn tail in the
//! last segment is truncated; a corrupt region followed by valid records
//! (a power-loss window) is compacted out and recorded as a **gap** floor
//! so peers that have not consumed past it are told to bootstrap instead
//! of silently skipping sequence numbers. Damage that cannot be bounded
//! this way (a corrupt non-last segment, a header mismatch) starts a
//! **new epoch** — the damaged directory is moved aside and a fresh
//! journal is created; peers observe the epoch change as a gap.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{BufMut, Bytes, BytesMut};
use oceanfs_core::{Counter, Gauge, Hlc, LabelSet, MetricRegistrar, NodeId};
use parking_lot::{Condvar, Mutex, RwLock};

use crate::error::{Error, Result};

/// Target size of one journal segment before rotation: 64 MiB
/// (ADR-0038 D2). A single batch may overshoot the target by its own
/// size; rotation happens on the next flush boundary.
pub const SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// Records between two in-memory read checkpoints. Bounds recovery memory
/// to O(records / 256) while keeping `read_range` seeks short.
const CHECKPOINT_INTERVAL: u64 = 256;

/// Maximum accepted journal key length (full store key `{bucket}\0{key}`).
/// S3 object keys are ≤ 1024 bytes and buckets ≤ 63, so 4 KiB is generous
/// while still bounding journal record memory.
pub const MAX_JOURNAL_KEY_BYTES: usize = 4096;

/// Little-endian decode helpers (fixed-size slices are guaranteed by the
/// callers' length checks; `copy_from_slice` cannot fail there).
fn le_u64(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    u64::from_le_bytes(buf)
}

fn le_u32(bytes: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes);
    u32::from_le_bytes(buf)
}

const MAGIC: &[u8; 8] = b"OFSMJRN1";
const VERSION: u32 = 1;
const MANIFEST_MAGIC: &[u8; 8] = b"OFSMMAN1";
const MANIFEST_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A 128-bit journal epoch. A metadata-pool replacement, a journal loss,
/// or detected unbounded damage creates a new epoch; peers treat an epoch
/// change as a gap and bootstrap (S3).
///
/// # Examples
///
/// ```
/// use oceanfs_storage::JournalEpoch;
///
/// let epoch = JournalEpoch::from_bytes([7u8; 16]);
/// assert_eq!(epoch.as_bytes(), &[7u8; 16]);
/// assert_eq!(epoch.to_hex().len(), 32);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JournalEpoch([u8; 16]);

impl JournalEpoch {
    /// Wraps raw epoch bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the raw 16 epoch bytes.
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(epoch.as_bytes().len(), 16);
    /// ```
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Returns the epoch as a lowercase hex string.
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(32);
        for byte in self.0 {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
        }
        out
    }

    fn random() -> Self {
        Self(rand::random::<[u8; 16]>())
    }
}

impl std::fmt::Display for JournalEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// The logical operation a journal entry records.
///
/// # Examples
///
/// ```
/// use oceanfs_storage::JournalOp;
///
/// assert_eq!(JournalOp::from_u8(JournalOp::Put.as_u8()), Some(JournalOp::Put));
/// assert_eq!(JournalOp::from_u8(JournalOp::Delete.as_u8()), Some(JournalOp::Delete));
/// assert_eq!(JournalOp::from_u8(0), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JournalOp {
    /// A live object row was applied (the entry's `hlc` is its version).
    Put,
    /// A plain tombstone was applied (the entry's `hlc` is the delete's
    /// version).
    Delete,
}

impl JournalOp {
    /// Wire/disk discriminant (1 = put, 2 = delete) per ADR-0038 D1.
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(JournalOp::Put.as_u8(), 1);
    /// ```
    pub fn as_u8(self) -> u8 {
        match self {
            JournalOp::Put => 1,
            JournalOp::Delete => 2,
        }
    }

    /// Decodes the wire/disk discriminant.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(JournalOp::Put),
            2 => Some(JournalOp::Delete),
            _ => None,
        }
    }
}

/// One metadata change record: `{seq, op, key, hlc}`, no row body.
///
/// `key` is the full store key (`{bucket}\0{key}`).
///
/// # Examples
///
/// ```ignore
/// // Produced by the journal; consumers receive owned entries:
/// let entry = JournalEntry {
///     seq: 1,
///     op: JournalOp::Put,
///     key: bytes::Bytes::from_static(b"bucket\0key"),
///     hlc: Hlc::new(1, 0),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    /// Per-node strictly increasing sequence number, assigned at append.
    pub seq: u64,
    /// The logical operation (`put` live state or `delete` tombstone).
    pub op: JournalOp,
    /// Full store key (`{bucket}\0{key}`).
    pub key: Bytes,
    /// The logical version the apply carried.
    pub hlc: Hlc,
}

/// Result of a bounded journal read.
///
/// # Examples
///
/// ```ignore
/// match journal.read_range(from_seq, 4096, 4 * 1024 * 1024)? {
///     JournalRead::Entries { entries, next_seq } => { /* apply */ }
///     JournalRead::Gap { oldest_seq } => { /* bootstrap (S3) */ }
/// }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum JournalRead {
    /// Entries after the requested position are available (possibly
    /// empty when the journal is fully consumed).
    Entries {
        /// The entries in ascending sequence order.
        entries: Vec<JournalEntry>,
        /// Exclusive resume point for the next read.
        next_seq: u64,
    },
    /// The requested position is below the replayable floor: the peer
    /// must bootstrap (S3) instead of skipping sequence numbers.
    Gap {
        /// The first replayable sequence position (exclusive floor).
        oldest_seq: u64,
    },
}

/// Result of one retention enforcement pass.
///
/// # Examples
///
/// ```ignore
/// let report = journal.enforce_retention(frontier, max_bytes, max_age)?;
/// if report.cap_forced {
///     // the frontier moved past a lagging peer: it will observe a gap
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimReport {
    /// Number of segments unlinked.
    pub segments_removed: u64,
    /// Bytes reclaimed by the unlink.
    pub bytes_removed: u64,
    /// The journal's replayable floor after the trim.
    pub base_seq: u64,
    /// True when a cap forced the floor past the slowest acked peer (the
    /// lagging peer will observe a gap).
    pub cap_forced: bool,
}

/// Journal metric handles (`oceanfs_metadata_journal_*`, ADR-0038 D9).
///
/// # Examples
///
/// ```ignore
/// let metrics = journal.metrics();
/// metrics.register(&registry);
/// ```
///
/// Registered only when the journal is enabled; a disabled node never
/// constructs a journal and exposes no series.
#[derive(Debug, Clone)]
pub struct JournalMetrics {
    /// `oceanfs_metadata_journal_appended_total` — change records
    /// appended.
    pub appended_total: Counter,
    /// `oceanfs_metadata_journal_bytes` — on-disk journal size.
    pub bytes: Gauge,
    /// `oceanfs_metadata_journal_append_errors_total` — append/fsync
    /// failures (every one fails the corresponding metadata write
    /// closed).
    pub append_errors_total: Counter,
    /// `oceanfs_metadata_journal_trim_seq` — lowest replayable sequence
    /// position (trim frontier).
    pub trim_seq: Gauge,
}

impl JournalMetrics {
    /// Creates the four journal metric handles.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let metrics = JournalMetrics::new();
    /// metrics.register(&registry);
    /// ```
    pub fn new() -> Self {
        let empty = LabelSet::empty();
        Self {
            appended_total: Counter::new(
                "oceanfs_metadata_journal_appended_total".into(),
                "Metadata change records appended to the journal".into(),
                empty.clone(),
            ),
            bytes: Gauge::new(
                "oceanfs_metadata_journal_bytes".into(),
                "On-disk metadata journal size in bytes".into(),
                empty.clone(),
            ),
            append_errors_total: Counter::new(
                "oceanfs_metadata_journal_append_errors_total".into(),
                "Metadata journal append/fsync failures (writes fail closed)".into(),
                empty.clone(),
            ),
            trim_seq: Gauge::new(
                "oceanfs_metadata_journal_trim_seq".into(),
                "Lowest replayable metadata journal sequence position".into(),
                empty,
            ),
        }
    }

    /// Registers all journal series with the given registrar.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// journal.metrics().register(&registry);
    /// ```
    pub fn register(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_counter(self.appended_total.clone());
        registrar.register_gauge(self.bytes.clone());
        registrar.register_counter(self.append_errors_total.clone());
        registrar.register_gauge(self.trim_seq.clone());
    }
}

impl Default for JournalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct FileMeta {
    file_no: u64,
    path: PathBuf,
    first_seq: Option<u64>,
    last_seq: Option<u64>,
    size: u64,
    modified: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy)]
struct Checkpoint {
    seq: u64,
    file_no: u64,
    offset: u64,
}

#[derive(Debug, Clone, Copy)]
struct Manifest {
    epoch: JournalEpoch,
    base_seq: u64,
    gap_floor_seq: u64,
    active_file: u64,
    generation: u64,
}

struct JournalInner {
    /// Open append handle to the active segment.
    active: File,
    active_file_no: u64,
    /// Size in bytes of the active segment on disk.
    active_size: u64,
    /// Serialized records not yet written to disk.
    pending: BytesMut,
    /// Number of records in `pending`.
    pending_count: u64,
    /// Last sequence covered by a completed fsync.
    synced_seq: u64,
    /// Next sequence to assign.
    next_seq: u64,
    /// True while one waiter is flushing; others wait on `flushed`.
    flushing: bool,
    /// Sticky failure: once an append/fsync fails, later appends fail.
    failed: Option<String>,
}

struct RecoveryOutcome {
    files: Vec<FileMeta>,
    checkpoints: Vec<Checkpoint>,
    last_seq: u64,
    epoch: JournalEpoch,
    base_seq: u64,
    gap_floor_seq: u64,
    new_epoch: bool,
}

/// Append-only, co-durable metadata change journal.
///
/// See the [module documentation](self) for the storage layout and
/// durability model.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_core::{Hlc, NodeId};
/// use oceanfs_storage::{JournalOp, MetadataJournal};
///
/// let journal = MetadataJournal::open(std::path::Path::new("/tmp/j"), &NodeId::new("n1"))?;
/// let seq = journal.append(JournalOp::Put, b"bucket\0key", Hlc::new(1, 0))?;
/// # Ok::<(), oceanfs_storage::Error>(())
/// ```
pub struct MetadataJournal {
    dir: PathBuf,
    node_id: NodeId,
    epoch: JournalEpoch,
    segment_target: u64,
    inner: Mutex<JournalInner>,
    /// Wakes group-commit waiters after a flush or a failure.
    flushed: Condvar,
    /// File index (ascending file number), rebuilt at open.
    files: RwLock<Vec<FileMeta>>,
    /// Sparse read checkpoints (ascending seq).
    checkpoints: RwLock<Vec<Checkpoint>>,
    /// Guards readers against whole-segment deletion (`read` for readers,
    /// `write` for trim). LOCK ORDER: retention → inner (a trimmer takes
    /// `retention.write` then `files.write`, never `inner`).
    retention: RwLock<()>,
    /// Highest sequence below which entries are not replayable because of
    /// a power-loss hole (0 = none).
    gap_floor: AtomicU64,
    /// Manifest generation counter.
    generation: AtomicU64,
    metrics: Arc<JournalMetrics>,
}

impl std::fmt::Debug for MetadataJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataJournal")
            .field("dir", &self.dir)
            .field("node_id", &self.node_id)
            .field("epoch", &self.epoch)
            .field("last_seq", &self.last_seq())
            .finish_non_exhaustive()
    }
}

impl MetadataJournal {
    /// Opens (or creates/reuses) the journal at `dir`.
    ///
    /// Creates the directory, recovers any existing segments (torn-tail
    /// truncation, power-loss hole compaction), builds the in-memory
    /// index/checkpoints, and ensures an active segment exists and is
    /// fsynced. Automatically starts a new epoch when the existing
    /// journal is damaged beyond bounded recovery.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created, a segment
    /// cannot be read/written, or the file set is structurally invalid.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use oceanfs_core::NodeId;
    /// use oceanfs_storage::MetadataJournal;
    ///
    /// let journal = MetadataJournal::open(
    ///     std::path::Path::new("/var/lib/oceanfs/metadata/metadata-journal"),
    ///     &NodeId::new("node-1"),
    /// )?;
    /// # Ok::<(), oceanfs_storage::Error>(())
    /// ```
    pub fn open(dir: &Path, node_id: &NodeId) -> Result<Self> {
        Self::open_inner(dir, node_id, SEGMENT_TARGET_BYTES)
    }

    fn open_inner(dir: &Path, node_id: &NodeId, segment_target: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let _ = std::fs::remove_file(dir.join("manifest.tmp"));

        let mut file_numbers = list_segment_numbers(dir)?;
        file_numbers.sort_unstable();

        let mut recovered = recover(dir, &file_numbers)?;
        if recovered.new_epoch {
            quarantine_damaged_dir(dir)?;
            std::fs::create_dir_all(dir)?;
            recovered.files.clear();
            recovered.checkpoints.clear();
            recovered.last_seq = 0;
            recovered.base_seq = 0;
            recovered.gap_floor_seq = 0;
        }
        let RecoveryOutcome {
            mut files,
            checkpoints,
            last_seq,
            epoch,
            base_seq,
            gap_floor_seq,
            ..
        } = recovered;

        // Ensure an active segment exists and is open for append.
        let active_file_no = files.last().map(|f| f.file_no).unwrap_or(1);
        let active_path = segment_path(dir, active_file_no);
        let first_seq = files.last().and_then(|f| f.first_seq).unwrap_or(last_seq + 1);
        ensure_segment(&active_path, node_id, epoch, first_seq)?;
        let active_size = std::fs::metadata(&active_path)?.len();
        let modified = std::fs::metadata(&active_path)?.modified().ok();
        if let Some(last) = files.last_mut() {
            if last.file_no == active_file_no {
                last.size = active_size;
                last.modified = modified;
                last.first_seq = last.first_seq.or(Some(first_seq));
            } else {
                files.push(FileMeta {
                    file_no: active_file_no,
                    path: active_path.clone(),
                    first_seq: Some(first_seq),
                    last_seq: None,
                    size: active_size,
                    modified,
                });
            }
        } else {
            files.push(FileMeta {
                file_no: active_file_no,
                path: active_path.clone(),
                first_seq: Some(first_seq),
                last_seq: None,
                size: active_size,
                modified,
            });
        }

        let total_bytes: u64 = files.iter().map(|f| f.size).sum();
        let journal = Self {
            dir: dir.to_path_buf(),
            node_id: node_id.clone(),
            epoch,
            segment_target,
            inner: Mutex::new(JournalInner {
                active: OpenOptions::new().append(true).read(true).open(&active_path)?,
                active_file_no,
                active_size,
                pending: BytesMut::new(),
                pending_count: 0,
                synced_seq: last_seq,
                next_seq: last_seq + 1,
                flushing: false,
                failed: None,
            }),
            flushed: Condvar::new(),
            files: RwLock::new(files),
            checkpoints: RwLock::new(checkpoints),
            retention: RwLock::new(()),
            gap_floor: AtomicU64::new(gap_floor_seq),
            generation: AtomicU64::new(1),
            metrics: Arc::new(JournalMetrics::new()),
        };
        journal.metrics.bytes.set(total_bytes);
        journal.metrics.trim_seq.set(journal.oldest_seq());
        journal.write_manifest_now(base_seq.max(journal.oldest_seq()), active_file_no)?;
        Ok(journal)
    }

    /// This journal's epoch (freshly minted when the previous journal was
    /// absent or damaged).
    ///
    /// # Examples
    ///
    /// ```
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let journal = oceanfs_storage::MetadataJournal::open(dir.path(), &oceanfs_core::NodeId::new("n")).unwrap();
    /// assert_eq!(journal.epoch().as_bytes().len(), 16);
    /// ```
    pub fn epoch(&self) -> JournalEpoch {
        self.epoch
    }

    /// The replayable floor: sequence position below which the journal
    /// cannot serve entries. A peer at or below this must bootstrap.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if peer_from_seq < journal.oldest_seq() {
    ///     // gap: the peer must bootstrap (S3)
    /// }
    /// ```
    pub fn oldest_seq(&self) -> u64 {
        let files = self.files.read();
        let physical_floor = files
            .first()
            .and_then(|f| f.first_seq)
            .map(|first| first.saturating_sub(1))
            .unwrap_or(0);
        drop(files);
        physical_floor.max(self.gap_floor.load(Ordering::Relaxed))
    }

    /// Highest sequence assigned so far (0 when empty).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(journal.last_seq(), 0); // freshly opened
    /// ```
    pub fn last_seq(&self) -> u64 {
        self.inner.lock().next_seq - 1
    }

    /// Total on-disk journal size in bytes.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert!(journal.on_disk_bytes() < 256 * 1024 * 1024);
    /// ```
    pub fn on_disk_bytes(&self) -> u64 {
        self.files.read().iter().map(|f| f.size).sum()
    }

    /// Returns the metric handles (registered only when enabled).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// journal.metrics().register(&registry);
    /// ```
    pub fn metrics(&self) -> &Arc<JournalMetrics> {
        &self.metrics
    }

    /// Test-only fault injection: poisons the journal so every append
    /// fails closed (J1 tests).
    #[cfg(test)]
    pub(crate) fn poison_for_test(&self, reason: &str) {
        self.inner.lock().failed = Some(reason.to_string());
    }

    /// Appends a single entry and makes it durable (group-committed).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let seq = journal.append(JournalOp::Put, b"bucket\0key", Hlc::new(1, 0))?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the key exceeds [`MAX_JOURNAL_KEY_BYTES`] or
    /// the append/fsync fails (the journal then fails every later append
    /// as well).
    pub fn append(&self, op: JournalOp, key: &[u8], hlc: Hlc) -> Result<u64> {
        let seqs = self.append_batch(&[(op, key, hlc)])?;
        seqs.into_iter().next().ok_or_else(|| Error::Journal("empty append batch".into()))
    }

    /// Appends a batch of entries and makes them durable with one group
    /// commit.
    ///
    /// Returns the assigned sequence numbers in input order. The call
    /// returns only after every returned sequence is durable (fsynced).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let seqs = journal.append_batch(&[
    ///     (JournalOp::Put, b"bucket\0a", Hlc::new(1, 0)),
    ///     (JournalOp::Delete, b"bucket\0b", Hlc::new(2, 0)),
    /// ])?;
    /// assert_eq!(seqs.len(), 2);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when any key exceeds [`MAX_JOURNAL_KEY_BYTES`] or
    /// the append/fsync fails. The batch is all-or-nothing from the
    /// caller's perspective: the store fails the metadata write closed.
    pub fn append_batch(&self, items: &[(JournalOp, &[u8], Hlc)]) -> Result<Vec<u64>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        for (_, key, _) in items {
            if key.len() > MAX_JOURNAL_KEY_BYTES {
                return Err(Error::Journal(format!(
                    "journal key of {} bytes exceeds the {} byte limit",
                    key.len(),
                    MAX_JOURNAL_KEY_BYTES
                )));
            }
        }

        let mut inner = self.inner.lock();
        if let Some(failure) = &inner.failed {
            self.metrics.append_errors_total.inc();
            return Err(Error::Journal(format!("journal is failed: {failure}")));
        }

        let mut seqs = Vec::with_capacity(items.len());
        for (op, key, hlc) in items {
            let seq = inner.next_seq;
            encode_record_into(&mut inner.pending, seq, *op, key, *hlc);
            inner.next_seq += 1;
            inner.pending_count += 1;
            seqs.push(seq);
        }
        let Some(target) = seqs.last().copied() else { return Ok(Vec::new()) };

        loop {
            if inner.synced_seq >= target {
                self.metrics.appended_total.add(seqs.len() as u64);
                return Ok(seqs);
            }
            if let Some(failure) = inner.failed.clone() {
                self.metrics.append_errors_total.inc();
                return Err(Error::Journal(format!("journal is failed: {failure}")));
            }
            if inner.flushing {
                self.flushed.wait(&mut inner);
                continue;
            }

            // Become the flusher: take the pending buffer and write it.
            inner.flushing = true;
            let buf = std::mem::take(&mut inner.pending);
            let flush_last_seq = inner.next_seq - 1;
            inner.pending_count = 0;
            let file = inner.active.try_clone()?;
            let base_size = inner.active_size;
            drop(inner);

            let write_result = (|| -> std::io::Result<()> {
                let mut file = file;
                file.write_all(&buf)?;
                file.sync_data()?;
                Ok(())
            })();

            inner = self.inner.lock();
            inner.flushing = false;

            match write_result {
                Ok(()) => {
                    inner.active_size = base_size + buf.len() as u64;
                    inner.synced_seq = inner.synced_seq.max(flush_last_seq);
                    if !buf.is_empty() {
                        let mut files = self.files.write();
                        if let Some(meta) =
                            files.iter_mut().find(|f| f.file_no == inner.active_file_no)
                        {
                            meta.size = inner.active_size;
                            meta.last_seq = Some(
                                meta.last_seq.map_or(flush_last_seq, |s| s.max(flush_last_seq)),
                            );
                            meta.modified = Some(SystemTime::now());
                        }
                    }
                    self.metrics.bytes.set(self.on_disk_bytes());
                    self.flushed.notify_all();
                }
                Err(error) => {
                    let message = error.to_string();
                    inner.failed = Some(message);
                    self.metrics.append_errors_total.inc();
                    self.flushed.notify_all();
                    return Err(Error::Io(error));
                }
            }

            // Rotate if the active segment reached its target. Rotation is
            // performed by the flusher (the single writer) after the fsync.
            if inner.active_size >= self.segment_target {
                if let Err(error) = self.rotate_locked(&mut inner) {
                    inner.failed = Some(error.to_string());
                    self.metrics.append_errors_total.inc();
                    self.flushed.notify_all();
                    return Err(error);
                }
            }
        }
    }

    /// Reads up to `max_entries` records / `max_bytes` encoded bytes
    /// strictly after `from_seq`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// match journal.read_range(from_seq, 4096, 4 * 1024 * 1024)? {
    ///     JournalRead::Entries { entries, next_seq } => { /* apply */ }
    ///     JournalRead::Gap { oldest_seq } => { /* bootstrap (S3) */ }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when a segment cannot be opened or read, or when
    /// a record outside the tolerated torn tail is structurally invalid.
    pub fn read_range(
        &self,
        from_seq: u64,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<JournalRead> {
        let _retention = self.retention.read();
        let oldest = self.oldest_seq();
        if from_seq < oldest {
            return Ok(JournalRead::Gap { oldest_seq: oldest });
        }

        let files_snapshot: Vec<(u64, PathBuf)> = {
            let files = self.files.read();
            files
                .iter()
                .filter(|f| f.last_seq.map(|s| s > from_seq).unwrap_or(false))
                .map(|f| (f.file_no, f.path.clone()))
                .collect()
        };

        if files_snapshot.is_empty() {
            return Ok(JournalRead::Entries { entries: Vec::new(), next_seq: from_seq });
        }

        let mut entries: Vec<JournalEntry> = Vec::with_capacity(max_entries.min(1024));
        let mut encoded_bytes: u64 = 0;
        let mut next_seq = from_seq;

        'files: for (file_no, path) in files_snapshot {
            let mut file = File::open(&path)?;
            let header_len = segment_header_len(&path, self.epoch)?;
            let start_offset = self.checkpoint_offset(file_no, from_seq, header_len);
            file.seek(SeekFrom::Start(start_offset))?;
            // Read at most the requested byte budget plus one maximum
            // record: `read_range` never slurps a whole 64 MiB segment for
            // a small pull, and an incomplete trailing record simply ends
            // the scan (the next pull resumes at `next_seq`).
            let remaining = file.metadata()?.len().saturating_sub(start_offset);
            let limit = max_bytes.saturating_add(MAX_JOURNAL_KEY_BYTES as u64 + 128).min(remaining);
            let mut buf = Vec::new();
            file.take(limit).read_to_end(&mut buf)?;
            let mut cursor = 0usize;
            while cursor < buf.len() {
                match decode_record(&buf[cursor..]) {
                    Ok(Some((entry, consumed))) => {
                        cursor += consumed;
                        if entry.seq <= from_seq {
                            continue;
                        }
                        encoded_bytes += consumed as u64;
                        next_seq = entry.seq;
                        entries.push(entry);
                        if entries.len() >= max_entries || encoded_bytes >= max_bytes {
                            break 'files;
                        }
                    }
                    // Torn tail: nothing further is durably framed.
                    Ok(None) => break 'files,
                    Err(detail) => {
                        return Err(Error::Journal(format!(
                            "corrupt journal record in segment {file_no}: {detail}"
                        )))
                    }
                }
            }
        }

        Ok(JournalRead::Entries { entries, next_seq })
    }

    fn checkpoint_offset(&self, file_no: u64, from_seq: u64, header_len: u64) -> u64 {
        let checkpoints = self.checkpoints.read();
        checkpoints
            .iter()
            .rev()
            .find(|cp| cp.file_no == file_no && cp.seq <= from_seq)
            .map(|cp| cp.offset)
            .unwrap_or(header_len)
    }

    /// Trims whole segments below `frontier_seq`, then applies the hard
    /// byte/age caps (which may override the frontier and open a gap for
    /// the lagging peer).
    ///
    /// The active segment is never removed. Returns what was trimmed.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let report = journal.enforce_retention(frontier, max_bytes, max_age)?;
    /// if report.cap_forced { /* the lagging peer will observe a gap */ }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when a segment cannot be stat'ed, unlinked, or
    /// the manifest/directory fsync fails.
    pub fn enforce_retention(
        &self,
        frontier_seq: u64,
        max_bytes: u64,
        max_age: Duration,
    ) -> Result<TrimReport> {
        let _retention = self.retention.write();
        let active_file_no = self.inner.lock().active_file_no;
        let now = SystemTime::now();
        let mut removed = 0u64;
        let mut removed_bytes = 0u64;
        let mut cap_forced = false;

        loop {
            let victim = {
                let files = self.files.read();
                let total: u64 = files.iter().map(|f| f.size).sum();
                let trimmable =
                    files.iter().filter(|f| f.file_no != active_file_no && f.last_seq.is_some());
                // Priority 1: fully acked files (frontier-driven trim).
                let acked = trimmable
                    .clone()
                    .find(|f| f.last_seq.map(|s| s <= frontier_seq).unwrap_or(false));
                if let Some(victim) = acked {
                    Some((victim.clone(), false))
                } else if total > max_bytes {
                    // Priority 2: byte cap overrides the frontier.
                    files
                        .iter()
                        .filter(|f| f.file_no != active_file_no && f.last_seq.is_some())
                        .min_by_key(|f| f.file_no)
                        .map(|f| (f.clone(), true))
                } else {
                    // Priority 3: age cap overrides the frontier.
                    files
                        .iter()
                        .filter(|f| f.file_no != active_file_no && f.last_seq.is_some())
                        .find(|f| {
                            f.modified
                                .and_then(|m| now.duration_since(m).ok())
                                .map(|age| age > max_age)
                                .unwrap_or(false)
                        })
                        .map(|f| (f.clone(), true))
                }
            };

            let Some((victim, forced)) = victim else { break };
            match std::fs::remove_file(&victim.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::Io(error)),
            }
            cap_forced |= forced && victim.last_seq.map(|s| s > frontier_seq).unwrap_or(false);
            removed += 1;
            removed_bytes += victim.size;
            let mut files = self.files.write();
            files.retain(|f| f.file_no != victim.file_no);
            {
                let surviving: Vec<u64> = files.iter().map(|f| f.file_no).collect();
                let mut checkpoints = self.checkpoints.write();
                checkpoints.retain(|cp| surviving.contains(&cp.file_no));
            }
        }

        if removed > 0 {
            fsync_dir(&self.dir)?;
        }

        let base_seq = self.oldest_seq();
        self.write_manifest_now(base_seq, active_file_no)?;
        self.metrics.bytes.set(self.on_disk_bytes());
        self.metrics.trim_seq.set(base_seq);

        Ok(TrimReport {
            segments_removed: removed,
            bytes_removed: removed_bytes,
            base_seq,
            cap_forced,
        })
    }

    // -- internal helpers -------------------------------------------------

    fn rotate_locked(&self, inner: &mut JournalInner) -> Result<()> {
        let new_no = inner.active_file_no + 1;
        let path = segment_path(&self.dir, new_no);
        let first_seq = if inner.pending_count == 0 {
            inner.next_seq
        } else {
            inner.next_seq - inner.pending_count
        };
        let mut header = Vec::with_capacity(64);
        encode_segment_header(&mut header, &self.node_id, self.epoch, first_seq);
        {
            let mut file =
                OpenOptions::new().create(true).write(true).truncate(true).open(&path)?;
            file.write_all(&header)?;
            file.sync_all()?;
        }
        inner.active = OpenOptions::new().append(true).read(true).open(&path)?;
        inner.active_file_no = new_no;
        inner.active_size = header.len() as u64;
        {
            let mut files = self.files.write();
            files.retain(|f| f.file_no != new_no);
            files.push(FileMeta {
                file_no: new_no,
                path,
                first_seq: Some(first_seq),
                last_seq: None,
                size: header.len() as u64,
                modified: Some(SystemTime::now()),
            });
        }
        {
            let mut cps = self.checkpoints.write();
            cps.retain(|cp| cp.file_no != new_no);
            cps.push(Checkpoint { seq: first_seq, file_no: new_no, offset: header.len() as u64 });
            cps.sort_by_key(|cp| cp.seq);
        }
        self.write_manifest_now(self.oldest_seq(), new_no)?;
        self.metrics.bytes.set(self.on_disk_bytes());
        Ok(())
    }

    /// Writes the manifest with an explicit active file number. Callers
    /// must already know the active file (never locks `inner`: the flush
    /// path calls this while holding the inner guard).
    fn write_manifest_now(&self, base_seq: u64, active_file: u64) -> Result<()> {
        let manifest = Manifest {
            epoch: self.epoch,
            base_seq,
            gap_floor_seq: self.gap_floor.load(Ordering::Relaxed),
            active_file,
            generation: self.generation.fetch_add(1, Ordering::Relaxed) + 1,
        };
        write_manifest(&self.dir, &manifest)
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn encode_record_into(out: &mut BytesMut, seq: u64, op: JournalOp, key: &[u8], hlc: Hlc) {
    let payload_len = 8 + 1 + 4 + key.len() + 8 + 4;
    out.reserve(4 + payload_len + 4);
    out.put_u32_le(payload_len as u32);
    let payload_start = out.len();
    out.put_u64_le(seq);
    out.put_u8(op.as_u8());
    out.put_u32_le(key.len() as u32);
    out.extend_from_slice(key);
    out.put_u64_le(hlc.wall_time());
    out.put_u32_le(hlc.logical());
    let crc = crc32fast::hash(&out[payload_start..]);
    out.put_u32_le(crc);
}

/// Decodes one record. Returns `Ok(None)` when the buffer is an incomplete
/// tail, `Err` for a structurally invalid/checksum-failed record.
fn decode_record(buf: &[u8]) -> std::result::Result<Option<(JournalEntry, usize)>, String> {
    if buf.len() < 8 {
        return Ok(None);
    }
    let payload_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let total = 4 + payload_len + 4;
    if buf.len() < total {
        return Ok(None);
    }
    let payload = &buf[4..4 + payload_len];
    let stored_crc = u32::from_le_bytes([
        buf[4 + payload_len],
        buf[5 + payload_len],
        buf[6 + payload_len],
        buf[7 + payload_len],
    ]);
    if crc32fast::hash(payload) != stored_crc {
        return Err("record checksum mismatch".into());
    }
    if payload.len() < 17 {
        return Err("record payload too short".into());
    }
    let seq = le_u64(&payload[0..8]);
    let op = JournalOp::from_u8(payload[8]).ok_or_else(|| "invalid op".to_string())?;
    let key_len = le_u32(&payload[9..13]) as usize;
    if key_len > MAX_JOURNAL_KEY_BYTES {
        return Err(format!("key length {key_len} exceeds limit"));
    }
    if payload.len() < 13 + key_len + 12 {
        return Err("record payload truncated".into());
    }
    let key = Bytes::copy_from_slice(&payload[13..13 + key_len]);
    let offset = 13 + key_len;
    let wall_time = le_u64(&payload[offset..offset + 8]);
    let logical = le_u32(&payload[offset + 8..offset + 12]);
    Ok(Some((JournalEntry { seq, op, key, hlc: Hlc::new(wall_time, logical) }, total)))
}

// ---------------------------------------------------------------------------
// Segment files & manifest
// ---------------------------------------------------------------------------

fn segment_path(dir: &Path, file_no: u64) -> PathBuf {
    dir.join(format!("{file_no:020}.jrn"))
}

fn list_segment_numbers(dir: &Path) -> Result<Vec<u64>> {
    let mut numbers = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(numbers),
        Err(error) => return Err(Error::Io(error)),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".jrn") {
            if let Ok(no) = stem.parse::<u64>() {
                numbers.push(no);
            }
        }
    }
    Ok(numbers)
}

fn encode_segment_header(out: &mut Vec<u8>, node_id: &NodeId, epoch: JournalEpoch, first_seq: u64) {
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    let id = node_id.as_str().as_bytes();
    out.extend_from_slice(&(id.len() as u16).to_le_bytes());
    out.extend_from_slice(id);
    out.extend_from_slice(epoch.as_bytes());
    out.extend_from_slice(&first_seq.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // reserved
    let crc = crc32fast::hash(out);
    out.extend_from_slice(&crc.to_le_bytes());
}

fn write_segment_header(
    path: &Path,
    node_id: &NodeId,
    epoch: JournalEpoch,
    first_seq: u64,
) -> Result<()> {
    let mut bytes = Vec::with_capacity(64);
    encode_segment_header(&mut bytes, node_id, epoch, first_seq);
    let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Ensures `path` exists with a non-empty segment header.
fn ensure_segment(
    path: &Path,
    node_id: &NodeId,
    epoch: JournalEpoch,
    first_seq: u64,
) -> Result<()> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.len() > 0 => Ok(()),
        _ => write_segment_header(path, node_id, epoch, first_seq),
    }
}

struct ParsedHeader {
    epoch: JournalEpoch,
    total_len: u64,
}

fn parse_segment_header(buf: &[u8]) -> Result<ParsedHeader> {
    if buf.len() < 8 + 4 + 2 || &buf[0..8] != MAGIC {
        return Err(Error::Journal("segment header magic mismatch".into()));
    }
    let version = le_u32(&buf[8..12]);
    if version != VERSION {
        return Err(Error::Journal(format!("unsupported segment version {version}")));
    }
    let id_len = u16::from_le_bytes([buf[12], buf[13]]) as usize;
    let fixed = 8 + 4 + 2 + id_len + 16 + 8 + 8 + 4;
    if buf.len() < fixed {
        return Err(Error::Journal("segment header truncated".into()));
    }
    let stored_crc = le_u32(&buf[fixed - 4..fixed]);
    if crc32fast::hash(&buf[..fixed - 4]) != stored_crc {
        return Err(Error::Journal("segment header checksum mismatch".into()));
    }
    let mut epoch = [0u8; 16];
    epoch.copy_from_slice(&buf[14 + id_len..30 + id_len]);
    Ok(ParsedHeader { epoch: JournalEpoch(epoch), total_len: fixed as u64 })
}

fn segment_header_len(path: &Path, epoch: JournalEpoch) -> Result<u64> {
    // Read only the fixed header (never the whole segment): the node id
    // length in the first 14 bytes determines the exact header size.
    let mut file = File::open(path)?;
    let mut prefix = [0u8; 14];
    file.read_exact(&mut prefix)?;
    let id_len = u16::from_le_bytes([prefix[12], prefix[13]]) as usize;
    let fixed = 8 + 4 + 2 + id_len + 16 + 8 + 8 + 4;
    let mut buf = vec![0u8; fixed];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;
    let header = parse_segment_header(&buf)?;
    if header.epoch != epoch {
        return Err(Error::Journal(format!(
            "segment {} carries epoch {} but the journal epoch is {}",
            path.display(),
            header.epoch,
            epoch
        )));
    }
    Ok(header.total_len)
}

fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(MANIFEST_MAGIC);
    bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    bytes.extend_from_slice(&manifest.generation.to_le_bytes());
    bytes.extend_from_slice(manifest.epoch.as_bytes());
    bytes.extend_from_slice(&manifest.base_seq.to_le_bytes());
    bytes.extend_from_slice(&manifest.gap_floor_seq.to_le_bytes());
    bytes.extend_from_slice(&manifest.active_file.to_le_bytes());
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());

    let tmp = dir.join("manifest.tmp");
    {
        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join("manifest"))?;
    fsync_dir(dir)?;
    Ok(())
}

fn read_manifest(dir: &Path) -> Result<Option<Manifest>> {
    let path = dir.join("manifest");
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::Io(error)),
    };
    if bytes.len() < 8 + 4 + 8 + 16 + 8 + 8 + 8 + 4 || &bytes[0..8] != MANIFEST_MAGIC {
        return Ok(None);
    }
    let stored_crc = le_u32(&bytes[bytes.len() - 4..]);
    if crc32fast::hash(&bytes[..bytes.len() - 4]) != stored_crc {
        return Ok(None);
    }
    let generation = le_u64(&bytes[12..20]);
    let mut epoch = [0u8; 16];
    epoch.copy_from_slice(&bytes[20..36]);
    let base_seq = le_u64(&bytes[36..44]);
    let gap_floor_seq = le_u64(&bytes[44..52]);
    let active_file = le_u64(&bytes[52..60]);
    Ok(Some(Manifest {
        epoch: JournalEpoch(epoch),
        base_seq,
        gap_floor_seq,
        active_file,
        generation,
    }))
}

fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

fn quarantine_damaged_dir(dir: &Path) -> Result<()> {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let mut target = dir.with_extension(format!("corrupt-{stamp}"));
    let mut counter = 0u32;
    while target.exists() {
        counter += 1;
        target = dir.with_extension(format!("corrupt-{stamp}-{counter}"));
    }
    std::fs::rename(dir, &target)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

fn recover(dir: &Path, file_numbers: &[u64]) -> Result<RecoveryOutcome> {
    if file_numbers.is_empty() {
        return Ok(RecoveryOutcome {
            files: Vec::new(),
            checkpoints: Vec::new(),
            last_seq: 0,
            epoch: JournalEpoch::random(),
            base_seq: 0,
            gap_floor_seq: 0,
            new_epoch: false,
        });
    }
    let manifest = read_manifest(dir)?;
    let mut epoch = manifest.as_ref().map(|m| m.epoch);
    let mut gap_floor_seq = manifest.as_ref().map(|m| m.gap_floor_seq).unwrap_or(0);
    let mut files: Vec<FileMeta> = Vec::with_capacity(file_numbers.len());
    let mut checkpoints: Vec<Checkpoint> = Vec::new();
    let mut last_seq: u64 = 0;
    let mut records_seen: u64 = 0;
    let mut previous_last: Option<u64> = None;
    let initial_gap_floor = gap_floor_seq;

    for (index, file_no) in file_numbers.iter().enumerate() {
        let is_last = index + 1 == file_numbers.len();
        let path = segment_path(dir, *file_no);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(Error::Io(error)),
        };

        let header = match parse_segment_header(&bytes) {
            Ok(header) => header,
            Err(error) => {
                if is_last {
                    tracing::warn!(path = %path.display(), %error, "metadata journal: dropping invalid active segment");
                    let _ = std::fs::remove_file(&path);
                    continue;
                }
                return Ok(new_epoch_outcome("segment header damage"));
            }
        };
        match epoch {
            Some(existing) if existing != header.epoch => {
                return Ok(new_epoch_outcome("mixed segment epochs"));
            }
            None => epoch = Some(header.epoch),
            _ => {}
        }

        let mut first_in_file: Option<u64> = None;
        let mut last_in_file: Option<u64> = None;
        let mut size = bytes.len() as u64;
        let mut cursor = header.total_len as usize;

        while cursor < bytes.len() {
            match decode_record(&bytes[cursor..]) {
                Ok(Some((entry, consumed))) => {
                    if let Some(prev) = last_in_file {
                        if entry.seq <= prev {
                            return Err(Error::Journal(format!(
                                "non-monotonic journal sequence in {}",
                                path.display()
                            )));
                        }
                        if entry.seq > prev + 1 {
                            // A sequence jump is a power-loss hole: the
                            // records between were never durably framed
                            // (or were compacted in a prior recovery that
                            // crashed before persisting the floor).
                            gap_floor_seq = gap_floor_seq.max(entry.seq - 1);
                        }
                    }
                    if first_in_file.is_none() {
                        if let Some(prev) = previous_last {
                            if entry.seq != prev + 1 {
                                return Ok(new_epoch_outcome("sequence discontinuity"));
                            }
                        }
                        first_in_file = Some(entry.seq);
                    }
                    last_in_file = Some(entry.seq);
                    records_seen += 1;
                    if records_seen % CHECKPOINT_INTERVAL == 0 {
                        checkpoints.push(Checkpoint {
                            seq: entry.seq,
                            file_no: *file_no,
                            offset: cursor as u64,
                        });
                    }
                    cursor += consumed;
                }
                other => {
                    let detail = match other {
                        Ok(None) => None,
                        Err(detail) => Some(detail),
                        Ok(Some(_)) => unreachable!("handled above"),
                    };
                    if !is_last {
                        return Ok(new_epoch_outcome("bad record in a non-active segment"));
                    }
                    match find_resync(&bytes, cursor, last_in_file) {
                        Some((offset, resumed_seq)) => {
                            let hole_end = resumed_seq - 1;
                            gap_floor_seq = gap_floor_seq.max(hole_end);
                            tracing::warn!(
                                path = %path.display(),
                                from = cursor,
                                to = offset,
                                gap_through = hole_end,
                                "metadata journal: power-loss hole compacted"
                            );
                            let mut compacted = Vec::with_capacity(bytes.len() - (offset - cursor));
                            compacted.extend_from_slice(&bytes[..cursor]);
                            compacted.extend_from_slice(&bytes[offset..]);
                            std::fs::write(&path, &compacted)?;
                            let file = OpenOptions::new().write(true).open(&path)?;
                            file.sync_all()?;
                            size = compacted.len() as u64;
                            // Re-scan the compacted suffix.
                            let mut scan = cursor;
                            while scan < compacted.len() {
                                match decode_record(&compacted[scan..]) {
                                    Ok(Some((entry, consumed))) => {
                                        if first_in_file.is_none() {
                                            first_in_file = Some(entry.seq);
                                        }
                                        last_in_file = Some(entry.seq);
                                        records_seen += 1;
                                        if records_seen % CHECKPOINT_INTERVAL == 0 {
                                            checkpoints.push(Checkpoint {
                                                seq: entry.seq,
                                                file_no: *file_no,
                                                offset: scan as u64,
                                            });
                                        }
                                        scan += consumed;
                                    }
                                    _ => break,
                                }
                            }
                        }
                        None => {
                            tracing::warn!(
                                path = %path.display(),
                                offset = cursor,
                                detail = detail.as_deref().unwrap_or("incomplete frame"),
                                "metadata journal: truncating tail (no valid data follows)"
                            );
                            let file = OpenOptions::new().write(true).open(&path)?;
                            file.set_len(cursor as u64)?;
                            file.sync_all()?;
                            size = cursor as u64;
                        }
                    }
                    break;
                }
            }
        }

        if let Some(last) = last_in_file {
            last_seq = last;
            previous_last = Some(last);
        }
        let modified = std::fs::metadata(&path).ok().and_then(|m| m.modified().ok());
        files.push(FileMeta {
            file_no: *file_no,
            path,
            first_seq: first_in_file,
            last_seq: last_in_file,
            size,
            modified,
        });
    }

    if files.is_empty() {
        return Ok(RecoveryOutcome {
            files,
            checkpoints,
            last_seq: 0,
            epoch: epoch.unwrap_or_else(JournalEpoch::random),
            base_seq: 0,
            gap_floor_seq: 0,
            new_epoch: false,
        });
    }

    let base_seq = if let Some(manifest) = &manifest {
        manifest.base_seq
    } else {
        files.first().and_then(|f| f.first_seq).map(|s| s - 1).unwrap_or(0)
    };

    // Persist a newly detected gap floor immediately (before the journal
    // starts serving): a crash between the compaction and the manifest
    // write would otherwise lose the marker — the jump re-detection above
    // makes this idempotent on the next open.
    if gap_floor_seq > initial_gap_floor {
        if let Some(epoch) = epoch {
            let active_file = *file_numbers.last().unwrap_or(&1);
            let generation = manifest.as_ref().map(|m| m.generation + 1).unwrap_or(1);
            if let Err(error) = write_manifest(
                dir,
                &Manifest { epoch, base_seq, gap_floor_seq, active_file, generation },
            ) {
                tracing::warn!(%error, "metadata journal: failed to persist the gap floor");
            }
        }
    }

    Ok(RecoveryOutcome {
        files,
        checkpoints,
        last_seq,
        epoch: epoch.unwrap_or_else(JournalEpoch::random),
        base_seq,
        gap_floor_seq,
        new_epoch: false,
    })
}

fn new_epoch_outcome(_reason: &str) -> RecoveryOutcome {
    RecoveryOutcome {
        files: Vec::new(),
        checkpoints: Vec::new(),
        last_seq: 0,
        epoch: JournalEpoch::random(),
        base_seq: 0,
        gap_floor_seq: 0,
        new_epoch: true,
    }
}

/// Searches for the next valid record frame after a corrupt region whose
/// sequence is greater than the last good sequence. Bounded to the rest
/// of the segment.
fn find_resync(bytes: &[u8], from: usize, last_seq_in_file: Option<u64>) -> Option<(usize, u64)> {
    let mut offset = from.saturating_add(1);
    while offset + 8 <= bytes.len() {
        if let Ok(Some((entry, _))) = decode_record(&bytes[offset..]) {
            if last_seq_in_file.map(|s| entry.seq > s).unwrap_or(true) {
                return Some((offset, entry.seq));
            }
        }
        offset += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeId {
        NodeId::new("node-1")
    }

    fn open_in(dir: &Path) -> MetadataJournal {
        MetadataJournal::open(dir, &node()).expect("journal opens")
    }

    fn open_tiny(dir: &Path, target: u64) -> MetadataJournal {
        MetadataJournal::open_inner(dir, &node(), target).expect("journal opens")
    }

    fn key(n: u64) -> Vec<u8> {
        format!("bucket\0key-{n}").into_bytes()
    }

    fn open_read(journal: &MetadataJournal, from: u64) -> (Vec<JournalEntry>, u64) {
        match journal.read_range(from, 1000, u64::MAX).expect("read succeeds") {
            JournalRead::Entries { entries, next_seq } => (entries, next_seq),
            JournalRead::Gap { oldest_seq } => panic!("unexpected gap at {oldest_seq}"),
        }
    }

    #[test]
    fn open_creates_epoch_manifest_and_active_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = open_in(dir.path());
        assert_eq!(journal.last_seq(), 0);
        assert_eq!(journal.oldest_seq(), 0);
        assert!(dir.path().join("manifest").exists());
        assert!(dir.path().join("00000000000000000001.jrn").exists());
        assert!(journal.on_disk_bytes() > 0, "header bytes are counted");
    }

    #[test]
    fn append_assigns_strictly_increasing_sequences() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = open_in(dir.path());
        let a = journal.append(JournalOp::Put, &key(1), Hlc::new(100, 0)).expect("append");
        let b = journal.append(JournalOp::Delete, &key(2), Hlc::new(101, 0)).expect("append");
        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(journal.last_seq(), 2);
    }

    #[test]
    fn read_range_roundtrips_entries_and_resume_point() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = open_in(dir.path());
        for i in 1..=5u64 {
            journal.append(JournalOp::Put, &key(i), Hlc::new(100 + i, 0)).expect("append");
        }

        let (entries, next) = open_read(&journal, 0);
        assert_eq!(entries.len(), 5);
        assert_eq!(next, 5);
        assert_eq!(entries[0].key.as_ref(), key(1).as_slice());
        assert_eq!(entries[4].seq, 5);
        assert_eq!(entries[4].hlc, Hlc::new(105, 0));

        let (resumed, next) = open_read(&journal, 3);
        assert_eq!(resumed.len(), 2);
        assert_eq!(resumed[0].seq, 4);
        assert_eq!(next, 5);

        let (empty, next) = open_read(&journal, 5);
        assert!(empty.is_empty());
        assert_eq!(next, 5);
    }

    #[test]
    fn read_range_respects_entry_and_byte_budgets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = open_in(dir.path());
        for i in 1..=10u64 {
            journal.append(JournalOp::Put, &key(i), Hlc::new(i, 0)).expect("append");
        }
        match journal.read_range(0, 3, u64::MAX).expect("read") {
            JournalRead::Entries { entries, next_seq } => {
                assert_eq!(entries.len(), 3);
                assert_eq!(next_seq, 3);
            }
            JournalRead::Gap { .. } => panic!("no gap expected"),
        }
        match journal.read_range(0, 100, 1).expect("read") {
            JournalRead::Entries { entries, .. } => {
                assert_eq!(entries.len(), 1, "at least one entry")
            }
            JournalRead::Gap { .. } => panic!("no gap expected"),
        }
    }

    #[test]
    fn reopen_preserves_epoch_and_continues_sequences() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (epoch, last) = {
            let journal = open_in(dir.path());
            journal.append(JournalOp::Put, &key(1), Hlc::new(1, 0)).expect("append");
            (journal.epoch(), journal.last_seq())
        };
        let journal = open_in(dir.path());
        assert_eq!(journal.epoch(), epoch, "restart keeps the epoch");
        assert_eq!(journal.last_seq(), last);
        let seq = journal.append(JournalOp::Put, &key(2), Hlc::new(2, 0)).expect("append");
        assert_eq!(seq, last + 1);
        let (entries, _) = open_read(&journal, 0);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn rotation_creates_contiguous_segments_and_reopen_reads_all() {
        let dir = tempfile::tempdir().expect("tempdir");
        let epoch;
        {
            let journal = open_tiny(dir.path(), 300);
            epoch = journal.epoch();
            for i in 1..=30u64 {
                journal.append(JournalOp::Put, &key(i), Hlc::new(i, 0)).expect("append");
            }
            let segments = list_segment_numbers(dir.path()).expect("list");
            assert!(segments.len() > 1, "rotation produced {:?}", segments);
            let (entries, next) = open_read(&journal, 0);
            assert_eq!(entries.len(), 30);
            assert_eq!(next, 30);
        }
        let journal = open_in(dir.path());
        assert_eq!(journal.epoch(), epoch);
        let (entries, _) = open_read(&journal, 0);
        assert_eq!(entries.len(), 30);
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(entry.seq, index as u64 + 1);
        }
    }

    #[test]
    fn group_commit_concurrent_appends_are_all_durable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Arc::new(open_tiny(dir.path(), 4096));
        let mut handles = Vec::new();
        for thread in 0..8u64 {
            let journal = Arc::clone(&journal);
            handles.push(std::thread::spawn(move || {
                for i in 0..50u64 {
                    journal
                        .append(JournalOp::Put, &key(thread * 1000 + i), Hlc::new(i + 1, 0))
                        .expect("append");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("thread joins");
        }
        let last = journal.last_seq();
        assert_eq!(last, 400);
        drop(journal);

        let reopened = open_in(dir.path());
        let (entries, _) = open_read(&reopened, 0);
        assert_eq!(entries.len(), 400);
        let mut seqs: Vec<u64> = entries.iter().map(|e| e.seq).collect();
        seqs.sort_unstable();
        seqs.dedup();
        assert_eq!(seqs.len(), 400, "every sequence is unique");
        assert_eq!(seqs[0], 1);
        assert_eq!(*seqs.last().expect("non-empty"), 400);
    }

    #[test]
    fn append_rejects_an_oversized_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = open_in(dir.path());
        let oversized = vec![b'k'; MAX_JOURNAL_KEY_BYTES + 1];
        let error = journal
            .append(JournalOp::Put, &oversized, Hlc::new(1, 0))
            .expect_err("oversized key is rejected");
        assert!(error.to_string().contains("exceeds"), "{error}");
        assert_eq!(journal.last_seq(), 0, "nothing was assigned");
    }

    #[test]
    fn poisoned_journal_fails_every_append_closed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = open_in(dir.path());
        journal.append(JournalOp::Put, &key(1), Hlc::new(1, 0)).expect("first append");
        journal.poison_for_test("injected fsync failure");
        let error = journal
            .append(JournalOp::Put, &key(2), Hlc::new(2, 0))
            .expect_err("poisoned journal fails closed");
        assert!(error.to_string().contains("failed"), "{error}");
        assert!(journal.metrics().append_errors_total.get() >= 1);
    }

    #[test]
    fn recovery_truncates_a_torn_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let journal = open_in(dir.path());
            for i in 1..=3u64 {
                journal.append(JournalOp::Put, &key(i), Hlc::new(i, 0)).expect("append");
            }
        }
        // Simulate a torn tail: a length prefix claiming a payload that was
        // never fully written.
        let path = segment_path(dir.path(), 1);
        let durable_len = std::fs::metadata(&path).expect("stat").len();
        let mut file = OpenOptions::new().append(true).open(&path).expect("open segment");
        file.write_all(&[0xff, 0x00, 0x00, 0x00, b'g', b'a', b'r', b'b', b'a', b'g', b'e'])
            .expect("write torn bytes");
        drop(file);

        let journal = open_in(dir.path());
        assert_eq!(journal.last_seq(), 3, "durable prefix survives");
        let (entries, _) = open_read(&journal, 0);
        assert_eq!(entries.len(), 3);
        assert_eq!(
            std::fs::metadata(&path).expect("stat").len(),
            durable_len,
            "the torn bytes were truncated"
        );
    }

    #[test]
    fn recovery_compacts_a_power_loss_hole_and_records_a_gap() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let journal = open_in(dir.path());
            for i in 1..=4u64 {
                journal.append(JournalOp::Put, &key(i), Hlc::new(i, 0)).expect("append");
            }
        }
        // Rewrite the segment as: header + record 1 + record 2 + garbage +
        // record 4 (record 3 lost in a power-loss window).
        let path = segment_path(dir.path(), 1);
        let bytes = std::fs::read(&path).expect("read segment");
        let header = parse_segment_header(&bytes).expect("header").total_len as usize;
        let mut offsets = Vec::new();
        let mut cursor = header;
        while cursor < bytes.len() {
            let (_, consumed) = decode_record(&bytes[cursor..]).expect("decode").expect("record");
            offsets.push((cursor, consumed));
            cursor += consumed;
        }
        assert_eq!(offsets.len(), 4);
        let mut damaged = Vec::new();
        damaged.extend_from_slice(&bytes[..offsets[2].0]);
        damaged.extend_from_slice(b"POWERLOSS");
        damaged.extend_from_slice(&bytes[offsets[3].0..]);
        std::fs::write(&path, &damaged).expect("write damaged segment");

        let journal = open_in(dir.path());
        assert_eq!(journal.last_seq(), 4, "valid entries after the hole survive");
        assert_eq!(journal.oldest_seq(), 3, "the hole is recorded as a floor");
        match journal.read_range(2, 100, u64::MAX).expect("read") {
            JournalRead::Gap { oldest_seq } => assert_eq!(oldest_seq, 3),
            JournalRead::Entries { .. } => panic!("expected a gap below the hole"),
        }
        let (entries, _) = open_read(&journal, 3);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq, 4);

        // The gap floor is persisted before serving (a crash between the
        // compaction and the manifest write is made idempotent by the
        // sequence-jump re-detection below).
        let manifest = read_manifest(dir.path()).expect("manifest").expect("manifest exists");
        assert_eq!(manifest.gap_floor_seq, 3);
    }

    #[test]
    fn recovery_detects_a_sequence_jump_as_a_gap() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let journal = open_in(dir.path());
            for i in 1..=4u64 {
                journal.append(JournalOp::Put, &key(i), Hlc::new(i, 0)).expect("append");
            }
        }
        // Drop record 3 entirely: the surviving records jump 2 -> 4. This
        // is the post-compaction shape a crashed recovery leaves behind
        // (and a genuine power-loss hole).
        let path = segment_path(dir.path(), 1);
        let bytes = std::fs::read(&path).expect("read segment");
        let header = parse_segment_header(&bytes).expect("header").total_len as usize;
        let mut offsets = Vec::new();
        let mut cursor = header;
        while cursor < bytes.len() {
            let (_, consumed) = decode_record(&bytes[cursor..]).expect("decode").expect("record");
            offsets.push((cursor, consumed));
            cursor += consumed;
        }
        assert_eq!(offsets.len(), 4);
        let mut jumped = Vec::new();
        jumped.extend_from_slice(&bytes[..offsets[2].0]);
        jumped.extend_from_slice(&bytes[offsets[3].0..]);
        std::fs::write(&path, &jumped).expect("write jumped segment");
        // Simulate a lost manifest gap marker (crash before persistence).
        let manifest_path = dir.path().join("manifest");
        let mut manifest = read_manifest(dir.path()).expect("manifest").expect("manifest exists");
        manifest.gap_floor_seq = 0;
        write_manifest(dir.path(), &manifest).expect("rewrite manifest");

        let journal = open_in(dir.path());
        assert_eq!(journal.last_seq(), 4);
        assert_eq!(journal.oldest_seq(), 3, "the jump is re-detected as a gap");
        match journal.read_range(2, 100, u64::MAX).expect("read") {
            JournalRead::Gap { oldest_seq } => assert_eq!(oldest_seq, 3),
            JournalRead::Entries { .. } => panic!("expected a gap"),
        }
    }

    #[test]
    fn trim_below_frontier_removes_whole_segments_and_advances_the_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = open_tiny(dir.path(), 300);
        for i in 1..=30u64 {
            journal.append(JournalOp::Put, &key(i), Hlc::new(i, 0)).expect("append");
        }
        let last = journal.last_seq();
        let report =
            journal.enforce_retention(last, u64::MAX, Duration::from_secs(86_400)).expect("trim");
        assert!(report.segments_removed >= 1, "some segments trimmed");
        assert!(!report.cap_forced, "frontier trim is not cap-forced");
        assert_eq!(journal.oldest_seq(), report.base_seq);
        match journal.read_range(0, 100, u64::MAX).expect("read") {
            JournalRead::Gap { oldest_seq } => assert_eq!(oldest_seq, report.base_seq),
            JournalRead::Entries { .. } => panic!("expected a gap below the new floor"),
        }
        let (entries, _) = open_read(&journal, report.base_seq);
        assert_eq!(entries.len() as u64, last - report.base_seq);
    }

    #[test]
    fn trim_byte_cap_overrides_the_frontier_and_forces_a_gap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = open_tiny(dir.path(), 300);
        for i in 1..=30u64 {
            journal.append(JournalOp::Put, &key(i), Hlc::new(i, 0)).expect("append");
        }
        let report =
            journal.enforce_retention(0, 1, Duration::from_secs(86_400)).expect("cap trim");
        assert!(report.segments_removed >= 1);
        assert!(report.cap_forced, "the byte cap forced the floor");
        assert!(journal.on_disk_bytes() < 1024, "only the active segment remains");
        match journal.read_range(0, 100, u64::MAX).expect("read") {
            JournalRead::Gap { oldest_seq } => assert_eq!(oldest_seq, report.base_seq),
            JournalRead::Entries { .. } => panic!("expected a gap after a cap trim"),
        }
    }

    #[test]
    fn damaged_non_active_segment_starts_a_new_epoch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let old_epoch = {
            let journal = open_tiny(dir.path(), 300);
            for i in 1..=30u64 {
                journal.append(JournalOp::Put, &key(i), Hlc::new(i, 0)).expect("append");
            }
            assert!(list_segment_numbers(dir.path()).expect("list").len() > 1);
            journal.epoch()
        };
        let first = segment_path(dir.path(), 1);
        std::fs::write(&first, b"NOT A JOURNAL SEGMENT").expect("corrupt header");

        let journal = open_in(dir.path());
        assert_ne!(journal.epoch(), old_epoch, "damage starts a new epoch");
        assert_eq!(journal.last_seq(), 0);
        let sibling = dir
            .path()
            .parent()
            .expect("parent")
            .read_dir()
            .expect("read parent")
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains("corrupt"));
        assert!(sibling, "the damaged directory was quarantined");
    }
}
