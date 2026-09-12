//! Durable per-peer metadata-sync watermarks (ADR-0038 D3; ae2 / S2).
//!
//! Each node tracks two directions per peer:
//!
//! - `consumed(P)` — the highest sequence of **P's** journal this node has
//!   applied;
//! - `acked(P)` — the highest sequence of **this node's** journal that P
//!   has confirmed consuming (learned from P's pull requests, or from the
//!   responder-side `acknowledged_seq` field).
//!
//! Records are persisted lazily under
//! `<metadata_pool_root>/metadata_sync/watermarks/<hex(peer)>.wm` with an
//! atomic temp+rename and a CRC trailer. A lost advance only re-consumes
//! entries (the apply path is idempotent), so lazy flushing is safe.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use dashmap::{DashMap, DashSet};
use oceanfs_core::NodeId;

use crate::error::Result;

const WATERMARK_MAGIC: &[u8; 8] = b"OFSMWM01";

/// One peer's durable watermark record.
///
/// # Examples
///
/// ```
/// use oceanfs_durability::PeerWatermark;
///
/// let watermark = PeerWatermark {
///     epoch: None,
///     consumed: 3,
///     acked: 1,
///     acked_epoch: None,
/// };
/// assert_eq!(watermark.consumed, 3);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PeerWatermark {
    /// The responder-journal epoch the `consumed` position refers to.
    pub epoch: Option<[u8; 16]>,
    /// Highest sequence of the peer's journal applied locally.
    pub consumed: u64,
    /// Highest sequence of **this node's** journal the peer confirmed.
    pub acked: u64,
    /// The epoch of **this node's** journal the `acked` position refers
    /// to. An ack from a previous epoch is meaningless (the peer never
    /// confirmed the new journal) and is treated as zero by the frontier.
    pub acked_epoch: Option<[u8; 16]>,
}

/// Durable, lazily-flushed per-peer watermarks.
///
/// # Examples
///
/// ```ignore
/// let store = WatermarkStore::open(&dir)?;
/// store.set_consumed(&peer, epoch, next_seq);
/// store.flush()?; // atomic temp+rename; a lost advance only re-consumes
/// ```
pub struct WatermarkStore {
    dir: PathBuf,
    records: DashMap<NodeId, PeerWatermark>,
    /// Peers known not to be consuming (their `FetchJournal` answered
    /// `unavailable`). Mixed kill-switch meshes must not let a disabled
    /// peer pin the trim frontier forever.
    non_consuming: DashSet<NodeId>,
    dirty: AtomicBool,
}

impl std::fmt::Debug for WatermarkStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatermarkStore")
            .field("dir", &self.dir)
            .field("peers", &self.records.len())
            .finish_non_exhaustive()
    }
}

impl WatermarkStore {
    /// Opens (or creates) the watermark directory and loads every valid
    /// `.wm` record. Corrupt records are ignored (a lost advance is
    /// idempotent on the apply side).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let store = WatermarkStore::open(&watermark_dir)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created or read.
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let records = DashMap::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(stem) = name.to_string_lossy().strip_suffix(".wm").map(str::to_owned) else {
                continue;
            };
            let Ok(bytes) = std::fs::read(entry.path()) else { continue };
            let Some((peer, record)) = decode_record(&stem, &bytes) else { continue };
            records.insert(peer, record);
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            records,
            non_consuming: DashSet::new(),
            dirty: AtomicBool::new(false),
        })
    }

    /// Returns the current watermark for `peer` (default all-zero).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let watermark = store.get(&peer);
    /// assert_eq!(store.get(&peer).consumed, watermark.consumed);
    /// ```
    pub fn get(&self, peer: &NodeId) -> PeerWatermark {
        self.records.get(peer).map(|r| *r).unwrap_or_default()
    }

    /// Advances `consumed(peer)` within an epoch. An epoch change resets
    /// the consumed position to `seq` (the old position is meaningless in
    /// the new epoch); the position never moves backwards within one
    /// epoch.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// store.set_consumed(&peer, *responder_epoch.as_bytes(), next_seq);
    /// ```
    pub fn set_consumed(&self, peer: &NodeId, epoch: [u8; 16], seq: u64) {
        let mut record = self.records.entry(peer.clone()).or_default();
        if record.epoch != Some(epoch) {
            record.epoch = Some(epoch);
            record.consumed = seq;
        } else {
            record.consumed = record.consumed.max(seq);
        }
        drop(record);
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Advances `acked(peer)` (a peer confirmed consuming this node's
    /// journal up to `seq`) within **this node's** current journal epoch
    /// `own_epoch`. A changed epoch resets the ack to `seq`: the peer's
    /// prior confirmation referred to a journal that no longer exists.
    /// Within one epoch the position never moves backwards.
    /// # Examples
    ///
    /// ```ignore
    /// store.set_acked(&peer, own_epoch, from_seq);
    /// ```
    pub fn set_acked(&self, peer: &NodeId, own_epoch: [u8; 16], seq: u64) {
        let mut record = self.records.entry(peer.clone()).or_default();
        if record.acked_epoch != Some(own_epoch) {
            record.acked_epoch = Some(own_epoch);
            record.acked = seq;
        } else {
            record.acked = record.acked.max(seq);
        }
        drop(record);
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Records that `peer` answered `unavailable` (disabled): it is
    /// excluded from the trim frontier until it consumes again.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if status.code() == tonic::Code::Unavailable {
    ///     store.mark_non_consuming(&peer);
    /// }
    /// ```
    pub fn mark_non_consuming(&self, peer: &NodeId) {
        self.non_consuming.insert(peer.clone());
    }

    /// Records that `peer` is consuming again.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// store.mark_consuming(&peer);
    /// ```
    pub fn mark_consuming(&self, peer: &NodeId) {
        self.non_consuming.remove(peer);
    }

    /// Whether `peer` is known to be consuming (not disabled). A
    /// non-consuming peer is excluded from the trim frontier and from
    /// cap-trim gap signals.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if store.is_consuming(&peer) { /* consider its ack */ }
    /// ```
    pub fn is_consuming(&self, peer: &NodeId) -> bool {
        !self.non_consuming.contains(peer)
    }

    /// The trim frontier: the minimum `acked` over `retained` peers that
    /// are known to consume **in the journal epoch `own_epoch`**. A
    /// record whose `acked_epoch` predates it counts as zero (the peer
    /// never acked the current journal — conservative). Returns `None`
    /// when no retained peer is (currently) consuming — the caller may
    /// then trim to its own head.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let frontier = store.frontier(&retained, *journal.epoch().as_bytes());
    /// ```
    pub fn frontier(&self, retained: &[NodeId], own_epoch: [u8; 16]) -> Option<u64> {
        retained
            .iter()
            .filter(|peer| !self.non_consuming.contains(*peer))
            .map(|peer| {
                let record = self.get(peer);
                if record.acked_epoch == Some(own_epoch) {
                    record.acked
                } else {
                    0
                }
            })
            .min()
    }

    /// Flushes every record atomically (temp+rename) and fsyncs the
    /// directory. A no-op when nothing changed.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// worker_cycle_end: store.flush()?; // lazy: a lost advance re-consumes
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when a record cannot be written or the directory
    /// cannot be fsynced.
    pub fn flush(&self) -> Result<()> {
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return Ok(());
        }
        for entry in self.records.iter() {
            write_record(&self.dir, entry.key(), entry.value())?;
        }
        fsync_dir(&self.dir)?;
        Ok(())
    }

    /// Flushes a single peer's record eagerly (used by the RPC handler
    /// after recording an ack).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// store.flush_peer(&requester)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be written.
    pub fn flush_peer(&self, peer: &NodeId) -> Result<()> {
        if let Some(record) = self.records.get(peer) {
            write_record(&self.dir, peer, record.value())?;
        }
        Ok(())
    }

    /// Number of peers with a persisted record (tests/observability).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(store.len(), 3); // one record per known peer
    /// ```
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the store has no records.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert!(WatermarkStore::open(&dir)?.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Whether a synchronous flush is pending.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert!(store.is_dirty()); // set_consumed marked it
    /// ```
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }
}

const RECORD_LEN: usize = 8 + 1 + 16 + 8 + 8 + 1 + 16 + 4;

fn encode_record(record: &PeerWatermark) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(RECORD_LEN);
    bytes.extend_from_slice(WATERMARK_MAGIC);
    bytes.push(u8::from(record.epoch.is_some()));
    bytes.extend_from_slice(&record.epoch.unwrap_or([0u8; 16]));
    bytes.extend_from_slice(&record.consumed.to_le_bytes());
    bytes.extend_from_slice(&record.acked.to_le_bytes());
    bytes.push(u8::from(record.acked_epoch.is_some()));
    bytes.extend_from_slice(&record.acked_epoch.unwrap_or([0u8; 16]));
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_record(stem: &str, bytes: &[u8]) -> Option<(NodeId, PeerWatermark)> {
    if bytes.len() != RECORD_LEN || &bytes[0..8] != WATERMARK_MAGIC {
        return None;
    }
    let stored_crc = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().ok()?);
    if crc32fast::hash(&bytes[..bytes.len() - 4]) != stored_crc {
        return None;
    }
    let peer = unhex_peer(stem)?;
    let has_epoch = bytes[8] == 1;
    let mut epoch = [0u8; 16];
    epoch.copy_from_slice(&bytes[9..25]);
    let consumed = u64::from_le_bytes(bytes[25..33].try_into().ok()?);
    let acked = u64::from_le_bytes(bytes[33..41].try_into().ok()?);
    let has_acked_epoch = bytes[41] == 1;
    let mut acked_epoch = [0u8; 16];
    acked_epoch.copy_from_slice(&bytes[42..58]);
    Some((
        peer,
        PeerWatermark {
            epoch: has_epoch.then_some(epoch),
            consumed,
            acked,
            acked_epoch: has_acked_epoch.then_some(acked_epoch),
        },
    ))
}

fn write_record(dir: &Path, peer: &NodeId, record: &PeerWatermark) -> Result<()> {
    let path = dir.join(format!("{}.wm", hex_peer(peer)));
    let tmp = path.with_extension("wm.tmp");
    {
        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp)?;
        file.write_all(&encode_record(record))?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

fn hex_peer(peer: &NodeId) -> String {
    let mut out = String::with_capacity(peer.as_str().len() * 2);
    for byte in peer.as_str().as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn unhex_peer(stem: &str) -> Option<NodeId> {
    if stem.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(stem.len() / 2);
    let chars: Vec<char> = stem.chars().collect();
    for pair in chars.chunks(2) {
        let high = pair[0].to_digit(16)?;
        let low = pair[1].to_digit(16)?;
        bytes.push((high * 16 + low) as u8);
    }
    String::from_utf8(bytes).ok().map(NodeId::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(name: &str) -> NodeId {
        NodeId::new(name)
    }

    #[test]
    fn watermarks_roundtrip_and_never_move_backwards() {
        let dir = tempfile::tempdir().unwrap();
        let store = WatermarkStore::open(dir.path()).unwrap();
        let p = peer("node-b");
        store.set_consumed(&p, [7u8; 16], 10);
        store.set_acked(&p, [9u8; 16], 5);
        store.set_consumed(&p, [7u8; 16], 8);
        store.set_acked(&p, [9u8; 16], 3);
        let record = store.get(&p);
        assert_eq!(record.epoch, Some([7u8; 16]));
        assert_eq!(record.consumed, 10, "consumed never moves backwards in an epoch");
        assert_eq!(record.acked, 5, "acked never moves backwards within our epoch");
        assert_eq!(record.acked_epoch, Some([9u8; 16]));
        assert!(store.is_dirty());
        store.flush().unwrap();
        assert!(!store.is_dirty());

        let reopened = WatermarkStore::open(dir.path()).unwrap();
        assert_eq!(reopened.get(&p), record);
    }

    #[test]
    fn epoch_changes_reset_both_directions() {
        let dir = tempfile::tempdir().unwrap();
        let store = WatermarkStore::open(dir.path()).unwrap();
        let p = peer("node-b");
        store.set_consumed(&p, [1u8; 16], 42);
        store.set_acked(&p, [5u8; 16], 42);
        assert_eq!(store.frontier(&[p.clone()], [5u8; 16]), Some(42));

        // The peer's journal epoch changed: consumed resets.
        store.set_consumed(&p, [2u8; 16], 0);
        assert_eq!(store.get(&p).consumed, 0);

        // OUR journal epoch changed: the peer's stale ack no longer counts
        // (it never confirmed the new journal) — conservative zero.
        assert_eq!(store.frontier(&[p.clone()], [6u8; 16]), Some(0));
        // A fresh ack in the new epoch advances it again.
        store.set_acked(&p, [6u8; 16], 7);
        assert_eq!(store.get(&p).acked, 7);
        assert_eq!(store.frontier(&[p], [6u8; 16]), Some(7));
    }

    #[test]
    fn frontier_is_the_min_acked_over_consuming_retained_peers() {
        let dir = tempfile::tempdir().unwrap();
        let store = WatermarkStore::open(dir.path()).unwrap();
        let a = peer("node-a");
        let b = peer("node-b");
        let c = peer("node-c");
        let epoch = [3u8; 16];
        store.set_acked(&a, epoch, 10);
        store.set_acked(&b, epoch, 4);
        store.set_acked(&c, epoch, 7);
        assert_eq!(store.frontier(&[a.clone(), b.clone(), c.clone()], epoch), Some(4));

        // A peer known to be disabled is excluded.
        store.mark_non_consuming(&b);
        assert_eq!(store.frontier(&[a.clone(), b.clone(), c.clone()], epoch), Some(7));
        store.mark_consuming(&b);
        assert_eq!(store.frontier(&[a.clone(), b.clone(), c.clone()], epoch), Some(4));

        // No consuming retained peers: no constraint.
        assert_eq!(store.frontier(&[], epoch), None);
    }

    #[test]
    fn corrupt_record_is_ignored_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let store = WatermarkStore::open(dir.path()).unwrap();
        let p = peer("node-b");
        store.set_consumed(&p, [3u8; 16], 9);
        store.flush().unwrap();
        let file = dir.path().join(format!("{}.wm", hex_peer(&p)));
        std::fs::write(&file, b"garbage").unwrap();
        let reopened = WatermarkStore::open(dir.path()).unwrap();
        assert_eq!(reopened.get(&p), PeerWatermark::default());
    }
}
