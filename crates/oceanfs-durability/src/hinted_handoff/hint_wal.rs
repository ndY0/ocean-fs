//! HintWal — persistent write-ahead log for hinted handoff records.
//!
//! Stores hinted handoff records in a sequential, append-only WAL file
//! with CRC32-protected framing. Supports replay after crash and truncation
//! after successful delivery.
//!
//! ## Frame Format
//!
//! Each entry is stored as:
//! ```text
//! [u32 LE: payload_len] [protobuf bytes: variable] [u32 LE: crc32]
//! ```
//!
//! The CRC32 covers the payload bytes only (not the length prefix).
//!
//! ## WalWriter Trait
//!
//! `HintWal` implements `oceanfs_storage_api::WalWriter` so it can be
//! used generically wherever a WAL writer is expected (ADR-0009 Part 2).

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use oceanfs_core::NodeId;
use parking_lot::Mutex;
use prost::Message;
use tracing::{info, warn};

use crate::{
    error::{Error, Result},
    hinted_handoff_rpc::{hint_record::Record, HintInline, HintRecord, HintSegmentRef},
};

/// A write-ahead log for hinted handoff records.
///
/// Each entry is length-prefixed protobuf with a trailing CRC32 checksum
/// for integrity verification on replay.
///
/// # Examples
///
/// ```ignore
/// // Requires tokio runtime; see unit tests.
/// use oceanfs_durability::{HintWal, HintedHandoffConfig};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let wal = HintWal::open("/tmp/hints.wal").await?;
/// let record = HintRecord { /* ... */ };
/// let pos = wal.write_hint(&record).await?;
/// let replayed = wal.replay().await?;
/// wal.truncate_after(pos).await?;
/// # Ok(())
/// # }
/// ```
pub struct HintWal {
    /// Path to the WAL file.
    path: PathBuf,
    /// WAL file handle, protected by a mutex for concurrent access.
    /// Uses `parking_lot::Mutex` — WAL I/O is not on the hot write path.
    file: Mutex<File>,
    /// Current byte position in the file (always at end after append).
    position: Mutex<u64>,
}

impl HintWal {
    /// Opens or creates a hinted handoff WAL file.
    ///
    /// If the file exists, resumes from the current end-of-file position.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or created.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(Error::Io)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(Error::Io)?;

        let existing_size = file.metadata().map_err(Error::Io)?.len();

        info!(
            path = %path.display(),
            existing_bytes = existing_size,
            "opened hint WAL"
        );

        Ok(Self { path, file: Mutex::new(file), position: Mutex::new(existing_size) })
    }

    /// Writes a hint record to the WAL and returns `(start_position, end_position)`.
    ///
    /// Serializes the record as protobuf, frames it with length prefix
    /// and CRC32 checksum, appends to the file, and fsyncs.
    ///
    /// The end position is the byte offset immediately after the frame,
    /// suitable for precise truncation.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the write or fsync fails.
    pub async fn write_hint(&self, record: &HintRecord) -> Result<(u64, u64)> {
        let payload = record.encode_to_vec();
        let frame = Self::build_frame(&payload);

        let (position, end_pos) = {
            let mut file = self.file.lock();
            let mut pos = self.position.lock();

            let start_pos = *pos;

            file.write_all(&frame).map_err(Error::Io)?;
            file.flush().map_err(Error::Io)?;
            // fsync for durability — hinted handoff entries must survive crashes.
            file.sync_all().map_err(Error::Io)?;

            *pos = start_pos + frame.len() as u64;

            (start_pos, *pos)
        };

        Ok((position, end_pos))
    }

    /// Replays all records from the WAL, returning `(start_position, end_position, HintRecord)` triples.
    ///
    /// Reads the entire file from offset 0, decodes each frame, verifies CRC32,
    /// and decodes the protobuf payload.
    ///
    /// The end position is the byte offset immediately after the frame,
    /// suitable for precise truncation.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, a frame is corrupted
    /// (CRC32 mismatch), or a record fails to decode.
    pub async fn replay(&self) -> Result<Vec<(u64, u64, HintRecord)>> {
        let mut file = self.file.lock();
        let file_size = file.metadata().map_err(Error::Io)?.len();

        if file_size == 0 {
            return Ok(Vec::new());
        }

        file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;

        let mut buffer = vec![0u8; file_size as usize];
        file.read_exact(&mut buffer).map_err(Error::Io)?;

        drop(file);

        let mut records = Vec::new();
        let mut cursor: u64 = 0;

        while (cursor as usize) < buffer.len() {
            let remaining = &buffer[cursor as usize..];

            // Need at least 8 bytes (4 length + 4 CRC minimum)
            if remaining.len() < 8 {
                break;
            }

            // Read the 4-byte little-endian payload length.
            let payload_len =
                u32::from_le_bytes([remaining[0], remaining[1], remaining[2], remaining[3]])
                    as usize;

            // Need payload_len bytes + 4 bytes CRC32.
            let frame_total = 4 + payload_len + 4;
            if remaining.len() < frame_total {
                warn!(
                    position = cursor,
                    expected = frame_total,
                    remaining = remaining.len(),
                    "hint WAL truncated frame at end of file"
                );
                break;
            }

            let payload = &remaining[4..4 + payload_len];
            let crc_bytes = &remaining[4 + payload_len..4 + payload_len + 4];
            let expected_crc =
                u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);

            // Verify CRC32.
            let actual_crc = crc32fast::hash(payload);
            if actual_crc != expected_crc {
                return Err(Error::Internal(format!(
                    "hint WAL CRC32 mismatch at position {}: expected {:#x}, got {:#x}",
                    cursor, expected_crc, actual_crc
                )));
            }

            // Decode protobuf.
            let record = HintRecord::decode(payload).map_err(|e| {
                Error::Internal(format!(
                    "hint WAL protobuf decode failure at position {}: {e}",
                    cursor
                ))
            })?;

            let end_position = cursor + frame_total as u64;
            records.push((cursor, end_position, record));
            cursor = end_position;
        }

        info!(
            path = %self.path.display(),
            record_count = records.len(),
            "replayed hint WAL"
        );

        Ok(records)
    }

    /// Truncates the WAL file after the given byte position.
    ///
    /// All entries at or after `position` are discarded. This is called
    /// after successful hint delivery to reclaim disk space.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the truncation fails.
    pub async fn truncate_after(&self, position: u64) -> Result<()> {
        let mut file = self.file.lock();
        let mut pos = self.position.lock();

        file.set_len(position).map_err(Error::Io)?;
        file.seek(SeekFrom::Start(position)).map_err(Error::Io)?;
        file.flush().map_err(Error::Io)?;

        *pos = position;

        info!(
            path = %self.path.display(),
            new_size = position,
            "truncated hint WAL"
        );

        Ok(())
    }

    /// Replays all entries, filters out those older than `ttl_secs`,
    /// truncates the WAL, and re-writes surviving entries.
    ///
    /// Entries without a `stored_at_secs` timestamp (from before this field was added)
    /// are preserved — they survive the filter.
    ///
    /// Returns the number of entries pruned.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL replay, truncation, or re-write fails.
    pub async fn prune_expired(&self, ttl_secs: u64) -> Result<usize> {
        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        let records = self.replay().await?;

        let (survivors, expired): (Vec<_>, Vec<_>) =
            records.into_iter().partition(|(_, _, record)| {
                !(record.stored_at_secs > 0
                    && now_secs.saturating_sub(record.stored_at_secs) >= ttl_secs)
            });

        if expired.is_empty() {
            return Ok(0);
        }

        let pruned = expired.len();

        // Clear the WAL.
        self.truncate_after(0).await?;

        // Re-write survivors.
        for (_, _, record) in &survivors {
            self.write_hint(record).await?;
        }

        Ok(pruned)
    }

    /// Returns the current WAL position (bytes written).
    pub async fn global_position(&self) -> u64 {
        *self.position.lock()
    }

    /// Returns the path to the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Builds a WAL frame from a protobuf payload.
    ///
    /// Format: `[u32 LE: len][payload bytes][u32 LE: crc32_of_payload]`
    fn build_frame(payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u32;
        let crc = crc32fast::hash(payload);

        let mut frame = Vec::with_capacity(4 + payload.len() + 4);
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&crc.to_le_bytes());
        frame
    }
}

// ---------------------------------------------------------------------------
// WalWriter trait implementation for HintWal
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl oceanfs_storage_api::WalWriter for HintWal {
    /// Appends raw bytes to the WAL, wrapped in a length+CRC32 frame.
    ///
    /// Returns the global WAL position of the newly written entry.
    async fn append(
        &self,
        entry_data: &[u8],
    ) -> std::result::Result<u64, oceanfs_storage_api::error::Error> {
        let frame = Self::build_frame(entry_data);

        let position = {
            let mut file = self.file.lock();
            let mut pos = self.position.lock();

            let start_pos = *pos;

            file.write_all(&frame).map_err(oceanfs_storage_api::error::Error::Io)?;
            file.flush().map_err(oceanfs_storage_api::error::Error::Io)?;
            file.sync_all().map_err(oceanfs_storage_api::error::Error::Io)?;

            *pos = start_pos + frame.len() as u64;
            start_pos
        };

        Ok(position)
    }

    /// Truncates the WAL at the given position.
    async fn truncate(
        &self,
        position: u64,
    ) -> std::result::Result<(), oceanfs_storage_api::error::Error> {
        self.truncate_after(position)
            .await
            .map_err(|e| oceanfs_storage_api::error::Error::Internal(e.to_string()))
    }

    /// Force-syncs the WAL file to disk.
    async fn sync(&self) -> std::result::Result<(), oceanfs_storage_api::error::Error> {
        let file = self.file.lock();
        file.sync_all().map_err(oceanfs_storage_api::error::Error::Io)
    }

    /// Returns the current global WAL position.
    async fn global_position(&self) -> u64 {
        self.global_position().await
    }
}

// ---------------------------------------------------------------------------
// HintRecord conversion helpers
// ---------------------------------------------------------------------------

impl HintRecord {
    /// Creates a new `HintRecord` from an `HintInline`.
    pub fn new_inline(
        intended_for: NodeId,
        bucket_id: oceanfs_core::BucketId,
        object_key: String,
        data: bytes::Bytes,
    ) -> Self {
        let proto_intended: oceanfs_core::proto::common::NodeId = intended_for.into();
        let proto_bucket: oceanfs_core::proto::common::BucketId = bucket_id.into();

        HintRecord {
            record: Some(Record::Inline(HintInline {
                intended_for: Some(proto_intended),
                bucket_id: Some(proto_bucket),
                object_key,
                data,
            })),
            stored_at_secs: 0,
        }
    }

    /// Creates a new `HintRecord` from an `HintSegmentRef`.
    pub fn new_segment_ref(
        intended_for: NodeId,
        bucket_id: oceanfs_core::BucketId,
        object_key: String,
        segment_id: oceanfs_core::SegmentId,
        offset: u64,
        length: u32,
    ) -> Self {
        let proto_intended: oceanfs_core::proto::common::NodeId = intended_for.into();
        let proto_bucket: oceanfs_core::proto::common::BucketId = bucket_id.into();
        let proto_segment: oceanfs_core::proto::common::SegmentId = segment_id.into();

        HintRecord {
            record: Some(Record::SegmentRef(HintSegmentRef {
                intended_for: Some(proto_intended),
                bucket_id: Some(proto_bucket),
                object_key,
                segment_id: Some(proto_segment),
                offset,
                length,
            })),
            stored_at_secs: 0,
        }
    }

    /// Returns the `intended_for` NodeId for this hint record.
    ///
    /// Returns `None` if the record type is not set or the `intended_for`
    /// field is missing.
    pub fn intended_for(&self) -> Option<NodeId> {
        match &self.record {
            Some(Record::Inline(h)) => h.intended_for.clone().map(NodeId::from),
            Some(Record::SegmentRef(h)) => h.intended_for.clone().map(NodeId::from),
            None => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use oceanfs_core::{BucketId, SegmentId};
    use tempfile::tempdir;

    use super::*;

    // ── T1.1: Write-and-replay roundtrip ──────────────────────────────

    #[tokio::test]
    async fn test_hint_wal_write_and_replay_roundtrip() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");

        let wal = HintWal::open(&wal_path).await.unwrap();

        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");

        // Write 5 HintInline records.
        for i in 0..5 {
            let record = HintRecord::new_inline(
                node_a.clone(),
                BucketId::new("bucket-a"),
                format!("key-inline-{i}"),
                vec![i as u8; 16].into(),
            );
            let _ = wal.write_hint(&record).await.unwrap();
        }

        // Write 3 HintSegmentRef records.
        for i in 0..3 {
            let record = HintRecord::new_segment_ref(
                node_b.clone(),
                BucketId::new("bucket-b"),
                format!("key-seg-{i}"),
                SegmentId::new(),
                i * 100,
                (i as u32 + 1) * 50,
            );
            let _ = wal.write_hint(&record).await.unwrap();
        }

        // Close and reopen.
        drop(wal);
        let wal2 = HintWal::open(&wal_path).await.unwrap();

        let records = wal2.replay().await.unwrap();
        assert_eq!(records.len(), 8);

        // Verify inline records.
        for (idx, (_start, _end, record)) in records.iter().enumerate().take(5) {
            let node = record.intended_for().unwrap();
            assert_eq!(node.to_string(), "node-a");

            if let Some(Record::Inline(h)) = &record.record {
                assert_eq!(h.object_key, format!("key-inline-{idx}"));
                assert_eq!(h.data.len(), 16);
                assert_eq!(h.data.as_ref(), &vec![idx as u8; 16]);
            } else {
                panic!("expected HintInline at index {idx}");
            }
        }

        // Verify segment ref records.
        for (idx, (_start, _end, record)) in records.iter().enumerate().skip(5) {
            let node = record.intended_for().unwrap();
            assert_eq!(node.to_string(), "node-b");

            let ref_idx = idx - 5;
            if let Some(Record::SegmentRef(h)) = &record.record {
                assert_eq!(h.object_key, format!("key-seg-{ref_idx}"));
                assert_eq!(h.offset, ref_idx as u64 * 100);
                assert_eq!(h.length, (ref_idx as u32 + 1) * 50);
            } else {
                panic!("expected HintSegmentRef at index {idx}");
            }
        }
    }

    // ── T1.2: Truncate after delivery ─────────────────────────────────

    #[tokio::test]
    async fn test_hint_wal_truncate_after_delivery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");

        let wal = HintWal::open(&wal_path).await.unwrap();

        // Write 10 records, tracking positions.
        let mut start_positions = Vec::new();
        for i in 0..10 {
            let record = HintRecord::new_inline(
                NodeId::new("n1"),
                BucketId::new("b"),
                format!("key-{i}"),
                vec![i as u8].into(),
            );
            let (pos, _end) = wal.write_hint(&record).await.unwrap();
            start_positions.push(pos);
        }

        assert_eq!(start_positions.len(), 10);

        // Truncate after the 5th record (keep records 0-4, discard 5-9).
        // Use start_positions[5] (start of 6th record) as the truncation boundary.
        let truncate_at = start_positions[5];
        wal.truncate_after(truncate_at).await.unwrap();

        let records = wal.replay().await.unwrap();
        assert_eq!(records.len(), 5, "only first 5 records should survive truncation");

        for (idx, (_start, _end, record)) in records.iter().enumerate() {
            if let Some(Record::Inline(h)) = &record.record {
                assert_eq!(h.object_key, format!("key-{idx}"));
            }
        }
    }

    // ── T1.3: Corrupt CRC → error ────────────────────────────────────

    #[tokio::test]
    async fn test_hint_wal_corrupt_record_crc_mismatch_error() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");

        let wal = HintWal::open(&wal_path).await.unwrap();

        // Write 3 records.
        for i in 0..3 {
            let record = HintRecord::new_inline(
                NodeId::new("n1"),
                BucketId::new("b"),
                format!("key-{i}"),
                vec![i as u8; 10].into(),
            );
            let _ = wal.write_hint(&record).await.unwrap();
        }
        drop(wal);

        // Corrupt the CRC32 of the 2nd record by writing garbage to the CRC bytes.
        {
            let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
            // The 1st record frame: [4(len)][payload(14=4+10)][4(crc)] = 22 bytes
            // Record 1: key-0, payload = protobuf of HintInline { ... }
            // We need to find the CRC position of the 2nd record.
            // Since we don't know the exact sizes, let's corrupt the entire file by
            // flipping bytes in the CRC area of the middle record.

            // For simplicity, corrupt the CRC32 of the 2nd record by modifying
            // a known offset. First record position is 0, so after first record,
            // 2nd record starts at some offset > 0.
            //
            // Let's use a simpler approach: overwrite the CRC bytes of the 2nd record.
            let file_len = file.metadata().unwrap().len();

            // Read all bytes, find the 2nd record's CRC area, and corrupt it.
            let mut data = vec![0u8; file_len as usize];
            {
                let mut read_file = OpenOptions::new().read(true).open(&wal_path).unwrap();
                use std::io::Read;
                read_file.read_exact(&mut data).unwrap();
            }

            // Parse the first record to find where the second starts.
            let first_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            let first_frame_total = 4 + first_len + 4;
            assert!(first_frame_total < data.len(), "first record must not span entire file");

            // Second record starts at `first_frame_total`.
            let second_len_start = first_frame_total;
            let second_len = u32::from_le_bytes([
                data[second_len_start],
                data[second_len_start + 1],
                data[second_len_start + 2],
                data[second_len_start + 3],
            ]) as usize;

            let second_crc_start = second_len_start + 4 + second_len;
            // Corrupt the CRC.
            file.seek(SeekFrom::Start(second_crc_start as u64)).unwrap();
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            file.flush().unwrap();
        }

        // Reopen and replay — should error on CRC mismatch.
        let wal2 = HintWal::open(&wal_path).await.unwrap();
        let result = wal2.replay().await;
        assert!(result.is_err(), "replay must fail on CRC mismatch");
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("CRC32 mismatch") || err_msg.contains("CRc"),
            "error must mention CRC mismatch: {err_msg}"
        );
    }

    // ── T1.4: WalWriter trait ────────────────────────────────────────

    #[tokio::test]
    async fn test_hint_wal_implements_wal_writer_trait() {
        use oceanfs_storage_api::WalWriter;

        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");
        let wal = HintWal::open(&wal_path).await.unwrap();

        // Write raw bytes via the WalWriter trait.
        let pos = wal.append(b"raw_test_bytes").await.unwrap();
        assert_eq!(pos, 0, "first write at position 0");

        // Sync.
        wal.sync().await.unwrap();

        // Global position should be > 0.
        let gp = wal.global_position().await;
        assert!(gp > 0, "global position should advance after write");

        // Truncate back to 0 via trait.
        wal.truncate(0).await.unwrap();
        assert_eq!(wal.global_position().await, 0);

        // Verify the WAL file is empty after truncation.
        drop(wal);
        let wal2 = HintWal::open(&wal_path).await.unwrap();
        let records = wal2.replay().await.unwrap();
        assert!(records.is_empty());
    }

    // ── Empty WAL replay ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_empty_wal_replay_returns_empty() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("empty.wal");
        let wal = HintWal::open(&wal_path).await.unwrap();
        let records = wal.replay().await.unwrap();
        assert!(records.is_empty());
    }

    /// Verifies that prune_expired() removes entries older than TTL
    /// and preserves newer entries.
    #[tokio::test]
    async fn test_prune_expired_removes_old_entries() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("hints.wal");
        let wal = HintWal::open(&wal_path).await.unwrap();

        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

        let ttl = 3600; // 1 hour

        // Hint 1: old — should be pruned.
        let mut r1 = HintRecord::new_inline(
            NodeId::new("n1"),
            BucketId::new("b"),
            "obj1".into(),
            vec![1, 2, 3].into(),
        );
        r1.stored_at_secs = now - ttl - 100;

        // Hint 2: borderline — just within TTL, should survive.
        let mut r2 = HintRecord::new_inline(
            NodeId::new("n2"),
            BucketId::new("b"),
            "obj2".into(),
            vec![4, 5, 6].into(),
        );
        r2.stored_at_secs = now - (ttl / 2);

        // Hint 3: very new — should survive.
        let mut r3 = HintRecord::new_inline(
            NodeId::new("n3"),
            BucketId::new("b"),
            "obj3".into(),
            vec![7, 8, 9].into(),
        );
        r3.stored_at_secs = now;

        wal.write_hint(&r1).await.unwrap();
        wal.write_hint(&r2).await.unwrap();
        wal.write_hint(&r3).await.unwrap();

        let pruned = wal.prune_expired(ttl).await.unwrap();
        assert_eq!(pruned, 1, "exactly 1 entry should be pruned");

        let survivors = wal.replay().await.unwrap();
        assert_eq!(survivors.len(), 2, "2 entries should survive");
        let keys: Vec<&str> = survivors
            .iter()
            .map(|(_, _, r)| match r.record.as_ref().unwrap() {
                Record::Inline(i) => i.object_key.as_str(),
                _ => "",
            })
            .collect();
        assert!(keys.contains(&"obj2"), "obj2 should survive");
        assert!(keys.contains(&"obj3"), "obj3 should survive");
        assert!(!keys.contains(&"obj1"), "obj1 should be pruned");
    }
}
