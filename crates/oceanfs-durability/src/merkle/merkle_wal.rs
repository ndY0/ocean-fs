//! MerkleWal — persistent write-ahead log for Merkle tree mutations.
//!
//! Stores incremental tree mutations (node insertions, updates, subtree
//! invalidations) in a sequential, append-only WAL file with CRC32-protected
//! framing. Supports replay after crash and truncation for compaction.
//!
//! ## Frame Format
//!
//! Each entry is stored as:
//! ```text
//! [u32 LE: payload_len] [binary-encoded MerkleWalEntry] [u32 LE: crc32]
//! ```
//!
//! The CRC32 covers the payload bytes only (not the length prefix).
//!
//! ## Binary Encoding
//!
//! MerkleWalEntry is encoded as a 1-byte variant tag followed by variant-specific data:
//! ```text
//! tag 0x00 (NodeInsert):     [0x00][u128 LE segment_id][u32 LE node_index][32 bytes hash]
//! tag 0x01 (NodeUpdate):     [0x01][u128 LE segment_id][u32 LE node_index][32 bytes old_hash][32 bytes new_hash]
//! tag 0x02 (SubtreeInvalidate): [0x02][u128 LE segment_id]
//! ```
//!
//! ## WalWriter Trait
//!
//! `MerkleWal` implements `oceanfs_storage_api::WalWriter` so it can be
//! used generically wherever a WAL writer is expected (ADR-0009 Part 2).
//!
//! # Examples
//!
//! ```ignore
//! use oceanfs_durability::merkle::{MerkleWal, MerkleWalEntry};
//! use oceanfs_core::SegmentId;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let wal = MerkleWal::open("/tmp/merkle.wal")?;
//! let entry = MerkleWalEntry::NodeInsert {
//!     segment_id: SegmentId::new(),
//!     node_index: 3,
//!     hash: [0x42; 32],
//! };
//! let pos = wal.log_mutation(&entry)?;
//! let replayed = wal.replay_mutations()?;
//! # Ok(())
//! # }
//! ```

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use oceanfs_core::SegmentId;
use parking_lot::Mutex;
use tracing::info;

use crate::{
    error::{Error, Result},
    merkle::tree_node::MerkleWalEntry,
};

/// Default maximum file size before rotating (64 MB).
const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 64 * 1024 * 1024;

/// The binary tag byte for NodeInsert.
const TAG_NODE_INSERT: u8 = 0x00;
/// The binary tag byte for NodeUpdate.
const TAG_NODE_UPDATE: u8 = 0x01;
/// The binary tag byte for SubtreeInvalidate.
const TAG_SUBTREE_INVALIDATE: u8 = 0x02;

/// A write-ahead log for incremental Merkle tree mutations.
///
/// Each entry is framed with a length prefix and trailing CRC32 checksum
/// for integrity verification on replay. The payload is a binary-encoded
/// [`MerkleWalEntry`].
pub struct MerkleWal {
    /// Path to the WAL file.
    path: PathBuf,
    /// WAL file handle, protected by a mutex for concurrent access.
    file: Mutex<File>,
    /// Current byte position in the file (always at end after append).
    position: Mutex<u64>,
    /// Maximum file size before rotation (unused in single-file mode).
    #[allow(dead_code)]
    max_file_size_bytes: u64,
}

impl MerkleWal {
    /// Opens or creates a Merkle WAL file.
    ///
    /// If the file exists, resumes from the current end-of-file position.
    /// The file is opened with `create(true).append(true).read(true)` for
    /// append-only sequential writes (perf rule 3.1).
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or created.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use oceanfs_durability::merkle::MerkleWal;
    ///
    /// let wal = MerkleWal::open("/var/lib/oceanfs/merkle.wal")?;
    /// ```
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| Error::Io(e))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| Error::Io(e))?;

        let existing_size = file.metadata().map_err(|e| Error::Io(e))?.len();

        info!(
            path = %path.display(),
            existing_bytes = existing_size,
            "opened merkle WAL"
        );

        Ok(Self {
            path,
            file: Mutex::new(file),
            position: Mutex::new(existing_size),
            max_file_size_bytes: DEFAULT_MAX_FILE_SIZE_BYTES,
        })
    }

    /// Logs a single Merkle tree mutation to the WAL.
    ///
    /// Encodes the entry into binary format, frames it with a length prefix
    /// and CRC32 checksum, appends to the file, and fsyncs.
    ///
    /// Returns the byte offset of the newly written entry (suitable for
    /// truncation).
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the write or fsync fails.
    pub fn log_mutation(&self, entry: &MerkleWalEntry) -> Result<u64> {
        let payload = Self::encode_entry(entry);
        let frame = Self::build_frame(&payload);

        let position = {
            let mut file = self.file.lock();
            let mut pos = self.position.lock();

            let start_pos = *pos;

            file.write_all(&frame).map_err(|e| Error::Io(e))?;
            file.flush().map_err(|e| Error::Io(e))?;
            // fsync for durability — tree mutations must survive crashes.
            file.sync_all().map_err(|e| Error::Io(e))?;

            *pos = start_pos + frame.len() as u64;

            start_pos
        };

        Ok(position)
    }

    /// Replays all mutations from the WAL.
    ///
    /// Reads the entire file from offset 0, decodes each frame, verifies CRC32,
    /// and decodes the binary entry payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, a frame is corrupted
    /// (CRC32 mismatch), or an entry fails to decode.
    pub fn replay_mutations(&self) -> Result<Vec<MerkleWalEntry>> {
        let mut file = self.file.lock();
        let file_size = file.metadata().map_err(|e| Error::Io(e))?.len();

        if file_size == 0 {
            return Ok(Vec::new());
        }

        file.seek(SeekFrom::Start(0)).map_err(|e| Error::Io(e))?;

        let mut buffer = vec![0u8; file_size as usize];
        file.read_exact(&mut buffer).map_err(|e| Error::Io(e))?;

        drop(file);

        let mut entries = Vec::new();
        let mut cursor: u64 = 0;

        while (cursor as usize) < buffer.len() {
            let remaining = &buffer[cursor as usize..];

            // Need at least 8 bytes (4 length + 4 CRC minimum).
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
                    "merkle WAL CRC32 mismatch at position {}: expected {:#x}, got {:#x}",
                    cursor, expected_crc, actual_crc
                )));
            }

            // Decode entry.
            let entry = Self::decode_entry(payload).map_err(|e| {
                Error::Internal(format!("merkle WAL decode failure at position {}: {e}", cursor))
            })?;

            entries.push(entry);
            cursor += frame_total as u64;
        }

        info!(
            path = %self.path.display(),
            entry_count = entries.len(),
            "replayed merkle WAL"
        );

        Ok(entries)
    }

    /// Returns the current WAL position (bytes written).
    pub fn global_position_sync(&self) -> u64 {
        *self.position.lock()
    }

    /// Returns the path to the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    // ------------------------------------------------------------------
    // Binary encoding / decoding
    // ------------------------------------------------------------------

    /// Encodes a `MerkleWalEntry` into a binary payload.
    fn encode_entry(entry: &MerkleWalEntry) -> Vec<u8> {
        match entry {
            MerkleWalEntry::NodeInsert { segment_id, node_index, hash } => {
                let mut buf = Vec::with_capacity(53); // 1 tag + 16 uuid + 4 u32 + 32 hash
                buf.push(TAG_NODE_INSERT);
                buf.extend_from_slice(segment_id.as_uuid().as_bytes());
                buf.extend_from_slice(&node_index.to_le_bytes());
                buf.extend_from_slice(hash);
                buf
            }
            MerkleWalEntry::NodeUpdate { segment_id, node_index, old_hash, new_hash } => {
                let mut buf = Vec::with_capacity(85); // 1 tag + 16 uuid + 4 u32 + 32 + 32
                buf.push(TAG_NODE_UPDATE);
                buf.extend_from_slice(segment_id.as_uuid().as_bytes());
                buf.extend_from_slice(&node_index.to_le_bytes());
                buf.extend_from_slice(old_hash);
                buf.extend_from_slice(new_hash);
                buf
            }
            MerkleWalEntry::SubtreeInvalidate { segment_id } => {
                let mut buf = Vec::with_capacity(17); // 1 tag + 16 uuid
                buf.push(TAG_SUBTREE_INVALIDATE);
                buf.extend_from_slice(segment_id.as_uuid().as_bytes());
                buf
            }
        }
    }

    /// Decodes a binary payload into a `MerkleWalEntry`.
    fn decode_entry(payload: &[u8]) -> Result<MerkleWalEntry> {
        if payload.is_empty() {
            return Err(Error::Storage("empty merkle WAL entry".into()));
        }

        let tag = payload[0];
        match tag {
            TAG_NODE_INSERT => {
                if payload.len() < 53 {
                    return Err(Error::Storage("truncated NodeInsert entry".into()));
                }
                let uuid_bytes: [u8; 16] = payload[1..17]
                    .try_into()
                    .map_err(|_| Error::Storage("invalid segment ID in NodeInsert".into()))?;
                let segment_id = SegmentId::from_uuid_bytes(uuid_bytes);
                let node_index =
                    u32::from_le_bytes([payload[17], payload[18], payload[19], payload[20]]);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&payload[21..53]);
                Ok(MerkleWalEntry::NodeInsert { segment_id, node_index, hash })
            }
            TAG_NODE_UPDATE => {
                if payload.len() < 85 {
                    return Err(Error::Storage("truncated NodeUpdate entry".into()));
                }
                let uuid_bytes: [u8; 16] = payload[1..17]
                    .try_into()
                    .map_err(|_| Error::Storage("invalid segment ID in NodeUpdate".into()))?;
                let segment_id = SegmentId::from_uuid_bytes(uuid_bytes);
                let node_index =
                    u32::from_le_bytes([payload[17], payload[18], payload[19], payload[20]]);
                let mut old_hash = [0u8; 32];
                old_hash.copy_from_slice(&payload[21..53]);
                let mut new_hash = [0u8; 32];
                new_hash.copy_from_slice(&payload[53..85]);
                Ok(MerkleWalEntry::NodeUpdate { segment_id, node_index, old_hash, new_hash })
            }
            TAG_SUBTREE_INVALIDATE => {
                if payload.len() < 17 {
                    return Err(Error::Storage("truncated SubtreeInvalidate entry".into()));
                }
                let uuid_bytes: [u8; 16] = payload[1..17].try_into().map_err(|_| {
                    Error::Storage("invalid segment ID in SubtreeInvalidate".into())
                })?;
                let segment_id = SegmentId::from_uuid_bytes(uuid_bytes);
                Ok(MerkleWalEntry::SubtreeInvalidate { segment_id })
            }
            _ => Err(Error::Storage(format!("unknown merkle WAL entry tag: {tag}"))),
        }
    }

    /// Builds a WAL frame from a binary payload.
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
// WalWriter trait implementation for MerkleWal
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl oceanfs_storage_api::WalWriter for MerkleWal {
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

            file.write_all(&frame).map_err(|e| oceanfs_storage_api::error::Error::Io(e))?;
            file.flush().map_err(|e| oceanfs_storage_api::error::Error::Io(e))?;
            file.sync_all().map_err(|e| oceanfs_storage_api::error::Error::Io(e))?;

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
        let mut file = self.file.lock();
        let mut pos = self.position.lock();

        file.set_len(position).map_err(|e| oceanfs_storage_api::error::Error::Io(e))?;
        file.seek(SeekFrom::Start(position))
            .map_err(|e| oceanfs_storage_api::error::Error::Io(e))?;
        file.flush().map_err(|e| oceanfs_storage_api::error::Error::Io(e))?;

        *pos = position;

        info!(
            path = %self.path.display(),
            new_size = position,
            "truncated merkle WAL"
        );

        Ok(())
    }

    /// Force-syncs the WAL file to disk.
    async fn sync(&self) -> std::result::Result<(), oceanfs_storage_api::error::Error> {
        let file = self.file.lock();
        file.sync_all().map_err(|e| oceanfs_storage_api::error::Error::Io(e))
    }

    /// Returns the current global WAL position.
    async fn global_position(&self) -> u64 {
        self.global_position_sync()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use oceanfs_core::SegmentId;

    use super::*;

    fn make_hash(val: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = val;
        h
    }

    // ── Log and replay roundtrip ──────────────────────────────────────

    #[test]
    fn test_merkle_wal_log_mutation_and_replay_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("merkle.wal");

        let wal = MerkleWal::open(&wal_path).unwrap();

        // Log 5 NodeInsert mutations.
        for i in 0..5u32 {
            let entry = MerkleWalEntry::NodeInsert {
                segment_id: SegmentId::new(),
                node_index: i,
                hash: make_hash(i as u8),
            };
            let pos = wal.log_mutation(&entry).unwrap();
            assert!(pos < wal.global_position_sync());
        }

        // Log 3 NodeUpdate mutations.
        for i in 0..3u32 {
            let entry = MerkleWalEntry::NodeUpdate {
                segment_id: SegmentId::new(),
                node_index: i + 10,
                old_hash: make_hash(i as u8),
                new_hash: make_hash((i + 100) as u8),
            };
            wal.log_mutation(&entry).unwrap();
        }

        // Log 1 SubtreeInvalidate.
        let invalidate = MerkleWalEntry::SubtreeInvalidate { segment_id: SegmentId::new() };
        wal.log_mutation(&invalidate).unwrap();

        // Close and reopen.
        drop(wal);
        let wal2 = MerkleWal::open(&wal_path).unwrap();

        let entries = wal2.replay_mutations().unwrap();
        assert_eq!(entries.len(), 9, "expected 5 inserts + 3 updates + 1 invalidate = 9 entries");

        // Verify first 5 are NodeInsert.
        for i in 0..5 {
            match &entries[i] {
                MerkleWalEntry::NodeInsert { node_index, hash, .. } => {
                    assert_eq!(*node_index, i as u32);
                    assert_eq!(hash[0], i as u8);
                }
                other => panic!("expected NodeInsert at index {i}, got {other:?}"),
            }
        }

        // Verify next 3 are NodeUpdate.
        for i in 0..3 {
            match &entries[5 + i] {
                MerkleWalEntry::NodeUpdate { node_index, old_hash, new_hash, .. } => {
                    assert_eq!(*node_index, (i + 10) as u32);
                    assert_eq!(old_hash[0], i as u8);
                    assert_eq!(new_hash[0], (i + 100) as u8);
                }
                other => panic!("expected NodeUpdate at index {}, got {other:?}", 5 + i),
            }
        }

        // Verify last is SubtreeInvalidate.
        assert!(
            matches!(&entries[8], MerkleWalEntry::SubtreeInvalidate { .. }),
            "expected SubtreeInvalidate at index 8, got {:?}",
            entries[8]
        );
    }

    // ── Empty WAL replay ─────────────────────────────────────────────

    #[test]
    fn test_empty_wal_replay_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("empty.wal");
        let wal = MerkleWal::open(&wal_path).unwrap();
        let entries = wal.replay_mutations().unwrap();
        assert!(entries.is_empty());
    }

    // ── Corrupt CRC → error ──────────────────────────────────────────

    #[test]
    fn test_merkle_wal_corrupt_crc_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("corrupt.wal");
        let wal = MerkleWal::open(&wal_path).unwrap();

        // Write 3 entries.
        for i in 0..3 {
            let entry = MerkleWalEntry::NodeInsert {
                segment_id: SegmentId::new(),
                node_index: i,
                hash: make_hash(i as u8),
            };
            wal.log_mutation(&entry).unwrap();
        }
        drop(wal);

        // Corrupt the CRC32 of the 2nd entry.
        {
            let file_len = {
                let file = OpenOptions::new().read(true).open(&wal_path).unwrap();
                file.metadata().unwrap().len()
            };
            let mut data = vec![0u8; file_len as usize];
            {
                let mut read_file = OpenOptions::new().read(true).open(&wal_path).unwrap();
                read_file.read_exact(&mut data).unwrap();
            }

            // Find the second entry's CRC and corrupt it.
            let first_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            let first_frame_total = 4 + first_len + 4;
            assert!(first_frame_total < data.len());

            let second_len_start = first_frame_total;
            let second_len = u32::from_le_bytes([
                data[second_len_start],
                data[second_len_start + 1],
                data[second_len_start + 2],
                data[second_len_start + 3],
            ]) as usize;

            let second_crc_start = second_len_start + 4 + second_len;

            let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
            file.seek(SeekFrom::Start(second_crc_start as u64)).unwrap();
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            file.flush().unwrap();
        }

        // Reopen — replay should fail on CRC mismatch.
        let wal2 = MerkleWal::open(&wal_path).unwrap();
        let result = wal2.replay_mutations();
        assert!(result.is_err(), "replay must fail on CRC mismatch");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("CRC32 mismatch"),
            "error must mention CRC mismatch: {err}"
        );
    }

    // ── WalWriter trait ──────────────────────────────────────────────

    #[test]
    fn test_merkle_wal_implements_wal_writer_trait() {
        use oceanfs_storage_api::WalWriter;

        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("trait.wal");
        let wal = MerkleWal::open(&wal_path).unwrap();

        // Write raw bytes via the WalWriter trait.
        let pos = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { wal.append(b"raw_test_bytes").await.unwrap() });
        assert_eq!(pos, 0, "first write at position 0");

        // Sync.
        tokio::runtime::Runtime::new().unwrap().block_on(async { wal.sync().await.unwrap() });

        // Global position should be > 0.
        let gp =
            tokio::runtime::Runtime::new().unwrap().block_on(async { wal.global_position().await });
        assert!(gp > 0, "global position should advance after write");

        // Truncate back to 0 via trait.
        tokio::runtime::Runtime::new().unwrap().block_on(async { wal.truncate(0).await.unwrap() });
        assert_eq!(wal.global_position_sync(), 0);
    }
}
