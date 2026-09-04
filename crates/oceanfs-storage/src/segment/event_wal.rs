//! Segment event WAL — the durable half of the segment-lifecycle design.
//!
//! ADR-0024 Decisions 1, 2, 4 (migration phase 2): a dedicated,
//! project-owned, append-only **event WAL** of plain files — checksummed
//! records, its **own** `WalSyncGroup` instance — that becomes the single
//! source of truth for segment lifecycle transitions
//! (Reserve / Seal / Delete). The data WAL stays a seekable pool of blob
//! bytes; ordering between the two logs is by position reference
//! (`DataWalPos` carried in `SealEvent`), never by a shared sequence
//! number (ADR-0024 Decision 2).
//!
//! This is deliberately NOT RocksDB (ADR-0023 direction): plain files
//! with the project's own WAL discipline, one more instance of the
//! proven `WalWriter`/`WalSyncGroup` machinery — not a new fsync
//! discipline (ADR-0024 Decision 4).
//!
//! ADR-0018 compliance note: ADR-0018's thrust was *fewer* WAL domains.
//! This domain deliberately extends it — the event log *replaces* a
//! RocksDB column family (the `segments` CF becomes a derived mirror,
//! then is removed in phase 3), netting **−1 external durability
//! domain**, and it is the single ordering authority for segment
//! lifecycle, not a parallel data path (ADR-0024 §Consequences).
//!
//! # On-disk record framing (explicit byte layout — perf 6.3, the
//! `WalEntry` discipline; no repr-padding surprises)
//!
//! ```text
//! EventRecord:
//!   magic        [4]   = b"EVL\1"
//!   version      [1]   = 1
//!   kind         [1]   0=Reserve, 1=Seal, 2=Delete, 3=MetadataRefresh
//!   reserved     [2]   = 0
//!   payload_len  [4]   LE, payload size
//!   segment_id   [16]
//!   payload      [payload_len]   tier(1) + ec_k(1) + ec_m(1) + flags(1)
//!                                | + merkle_root(32) + data_wal_pos(12)
//!                                | + [repacked_from(16)] for Seal
//!                                | merkle_flag(1) + [merkle_root(32)]
//!                                |   for MetadataRefresh
//!   crc32        [4]   over all preceding bytes
//! ```
//!
//! Header is 28 bytes (4+1+1+2+4+16); Reserve payload is 4 bytes, Seal
//! payload 48 bytes (4+32+12) or 64 bytes with the compaction
//! `repacked_from` marker, Delete payload 0 bytes, MetadataRefresh
//! payload 1 or 33 bytes. `data_wal_pos` is `file_seq(4, LE) +
//! offset(8, LE)`.
//!
//! The Seal payload's flags byte (payload\[3\], was reserved): bit 0 set
//! means the compaction `repacked_from` segment id follows the position
//! (`+16` bytes) — ADR-0025 Decision 4's compaction marker. Records
//! written before the marker existed (flags = 0, 48-byte payload) decode
//! unchanged to `repacked_from: None`.

use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use oceanfs_core::{
    Counter, EventWalConfig, Gauge, HashOutput, LabelSet, MetricRegistrar, SegmentId, SizeTier,
};
use tokio::sync::Mutex;

use crate::{
    error::{Error, Result},
    wal::WalSyncGroup,
};

/// Magic bytes at the start of every event record (4 bytes: "EVL\1").
pub(crate) const EVENT_RECORD_MAGIC: [u8; 4] = [b'E', b'V', b'L', 1];

/// On-disk format version of event records.
pub(crate) const EVENT_RECORD_VERSION: u8 = 1;

/// Fixed header size of an event record: magic(4) + version(1) + kind(1)
/// + reserved(2) + payload_len(4) + segment_id(16) = 28.
pub(crate) const EVENT_RECORD_HEADER_SIZE: usize = 28;

/// Payload size of a `ReserveEvent`: tier(1) + ec_k(1) + ec_m(1) + reserved(1).
pub(crate) const RESERVE_PAYLOAD_SIZE: usize = 4;

/// Payload size of a `SealEvent`: 4 + merkle_root(32) + data_wal_pos(12).
pub(crate) const SEAL_PAYLOAD_SIZE: usize = 48;

/// Extra payload bytes of a `SealEvent` carrying the compaction
/// `repacked_from` marker (16 — a segment id).
pub(crate) const SEAL_REPACKED_FROM_SIZE: usize = 16;

/// Extra payload bytes of a `SealEvent` carrying the storage pool id
/// (4 — u32 LE; ADR-0029 f5, the durable segment→pool mapping).
pub(crate) const SEAL_POOL_ID_SIZE: usize = 4;

/// Flags byte of the Seal payload: bit 0 set → `repacked_from` present;
/// bit 1 set → `pool_id` present (ADR-0029 f5). Flags are never combined
/// with a legacy length: a payload length mismatch rejects the record.
pub(crate) const SEAL_FLAG_REPACKED_FROM: u8 = 1;
pub(crate) const SEAL_FLAG_POOL_ID: u8 = 2;

/// Payload size of a `DeleteEvent`.
pub(crate) const DELETE_PAYLOAD_SIZE: usize = 0;

/// Payload size of a `MetadataRefreshEvent` without a root
/// (merkle_flag(1)).
pub(crate) const REFRESH_PAYLOAD_SIZE: usize = 1;

/// Extra payload bytes of a `MetadataRefreshEvent` carrying a root
/// (merkle_root(32)).
pub(crate) const REFRESH_ROOT_SIZE: usize = 32;

/// Maximum number of `storage_locations` entries carried by an extended
/// `MetadataRefreshEvent` (ADR-0030). Matches the `SmallVec<[NodeId; 16]>`
/// capacity the registry stores.
pub(crate) const REFRESH_MAX_LOCATIONS: usize = 16;

/// Maximum encoded length of one `NodeId` in an extended refresh
/// (len(1) + utf8 bytes). Node ids are short host/container names; 255
/// bounds the on-disk record.
pub(crate) const REFRESH_MAX_NODE_ID_LEN: usize = 255;

/// Largest possible payload size — the extended MetadataRefresh payload
/// (merkle_flag(1) + root(32) + loc_count(1) + 16 × (len(1) + 255)) is
/// the largest variant. Bounds the reader's allocation so a corrupt
/// `payload_len` can never allocate arbitrarily.
pub(crate) const MAX_PAYLOAD_SIZE: usize = REFRESH_PAYLOAD_SIZE
    + REFRESH_ROOT_SIZE
    + 1
    + REFRESH_MAX_LOCATIONS * (1 + REFRESH_MAX_NODE_ID_LEN);

/// Record kind bytes on disk.
pub(crate) const KIND_RESERVE: u8 = 0;
pub(crate) const KIND_SEAL: u8 = 1;
pub(crate) const KIND_DELETE: u8 = 2;
pub(crate) const KIND_METADATA_REFRESH: u8 = 3;

/// Maximum number of waiters per event fsync batch.
const DEFAULT_EVENT_SYNC_MAX_WAITERS: usize = 64;

/// Position of an entry in the **data** WAL (file sequence + in-file
/// offset) — ADR-0024 Decision 2.
///
/// Carried by [`SealEvent`]; it makes the data WAL seekable: recovery
/// knows exactly which entries belong to a reserved-unsealed segment, and
/// the retention logic knows when a segment's data entries became garbage
/// (a durable `SealEvent`/`DeleteEvent` at/after the entry's position).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DataWalPos {
    /// Sequence number of the data WAL file (`wal_{seq:08}.log`).
    pub file_seq: u32,
    /// Byte offset of the entry within that file.
    pub offset: u64,
}

/// Position of an event record in the event log (file sequence +
/// in-file offset).
///
/// Returned by [`EventWal::append`], consumed by `read_from` and the
/// checkpoint trigger (`bytes_since`). Positions are monotonic across
/// appends: within a file the offset increases; across rotation the file
/// sequence increases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventWalPos {
    /// Sequence number of the event WAL file (`evl_{seq:08}.log`).
    pub file_seq: u32,
    /// Byte offset of the record within that file.
    pub offset: u64,
}

/// A segment **Reserve** event — replaces the phantom CF write
/// (ADR-0024 Decision 1).
///
/// Appended before the segment's first `DataEntry`; at recovery it makes
/// the segment's data entries meaningful instead of garbage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReserveEvent {
    /// The segment being reserved.
    pub segment_id: SegmentId,
    /// The segment's storage tier.
    pub tier: SizeTier,
    /// Erasure-coding data shard count.
    pub ec_k: u8,
    /// Erasure-coding parity shard count.
    pub ec_m: u8,
}

/// A segment **Seal** event — replaces the `sealed_at` CF write
/// (ADR-0024 Decision 1).
///
/// The full repacked metadata travels through the seal: `merkle_root` and
/// the compression-derived fields are seal inputs, not compactor
/// afterthoughts (the BadDigest defect is impossible).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealEvent {
    /// The segment being sealed.
    pub segment_id: SegmentId,
    /// The segment's storage tier.
    pub tier: SizeTier,
    /// Erasure-coding data shard count.
    pub ec_k: u8,
    /// Erasure-coding parity shard count.
    pub ec_m: u8,
    /// The seal-time Merkle root over the segment's data section.
    pub merkle_root: HashOutput,
    /// Position of the segment's **last** data entry in the data WAL
    /// (ADR-0024 Decision 2). `(0, 0)` when the segment has no recorded
    /// data entries (replayed segments whose WAL entries were truncated
    /// away — nothing to seek or sweep).
    pub data_wal_pos: DataWalPos,
    /// The compaction marker (ADR-0025 Decision 4): when this segment is
    /// a GC-repacked replacement, the id of the source segment it was
    /// repacked from. `None` for ordinary sealed segments. Recovery uses
    /// the marker to identify incomplete compaction units (crash-window
    /// rows 7–9) with a single objects-CF read per unit.
    pub repacked_from: Option<SegmentId>,
    /// The storage pool holding this segment's `.dat` (ADR-0029 f5).
    ///
    /// `0` = the legacy `{data_dir}/segments` root. Legacy event records
    /// (written before this field existed) decode with `pool_id = 0` —
    /// the flags byte + payload length discriminate the variants.
    pub pool_id: u32,
}

/// A segment **Delete** event — replaces the deleted-marker CF write
/// (ADR-0024 Decision 1).
///
/// Appended durably **before** the `.dat` unlink; recovery folds "Deleted"
/// and the orphan reaper sweeps the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteEvent {
    /// The segment being deleted.
    pub segment_id: SegmentId,
}

/// A segment **metadata refresh** — the machine's post-repair anchor
/// update (ADR-0025 Decision 3: the machine's entry metadata is the
/// scrub/AE anchor).
///
/// Not a lifecycle transition: the state is unchanged (the segment stays
/// `Sealed`). The refresh replaces the heal worker's post-repair
/// `put_segment(merkle_root: None)` CF write (the `segments` CF is
/// removed); the fold swaps the entry's `merkle_root` so the stale
/// anchor never survives a restart.
///
/// ADR-0030 extends the event to optionally carry a `storage_locations`
/// set — the durable post-repair holder stamp (the re-replication
/// worker records the target as a new holder through the event-WAL, the
/// single durable writer). `None` keeps the legacy anchor-only shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRefreshEvent {
    /// The segment whose anchor is refreshed.
    pub segment_id: SegmentId,
    /// The new anchor: `None` invalidates the root until rebuilt.
    pub merkle_root: Option<HashOutput>,
    /// The new holder set (ADR-0030): `Some` replaces
    /// `storage_locations` durably; `None` leaves it untouched.
    pub storage_locations: Option<smallvec::SmallVec<[oceanfs_core::NodeId; 16]>>,
}

/// A segment lifecycle transition — the only record family of the event
/// log (ADR-0024 Decision 1).
///
/// The variants are deliberately size-non-uniform: the extended
/// `MetadataRefreshEvent` (ADR-0030) carries an optional `SmallVec`
/// holder set — larger than the fixed-size Reserve/Seal/Delete records.
/// The event is built once per transition and dropped after the WAL
/// append + fold; it is never stored in a vec/array of `SegmentEvent`,
/// so the enum's size is the small fixed variants' size plus the
/// `SmallVec` inline capacity — a bounded, non-hot-path cost.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum SegmentEvent {
    /// Segment reserved (replaces the phantom CF write).
    Reserve(ReserveEvent),
    /// Segment sealed (replaces the `sealed_at` CF write).
    Seal(SealEvent),
    /// Segment deleted (replaces the deleted-marker CF write).
    Delete(DeleteEvent),
    /// Metadata refresh (the post-repair anchor invalidation; no state
    /// change).
    MetadataRefresh(MetadataRefreshEvent),
}

impl SegmentEvent {
    /// Returns the segment this event refers to.
    pub fn segment_id(&self) -> SegmentId {
        match self {
            SegmentEvent::Reserve(evt) => evt.segment_id,
            SegmentEvent::Seal(evt) => evt.segment_id,
            SegmentEvent::Delete(evt) => evt.segment_id,
            SegmentEvent::MetadataRefresh(evt) => evt.segment_id,
        }
    }

    /// Serializes the event to a full on-disk record
    /// (header + payload + crc32).
    ///
    /// The layout is fixed and byte-explicit (see the module docs) — the
    /// `WalEntry` discipline (perf 6.3). The CRC covers every preceding
    /// byte (header + payload).
    pub fn to_record_bytes(&self) -> Vec<u8> {
        let (kind, payload) = match self {
            SegmentEvent::Reserve(evt) => {
                let mut payload = Vec::with_capacity(RESERVE_PAYLOAD_SIZE);
                payload.push(tier_to_u8(evt.tier));
                payload.push(evt.ec_k);
                payload.push(evt.ec_m);
                payload.push(0); // reserved
                (KIND_RESERVE, payload)
            }
            SegmentEvent::Seal(evt) => {
                // Backward-compatible extension: the pool_id is appended
                // only when non-zero, flagged in the flags byte — legacy
                // logs (no pool id) keep their exact byte layout, and
                // new records with pool_id = 0 stay byte-identical too.
                let extra = if evt.repacked_from.is_some() { SEAL_REPACKED_FROM_SIZE } else { 0 }
                    + if evt.pool_id != 0 { SEAL_POOL_ID_SIZE } else { 0 };
                let mut payload = Vec::with_capacity(SEAL_PAYLOAD_SIZE + extra);
                payload.push(tier_to_u8(evt.tier));
                payload.push(evt.ec_k);
                payload.push(evt.ec_m);
                let mut flags = 0u8;
                if evt.repacked_from.is_some() {
                    flags |= SEAL_FLAG_REPACKED_FROM;
                }
                if evt.pool_id != 0 {
                    flags |= SEAL_FLAG_POOL_ID;
                }
                payload.push(flags);
                payload.extend_from_slice(evt.merkle_root.as_bytes());
                payload.extend_from_slice(&evt.data_wal_pos.file_seq.to_le_bytes());
                payload.extend_from_slice(&evt.data_wal_pos.offset.to_le_bytes());
                if let Some(old) = evt.repacked_from {
                    payload.extend_from_slice(old.as_uuid().as_bytes());
                }
                if evt.pool_id != 0 {
                    payload.extend_from_slice(&evt.pool_id.to_le_bytes());
                }
                (KIND_SEAL, payload)
            }
            SegmentEvent::Delete(_) => (KIND_DELETE, Vec::new()),
            SegmentEvent::MetadataRefresh(evt) => {
                // Flags byte: bit 0 = merkle_root present, bit 1 =
                // storage_locations present (ADR-0030). Legacy records
                // (bits 0/1 = 0x00/0x01) keep their exact byte layout;
                // the locations section is appended only when present.
                let mut flags = 0u8;
                if evt.merkle_root.is_some() {
                    flags |= 1;
                }
                if evt.storage_locations.is_some() {
                    flags |= 2;
                }
                let mut payload = Vec::with_capacity(
                    REFRESH_PAYLOAD_SIZE
                        + REFRESH_ROOT_SIZE
                        + 1
                        + evt.storage_locations.as_ref().map_or(0, |l| l.len() * (1 + 8)),
                );
                payload.push(flags);
                if let Some(root) = evt.merkle_root {
                    payload.extend_from_slice(root.as_bytes());
                }
                if let Some(locations) = &evt.storage_locations {
                    payload.push(locations.len() as u8);
                    for loc in locations.iter() {
                        let bytes = loc.as_str().as_bytes();
                        payload.push(bytes.len() as u8);
                        payload.extend_from_slice(bytes);
                    }
                }
                (KIND_METADATA_REFRESH, payload)
            }
        };

        let segment_id = self.segment_id();
        let mut buf = Vec::with_capacity(EVENT_RECORD_HEADER_SIZE + payload.len() + 4);
        buf.extend_from_slice(&EVENT_RECORD_MAGIC);
        buf.push(EVENT_RECORD_VERSION);
        buf.push(kind);
        buf.extend_from_slice(&[0u8; 2]); // reserved
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(segment_id.as_uuid().as_bytes());
        buf.extend_from_slice(&payload);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Deserializes a full on-disk record (header + payload + crc32).
    ///
    /// Returns `None` on any framing error (bad magic, unsupported
    /// version, unknown kind, implausible payload length, payload/kind
    /// mismatch, or CRC mismatch).
    pub fn from_record_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < EVENT_RECORD_HEADER_SIZE + 4 {
            return None;
        }
        if bytes[0..4] != EVENT_RECORD_MAGIC {
            return None;
        }
        if bytes[4] != EVENT_RECORD_VERSION {
            return None;
        }
        let kind = bytes[5];
        let payload_len = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;
        let expected_total = EVENT_RECORD_HEADER_SIZE + payload_len + 4;
        if bytes.len() != expected_total {
            return None;
        }
        let stored_crc =
            u32::from_le_bytes(bytes[expected_total - 4..expected_total].try_into().ok()?);
        if stored_crc != crc32fast::hash(&bytes[..expected_total - 4]) {
            return None;
        }
        let segment_id = SegmentId::from_uuid_bytes(bytes[12..28].try_into().ok()?);
        let payload = &bytes[EVENT_RECORD_HEADER_SIZE..EVENT_RECORD_HEADER_SIZE + payload_len];
        decode_payload(kind, segment_id, payload)
    }
}

/// Stable tier byte mapping for the on-disk payload.
///
/// The catch-all arm covers future `#[non_exhaustive]` tiers: they are
/// written as `0xFF` (reserved) and rejected by [`tier_from_u8`] on
/// decode, so an unknown tier never round-trips silently.
fn tier_to_u8(tier: SizeTier) -> u8 {
    match tier {
        SizeTier::Inline => 0,
        SizeTier::Small => 1,
        SizeTier::Standard => 2,
        SizeTier::Multi => 3,
        _ => 0xFF, // reserved for future tiers; decode rejects it
    }
}

/// Inverse of [`tier_to_u8`]; `None` for unknown byte values.
fn tier_from_u8(value: u8) -> Option<SizeTier> {
    match value {
        0 => Some(SizeTier::Inline),
        1 => Some(SizeTier::Small),
        2 => Some(SizeTier::Standard),
        3 => Some(SizeTier::Multi),
        _ => None,
    }
}

/// Decodes an event payload for the given kind and header `segment_id`.
fn decode_payload(kind: u8, segment_id: SegmentId, payload: &[u8]) -> Option<SegmentEvent> {
    match kind {
        KIND_RESERVE if payload.len() == RESERVE_PAYLOAD_SIZE => {
            Some(SegmentEvent::Reserve(ReserveEvent {
                segment_id,
                tier: tier_from_u8(payload[0])?,
                ec_k: payload[1],
                ec_m: payload[2],
            }))
        }
        KIND_SEAL
            if payload.len() == SEAL_PAYLOAD_SIZE && payload[3] & !SEAL_FLAG_REPACKED_FROM == 0 =>
        {
            Some(SegmentEvent::Seal(SealEvent {
                segment_id,
                tier: tier_from_u8(payload[0])?,
                ec_k: payload[1],
                ec_m: payload[2],
                merkle_root: HashOutput::from_bytes(payload[4..36].try_into().ok()?),
                data_wal_pos: DataWalPos {
                    file_seq: u32::from_le_bytes(payload[36..40].try_into().ok()?),
                    offset: u64::from_le_bytes(payload[40..48].try_into().ok()?),
                },
                repacked_from: None,
                pool_id: 0,
            }))
        }
        KIND_SEAL
            if payload.len() == SEAL_PAYLOAD_SIZE + SEAL_POOL_ID_SIZE
                && payload[3] & !(SEAL_FLAG_REPACKED_FROM | SEAL_FLAG_POOL_ID) == 0
                && payload[3] & SEAL_FLAG_POOL_ID != 0 =>
        {
            Some(SegmentEvent::Seal(SealEvent {
                segment_id,
                tier: tier_from_u8(payload[0])?,
                ec_k: payload[1],
                ec_m: payload[2],
                merkle_root: HashOutput::from_bytes(payload[4..36].try_into().ok()?),
                data_wal_pos: DataWalPos {
                    file_seq: u32::from_le_bytes(payload[36..40].try_into().ok()?),
                    offset: u64::from_le_bytes(payload[40..48].try_into().ok()?),
                },
                repacked_from: None,
                pool_id: u32::from_le_bytes(payload[48..52].try_into().ok()?),
            }))
        }
        KIND_SEAL
            if payload.len() == SEAL_PAYLOAD_SIZE + SEAL_REPACKED_FROM_SIZE
                && payload[3] & !SEAL_FLAG_REPACKED_FROM == 0
                && payload[3] & SEAL_FLAG_REPACKED_FROM != 0 =>
        {
            Some(SegmentEvent::Seal(SealEvent {
                segment_id,
                tier: tier_from_u8(payload[0])?,
                ec_k: payload[1],
                ec_m: payload[2],
                merkle_root: HashOutput::from_bytes(payload[4..36].try_into().ok()?),
                data_wal_pos: DataWalPos {
                    file_seq: u32::from_le_bytes(payload[36..40].try_into().ok()?),
                    offset: u64::from_le_bytes(payload[40..48].try_into().ok()?),
                },
                repacked_from: Some(SegmentId::from_uuid_bytes(payload[48..64].try_into().ok()?)),
                pool_id: 0,
            }))
        }
        KIND_SEAL
            if payload.len() == SEAL_PAYLOAD_SIZE + SEAL_REPACKED_FROM_SIZE + SEAL_POOL_ID_SIZE
                && payload[3] & !(SEAL_FLAG_REPACKED_FROM | SEAL_FLAG_POOL_ID) == 0
                && payload[3] & (SEAL_FLAG_REPACKED_FROM | SEAL_FLAG_POOL_ID)
                    == (SEAL_FLAG_REPACKED_FROM | SEAL_FLAG_POOL_ID) =>
        {
            Some(SegmentEvent::Seal(SealEvent {
                segment_id,
                tier: tier_from_u8(payload[0])?,
                ec_k: payload[1],
                ec_m: payload[2],
                merkle_root: HashOutput::from_bytes(payload[4..36].try_into().ok()?),
                data_wal_pos: DataWalPos {
                    file_seq: u32::from_le_bytes(payload[36..40].try_into().ok()?),
                    offset: u64::from_le_bytes(payload[40..48].try_into().ok()?),
                },
                repacked_from: Some(SegmentId::from_uuid_bytes(payload[48..64].try_into().ok()?)),
                pool_id: u32::from_le_bytes(payload[64..68].try_into().ok()?),
            }))
        }
        KIND_DELETE if payload.len() == DELETE_PAYLOAD_SIZE => {
            Some(SegmentEvent::Delete(DeleteEvent { segment_id }))
        }
        KIND_METADATA_REFRESH if payload.len() == REFRESH_PAYLOAD_SIZE && payload[0] == 0 => {
            Some(SegmentEvent::MetadataRefresh(MetadataRefreshEvent {
                segment_id,
                merkle_root: None,
                storage_locations: None,
            }))
        }
        KIND_METADATA_REFRESH
            if payload.len() == REFRESH_PAYLOAD_SIZE + REFRESH_ROOT_SIZE && payload[0] == 1 =>
        {
            Some(SegmentEvent::MetadataRefresh(MetadataRefreshEvent {
                segment_id,
                merkle_root: Some(HashOutput::from_bytes(payload[1..33].try_into().ok()?)),
                storage_locations: None,
            }))
        }
        // Extended refresh (ADR-0030): flags byte bit 1 = locations
        // present. Layout: [flags][merkle_root(32) if bit 0][count(1)]
        // [(len(1) + utf8)*count]. The payload length is bounds-checked
        // BEFORE any slice or index so a crafted (CRC-valid) short — or
        // empty — record is rejected as invalid, never panicked on.
        KIND_METADATA_REFRESH if !payload.is_empty() && payload[0] & 2 != 0 => {
            let locs_start = 1 + if payload[0] & 1 != 0 { REFRESH_ROOT_SIZE } else { 0 };
            if payload.len() < locs_start {
                return None;
            }
            let merkle_root = if payload[0] & 1 != 0 {
                Some(HashOutput::from_bytes(payload[1..33].try_into().ok()?))
            } else {
                None
            };
            if payload.len() < locs_start + 1 {
                return None;
            }
            let count = payload[locs_start] as usize;
            if count > REFRESH_MAX_LOCATIONS {
                return None;
            }
            let mut storage_locations = smallvec::SmallVec::<[oceanfs_core::NodeId; 16]>::new();
            let mut cursor = locs_start + 1;
            for _ in 0..count {
                if payload.len() < cursor + 1 {
                    return None;
                }
                let len = payload[cursor] as usize;
                cursor += 1;
                if payload.len() < cursor + len || len > REFRESH_MAX_NODE_ID_LEN {
                    return None;
                }
                let id = std::str::from_utf8(&payload[cursor..cursor + len]).ok()?;
                storage_locations.push(oceanfs_core::NodeId::new(id));
                cursor += len;
            }
            Some(SegmentEvent::MetadataRefresh(MetadataRefreshEvent {
                segment_id,
                merkle_root,
                storage_locations: Some(storage_locations),
            }))
        }
        _ => None,
    }
}

/// Event WAL file name for a sequence number (`evl_{seq:08}.log`).
fn evl_file_path(dir: &Path, seq: u32) -> PathBuf {
    dir.join(format!("evl_{seq:08}.log"))
}

/// Parses an event WAL file name, returning its sequence number.
fn parse_evl_name(name: &str) -> Option<u32> {
    let seq = name.strip_prefix("evl_")?.strip_suffix(".log")?;
    seq.parse::<u32>().ok()
}

/// Maximum file sequence number: the event WAL's file sequence is
/// `u32`, matching the on-disk `data_wal_pos`/`EventWalPos` framing.
///
/// The packed-position encoding reserves 48 bits for the in-file offset —
/// far beyond any realistic `event_wal_file_size_bytes` (default 64 MB;
/// 2^48 bytes per file would require a 256 TB file).
const PACKED_OFFSET_BITS: u64 = 48;
const PACKED_OFFSET_MASK: u64 = (1 << PACKED_OFFSET_BITS) - 1;

impl EventWalPos {
    /// Packs `(file_seq, offset)` into one u64 so a position can be
    /// published with a single atomic store (the event wal's `latest_pos`
    /// and the checkpoint's cached covered position).
    ///
    /// The file sequence dominates the high bits, so the packed value is
    /// monotonic in the same order as `EventWalPos` (file_seq, then
    /// offset).
    pub(crate) fn packed(self) -> u64 {
        ((self.file_seq as u64) << PACKED_OFFSET_BITS) | (self.offset & PACKED_OFFSET_MASK)
    }

    /// Unpacks a value written by [`packed`](Self::packed).
    pub(crate) fn from_packed(packed: u64) -> EventWalPos {
        EventWalPos {
            file_seq: (packed >> PACKED_OFFSET_BITS) as u32,
            offset: packed & PACKED_OFFSET_MASK,
        }
    }
}

/// The mutable append state of the event WAL (one writer at a time).
struct WriteState {
    /// Current event WAL file (append-only; never seeked or rewritten —
    /// perf 3.1).
    file: std::fs::File,
    /// Current file sequence number.
    file_seq: u32,
    /// Current byte position within the current file.
    position: u64,
}

/// The segment event WAL — a dedicated, append-only, checksummed log with
/// its **own** fsync group (ADR-0024 Decisions 1 & 4).
///
/// Plain files under the configured directory (`evl_{seq:08}.log`),
/// rotated at `event_wal_file_size_bytes`. Rotation never deletes:
/// retention/truncation is the checkpoint feature's job
/// (`event-wal-checkpoint`). Every [`append`](Self::append) is durable on
/// return through the event group's group-commit fsync.
///
/// # Examples
///
/// ```ignore
/// // Requires a tokio runtime and a temp directory; the unit tests in
/// // this module exercise the full lifecycle.
/// use oceanfs_core::{EventWalConfig, SegmentId, SizeTier};
/// use oceanfs_storage::segment::event_wal::{
///     EventWal, EventWalPos, ReserveEvent, SegmentEvent,
/// };
///
/// # #[tokio::main]
/// # async fn main() {
/// let dir = std::env::temp_dir().join("event-wal-example");
/// let wal = EventWal::open(dir.clone(), &EventWalConfig::default()).await.unwrap();
/// let evt = SegmentEvent::Reserve(ReserveEvent {
///     segment_id: SegmentId::new(),
///     tier: SizeTier::Standard,
///     ec_k: 4,
///     ec_m: 2,
/// });
/// let pos = wal.append(evt).await.unwrap();
/// let events: Vec<_> = wal
///     .read_from(EventWalPos { file_seq: 0, offset: 0 })
///     .collect::<Result<_>>()
///     .unwrap();
/// assert_eq!(events.len(), 1);
/// # }
/// ```
pub struct EventWal {
    /// Directory holding the `evl_{seq:08}.log` files.
    dir: PathBuf,
    /// Rotation and fsync-group configuration.
    config: EventWalConfig,
    /// Current file + sequence + position (append-only writer state).
    state: Arc<Mutex<WriteState>>,
    /// The latest write position `(file_seq, offset)` published as one
    /// packed atomic — the lock-free `latest_pos` read. A single store
    /// per append/rotation keeps it atomic: a reader never observes the
    /// file sequence and offset from different points in time (no
    /// transient regression across rotation).
    latest_packed: AtomicU64,
    /// The event log's **own** `WalSyncGroup` instance (ADR-0024
    /// Decision 4): its waiter list, batch window, and backpressure are
    /// fully independent of the data group's.
    sync_group: WalSyncGroup,
    /// Cumulative bytes written since open (sum of all on-disk file
    /// sizes) — the base of `bytes_since`.
    bytes_total: AtomicU64,
    /// Cumulative byte total at the start of each file:
    /// `(file_seq, cumulative_before)`, sorted by `file_seq`. Rebuilt at
    /// open from the directory; extended at rotation.
    file_bases: parking_lot::Mutex<Vec<(u32, u64)>>,
    /// Number of event WAL files (highest `file_seq` + 1) — the files
    /// gauge.
    file_count: AtomicU64,
    /// `oceanfs_event_wal_bytes` gauge.
    bytes_gauge: Gauge,
    /// `oceanfs_event_wal_files` gauge.
    files_gauge: Gauge,
    /// `oceanfs_event_wal_append_count` counter.
    append_counter: Counter,
    // Test seams (DoD "own fsync group" fault injection): the fields are
    // written by every fsync round / read by the cfg(test) accessors
    // below; the `#[allow(dead_code)]` covers non-test builds where the
    // fields are only ever cloned into the fsync closure.
    /// Number of fsync rounds performed by the event group (test
    /// observability of the batch window; one relaxed atomic increment
    /// per batch — perf 11.1). Shared with the fsync closure.
    #[allow(dead_code)]
    fsync_count: Arc<AtomicU64>,
    #[allow(dead_code)]
    stall_fsync: Arc<AtomicBool>,
}

impl EventWal {
    /// Opens (or resumes) the event WAL in `dir`.
    ///
    /// Creates the directory if absent and resumes at the end of the
    /// highest-numbered existing file. **Self-heals the torn tail**: a
    /// crash mid-record leaves a partial record at the end of the last
    /// file — it is truncated to the last good record boundary so
    /// subsequent appends start clean and the fold never sees a "torn
    /// record followed by valid records" state (which would be
    /// indistinguishable from mid-log corruption). Mid-log corruption is
    /// left untouched — the recovery fold aborts on it loudly.
    ///
    /// Assumes the log is quiescent (startup); concurrent opens of the
    /// same directory are not supported (single-writer).
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be created, the
    /// current file cannot be opened, or the tail scan fails.
    pub async fn open(dir: PathBuf, config: &EventWalConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&dir).await?;

        // Scan existing files, computing per-file cumulative bases and
        // the resume position (the end of the highest-numbered file).
        let mut files: Vec<(u32, u64)> = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(seq) = parse_evl_name(&name) {
                let size = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
                files.push((seq, size));
            }
        }
        files.sort_by_key(|(seq, _)| *seq);

        let max_seq = files.last().map(|(seq, _)| *seq).unwrap_or(0);
        let path = evl_file_path(&dir, max_seq);
        let file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;

        // Self-heal the torn tail of the last file (see the method
        // docs). The truncated size becomes the resume position and the
        // base for the cumulative byte accounting.
        let existing_size = Self::truncate_torn_tail(&dir, max_seq, file.metadata()?.len())?;
        if let Some((_, size)) = files.last_mut() {
            *size = existing_size;
        }

        let mut file_bases: Vec<(u32, u64)> = Vec::with_capacity(files.len());
        let mut cumulative: u64 = 0;
        for (seq, size) in &files {
            file_bases.push((*seq, cumulative));
            cumulative += size;
        }

        let state =
            Arc::new(Mutex::new(WriteState { file, file_seq: max_seq, position: existing_size }));
        let stall_fsync = Arc::new(AtomicBool::new(false));
        let fsync_count = Arc::new(AtomicU64::new(0));
        let sync_group = Self::create_sync_group(
            state.clone(),
            config.event_wal_fsync_batch_timeout_ms,
            stall_fsync.clone(),
            fsync_count.clone(),
        );

        Ok(Self {
            dir,
            config: config.clone(),
            state,
            latest_packed: AtomicU64::new(
                EventWalPos { file_seq: max_seq, offset: existing_size }.packed(),
            ),
            sync_group,
            bytes_total: AtomicU64::new(cumulative),
            file_bases: parking_lot::Mutex::new(file_bases),
            file_count: AtomicU64::new(max_seq as u64 + 1),
            fsync_count,
            bytes_gauge: Gauge::new(
                "oceanfs_event_wal_bytes".into(),
                "Cumulative bytes written to the segment event WAL".into(),
                LabelSet::empty(),
            ),
            files_gauge: Gauge::new(
                "oceanfs_event_wal_files".into(),
                "Number of segment event WAL files on disk".into(),
                LabelSet::empty(),
            ),
            append_counter: Counter::new(
                "oceanfs_event_wal_append_count".into(),
                "Number of segment lifecycle events appended to the event WAL".into(),
                LabelSet::empty(),
            ),
            stall_fsync,
        })
    }

    /// Truncates the torn tail of the last event WAL file, returning the
    /// truncated size.
    ///
    /// Scans the file's records; on a clean end or a torn record at the
    /// log tail, the file is truncated to the last good record boundary.
    /// On mid-log corruption ([`Error::CorruptEventLog`]) the file is
    /// left untouched (the recovery fold aborts on it loudly) and the
    /// original size is returned.
    fn truncate_torn_tail(dir: &Path, file_seq: u32, size: u64) -> Result<u64> {
        if size == 0 {
            return Ok(0);
        }
        let mut reader = EventWalReader {
            dir: dir.to_path_buf(),
            next: EventWalPos { file_seq, offset: 0 },
            file: None,
            done: false,
        };
        let mut last_good_end: u64 = 0;
        let mut outcome = None;
        while outcome.is_none() {
            match reader.next() {
                Some(Ok(_)) => last_good_end = reader.next.offset,
                Some(Err(Error::TornEventRecord { .. })) => outcome = Some(true),
                Some(Err(Error::CorruptEventLog { .. })) => {
                    // Mid-log corruption: leave the log untouched; the
                    // recovery fold aborts on it loudly.
                    return Ok(size);
                }
                Some(Err(e)) => return Err(e),
                None => outcome = Some(false),
            }
        }
        if outcome == Some(true) && last_good_end < size {
            let path = evl_file_path(dir, file_seq);
            let file = std::fs::OpenOptions::new().write(true).open(&path)?;
            file.set_len(last_good_end)?;
            // Make the truncation durable before any new append lands.
            file.sync_data()?;
            Ok(last_good_end)
        } else {
            Ok(size)
        }
    }

    /// Returns the directory holding the `evl_{seq:08}.log` files (the
    /// checkpoint manager's home — `checkpoint-*` files live beside
    /// them). Test-only accessor: production wiring passes the directory
    /// explicitly.
    #[cfg(test)]
    pub(crate) fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    /// Appends an event record; **durable on return** (group-commit
    /// fsync through the event group).
    ///
    /// Sequential-only I/O: the record is written at the current file
    /// position and the file is never seeked or rewritten (perf 3.1).
    /// Rotates to a new `evl_{seq:08}.log` when the record would cross
    /// `event_wal_file_size_bytes`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the write fails or the sync group shuts
    /// down.
    pub async fn append(&self, evt: SegmentEvent) -> Result<EventWalPos> {
        let data = evt.to_record_bytes();
        let record_size = data.len() as u64;

        let (file_seq, offset) = {
            let mut state = self.state.lock().await;
            if state.position + record_size > self.config.event_wal_file_size_bytes {
                self.rotate(&mut state).await?;
            }
            state.file.write_all(&data)?;
            state.file.flush()?;
            let written_offset = state.position;
            state.position += record_size;
            // Publish the new write position atomically (one store for
            // both fields — the lock-free `latest_pos` read).
            self.latest_packed.store(
                EventWalPos { file_seq: state.file_seq, offset: state.position }.packed(),
                Ordering::Release,
            );
            (state.file_seq, written_offset)
        };

        self.bytes_total.fetch_add(record_size, Ordering::Relaxed);

        // Register with the event group's group commit for batched fsync.
        let rx = self.sync_group.submit().await.map_err(|e| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                format!("event WAL sync group shut down: {e}"),
            ))
        })?;
        rx.await.map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "event WAL sync group dropped",
            ))
        })?;

        self.append_counter.inc();
        self.bytes_gauge.set(self.bytes_total.load(Ordering::Relaxed));
        Ok(EventWalPos { file_seq, offset })
    }

    /// Truncates the event log **before** `pos`: deletes every event
    /// file fully covered by `pos` (the checkpoint's coverage contract —
    /// events at/after `pos` are never touched, since the fold starts at
    /// `pos`).
    ///
    /// A file is fully covered when its sequence is below `pos.file_seq`
    /// (rotated away before the covered point), or when it is the
    /// **straddling** file (`seq == pos.file_seq`) whose size is at most
    /// `pos.offset` and which has already rotated (the writer moved on).
    /// The current write file is never deleted or trimmed: bytes beyond
    /// `pos.offset` in it are appends that landed after the snapshot was
    /// taken and must survive (the fold reads them from `pos`), and when
    /// it holds only covered bytes the trim would be a no-op — its
    /// covered prefix is reclaimed at the next rotation instead. In no
    /// case is a mid-file trim performed, because any byte at/after
    /// `pos.offset` belongs to an event the fold still needs.
    ///
    /// The writer's accounting is rebuilt under the write lock so
    /// `bytes_since` and `latest_pos` stay correct after the truncation:
    /// the cumulative file bases and the byte totals are recomputed from
    /// the surviving files, and the gauges follow.
    ///
    /// Returns the number of bytes removed (the checkpoint's
    /// `oceanfs_event_wal_truncated_bytes` input).
    pub(crate) async fn truncate_before(&self, pos: EventWalPos) -> Result<u64> {
        let state = self.state.lock().await;

        // Inventory the files (the dir scan happens under the write lock
        // so the accounting rebuild is consistent with the deletion).
        let mut files: Vec<(u32, u64)> = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&self.dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(seq) = parse_evl_name(&name) {
                let size = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
                files.push((seq, size));
            }
        }
        files.sort_by_key(|(seq, _)| *seq);

        // Delete every file fully covered by pos. The current write file
        // is never deleted — even by a mutated position past the covered
        // point (the writer holds it open; its covered prefix is
        // redundant but reclaimed at the next rotation).
        let mut removed: u64 = 0;
        for (seq, size) in &files {
            let fully_covered = *seq != state.file_seq
                && (*seq < pos.file_seq || (*seq == pos.file_seq && *size <= pos.offset));
            if fully_covered {
                tokio::fs::remove_file(evl_file_path(&self.dir, *seq)).await?;
                removed += size;
            }
        }

        // Rebuild the accounting from the on-disk state (the deletion
        // above is the source of truth).
        let mut remaining: Vec<(u32, u64)> = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&self.dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(seq) = parse_evl_name(&name) {
                let size = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
                remaining.push((seq, size));
            }
        }
        remaining.sort_by_key(|(seq, _)| *seq);

        let mut file_bases: Vec<(u32, u64)> = Vec::with_capacity(remaining.len());
        let mut cumulative: u64 = 0;
        for (seq, size) in &remaining {
            file_bases.push((*seq, cumulative));
            cumulative += size;
        }
        *self.file_bases.lock() = file_bases;
        self.bytes_total.store(cumulative, Ordering::Relaxed);
        self.file_count.store(remaining.len() as u64, Ordering::Relaxed);
        self.latest_packed.store(
            EventWalPos { file_seq: state.file_seq, offset: state.position }.packed(),
            Ordering::Release,
        );
        self.bytes_gauge.set(cumulative);
        self.files_gauge.set(remaining.len() as u64);
        Ok(removed)
    }

    /// Returns the position where the next record will be written.
    ///
    /// Lock-free: decodes the atomically-published `(file_seq, offset)`
    /// pair. The pair is published with a single store, so a read never
    /// mixes the file sequence and offset from different points in time
    /// (monotonic even across rotation). After
    /// [`append`](Self::append) returns, `latest_pos` is at or past that
    /// record's end.
    pub fn latest_pos(&self) -> EventWalPos {
        EventWalPos::from_packed(self.latest_packed.load(Ordering::Acquire))
    }

    /// Returns an iterator over the records at or after `pos`, in append
    /// order.
    ///
    /// Stops with [`Error::TornEventRecord`] at the **first** bad record
    /// (bad magic, unsupported version, truncated header/payload, or CRC
    /// mismatch) — the stop-at-first-bad-tail semantics the recovery
    /// fold consumes. Records are never silently skipped or truncated
    /// mid-file. A clean end-of-file at a record boundary advances to the
    /// next file; when no next file exists the iterator ends.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Requires a tokio runtime (open is async); the unit tests in
    /// // this module exercise the reader against live logs.
    /// use oceanfs_core::EventWalConfig;
    /// use oceanfs_storage::segment::event_wal::{EventWal, EventWalPos};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let dir = std::env::temp_dir().join("event-wal-reader-example");
    /// let wal = EventWal::open(dir, &EventWalConfig::default()).await.unwrap();
    /// let reader = wal.read_from(EventWalPos { file_seq: 0, offset: 0 });
    /// for record in reader {
    ///     let (pos, evt) = record.unwrap();
    ///     println!("{pos:?}: {evt:?}");
    /// }
    /// # }
    /// ```
    pub fn read_from(&self, pos: EventWalPos) -> EventWalReader {
        EventWalReader { dir: self.dir.clone(), next: pos, file: None, done: false }
    }

    /// Returns the number of bytes appended to the event log since
    /// `pos`.
    ///
    /// The checkpoint feature's trigger input (ADR-0024 Decision 3): the
    /// event log is checkpointed before it can grow past
    /// `event_wal_checkpoint_bytes` of replayable data. Valid for
    /// positions returned by `append`/`read_from` since this instance
    /// opened (or resumed); for positions in files rotated away before
    /// this open the base falls back to 0 (all bytes count).
    pub fn bytes_since(&self, pos: EventWalPos) -> u64 {
        let total = self.bytes_total.load(Ordering::Relaxed);
        let base = self.base_before(pos.file_seq).unwrap_or(0);
        total.saturating_sub(base + pos.offset)
    }

    /// Registers the event WAL metrics with a metrics registrar.
    ///
    /// `oceanfs_event_wal_bytes`, `oceanfs_event_wal_files`,
    /// `oceanfs_event_wal_append_count` (perf 11.1 — atomic counters, no
    /// lock on the append path beyond the write mutex itself).
    pub fn register_metrics(&self, registrar: &dyn MetricRegistrar) {
        registrar.register_gauge(self.bytes_gauge.clone());
        registrar.register_gauge(self.files_gauge.clone());
        registrar.register_counter(self.append_counter.clone());
    }

    /// Rotates to the next file: syncs the current file, opens
    /// `evl_{seq+1:08}.log`, records the new file's cumulative base.
    ///
    /// Rotation never deletes old files — retention is the checkpoint
    /// feature's job (`event-wal-checkpoint`).
    async fn rotate(&self, state: &mut WriteState) -> Result<()> {
        // The previous file must be durable before appends move on (the
        // old file is sealed by this barrier; the reader advances across
        // rotation by file sequence).
        state.file.sync_all()?;

        let new_seq = state.file_seq + 1;
        let path = evl_file_path(&self.dir, new_seq);
        let file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;

        // The base of the new file is the cumulative byte total of all
        // prior files (recorded BEFORE this file receives any writes).
        let total_before = self.bytes_total.load(Ordering::Relaxed);
        self.file_bases.lock().push((new_seq, total_before));

        state.file = file;
        state.file_seq = new_seq;
        state.position = 0;

        // Publish the new file atomically (one store for both fields —
        // a concurrent `latest_pos` never sees the old sequence with
        // offset 0, which would regress below earlier positions).
        self.latest_packed
            .store(EventWalPos { file_seq: new_seq, offset: 0 }.packed(), Ordering::Release);
        let file_count = new_seq as u64 + 1;
        self.file_count.store(file_count, Ordering::Relaxed);
        self.files_gauge.set(file_count);
        Ok(())
    }

    /// Cumulative byte total at the start of `file_seq`'s file (the sum
    /// of all prior files' sizes), or `None` when the position predates
    /// every recorded base.
    fn base_before(&self, file_seq: u32) -> Option<u64> {
        let bases = self.file_bases.lock();
        // The bases are sorted by file_seq; the last base at or before
        // `file_seq` is the cumulative total before that file.
        bases.iter().rev().find(|(seq, _)| *seq <= file_seq).map(|(_, base)| *base)
    }

    /// Creates the event log's **own** `WalSyncGroup` instance
    /// (ADR-0024 Decision 4).
    ///
    /// The closure syncs the current event file. Unlike the data WAL's
    /// closure (which `try_lock`s and skips a busy batch), this closure
    /// **blocks** on the write mutex inside `spawn_blocking`: an event
    /// append must be durable on return (the DoD's "durable on return"
    /// contract), and the write lock is held for microseconds (write_all
    /// + flush), so blocking is safe and closes the skip-wake gap.
    fn create_sync_group(
        state: Arc<Mutex<WriteState>>,
        batch_timeout_ms: u64,
        stall_fsync: Arc<AtomicBool>,
        fsync_count: Arc<AtomicU64>,
    ) -> WalSyncGroup {
        WalSyncGroup::new(
            move || {
                let state = Arc::clone(&state);
                let stall = Arc::clone(&stall_fsync);
                let fsync_count = Arc::clone(&fsync_count);
                async move {
                    if stall.load(Ordering::Acquire) {
                        // Test seam: an injected blocking fsync proves
                        // the event group is independent of the data
                        // group (DoD "own fsync group").
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                    let result = tokio::task::spawn_blocking(move || {
                        // Block (not try_lock): an append must be durable
                        // on return; the write lock is held only for
                        // write_all + flush.
                        let guard = state.blocking_lock();
                        guard.file.sync_data().map_err(|e| {
                            crate::error::Error::Io(std::io::Error::new(
                                e.kind(),
                                format!("event WAL fsync failed: {e}"),
                            ))
                        })
                    })
                    .await;
                    fsync_count.fetch_add(1, Ordering::Relaxed);
                    match result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(join_err) => Err(crate::error::Error::Io(std::io::Error::other(
                            format!("event WAL fsync join error: {join_err}"),
                        ))),
                    }
                }
            },
            batch_timeout_ms,
            DEFAULT_EVENT_SYNC_MAX_WAITERS,
        )
    }

    /// Number of fsync rounds performed by the event group — test
    /// observability for the batch-window invariant (the group commit
    /// must batch concurrent appends into few fsyncs).
    #[cfg(test)]
    pub(crate) fn fsync_count_for_test(&self) -> u64 {
        self.fsync_count.load(Ordering::Relaxed)
    }

    /// Test seam: makes the event group's fsync closure block for 5
    /// seconds before syncing — an injected stalled fsync.
    #[cfg(test)]
    pub(crate) fn set_stall_fsync_for_test(&self, stall: bool) {
        self.stall_fsync.store(stall, Ordering::Release);
    }
}

/// Iterator over event records at or after a position
/// ([`EventWal::read_from`]).
///
/// `Item = Result<(EventWalPos, SegmentEvent)>`: yields the position of
/// each record alongside the decoded event. Stops with
/// [`Error::TornEventRecord`] at the first bad record (stop-at-first-bad-
/// tail semantics); a clean end-of-file at a record boundary advances to
/// the next file and ends when no next file exists.
pub struct EventWalReader {
    /// Directory holding the `evl_{seq:08}.log` files.
    dir: PathBuf,
    /// Position of the next record to read.
    next: EventWalPos,
    /// Open file for the current sequence.
    file: Option<(u32, std::fs::File)>,
    /// Whether the iterator has terminated (clean end or torn record).
    done: bool,
}

impl Iterator for EventWalReader {
    type Item = Result<(EventWalPos, SegmentEvent)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // Open the file for the current sequence when not already open,
        // and position the cursor at the record boundary.
        let needs_open = match &self.file {
            Some((seq, _)) => *seq != self.next.file_seq,
            None => true,
        };
        if needs_open {
            let path = evl_file_path(&self.dir, self.next.file_seq);
            match std::fs::OpenOptions::new().read(true).open(&path) {
                Ok(mut file) => {
                    if let Err(io) = file.seek(SeekFrom::Start(self.next.offset)) {
                        return Some(Err(Error::Io(io)));
                    }
                    self.file = Some((self.next.file_seq, file));
                }
                Err(_) => {
                    // The file may have been deleted by a checkpoint
                    // truncation (its events are covered): skip to the
                    // next existing file, whose events are at/after the
                    // requested position.
                    if self.skip_to_next_existing_file() {
                        return self.next();
                    }
                    // No such file and none after it: clean end of the log.
                    self.done = true;
                    return None;
                }
            }
        }
        let file = match self.file.as_mut() {
            Some((_, file)) => file,
            None => {
                self.done = true;
                return None;
            }
        };

        // Read the fixed 28-byte header.
        let mut header = [0u8; EVENT_RECORD_HEADER_SIZE];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Distinguish a clean record boundary (0 bytes read: the
                // file ends exactly here) from a torn tail (a partial
                // header: the crash cut the record mid-write).
                let pos_after = match file.stream_position() {
                    Ok(p) => p,
                    Err(io) => return Some(Err(Error::Io(io))),
                };
                if pos_after == self.next.offset {
                    self.advance_to_next_file();
                    return self.next();
                }
                // A partial header consumes to the file end — nothing
                // valid can follow it (the reader hit EOF).
                return self.bad_record("truncated record header", pos_after);
            }
            Err(e) => return Some(Err(Error::Io(e))),
        }

        if header[0..4] != EVENT_RECORD_MAGIC {
            // A full header with bad magic is disk corruption, never a
            // crash window (a torn write leaves a truncated header).
            // Classify by the minimum record size (28 header + 0 payload
            // + 4 crc = 32): bytes beyond it mean valid data follows.
            return self.bad_record("bad record magic", self.next.offset + 32);
        }
        if header[4] != EVENT_RECORD_VERSION {
            return self.bad_record("unsupported record version", self.next.offset + 32);
        }
        let kind = header[5];
        let payload_len = u32::from_le_bytes(header[8..12].try_into().ok()?) as usize;
        if payload_len > MAX_PAYLOAD_SIZE {
            return self.bad_record("implausible payload length", self.next.offset + 32);
        }

        // Read payload + crc32 (bounded by MAX_PAYLOAD_SIZE + 4).
        let mut tail = vec![0u8; payload_len + 4];
        match file.read_exact(&mut tail) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // A partial payload consumes to the file end — nothing
                // valid can follow it (the reader hit EOF).
                let end = match file.stream_position() {
                    Ok(p) => p,
                    Err(io) => return Some(Err(Error::Io(io))),
                };
                return self.bad_record("truncated record payload", end);
            }
            Err(e) => return Some(Err(Error::Io(e))),
        }

        // CRC over all preceding bytes (header + payload). The record's
        // exact end is known (28 + payload_len + 4) — bytes beyond it
        // mean a valid record follows (mid-log corruption).
        let stored_crc = u32::from_le_bytes(tail[payload_len..payload_len + 4].try_into().ok()?);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header);
        hasher.update(&tail[..payload_len]);
        if stored_crc != hasher.finalize() {
            return self.bad_record(
                "checksum mismatch",
                self.next.offset + (EVENT_RECORD_HEADER_SIZE + payload_len + 4) as u64,
            );
        }

        let segment_id = SegmentId::from_uuid_bytes(header[12..28].try_into().ok()?);
        let evt = match decode_payload(kind, segment_id, &tail[..payload_len]) {
            Some(evt) => evt,
            None => {
                return self.bad_record(
                    "invalid event payload",
                    self.next.offset + (EVENT_RECORD_HEADER_SIZE + payload_len + 4) as u64,
                );
            }
        };

        let record_pos = self.next;
        self.next.offset += (EVENT_RECORD_HEADER_SIZE + payload_len + 4) as u64;
        Some(Ok((record_pos, evt)))
    }
}

impl EventWalReader {
    /// Classifies a bad record as a torn tail or mid-log corruption and
    /// terminates the iteration with the corresponding error.
    ///
    /// `record_end_offset` is the offset (within the current file) where
    /// the bad record ends: for truncated records it is the file end
    /// (nothing valid can follow); for fully-present records it is the
    /// record's exact end. When valid bytes exist after that point —
    /// more data in the current file or any later file — the log
    /// continues past the bad record, which is disk corruption
    /// ([`Error::CorruptEventLog`]), not a crash window. Otherwise the
    /// bad record is the log tail ([`Error::TornEventRecord`] — the
    /// recovery fold stops at the last good record).
    fn bad_record(
        &mut self,
        detail: &'static str,
        record_end_offset: u64,
    ) -> Option<Result<(EventWalPos, SegmentEvent)>> {
        self.done = true;
        let pos = self.next;
        let after_here = match &self.file {
            Some((_, file)) => {
                file.metadata().map(|m| m.len()).unwrap_or(0).saturating_sub(record_end_offset)
            }
            None => 0,
        };
        let after_later = self.bytes_in_later_files(pos.file_seq);
        if after_here + after_later > 0 {
            Some(Err(Error::CorruptEventLog { pos, detail }))
        } else {
            Some(Err(Error::TornEventRecord { pos, detail }))
        }
    }

    /// Total bytes stored in event WAL files with a sequence greater
    /// than `file_seq` (the tail-vs-mid-log classification input).
    fn bytes_in_later_files(&self, file_seq: u32) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Some(seq) = parse_evl_name(&name) {
                    if seq > file_seq {
                        total += entry.metadata().map(|m| m.len()).unwrap_or(0);
                    }
                }
            }
        }
        total
    }

    /// Advances to the next file sequence; ends the iteration when no
    /// later file exists (gaps — files deleted by checkpoint truncation —
    /// are skipped).
    fn advance_to_next_file(&mut self) {
        if !self.skip_to_next_existing_file() {
            self.done = true;
            self.file = None;
        }
    }

    /// Skips the iterator to the smallest existing event file with a
    /// sequence greater than the current one, starting at offset 0.
    /// Returns `false` when no such file exists.
    fn skip_to_next_existing_file(&mut self) -> bool {
        let after_seq = self.next.file_seq;
        let Ok(entries) = std::fs::read_dir(&self.dir) else { return false };
        let mut next_seq: Option<u32> = None;
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(seq) = parse_evl_name(&name) {
                if seq > after_seq && next_seq.map(|n| seq < n).unwrap_or(true) {
                    next_seq = Some(seq);
                }
            }
        }
        let Some(seq) = next_seq else { return false };
        let path = evl_file_path(&self.dir, seq);
        match std::fs::OpenOptions::new().read(true).open(&path) {
            Ok(file) => {
                self.file = Some((seq, file));
                self.next = EventWalPos { file_seq: seq, offset: 0 };
                true
            }
            Err(_) => false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{
        io::SeekFrom,
        sync::atomic::{AtomicU32, Ordering as AtomicOrdering},
    };

    use oceanfs_core::WalConfig;

    use super::*;
    use crate::wal::WalSyncGroup;

    /// Record sizes used by rotation tests (bytes):
    /// reserve = 36, seal = 80, delete = 32.
    const RESERVE_RECORD_SIZE: u64 = 36;
    const SEAL_RECORD_SIZE: u64 = 80;
    const DELETE_RECORD_SIZE: u64 = 32;

    fn test_config(dir: &Path) -> EventWalConfig {
        EventWalConfig {
            event_wal_dir: dir.to_path_buf(),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024 * 1024,
        }
    }

    fn reserve_event(id: SegmentId) -> SegmentEvent {
        SegmentEvent::Reserve(ReserveEvent {
            segment_id: id,
            tier: SizeTier::Standard,
            ec_k: 4,
            ec_m: 2,
        })
    }

    fn seal_event(id: SegmentId) -> SegmentEvent {
        SegmentEvent::Seal(SealEvent {
            pool_id: 0,
            segment_id: id,
            tier: SizeTier::Standard,
            ec_k: 4,
            ec_m: 2,
            merkle_root: HashOutput::from_bytes([0xAB; 32]),
            data_wal_pos: DataWalPos { file_seq: 3, offset: 4096 },
            repacked_from: None,
        })
    }

    fn seal_event_with_repacked(id: SegmentId, old: SegmentId) -> SegmentEvent {
        SegmentEvent::Seal(SealEvent {
            pool_id: 0,
            segment_id: id,
            tier: SizeTier::Standard,
            ec_k: 4,
            ec_m: 2,
            merkle_root: HashOutput::from_bytes([0xAB; 32]),
            data_wal_pos: DataWalPos { file_seq: 3, offset: 4096 },
            repacked_from: Some(old),
        })
    }

    fn delete_event(id: SegmentId) -> SegmentEvent {
        SegmentEvent::Delete(DeleteEvent { segment_id: id })
    }

    fn refresh_event(id: SegmentId) -> SegmentEvent {
        SegmentEvent::MetadataRefresh(MetadataRefreshEvent {
            segment_id: id,
            merkle_root: Some(HashOutput::from_bytes([0xCD; 32])),
            storage_locations: None,
        })
    }

    async fn open_wal(dir: &Path) -> EventWal {
        EventWal::open(dir.to_path_buf(), &test_config(dir)).await.unwrap()
    }

    // ------------------------------------------------------------------
    // Record encoding / decoding
    // ------------------------------------------------------------------

    #[test]
    fn record_roundtrip_is_byte_exact() {
        let id = SegmentId::new();
        for evt in [reserve_event(id), seal_event(id), delete_event(id), refresh_event(id)] {
            let bytes = evt.to_record_bytes();
            let decoded = SegmentEvent::from_record_bytes(&bytes).expect("record decodes");
            assert_eq!(decoded, evt, "record must round-trip byte-exact");
        }
    }

    /// ADR-0029 f5: the seal event's pool_id survives the wire format.
    #[test]
    fn seal_event_pool_id_roundtrips() {
        let id = SegmentId::new();
        let evt = SegmentEvent::Seal(SealEvent {
            pool_id: 3,
            ..match seal_event(id) {
                SegmentEvent::Seal(e) => e,
                _ => unreachable!(),
            }
        });
        let bytes = evt.to_record_bytes();
        let decoded = SegmentEvent::from_record_bytes(&bytes).expect("record decodes");
        assert_eq!(decoded, evt, "pool_id must round-trip");
    }

    /// ADR-0029 f5: seal events carrying `repacked_from` + pool_id both
    /// survive (the longest payload variant).
    #[test]
    fn seal_event_repacked_with_pool_id_roundtrips() {
        let id = SegmentId::new();
        let old = SegmentId::new();
        let mut evt = seal_event_with_repacked(id, old);
        if let SegmentEvent::Seal(seal) = &mut evt {
            seal.pool_id = 7;
        }
        let bytes = evt.to_record_bytes();
        let decoded = SegmentEvent::from_record_bytes(&bytes).expect("record decodes");
        assert_eq!(decoded, evt);
    }

    /// ADR-0029 f5: legacy seal records (written before pool_id existed —
    /// the 48-byte payload without the pool flag) decode with pool_id 0,
    /// and pool_id-0 records stay byte-identical to the pre-f5 format.
    #[test]
    fn legacy_seal_record_decodes_pool_id_zero() {
        let id = SegmentId::new();
        // A pool_id-0 event serializes exactly like the pre-f5 format.
        let evt = seal_event(id);
        let bytes = evt.to_record_bytes();
        // 28 header + 48 payload + 4 crc = 80 — the legacy record size.
        assert_eq!(bytes.len(), EVENT_RECORD_HEADER_SIZE + SEAL_PAYLOAD_SIZE + 4);
        let decoded = SegmentEvent::from_record_bytes(&bytes).expect("legacy record decodes");
        match decoded {
            SegmentEvent::Seal(seal) => {
                assert_eq!(seal.pool_id, 0, "legacy records default to the legacy root");
                assert_eq!(seal.repacked_from, None);
            }
            other => panic!("expected Seal, got {other:?}"),
        }
    }

    #[test]
    fn record_sizes_match_the_framing_doc() {
        assert_eq!(
            reserve_event(SegmentId::new()).to_record_bytes().len() as u64,
            RESERVE_RECORD_SIZE
        );
        assert_eq!(seal_event(SegmentId::new()).to_record_bytes().len() as u64, SEAL_RECORD_SIZE);
        assert_eq!(
            delete_event(SegmentId::new()).to_record_bytes().len() as u64,
            DELETE_RECORD_SIZE
        );
        // MetadataRefresh with a root: 28 header + 1 + 32 + 4 crc = 65.
        assert_eq!(
            refresh_event(SegmentId::new()).to_record_bytes().len(),
            EVENT_RECORD_HEADER_SIZE + REFRESH_PAYLOAD_SIZE + REFRESH_ROOT_SIZE + 4
        );
    }

    #[test]
    fn metadata_refresh_roundtrip_preserves_the_anchor() {
        let id = SegmentId::new();
        let with_root = refresh_event(id);
        let decoded =
            SegmentEvent::from_record_bytes(&with_root.to_record_bytes()).expect("record decodes");
        assert_eq!(decoded, with_root);
        // The invalidating form (merkle_root: None) round-trips too.
        let invalidate = SegmentEvent::MetadataRefresh(MetadataRefreshEvent {
            segment_id: id,
            merkle_root: None,
            storage_locations: None,
        });
        let decoded =
            SegmentEvent::from_record_bytes(&invalidate.to_record_bytes()).expect("record decodes");
        assert_eq!(decoded, invalidate);
    }

    /// ADR-0030: the extended MetadataRefresh carrying a
    /// `storage_locations` set round-trips byte-exact, and the legacy
    /// anchor-only records (no locations section) still decode with
    /// `storage_locations: None`.
    #[test]
    fn metadata_refresh_with_locations_roundtrips() {
        let id = SegmentId::new();
        let mut locations = smallvec::SmallVec::<[oceanfs_core::NodeId; 16]>::new();
        locations.push(oceanfs_core::NodeId::new("node-b"));
        locations.push(oceanfs_core::NodeId::new("node-c"));
        let evt = SegmentEvent::MetadataRefresh(MetadataRefreshEvent {
            segment_id: id,
            merkle_root: Some(HashOutput::from_bytes([0xCD; 32])),
            storage_locations: Some(locations),
        });
        let bytes = evt.to_record_bytes();
        let decoded = SegmentEvent::from_record_bytes(&bytes).expect("extended record decodes");
        assert_eq!(decoded, evt, "locations + root must round-trip byte-exact");
    }

    /// ADR-0030: an extended refresh WITHOUT a merkle root (locations
    /// only) round-trips too.
    #[test]
    fn metadata_refresh_locations_only_roundtrips() {
        let id = SegmentId::new();
        let mut locations = smallvec::SmallVec::<[oceanfs_core::NodeId; 16]>::new();
        locations.push(oceanfs_core::NodeId::new("node-x"));
        let evt = SegmentEvent::MetadataRefresh(MetadataRefreshEvent {
            segment_id: id,
            merkle_root: None,
            storage_locations: Some(locations),
        });
        let decoded =
            SegmentEvent::from_record_bytes(&evt.to_record_bytes()).expect("record decodes");
        assert_eq!(decoded, evt);
    }

    /// ADR-0030: a corrupt extended refresh (node id claims more bytes
    /// than the record holds) is rejected by the decoder — the reader's
    /// length discipline.
    #[test]
    fn metadata_refresh_rejects_overlong_node_id() {
        let id = SegmentId::new();
        // flags(3 = root + locations) + root(32) + count(1) + len(1).
        // The len byte claims 255 bytes but the record holds none — the
        // decoder must bound-check and reject (no OOB read).
        let payload = vec![0u8; 1 + 32 + 1 + 1];
        let mut payload = payload;
        payload[0] = 3; // merkle + locations
        payload[33] = 1; // count = 1
        payload[34] = 255; // node id length claims 255 bytes — absent
        let mut buf = Vec::with_capacity(28 + payload.len() + 4);
        buf.extend_from_slice(&EVENT_RECORD_MAGIC);
        buf.push(EVENT_RECORD_VERSION);
        buf.push(KIND_METADATA_REFRESH);
        buf.extend_from_slice(&[0u8; 2]);
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(id.as_uuid().as_bytes());
        buf.extend_from_slice(&payload);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        assert!(SegmentEvent::from_record_bytes(&buf).is_none(), "over-long id must be rejected");
    }

    /// ADR-0030: a CRC-valid metadata-refresh record with an EMPTY
    /// payload is rejected as invalid — the extended-refresh decoder
    /// must not index `payload[0]` before checking the length (a crafted
    /// or legacy zero-length record must surface the bad-record path,
    /// never a panic).
    #[test]
    fn metadata_refresh_empty_payload_is_rejected_not_panicked_on() {
        let id = SegmentId::new();
        let mut buf = Vec::with_capacity(EVENT_RECORD_HEADER_SIZE + 4);
        buf.extend_from_slice(&EVENT_RECORD_MAGIC);
        buf.push(EVENT_RECORD_VERSION);
        buf.push(KIND_METADATA_REFRESH);
        buf.extend_from_slice(&[0u8; 2]);
        buf.extend_from_slice(&0u32.to_le_bytes()); // payload_len = 0
        buf.extend_from_slice(id.as_uuid().as_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        assert!(SegmentEvent::from_record_bytes(&buf).is_none(), "empty payload must be rejected");
    }

    #[test]
    fn seal_roundtrip_preserves_merkle_root_and_data_wal_pos() {
        let evt = seal_event(SegmentId::new());
        let decoded =
            SegmentEvent::from_record_bytes(&evt.to_record_bytes()).expect("record decodes");
        match decoded {
            SegmentEvent::Seal(seal) => {
                assert_eq!(seal.merkle_root, HashOutput::from_bytes([0xAB; 32]));
                assert_eq!(seal.data_wal_pos, DataWalPos { file_seq: 3, offset: 4096 });
                assert_eq!(seal.tier, SizeTier::Standard);
                assert_eq!(seal.ec_k, 4);
                assert_eq!(seal.ec_m, 2);
                assert_eq!(seal.repacked_from, None);
            }
            other => panic!("expected seal event, got {other:?}"),
        }
    }

    #[test]
    fn seal_roundtrip_preserves_the_compaction_repacked_from_marker() {
        let id = SegmentId::new();
        let old = SegmentId::new();
        let evt = seal_event_with_repacked(id, old);
        let bytes = evt.to_record_bytes();
        // The marked payload is exactly SEAL_REPACKED_FROM_SIZE longer.
        assert_eq!(
            bytes.len(),
            EVENT_RECORD_HEADER_SIZE + SEAL_PAYLOAD_SIZE + SEAL_REPACKED_FROM_SIZE + 4,
            "the repacked_from marker must extend the record by 16 bytes"
        );
        let decoded = SegmentEvent::from_record_bytes(&bytes).expect("record decodes");
        assert_eq!(decoded, evt, "the marker must round-trip byte-exact");
        match decoded {
            SegmentEvent::Seal(seal) => assert_eq!(seal.repacked_from, Some(old)),
            other => panic!("expected seal event, got {other:?}"),
        }
    }

    #[test]
    fn unmarked_seal_records_still_decode_with_the_marker_absent() {
        // The payload layout before the compaction marker existed
        // (flags byte = 0, 48-byte payload) must decode unchanged — old
        // records in an existing event log are readable.
        let id = SegmentId::new();
        let mut payload = Vec::with_capacity(SEAL_PAYLOAD_SIZE);
        payload.push(tier_to_u8(SizeTier::Standard));
        payload.push(4);
        payload.push(2);
        payload.push(0); // flags — no repacked_from
        payload.extend_from_slice(&[0xAB; 32]);
        payload.extend_from_slice(&3u32.to_le_bytes());
        payload.extend_from_slice(&4096u64.to_le_bytes());
        let mut buf = Vec::with_capacity(EVENT_RECORD_HEADER_SIZE + payload.len() + 4);
        buf.extend_from_slice(&EVENT_RECORD_MAGIC);
        buf.push(EVENT_RECORD_VERSION);
        buf.push(KIND_SEAL);
        buf.extend_from_slice(&[0u8; 2]);
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(id.as_uuid().as_bytes());
        buf.extend_from_slice(&payload);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        let decoded = SegmentEvent::from_record_bytes(&buf).expect("legacy record decodes");
        match decoded {
            SegmentEvent::Seal(seal) => assert_eq!(seal.repacked_from, None),
            other => panic!("expected seal event, got {other:?}"),
        }
    }

    #[test]
    fn single_flipped_byte_fails_crc() {
        let evt = seal_event(SegmentId::new());
        let mut bytes = evt.to_record_bytes();
        // Flip one byte in the payload (merkle root area) — CRC must fail.
        let idx = EVENT_RECORD_HEADER_SIZE + 10;
        bytes[idx] ^= 0xFF;
        assert!(
            SegmentEvent::from_record_bytes(&bytes).is_none(),
            "CRC must catch a single flipped byte"
        );
    }

    #[test]
    fn single_flipped_byte_in_header_fails_crc() {
        let evt = reserve_event(SegmentId::new());
        let mut bytes = evt.to_record_bytes();
        bytes[12] ^= 0x01; // segment_id byte
        assert!(SegmentEvent::from_record_bytes(&bytes).is_none());
    }

    #[test]
    fn wrong_magic_is_rejected() {
        let evt = reserve_event(SegmentId::new());
        let mut bytes = evt.to_record_bytes();
        bytes[0] = b'X';
        assert!(SegmentEvent::from_record_bytes(&bytes).is_none());
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let evt = reserve_event(SegmentId::new());
        let mut bytes = evt.to_record_bytes();
        bytes[5] = 0x7F;
        // Recompute the CRC so the failure is attributable to the kind.
        let crc = crc32fast::hash(&bytes[..bytes.len() - 4]);
        let crc_bytes = crc.to_le_bytes();
        let crc_at = bytes.len() - 4;
        bytes[crc_at..].copy_from_slice(&crc_bytes);
        assert!(SegmentEvent::from_record_bytes(&bytes).is_none());
    }

    #[test]
    fn truncated_record_is_rejected() {
        let evt = seal_event(SegmentId::new());
        let bytes = evt.to_record_bytes();
        for cut in [bytes.len() - 1, EVENT_RECORD_HEADER_SIZE + 4, EVENT_RECORD_HEADER_SIZE - 1] {
            assert!(
                SegmentEvent::from_record_bytes(&bytes[..cut]).is_none(),
                "truncation at {cut} must be rejected"
            );
        }
    }

    #[test]
    fn unknown_tier_byte_is_rejected() {
        let evt = reserve_event(SegmentId::new());
        let mut bytes = evt.to_record_bytes();
        bytes[EVENT_RECORD_HEADER_SIZE] = 0x42; // tier byte
        let crc = crc32fast::hash(&bytes[..bytes.len() - 4]);
        let crc_bytes = crc.to_le_bytes();
        let crc_at = bytes.len() - 4;
        bytes[crc_at..].copy_from_slice(&crc_bytes);
        assert!(SegmentEvent::from_record_bytes(&bytes).is_none());
    }

    // ------------------------------------------------------------------
    // Append / read round trip
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn append_read_round_trip_returns_three_events_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        let id = SegmentId::new();

        let p1 = wal.append(reserve_event(id)).await.unwrap();
        let p2 = wal.append(seal_event(id)).await.unwrap();
        let p3 = wal.append(delete_event(id)).await.unwrap();
        assert!(p1 < p2 && p2 < p3, "positions must be monotonic: {p1:?} < {p2:?} < {p3:?}");

        let events: Vec<(EventWalPos, SegmentEvent)> =
            wal.read_from(EventWalPos { file_seq: 0, offset: 0 }).collect::<Result<_>>().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].0, p1);
        assert_eq!(events[1].0, p2);
        assert_eq!(events[2].0, p3);
        assert_eq!(events[0].1, reserve_event(id));
        assert_eq!(events[1].1, seal_event(id));
        assert_eq!(events[2].1, delete_event(id));
    }

    #[tokio::test]
    async fn read_from_mid_log_position_yields_remaining_records() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        let id = SegmentId::new();
        wal.append(reserve_event(id)).await.unwrap();
        let p2 = wal.append(seal_event(id)).await.unwrap();

        let events: Vec<(EventWalPos, SegmentEvent)> =
            wal.read_from(p2).collect::<Result<_>>().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1, seal_event(id));
    }

    #[tokio::test]
    async fn append_positions_are_monotonic_across_concurrent_appends() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(open_wal(dir.path()).await);
        let mut handles = Vec::new();
        for _ in 0..16 {
            let wal = Arc::clone(&wal);
            handles.push(tokio::spawn(async move {
                let id = SegmentId::new();
                let mut positions = Vec::new();
                for _ in 0..8 {
                    positions.push(wal.append(reserve_event(id)).await.unwrap());
                }
                positions
            }));
        }
        let mut all: Vec<EventWalPos> = Vec::new();
        for handle in handles {
            all.extend(handle.await.unwrap());
        }
        all.sort();
        let mut prev = EventWalPos { file_seq: 0, offset: 0 };
        for pos in &all {
            assert!(pos >= &prev, "positions must be strictly monotonic across appends");
            prev = *pos;
        }
        // Every position must be distinct.
        let distinct: std::collections::HashSet<EventWalPos> = all.iter().copied().collect();
        assert_eq!(distinct.len(), all.len(), "each append must return a unique position");
    }

    #[tokio::test]
    async fn latest_pos_tracks_append_tail() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        assert_eq!(wal.latest_pos(), EventWalPos { file_seq: 0, offset: 0 });
        let pos = wal.append(reserve_event(SegmentId::new())).await.unwrap();
        let latest = wal.latest_pos();
        assert!(latest >= pos, "latest_pos must be at/past the last append's end");
        assert_eq!(latest.file_seq, pos.file_seq);
    }

    #[tokio::test]
    async fn latest_pos_never_regresses_across_concurrent_rotation() {
        // Rotation publishes (file_seq, offset) with ONE atomic store; a
        // concurrent lock-free reader must never observe a position
        // below one it already saw (the old sequence with offset 0).
        let dir = tempfile::tempdir().unwrap();
        let config = EventWalConfig {
            event_wal_dir: dir.path().to_path_buf(),
            event_wal_file_size_bytes: 64, // rotate on every seal
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024,
        };
        let wal = Arc::new(EventWal::open(dir.path().to_path_buf(), &config).await.unwrap());

        // Reader task: samples latest_pos continuously.
        let reader_wal = Arc::clone(&wal);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let reader = tokio::spawn(async move {
            let mut last = EventWalPos { file_seq: 0, offset: 0 };
            loop {
                let pos = reader_wal.latest_pos();
                assert!(
                    pos >= last,
                    "latest_pos must be monotonic even across rotation: {last:?} -> {pos:?}"
                );
                last = pos;
                // Stop when the writer dropped the stop sender.
                if stop_rx.try_recv().is_err() {
                    break;
                }
            }
        });

        // Writer: appends that force rotations (seal records are 80
        // bytes > 64-byte files) while the reader samples.
        let id = SegmentId::new();
        for _ in 0..20 {
            wal.append(seal_event(id)).await.unwrap();
        }
        drop(stop_tx);
        reader.await.unwrap();
        assert!(wal.latest_pos().file_seq >= 20, "rotations must have happened");
    }

    #[tokio::test]
    async fn append_is_durable_on_return() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        wal.append(reserve_event(SegmentId::new())).await.unwrap();
        // Re-open and read: the record survived (durable on return).
        let reopened = open_wal(dir.path()).await;
        let events: Vec<_> = reopened
            .read_from(EventWalPos { file_seq: 0, offset: 0 })
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(events.len(), 1);
    }

    // ------------------------------------------------------------------
    // Rotation
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn rotation_opens_new_file_at_threshold() {
        let dir = tempfile::tempdir().unwrap();
        // File size 64: reserve (36) fits in file 0 at offset 36; the
        // seal (80) no longer fits and rotates; the next seal (80) also
        // rotates.
        let config = EventWalConfig {
            event_wal_dir: dir.path().to_path_buf(),
            event_wal_file_size_bytes: 64,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024,
        };
        let wal = EventWal::open(dir.path().to_path_buf(), &config).await.unwrap();
        let id = SegmentId::new();

        let p1 = wal.append(reserve_event(id)).await.unwrap();
        assert_eq!(p1.file_seq, 0, "first record lands in file 0");
        assert_eq!(p1.offset, 0);
        // The seal must rotate: 36 + 80 > 64.
        let p2 = wal.append(seal_event(id)).await.unwrap();
        assert_eq!(p2.file_seq, 1, "seal rotates to file 1");
        assert_eq!(p2.offset, 0, "rotated file starts at offset 0");
        // The next seal rotates again: 0 + 80 > 64.
        let p3 = wal.append(seal_event(id)).await.unwrap();
        assert_eq!(p3.file_seq, 2, "third record rotates to file 2");

        // The reader spans the rotation boundary transparently.
        let events: Vec<(EventWalPos, SegmentEvent)> =
            wal.read_from(EventWalPos { file_seq: 0, offset: 0 }).collect::<Result<_>>().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].0, p1);
        assert_eq!(events[1].0, p2);
        assert_eq!(events[2].0, p3);
    }

    #[tokio::test]
    async fn reopen_after_rotation_resumes_at_latest_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = EventWalConfig {
            event_wal_dir: dir.path().to_path_buf(),
            event_wal_file_size_bytes: 64,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024,
        };
        let id = SegmentId::new();
        {
            let wal = EventWal::open(dir.path().to_path_buf(), &config).await.unwrap();
            wal.append(reserve_event(id)).await.unwrap();
            wal.append(seal_event(id)).await.unwrap(); // rotates to file 1
        }
        let reopened = EventWal::open(dir.path().to_path_buf(), &config).await.unwrap();
        assert_eq!(reopened.latest_pos().file_seq, 1, "reopen resumes at the newest file");
        // The resumed file (80 bytes) already exceeds the 64-byte
        // threshold, so the next record rotates immediately.
        let p = reopened.append(delete_event(id)).await.unwrap();
        assert_eq!(p.file_seq, 2);
        assert_eq!(p.offset, 0);

        let events: Vec<_> = reopened
            .read_from(EventWalPos { file_seq: 0, offset: 0 })
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(events.len(), 3, "all three records readable across rotation + reopen");
    }

    #[tokio::test]
    async fn rotation_keeps_old_files() {
        let dir = tempfile::tempdir().unwrap();
        let config = EventWalConfig {
            event_wal_dir: dir.path().to_path_buf(),
            event_wal_file_size_bytes: 64,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024,
        };
        let wal = EventWal::open(dir.path().to_path_buf(), &config).await.unwrap();
        let id = SegmentId::new();
        wal.append(reserve_event(id)).await.unwrap();
        wal.append(seal_event(id)).await.unwrap(); // rotates to file 1
        let evl_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("evl_"))
            .collect();
        assert_eq!(
            evl_files.len(),
            2,
            "rotation must keep the previous file (retention is the checkpoint's job)"
        );
    }

    // ------------------------------------------------------------------
    // Torn tail semantics
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn torn_tail_partial_header_surfaces_torn_record() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        let id = SegmentId::new();
        wal.append(reserve_event(id)).await.unwrap();
        wal.append(seal_event(id)).await.unwrap();

        // Simulate a crash mid-record: truncate the file at a partial
        // header (second record starts at offset 36; cut at 36 + 10).
        let path = evl_file_path(dir.path(), 0);
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(36 + 10).unwrap();
        drop(file);

        let mut reader = wal.read_from(EventWalPos { file_seq: 0, offset: 0 });
        let first = reader.next().expect("first record intact").unwrap();
        assert_eq!(first.1, reserve_event(id));
        let torn = reader.next().expect("second record is torn").expect_err("torn tail must error");
        match torn {
            Error::TornEventRecord { pos, detail } => {
                assert_eq!(pos.offset, 36, "torn position is the second record's start");
                assert!(detail.contains("header"));
            }
            other => panic!("expected TornEventRecord, got {other:?}"),
        }
        assert!(reader.next().is_none(), "iterator must stop after the torn record");
    }

    #[tokio::test]
    async fn torn_tail_partial_payload_surfaces_torn_record() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        let id = SegmentId::new();
        wal.append(reserve_event(id)).await.unwrap();
        wal.append(seal_event(id)).await.unwrap();

        // Cut inside the seal payload (record at 36; header 28 + partial
        // payload 10 = 74).
        let path = evl_file_path(dir.path(), 0);
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(36 + 28 + 10).unwrap();
        drop(file);

        let mut reader = wal.read_from(EventWalPos { file_seq: 0, offset: 0 });
        assert!(reader.next().unwrap().is_ok());
        let torn = reader.next().expect("torn tail must error").expect_err("torn tail must error");
        match torn {
            Error::TornEventRecord { detail, .. } => assert!(detail.contains("payload")),
            other => panic!("expected TornEventRecord, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn torn_tail_corrupt_byte_surfaces_torn_record() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        let id = SegmentId::new();
        wal.append(reserve_event(id)).await.unwrap();
        wal.append(seal_event(id)).await.unwrap();

        // Flip a byte inside the second record (mid-file corruption is
        // treated as a torn tail per the stop-at-first-bad contract).
        let path = evl_file_path(dir.path(), 0);
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(36 + 30)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        drop(file);

        let mut reader = wal.read_from(EventWalPos { file_seq: 0, offset: 0 });
        assert!(reader.next().unwrap().is_ok());
        match reader.next().unwrap().expect_err("corrupt record must error") {
            Error::TornEventRecord { detail, .. } => assert!(detail.contains("checksum")),
            other => panic!("expected TornEventRecord, got {other:?}"),
        }
        assert!(reader.next().is_none());
    }

    #[tokio::test]
    async fn clean_file_boundary_advances_without_torn_error() {
        // A file ending exactly at a record boundary must NOT produce a
        // torn record; the reader ends cleanly.
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        wal.append(reserve_event(SegmentId::new())).await.unwrap();
        let reader = wal.read_from(EventWalPos { file_seq: 0, offset: 0 });
        let events: Vec<_> = reader.collect::<Result<_>>().unwrap();
        assert_eq!(events.len(), 1, "a clean tail must not be reported torn");
    }

    // ------------------------------------------------------------------
    // bytes_since (checkpoint trigger input)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn bytes_since_counts_appended_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        let start = EventWalPos { file_seq: 0, offset: 0 };
        assert_eq!(wal.bytes_since(start), 0);

        let id = SegmentId::new();
        wal.append(reserve_event(id)).await.unwrap();
        let after1 = wal.bytes_since(start);
        assert_eq!(after1, RESERVE_RECORD_SIZE, "bytes since start = the reserve record size");

        wal.append(seal_event(id)).await.unwrap();
        let after2 = wal.bytes_since(start);
        assert_eq!(after2, RESERVE_RECORD_SIZE + SEAL_RECORD_SIZE);

        // bytes_since a position AFTER a record counts only the later
        // bytes (the position must be the record boundary, not its start).
        let after_reserve = EventWalPos { file_seq: 0, offset: RESERVE_RECORD_SIZE };
        assert_eq!(wal.bytes_since(after_reserve), SEAL_RECORD_SIZE);
    }

    #[tokio::test]
    async fn bytes_since_spans_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let config = EventWalConfig {
            event_wal_dir: dir.path().to_path_buf(),
            event_wal_file_size_bytes: 64,
            event_wal_fsync_batch_timeout_ms: 10,
            event_wal_checkpoint_bytes: 1024,
        };
        let wal = EventWal::open(dir.path().to_path_buf(), &config).await.unwrap();
        let id = SegmentId::new();
        let start = EventWalPos { file_seq: 0, offset: 0 };
        wal.append(reserve_event(id)).await.unwrap(); // file 0, offset 0
        let p2 = wal.append(seal_event(id)).await.unwrap(); // file 1, offset 0
        wal.append(delete_event(id)).await.unwrap(); // file 1, offset 80

        // The total since the start equals the sum of all record sizes.
        assert_eq!(
            wal.bytes_since(start),
            RESERVE_RECORD_SIZE + SEAL_RECORD_SIZE + DELETE_RECORD_SIZE
        );
        // Since the file-1 position: only the later two records count.
        assert_eq!(wal.bytes_since(p2), SEAL_RECORD_SIZE + DELETE_RECORD_SIZE);
    }

    // ------------------------------------------------------------------
    // Own fsync group (ADR-0024 Decision 4) — injected blocking fsyncs
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn stalled_data_group_does_not_delay_event_appends() {
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;

        // A stalled "data" group: its fsync closure blocks for 10 s and
        // holds a pending waiter — simulating a stalled data WAL group
        // commit.
        let stalled = WalSyncGroup::new(
            || async {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                Ok(())
            },
            50,
            64,
        );
        let _stalled_waiter = stalled.submit().await.unwrap();

        // Event appends must complete promptly despite the stalled group.
        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(1500),
            wal.append(reserve_event(SegmentId::new())),
        )
        .await;
        assert!(result.is_ok(), "event append must not wait on the stalled data group");
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
    }

    #[tokio::test]
    async fn stalled_event_group_does_not_delay_data_group() {
        // The event group's fsync closure is injected-blocked via the
        // test seam; a real data WAL writer must keep appending
        // unaffected (its own group is a separate instance).
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(open_wal(dir.path()).await);
        wal.set_stall_fsync_for_test(true);

        let data_dir = tempfile::tempdir().unwrap();
        let data_config = WalConfig {
            data_dir: data_dir.path().to_path_buf(),
            max_file_size_bytes: 1024 * 1024,
            fsync_batch_timeout_ms: 5,
            wal_use_sync_file_range: false,
        };
        let data_wal = crate::wal::WalWriter::open(&data_config).await.unwrap();

        // A pending event append blocks on the stalled event group.
        let id = SegmentId::new();
        let event_append = tokio::spawn({
            let wal = Arc::clone(&wal);
            async move { wal.append(reserve_event(id)).await }
        });

        // Data appends must complete while the event group is stalled.
        let start = std::time::Instant::now();
        let entry = crate::wal::WalEntry::new(
            SegmentId::new(),
            0,
            3,
            3,
            0,
            0,
            0,
            HashOutput::from_bytes([0u8; 32]),
            vec![1, 2, 3].into(),
        );
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(1500), data_wal.append(entry))
                .await;
        assert!(result.is_ok(), "data append must not wait on the stalled event group");
        assert!(start.elapsed() < std::time::Duration::from_secs(2));

        // The event append is still pending (stalled), proving the event
        // group was the one blocked.
        assert!(
            !event_append.is_finished(),
            "event append must still be blocked on its stalled group"
        );
    }

    #[tokio::test]
    async fn event_batch_window_governs_fsync_cadence() {
        // The event group's batch window is governed by
        // `event_wal_fsync_batch_timeout_ms` only: with a wide window,
        // concurrent appends are collected into ONE fsync round (group
        // commit), and each append completes within the window + margin.
        let dir = tempfile::tempdir().unwrap();
        let config = EventWalConfig {
            event_wal_dir: dir.path().to_path_buf(),
            event_wal_file_size_bytes: 1024 * 1024,
            event_wal_fsync_batch_timeout_ms: 200, // wide window
            event_wal_checkpoint_bytes: 1024,
        };
        let wal = Arc::new(EventWal::open(dir.path().to_path_buf(), &config).await.unwrap());

        let mut handles = Vec::new();
        for _ in 0..8 {
            let wal = Arc::clone(&wal);
            handles.push(tokio::spawn(async move {
                let start = std::time::Instant::now();
                let pos = wal.append(reserve_event(SegmentId::new())).await.unwrap();
                (pos, start.elapsed())
            }));
        }
        for handle in handles {
            let (_, elapsed) = handle.await.unwrap();
            assert!(
                elapsed < std::time::Duration::from_millis(1500),
                "append must complete within the event batch window + margin, took {elapsed:?}"
            );
        }
        // One fsync round must serve the whole burst (the 200 ms window
        // is far wider than the submission spread).
        assert_eq!(
            wal.fsync_count_for_test(),
            1,
            "the event batch window must batch concurrent appends into a single fsync round"
        );
    }

    #[tokio::test]
    async fn independent_sync_groups_have_independent_waiter_lists() {
        // Two WalSyncGroup instances never share waiters: a submission
        // to one resolves via ITS OWN flusher, never the other's.
        let flush_count_a = Arc::new(AtomicU32::new(0));
        let flush_count_b = Arc::new(AtomicU32::new(0));
        let group_a = WalSyncGroup::new(
            {
                let count = Arc::clone(&flush_count_a);
                move || {
                    let count = Arc::clone(&count);
                    async move {
                        count.fetch_add(1, AtomicOrdering::SeqCst);
                        Ok(())
                    }
                }
            },
            20,
            64,
        );
        let group_b = WalSyncGroup::new(
            {
                let count = Arc::clone(&flush_count_b);
                move || {
                    let count = Arc::clone(&count);
                    async move {
                        count.fetch_add(1, AtomicOrdering::SeqCst);
                        Ok(())
                    }
                }
            },
            20,
            64,
        );

        let rx_a = group_a.submit().await.unwrap();
        let rx_b = group_b.submit().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), rx_a).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), rx_b).await.unwrap().unwrap();
        assert_eq!(flush_count_a.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(flush_count_b.load(AtomicOrdering::SeqCst), 1);
    }

    // ------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn open_truncates_torn_tail_and_resumes_cleanly() {
        // A crash mid-record leaves a partial record at the end of the
        // last file; open() must truncate it so appends resume at a
        // clean record boundary (self-healing log).
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        {
            let wal = open_wal(dir.path()).await;
            wal.append(reserve_event(id)).await.unwrap();
            wal.append(seal_event(id)).await.unwrap();
        }
        // Crash mid-record: truncate at 36 + 10 (partial header of the
        // second record).
        let path = evl_file_path(dir.path(), 0);
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(36 + 10).unwrap();
        drop(file);

        let reopened = open_wal(dir.path()).await;
        assert_eq!(
            reopened.latest_pos().offset,
            36,
            "open must truncate the torn tail back to the last good boundary"
        );
        // Only the intact record is readable.
        let events: Vec<_> = reopened
            .read_from(EventWalPos { file_seq: 0, offset: 0 })
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1, reserve_event(id));

        // New appends land at the clean boundary and remain readable.
        let p = reopened.append(delete_event(id)).await.unwrap();
        assert_eq!(p.offset, 36);
        let events: Vec<_> = reopened
            .read_from(EventWalPos { file_seq: 0, offset: 0 })
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn open_keeps_mid_log_corruption_untouched() {
        // Mid-log corruption (valid data after the bad record) must NOT
        // be truncated away by open — the recovery fold aborts on it
        // loudly instead of silently losing records.
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        {
            let wal = open_wal(dir.path()).await;
            wal.append(reserve_event(id)).await.unwrap();
            wal.append(seal_event(id)).await.unwrap();
            wal.append(delete_event(id)).await.unwrap();
        }
        // Corrupt the SECOND record (flip a payload byte, recompute
        // nothing): a valid record follows it.
        let path = evl_file_path(dir.path(), 0);
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(36 + 30)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        drop(file);

        let size_before = std::fs::metadata(&path).unwrap().len();
        let reopened = open_wal(dir.path()).await;
        let size_after = std::fs::metadata(&path).unwrap().len();
        assert_eq!(size_before, size_after, "open must not truncate past mid-log corruption");

        // The reader surfaces CorruptEventLog at the corrupt record —
        // the fold aborts there.
        let mut reader = reopened.read_from(EventWalPos { file_seq: 0, offset: 0 });
        assert!(reader.next().unwrap().is_ok());
        match reader.next().unwrap().expect_err("corrupt record must error") {
            Error::CorruptEventLog { pos, detail } => {
                assert_eq!(pos.offset, 36);
                assert!(detail.contains("checksum"));
            }
            other => panic!("expected CorruptEventLog, got {other:?}"),
        }
        assert!(reader.next().is_none(), "iterator must stop after the corrupt record");
    }

    #[tokio::test]
    async fn reader_classifies_tail_crc_failure_as_torn_not_corrupt() {
        // A CRC failure on the LAST record (nothing follows) is a torn
        // tail, not mid-log corruption.
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        let id = SegmentId::new();
        wal.append(reserve_event(id)).await.unwrap();
        wal.append(seal_event(id)).await.unwrap();

        let path = evl_file_path(dir.path(), 0);
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(36 + 30)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        drop(file);

        let mut reader = wal.read_from(EventWalPos { file_seq: 0, offset: 0 });
        assert!(reader.next().unwrap().is_ok());
        match reader.next().unwrap().expect_err("bad tail record must error") {
            Error::TornEventRecord { pos, .. } => assert_eq!(pos.offset, 36),
            other => panic!("expected TornEventRecord for a tail record, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn register_metrics_accepts_a_registrar() {
        struct NoopRegistrar;
        impl MetricRegistrar for NoopRegistrar {
            fn register_counter(&self, _counter: oceanfs_core::Counter) {}
            fn register_gauge(&self, _gauge: oceanfs_core::Gauge) {}
            fn register_histogram(&self, _histogram: std::sync::Arc<oceanfs_core::Histogram>) {}
        }
        let dir = tempfile::tempdir().unwrap();
        let wal = open_wal(dir.path()).await;
        wal.append(reserve_event(SegmentId::new())).await.unwrap();
        wal.register_metrics(&NoopRegistrar);
    }
}
