//! Segment integrity verification and EC parity repair.
//!
//! Sealed segment files carry a BLAKE3 checksum of the data section in
//! the header and (format v2) a streaming-EC parity section with a
//! per-shard hash table. [`verify_and_repair_segment`] is the read
//! path's self-healing entry point:
//!
//! 1. Verify the whole-data checksum (fast path — healthy files cost one
//!    hash pass and are left untouched).
//! 2. On mismatch, locate corrupt shards per stripe via the hash table.
//! 3. If at most `m` shards of a stripe are corrupt, reconstruct the
//!    data shards via Cauchy decode and rewrite the corrupt shards.
//!
//! The final partial stripe (the segment tail, up to one stripe) is not
//! covered by parity and is therefore not repairable — reads of a
//! corrupt tail fail with [`Error::SegmentCorrupt`] and fall back to the
//! replica-fetch recovery path.

use std::path::Path;

use oceanfs_core::{CodecConfig, SegmentId};
use oceanfs_ec::{CauchyEncoder, Decoder, Encoder};
use oceanfs_hash::{Blake3Hasher, Hasher};

use crate::{
    error::{Error, Result},
    segment::{
        header::SegmentHeader,
        parity_section::{ParitySection, PARITY_SECTION_HEADER_SIZE},
    },
};

/// Verifies a sealed segment file's data section and repairs corrupt
/// stripes from the stored parity shards.
///
/// Returns the number of repaired stripes. `Ok(0)` when the file is
/// healthy, has no parity section (v1 format), or has no encoded
/// stripes. Errors with [`Error::SegmentCorrupt`] when corruption is
/// detected but cannot be repaired (more than `m` corrupt shards, or a
/// corrupt un-encoded tail).
pub(crate) fn verify_and_repair_segment(
    path: &Path,
    ec_decoder: Option<&dyn Decoder>,
    ec_encoder: Option<&dyn Encoder>,
) -> Result<usize> {
    // Streamed verification: the previous implementation loaded the whole
    // file with std::fs::read just to hash it — the read path calls this
    // on every segment's first touch, so under sustained load the
    // concurrent full-file buffers (segments ~10 MB each) formed
    // multi-GB anonymous-memory bursts (hundreds of fds + anon==RSS,
    // OOM-killing 4 GB SUT VMs). Header-only + chunked BLAKE3 keeps the
    // same integrity guarantee in constant memory.
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path)
        .map_err(|e| Error::Io(std::io::Error::other(format!("open {}: {e}", path.display()))))?;

    // The on-disk header is 76 (v1) or 92 (v2) bytes; read a fixed
    // 128-byte prefix so the version is known before the data offset.
    let mut header_buf = [0u8; 128];
    let got = file
        .read(&mut header_buf)
        .map_err(|e| Error::Io(std::io::Error::other(format!("read {}: {e}", path.display()))))?;
    if got < SegmentHeader::header_size(1) {
        return Err(Error::SegmentCorrupt(SegmentId::default()));
    }
    let header = SegmentHeader::from_bytes(&header_buf)
        .ok_or_else(|| Error::SegmentCorrupt(SegmentId::default()))?;
    let hdr_size = header.serialized_size();
    let data_end = header.data_end() as usize;
    let file_len = file
        .metadata()
        .map_err(|e| Error::Io(std::io::Error::other(format!("stat {}: {e}", path.display()))))?
        .len() as usize;
    if data_end > file_len {
        return Err(Error::SegmentCorrupt(header.segment_id));
    }

    // Fast path: the checksum matches — healthy file. For v2 files the
    // checksum covers data + parity section. Stream both sections in
    // 1 MiB chunks (perf rule 5.2: never buffer the full blob).
    let mut hasher = Blake3Hasher::new();
    let mut chunk = vec![0u8; 1024 * 1024];
    let mut read_range = |mut f: &std::fs::File, start: usize, end: usize| -> Result<()> {
        f.seek(SeekFrom::Start(start as u64))
            .map_err(|e| Error::Io(std::io::Error::other(format!("seek: {e}"))))?;
        let mut remaining = end.saturating_sub(start);
        while remaining > 0 {
            let want = remaining.min(chunk.len());
            let n = f
                .read(&mut chunk[..want])
                .map_err(|e| Error::Io(std::io::Error::other(format!("read: {e}"))))?;
            if n == 0 {
                return Err(Error::SegmentCorrupt(header.segment_id));
            }
            hasher.update(&chunk[..n]);
            remaining -= n;
        }
        Ok(())
    };
    read_range(&file, hdr_size, data_end)?;
    if header.parity_size > 0 {
        let section_start = header.parity_offset as usize;
        let section_end = section_start + header.parity_size as usize;
        if section_end > file_len {
            return Err(Error::SegmentCorrupt(header.segment_id));
        }
        read_range(&file, section_start, section_end)?;
    }
    let computed = hasher.finalize();
    if computed.as_bytes() == &header.checksum {
        return Ok(0);
    }

    tracing::warn!(
        segment_id = %header.segment_id,
        "segment checksum mismatch; attempting parity repair"
    );

    // No parity section (v1 format or plain segment) — cannot repair.
    if header.parity_offset == 0 || header.parity_size == 0 {
        return Err(Error::SegmentCorrupt(header.segment_id));
    }
    // Repair path: corruption is rare, so load the file fully here (the
    // streaming fast path above keeps the healthy case in O(1) memory).
    let file = std::fs::read(path)
        .map_err(|e| Error::Io(std::io::Error::other(format!("read {}: {e}", path.display()))))?;
    let data = &file[hdr_size..data_end];
    let section_start = header.parity_offset as usize;
    let section_end = section_start + header.parity_size as usize;
    if section_end > file.len() {
        return Err(Error::SegmentCorrupt(header.segment_id));
    }
    let section = ParitySection::parse(&file[section_start..section_end])
        .ok_or(Error::SegmentCorrupt(header.segment_id))?;

    // The data section must cover all encoded stripes.
    if data.len() < section.stripe_count * section.stripe_len() {
        return Err(Error::SegmentCorrupt(header.segment_id));
    }

    let mut repaired = 0usize;
    for stripe in 0..section.stripe_count {
        let total_shards = section.k + section.m;
        let mut corrupt: Vec<usize> = Vec::new();
        for idx in 0..total_shards {
            let shard = if idx < section.k {
                section.data_shard(data, stripe, idx)
            } else {
                section.parity_shard(stripe, idx - section.k)
            };
            let computed_hash = *blake3::hash(shard).as_bytes();
            if computed_hash != *section.shard_hash(stripe, idx) {
                corrupt.push(idx);
            }
        }
        if corrupt.is_empty() {
            continue;
        }
        if corrupt.len() > section.m {
            return Err(Error::SegmentCorrupt(header.segment_id));
        }

        // Build the decode input: surviving shards Some, corrupt None.
        let mut available: Vec<Option<&[u8]>> = Vec::with_capacity(total_shards);
        for idx in 0..total_shards {
            if corrupt.contains(&idx) {
                available.push(None);
            } else if idx < section.k {
                available.push(Some(section.data_shard(data, stripe, idx)));
            } else {
                available.push(Some(section.parity_shard(stripe, idx - section.k)));
            }
        }
        // The injected decoder/encoder (the node wires the
        // AccelDispatcher) keep corruption repair observable through the
        // accel metrics; fall back to the plain Cauchy codec when unset.
        let fallback_decoder = CauchyEncoder::new(CodecConfig {
            data_shards: section.k as u8,
            parity_shards: section.m as u8,
            strip_size_bytes: section.strip,
            ..Default::default()
        });
        let fallback_encoder = CauchyEncoder::new(CodecConfig {
            data_shards: section.k as u8,
            parity_shards: section.m as u8,
            strip_size_bytes: section.strip,
            ..Default::default()
        });
        let codec: &dyn Decoder = ec_decoder.unwrap_or(&fallback_decoder);
        let codec_enc: &dyn Encoder = ec_encoder.unwrap_or(&fallback_encoder);
        let recovered =
            codec.decode(&available, section.k as u8, section.m as u8).map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "EC decode failed for stripe {stripe} of {}: {e}",
                    header.segment_id
                )))
            })?;
        if recovered.len() != section.k {
            return Err(Error::SegmentCorrupt(header.segment_id));
        }
        // The recovered data shards must match the stored hash table.
        for (d, shard) in recovered.iter().enumerate() {
            let computed_hash = *blake3::hash(shard).as_bytes();
            if computed_hash != *section.shard_hash(stripe, d) {
                return Err(Error::SegmentCorrupt(header.segment_id));
            }
        }

        // Rewrite the corrupt data shards in place.
        for &d in corrupt.iter().filter(|&&i| i < section.k) {
            let offset = hdr_size + stripe * section.stripe_len() + d * section.strip;
            write_range(path, offset, &recovered[d])?;
        }
        // If any parity shard was corrupt, re-encode and rewrite it.
        if corrupt.iter().any(|&i| i >= section.k) {
            let data_shards: Vec<&[u8]> = recovered.iter().map(|s| s.as_ref()).collect();
            let parity = codec_enc.encode(&data_shards, section.m as u8).map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "EC re-encode failed for stripe {stripe} of {}: {e}",
                    header.segment_id
                )))
            })?;
            for &p in corrupt.iter().filter(|&&i| i >= section.k) {
                let shard = &parity[p - section.k];
                let offset = section_start
                    + PARITY_SECTION_HEADER_SIZE
                    + (stripe * section.m + (p - section.k)) * section.strip;
                write_range(path, offset, shard)?;
            }
        }
        repaired += 1;
    }

    if repaired > 0 {
        tracing::info!(
            segment_id = %header.segment_id,
            repaired_stripes = repaired,
            "segment repaired from EC parity"
        );
    } else {
        // The checksum mismatched but every encoded stripe verified:
        // the corruption is in the un-encoded tail.
        return Err(Error::SegmentCorrupt(header.segment_id));
    }
    Ok(repaired)
}

/// Writes `bytes` at `offset` in the segment file (best-effort repair).
fn write_range(path: &Path, offset: usize, bytes: &[u8]) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| Error::Io(std::io::Error::other(format!("open {}: {e}", path.display()))))?;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.write_all(bytes)?;
    file.sync_data().ok(); // best effort
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::PathBuf;

    use bytes::Bytes;
    use oceanfs_core::SegmentId;
    use oceanfs_ec::Encoder;

    use super::*;
    use crate::segment::parity_section::build_parity_section;

    /// Builds a v2 segment file with 2 encoded stripes (k=4, m=2,
    /// strip=64 → 512 bytes of data). When `corrupt` is Some((file_offset,
    /// replacement_byte)), one byte at that file offset is overwritten.
    fn make_v2_file(
        dir: &tempfile::TempDir,
        id: SegmentId,
        corrupt: Option<(usize, u8)>,
    ) -> (PathBuf, Vec<u8>) {
        const K: u8 = 4;
        const M: u8 = 2;
        const STRIP: usize = 64;
        let data: Vec<u8> = (0..512u32).map(|i| (i % 251) as u8).collect();
        let codec = oceanfs_ec::CauchyEncoder::new(CodecConfig {
            data_shards: K,
            parity_shards: M,
            strip_size_bytes: STRIP,
            ..Default::default()
        });
        let mut parity: Vec<Bytes> = Vec::new();
        for stripe in 0..2 {
            let shards: Vec<&[u8]> = (0..4)
                .map(|d| &data[stripe * 256 + d * STRIP..stripe * 256 + (d + 1) * STRIP])
                .collect();
            parity.extend(codec.encode(&shards, M).unwrap());
        }
        let section = build_parity_section(&data, K, M, Some(&parity)).unwrap();
        let mut hasher = blake3::Hasher::new();
        hasher.update(&data);
        hasher.update(&section);
        let checksum = *hasher.finalize().as_bytes();
        let data_offset = crate::segment::header::SEGMENT_HEADER_SIZE;
        let hdr = SegmentHeader::with_parity(
            id,
            data.len() as u64,
            0,
            (data_offset + data.len() + section.len()) as u64,
            checksum,
            (data_offset + data.len()) as u64,
            section.len() as u64,
        );
        let mut file = hdr.to_bytes();
        file.extend_from_slice(&data);
        file.extend_from_slice(&section);
        if let Some((offset, val)) = corrupt {
            file[offset] = val;
        }
        let path = dir.path().join(format!("{id}.dat"));
        std::fs::write(&path, &file).unwrap();
        (path, data)
    }

    #[test]
    fn repair_healthy_v2_file_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = make_v2_file(&dir, SegmentId::new(), None);
        assert_eq!(verify_and_repair_segment(&path, None, None).unwrap(), 0);
    }

    #[test]
    fn repair_restores_corrupt_data_shard() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        // Corrupt one byte inside data shard 2 of stripe 0.
        let data_offset = crate::segment::header::SEGMENT_HEADER_SIZE;
        let (path, original) = make_v2_file(&dir, id, Some((data_offset + 2 * 64 + 10, 0xFF)));

        let repaired = verify_and_repair_segment(&path, None, None).unwrap();
        assert_eq!(repaired, 1, "one stripe must be repaired");

        // The file is now healed: re-verification is a fast-path pass.
        assert_eq!(verify_and_repair_segment(&path, None, None).unwrap(), 0);

        let file = std::fs::read(&path).unwrap();
        assert_eq!(
            &file[data_offset..data_offset + original.len()],
            &original[..],
            "corrupt data shard must be restored exactly"
        );
    }

    #[test]
    fn repair_restores_corrupt_parity_shard_by_reencode() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let (path, original) = make_v2_file(&dir, id, None);
        // Corrupt the first parity shard of stripe 0: its file offset is
        // the section start + section header (shard index 0).
        let file = std::fs::read(&path).unwrap();
        let hdr = SegmentHeader::from_bytes(&file).unwrap();
        let corrupt_offset = hdr.parity_offset as usize + PARITY_SECTION_HEADER_SIZE;
        let mut file = file;
        file[corrupt_offset] ^= 0x5A;
        std::fs::write(&path, &file).unwrap();

        let repaired = verify_and_repair_segment(&path, None, None).unwrap();
        assert_eq!(repaired, 1);
        let file = std::fs::read(&path).unwrap();
        assert_eq!(
            &file[hdr.serialized_size()..hdr.serialized_size() + original.len()],
            &original[..],
            "data must be untouched by a parity-only repair"
        );

        // The repaired parity shard must match a fresh encode of stripe 0.
        let section = ParitySection::parse(&file[hdr.parity_offset as usize..]).unwrap();
        let codec = oceanfs_ec::CauchyEncoder::new(CodecConfig {
            data_shards: 4,
            parity_shards: 2,
            strip_size_bytes: 64,
            ..Default::default()
        });
        let data_shards: Vec<&[u8]> = (0..4).map(|d| section.data_shard(&original, 0, d)).collect();
        let fresh = codec.encode(&data_shards, 2).unwrap();
        let repaired_shard = section.parity_shard(0, 0);
        assert_eq!(
            repaired_shard,
            &fresh[0][..],
            "repaired parity shard must equal a fresh encode"
        );
    }

    #[test]
    fn repair_rejects_more_than_m_corrupt_shards() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        // Corrupt one byte in each of the 3 first data shards of stripe 0
        // (m=2 → at most 2 corrupt shards are recoverable).
        let data_offset = crate::segment::header::SEGMENT_HEADER_SIZE;
        let (path, _) = make_v2_file(&dir, id, None);
        let mut file = std::fs::read(&path).unwrap();
        for d in 0..3 {
            file[data_offset + d * 64 + 5] ^= 0x01;
        }
        std::fs::write(&path, &file).unwrap();

        let result = verify_and_repair_segment(&path, None, None);
        assert!(matches!(result, Err(Error::SegmentCorrupt(_))));
    }

    #[test]
    fn repair_v1_file_without_parity_errors_on_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let id = SegmentId::new();
        let data = vec![0x11u8; 256];
        let path = dir.path().join(format!("{id}.dat"));
        // v1 header (76 bytes) with a WRONG checksum.
        let mut file = vec![0u8; crate::segment::header::SEGMENT_HEADER_SIZE_V1];
        file[0..4].copy_from_slice(&crate::segment::header::SEGMENT_MAGIC);
        file[4..6].copy_from_slice(&1u16.to_le_bytes());
        file[22..30].copy_from_slice(&(data.len() as u64).to_le_bytes());
        file[34..42].copy_from_slice(&((76 + data.len()) as u64).to_le_bytes());
        file.extend_from_slice(&data);
        std::fs::write(&path, &file).unwrap();

        let result = verify_and_repair_segment(&path, None, None);
        assert!(
            matches!(result, Err(Error::SegmentCorrupt(_))),
            "v1 corruption without parity must be unrepairable"
        );
    }
}
