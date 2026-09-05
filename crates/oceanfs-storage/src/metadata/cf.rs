//! Column family names and key encoding for the metadata store.
//!
//! The `deletions` column family holds two kinds of dead-chunk record
//! (ADR-0034 D2), distinguished by key shape:
//!
//! - **Plain tombstones** (deletes) use the exact object key
//!   `{bucket}\0{key}` — byte-identical to the pre-accounting layout.
//! - **Supersede records** (overwrites) append a self-describing tail to
//!   the plain key so they sort within the same bucket prefix yet can
//!   never be observed by the exact-key tombstone operations
//!   (`has_tombstone`, `get_tombstone`, `delete_tombstone`), which read
//!   only the plain key.
//!
//! Only the `objects` + `deletions` column families exist (ADR-0025
//! Decision 3: the `segments` and `deleted_segments` CFs were removed;
//! segment lifecycle state lives in the event log + checkpoint + registry).

use oceanfs_core::Hlc;

/// Column family name for object metadata.
pub(crate) const CF_OBJECTS: &str = "objects";

/// Column family name for deletion tombstones and supersede records.
pub(crate) const CF_DELETIONS: &str = "deletions";

/// Kind marker for a supersede dead-chunk record.
///
/// `0x01` is reserved; plain tombstones have NO suffix (so exact-key ops
/// and pre-feature records remain unchanged — a plain key is simply the
/// `{bucket}\0{key}` object key with nothing appended).
pub(crate) const SUPERSEDE_KIND: u8 = 0x02;

/// Fixed tail appended after the plain `{bucket}\0{key}` on a supersede
/// key: `[0x00] [0x02] [key_len u16 BE] [wall_time u64 LE] [logical u32 LE]`.
const SUPERSEDE_TAIL_SIZE: usize = 1 + 1 + 2 + 8 + 4;

/// Decoded view of a `deletions`-CF key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeletionsKey {
    /// A plain tombstone key: `{bucket}\0{key}`.
    Plain {
        /// Owning bucket.
        bucket: String,
        /// Object key.
        key: String,
    },
    /// A versioned supersede key: `{bucket}\0{key}\0 0x02 key_len version`.
    Supersede {
        /// Owning bucket.
        bucket: String,
        /// Object key.
        key: String,
        /// The superseded version's HLC (the version discriminator).
        version: Hlc,
    },
}

/// Encodes a bucket and object key into a RocksDB key.
///
/// Format: `{bucket_name}\0{object_key}`
pub(crate) fn encode_object_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(bucket.len() + 1 + key.len());
    buf.extend_from_slice(bucket.as_bytes());
    buf.push(0);
    buf.extend_from_slice(key.as_bytes());
    buf
}

/// Decodes a RocksDB key back into bucket and object key components.
pub(crate) fn decode_object_key(data: &[u8]) -> Option<(&str, &str)> {
    let null_pos = data.iter().position(|&b| b == 0)?;
    let bucket = std::str::from_utf8(&data[..null_pos]).ok()?;
    let key = std::str::from_utf8(&data[null_pos + 1..]).ok()?;
    Some((bucket, key))
}

/// Encodes a versioned supersede dead-chunk key.
///
/// Layout (all offsets relative to the end of the plain `{bucket}\0{key}`):
///
/// ```text
/// [0x00]                          separator
/// [SUPERSEDE_KIND = 0x02]         marker
/// [key_len: u16 BE]               object key byte length (self-check)
/// [version: u64 LE ++ u32 LE]     superseded version's HLC
/// ```
///
/// The `key_len` self-check makes the parse exact even though object keys
/// may contain arbitrary bytes after the first NUL: a key is a supersede
/// iff its tail parses as the layout above AND `key_len` equals the number
/// of bytes between the first NUL and the tail. Object keys arriving on
/// the HTTP path are URL path segments (bounded to ~1 KiB) and cannot
/// contain the `\0` + marker sequence in practice; the self-check makes a
/// misparse require a crafted key.
///
/// Object keys are S3 URL path segments, bounded far below `u16::MAX`, so
/// the `key_len` truncation in `encode_supersede_key` cannot lose data.
pub(crate) fn encode_supersede_key(bucket: &str, key: &str, version: Hlc) -> Vec<u8> {
    let key_len = key.len();
    debug_assert!(key_len <= u16::MAX as usize, "object key exceeds u16 length");
    let mut buf = Vec::with_capacity(bucket.len() + 1 + key.len() + SUPERSEDE_TAIL_SIZE);
    buf.extend_from_slice(bucket.as_bytes());
    buf.push(0);
    buf.extend_from_slice(key.as_bytes());
    buf.push(0);
    buf.push(SUPERSEDE_KIND);
    buf.extend_from_slice(&(key_len as u16).to_be_bytes());
    buf.extend_from_slice(&version.wall_time().to_le_bytes());
    buf.extend_from_slice(&version.logical().to_le_bytes());
    buf
}

/// Decodes a `deletions`-CF key into its [`DeletionsKey`] classification.
///
/// A plain tombstone key and a supersede key sharing the same
/// `(bucket, key)` classify distinctly: the supersede key's tail is
/// validated against the self-describing layout, so a plain key (whose
/// object key cannot contain NUL) never classifies as a supersede. A
/// malformed supersede-shaped key (tail present but failing the self-check)
/// decodes to `None` — the record is skipped, never surfaced as a plain
/// tombstone with a garbage key.
pub(crate) fn decode_deletions_key(data: &[u8]) -> Option<DeletionsKey> {
    if data.len() >= SUPERSEDE_TAIL_SIZE {
        let (head, tail) = data.split_at(data.len() - SUPERSEDE_TAIL_SIZE);
        if tail[0] == 0 && tail[1] == SUPERSEDE_KIND {
            // A supersede-shaped key. The bucket boundary is the first NUL
            // of `head`; the bytes between it and the tail must be exactly
            // `key_len` bytes for the parse to be exact.
            let null_pos = head.iter().position(|&b| b == 0)?;
            let key_bytes = &head[null_pos + 1..];
            let key_len = u16::from_be_bytes([tail[2], tail[3]]) as usize;
            if key_bytes.len() != key_len {
                return None;
            }
            let bucket = std::str::from_utf8(&head[..null_pos]).ok()?;
            let key = std::str::from_utf8(key_bytes).ok()?;
            let wall_time = u64::from_le_bytes(tail[4..12].try_into().ok()?);
            let logical = u32::from_le_bytes(tail[12..16].try_into().ok()?);
            return Some(DeletionsKey::Supersede {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version: Hlc::new(wall_time, logical),
            });
        }
    }
    let (bucket, key) = decode_object_key(data)?;
    Some(DeletionsKey::Plain { bucket: bucket.to_string(), key: key.to_string() })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn object_key_roundtrip() {
        let encoded = encode_object_key("photos", "cat.jpg");
        let (bucket, key) = decode_object_key(&encoded).unwrap();
        assert_eq!(bucket, "photos");
        assert_eq!(key, "cat.jpg");
    }

    #[test]
    fn object_key_handles_empty_parts() {
        let encoded = encode_object_key("", "");
        let (bucket, key) = decode_object_key(&encoded).unwrap();
        assert_eq!(bucket, "");
        assert_eq!(key, "");
    }

    #[test]
    fn supersede_key_roundtrip_preserves_version() {
        let version = Hlc::new(1_700_000_000_123, 7);
        let encoded = encode_supersede_key("photos", "cat.jpg", version);
        match decode_deletions_key(&encoded).unwrap() {
            DeletionsKey::Supersede { bucket, key, version: got } => {
                assert_eq!(bucket, "photos");
                assert_eq!(key, "cat.jpg");
                assert_eq!(got, version);
            }
            other => panic!("expected supersede, got {other:?}"),
        }
    }

    #[test]
    fn supersede_key_with_empty_parts_roundtrips() {
        let version = Hlc::new(5, 0);
        let encoded = encode_supersede_key("", "", version);
        match decode_deletions_key(&encoded).unwrap() {
            DeletionsKey::Supersede { bucket, key, version: got } => {
                assert_eq!(bucket, "");
                assert_eq!(key, "");
                assert_eq!(got, version);
            }
            other => panic!("expected supersede, got {other:?}"),
        }
    }

    #[test]
    fn plain_key_classifies_as_plain_not_supersede() {
        // The same (bucket, key) used by a supersede record, encoded as a
        // plain tombstone key, must classify as Plain — the exact-key
        // tombstone operations must never observe a supersede.
        let plain = encode_object_key("photos", "cat.jpg");
        match decode_deletions_key(&plain).unwrap() {
            DeletionsKey::Plain { bucket, key } => {
                assert_eq!(bucket, "photos");
                assert_eq!(key, "cat.jpg");
            }
            other => panic!("expected plain, got {other:?}"),
        }
    }

    #[test]
    fn plain_and_supersede_for_same_key_classify_distinctly() {
        let bucket = "b";
        let key = "k";
        let plain = encode_object_key(bucket, key);
        let supersede = encode_supersede_key(bucket, key, Hlc::new(9, 1));
        assert_eq!(
            decode_deletions_key(&plain).unwrap(),
            DeletionsKey::Plain { bucket: bucket.to_string(), key: key.to_string() }
        );
        assert!(matches!(
            decode_deletions_key(&supersede).unwrap(),
            DeletionsKey::Supersede { .. }
        ));
    }

    #[test]
    fn supersede_shaped_key_with_bad_length_is_rejected() {
        // Corrupt the self-check: flip the low byte of key_len so it no
        // longer matches the bytes between the first NUL and the tail.
        let mut encoded = encode_supersede_key("b", "key", Hlc::new(9, 1));
        let last = encoded.len();
        encoded[last - 13] ^= 0xff; // key_len low byte
        assert!(decode_deletions_key(&encoded).is_none());
    }

    #[test]
    fn supersede_shaped_key_with_wrong_marker_is_plain() {
        // A key whose final bytes are not separator+marker falls through
        // to the plain decode path.
        let bucket = "b";
        let key = "abcdefghijklmnopqrstuvwxyz0123456789";
        let plain = encode_object_key(bucket, key);
        match decode_deletions_key(&plain).unwrap() {
            DeletionsKey::Plain { key: got, .. } => assert_eq!(got, key),
            other => panic!("expected plain, got {other:?}"),
        }
    }

    #[test]
    fn supersede_key_with_binary_safe_object_key_roundtrips() {
        // The key_len self-check keeps the parse exact even when the key
        // itself ends in bytes that resemble the tail layout.
        let key = "k\x02\x00\x05";
        let encoded = encode_supersede_key("b", key, Hlc::new(1, 2));
        match decode_deletions_key(&encoded).unwrap() {
            DeletionsKey::Supersede { key: got, version, .. } => {
                assert_eq!(got, key);
                assert_eq!(version, Hlc::new(1, 2));
            }
            other => panic!("expected supersede, got {other:?}"),
        }
    }
}
