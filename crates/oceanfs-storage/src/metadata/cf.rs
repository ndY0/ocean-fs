//! Column family names and key encoding for the metadata store.

/// Column family name for object metadata.
pub(crate) const CF_OBJECTS: &str = "objects";

/// Column family name for segment metadata.
pub(crate) const CF_SEGMENTS: &str = "segments";

/// Column family name for deletion tombstones.
pub(crate) const CF_DELETIONS: &str = "deletions";

/// All column families used by the metadata store.
pub(crate) const ALL_COLUMN_FAMILIES: &[&str] = &[CF_OBJECTS, CF_SEGMENTS, CF_DELETIONS];

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
#[allow(dead_code)]
pub(crate) fn decode_object_key(data: &[u8]) -> Option<(&str, &str)> {
    let null_pos = data.iter().position(|&b| b == 0)?;
    let bucket = std::str::from_utf8(&data[..null_pos]).ok()?;
    let key = std::str::from_utf8(&data[null_pos + 1..]).ok()?;
    Some((bucket, key))
}

/// Encodes a segment ID into a RocksDB key.
///
/// Format: `segment:{uuid_bytes}`
pub(crate) fn encode_segment_key(id: &oceanfs_core::SegmentId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(24);
    buf.extend_from_slice(b"segment:");
    buf.extend_from_slice(id.as_uuid().as_bytes());
    buf
}

/// Encodes a bucket and object key into a deletion tombstone key.
#[allow(dead_code)]
pub(crate) fn encode_deletion_key(bucket: &str, key: &str) -> Vec<u8> {
    // Same format as object keys — deletions are looked up by the same key.
    encode_object_key(bucket, key)
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
    fn segment_key_is_unique_per_id() {
        let id1 = oceanfs_core::SegmentId::new();
        let id2 = oceanfs_core::SegmentId::new();
        assert_ne!(encode_segment_key(&id1), encode_segment_key(&id2));
    }
}
