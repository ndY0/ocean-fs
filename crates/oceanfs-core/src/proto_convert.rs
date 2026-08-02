//! Proto-to-domain type conversions.
//!
//! Provides `From`/`TryFrom` conversions between protobuf message types
//! (generated in `crate::proto`) and domain types (in `crate`).
//! This prevents the domain from being coupled to protobuf field layouts.

use std::convert::TryFrom;

use crate::{BucketId, HashOutput, Hlc, NodeId, ObjectKey, SegmentId};

/// Error returned when proto-to-domain conversion fails.
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    /// Invalid HLC wall_time (negative or out of range).
    #[error("invalid HLC wall_time: {0}")]
    InvalidHlcWallTime(i64),
    /// Invalid segment ID length (expected 16 bytes).
    #[error("invalid segment ID length: expected 16, got {0}")]
    InvalidSegmentIdLength(usize),
    /// Invalid hash output length (expected 32 bytes).
    #[error("invalid hash output length: expected 32, got {0}")]
    InvalidHashLength(usize),
}

// ---------------------------------------------------------------------------
// BucketId
// ---------------------------------------------------------------------------

impl From<BucketId> for crate::proto::common::BucketId {
    fn from(value: BucketId) -> Self {
        crate::proto::common::BucketId { name: value.as_str().to_string() }
    }
}

impl From<crate::proto::common::BucketId> for BucketId {
    fn from(value: crate::proto::common::BucketId) -> Self {
        BucketId::new(value.name)
    }
}

// ---------------------------------------------------------------------------
// ObjectKey
// ---------------------------------------------------------------------------

impl From<ObjectKey> for crate::proto::common::ObjectKey {
    fn from(value: ObjectKey) -> Self {
        crate::proto::common::ObjectKey { key: value.as_str().to_string() }
    }
}

impl From<crate::proto::common::ObjectKey> for ObjectKey {
    fn from(value: crate::proto::common::ObjectKey) -> Self {
        ObjectKey::new(value.key)
    }
}

// ---------------------------------------------------------------------------
// NodeId
// ---------------------------------------------------------------------------

impl From<NodeId> for crate::proto::common::NodeId {
    fn from(value: NodeId) -> Self {
        crate::proto::common::NodeId { id: value.as_str().to_string() }
    }
}

impl From<crate::proto::common::NodeId> for NodeId {
    fn from(value: crate::proto::common::NodeId) -> Self {
        NodeId::new(value.id)
    }
}

// ---------------------------------------------------------------------------
// SegmentId
// ---------------------------------------------------------------------------

impl From<SegmentId> for crate::proto::common::SegmentId {
    fn from(value: SegmentId) -> Self {
        let uuid = value.as_uuid();
        let uuid_bytes = uuid.as_bytes();
        crate::proto::common::SegmentId { id: uuid_bytes.to_vec() }
    }
}

impl TryFrom<crate::proto::common::SegmentId> for SegmentId {
    type Error = ConversionError;

    fn try_from(value: crate::proto::common::SegmentId) -> Result<Self, Self::Error> {
        let bytes = &value.id;
        if bytes.len() != 16 {
            return Err(ConversionError::InvalidSegmentIdLength(bytes.len()));
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        Ok(SegmentId::from_uuid_bytes(arr))
    }
}

// ---------------------------------------------------------------------------
// HashOutput
// ---------------------------------------------------------------------------

impl From<HashOutput> for crate::proto::common::HashOutput {
    fn from(value: HashOutput) -> Self {
        crate::proto::common::HashOutput { hash: value.as_bytes().to_vec() }
    }
}

impl TryFrom<crate::proto::common::HashOutput> for HashOutput {
    type Error = ConversionError;

    fn try_from(value: crate::proto::common::HashOutput) -> Result<Self, Self::Error> {
        let bytes = &value.hash;
        if bytes.len() != 32 {
            return Err(ConversionError::InvalidHashLength(bytes.len()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(bytes);
        Ok(HashOutput::from_bytes(arr))
    }
}

// ---------------------------------------------------------------------------
// Hlc / HlcTimestamp
// ---------------------------------------------------------------------------

impl From<Hlc> for crate::proto::common::HlcTimestamp {
    fn from(value: Hlc) -> Self {
        crate::proto::common::HlcTimestamp {
            wall_time: value.wall_time(),
            logical: value.logical(),
        }
    }
}

impl TryFrom<crate::proto::common::HlcTimestamp> for Hlc {
    type Error = ConversionError;

    fn try_from(value: crate::proto::common::HlcTimestamp) -> Result<Self, Self::Error> {
        Ok(Hlc::new(value.wall_time, value.logical))
    }
}

// ---------------------------------------------------------------------------
// ShardIndex
// ---------------------------------------------------------------------------

impl From<u8> for crate::proto::common::ShardIndex {
    fn from(value: u8) -> Self {
        crate::proto::common::ShardIndex { index: value as u32 }
    }
}

impl From<crate::proto::common::ShardIndex> for u8 {
    fn from(value: crate::proto::common::ShardIndex) -> Self {
        value.index as u8
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn bucket_id_roundtrip() {
        let domain = BucketId::new("my-bucket");
        let proto: crate::proto::common::BucketId = domain.clone().into();
        let back: BucketId = proto.into();
        assert_eq!(domain, back);
    }

    #[test]
    fn object_key_roundtrip() {
        let domain = ObjectKey::new("path/to/object");
        let proto: crate::proto::common::ObjectKey = domain.clone().into();
        let back: ObjectKey = proto.into();
        assert_eq!(domain, back);
    }

    #[test]
    fn node_id_roundtrip() {
        let domain = NodeId::new("node-42");
        let proto: crate::proto::common::NodeId = domain.clone().into();
        let back: NodeId = proto.into();
        assert_eq!(domain, back);
    }

    #[test]
    fn segment_id_roundtrip() {
        let domain = SegmentId::new();
        let proto: crate::proto::common::SegmentId = domain.into();
        let back = SegmentId::try_from(proto.clone()).expect("valid segment ID");
        assert_eq!(domain, back);
    }

    #[test]
    fn segment_id_invalid_length() {
        let proto = crate::proto::common::SegmentId { id: b"too-short".to_vec() };
        let result = SegmentId::try_from(proto);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConversionError::InvalidSegmentIdLength(len) => assert_eq!(len, 9),
            _ => panic!("expected InvalidSegmentIdLength"),
        }
    }

    #[test]
    fn hash_output_roundtrip() {
        let data = [42u8; 32];
        let domain = HashOutput::from_bytes(data);
        let proto: crate::proto::common::HashOutput = domain.into();
        let back = HashOutput::try_from(proto).expect("valid hash");
        assert_eq!(domain, back);
    }

    #[test]
    fn hash_output_invalid_length() {
        let proto = crate::proto::common::HashOutput { hash: b"too-short".to_vec() };
        let result = HashOutput::try_from(proto);
        assert!(result.is_err());
    }

    #[test]
    fn hlc_roundtrip() {
        let domain = Hlc::new(1000, 42);
        let proto: crate::proto::common::HlcTimestamp = domain.into();
        let back = Hlc::try_from(proto).expect("valid HLC");
        assert_eq!(domain, back);
    }

    #[test]
    fn shard_index_roundtrip() {
        let idx: u8 = 3;
        let proto: crate::proto::common::ShardIndex = idx.into();
        let back: u8 = proto.into();
        assert_eq!(idx, back);
    }

    #[test]
    fn shard_index_zero() {
        let proto: crate::proto::common::ShardIndex = 0u8.into();
        let back: u8 = proto.into();
        assert_eq!(back, 0);
    }
}
