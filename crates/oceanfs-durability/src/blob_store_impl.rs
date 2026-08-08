//! Trait impl bridge: implements `SegmentDataStore` for `oceanfs_storage::BlobStore`.
//!
//! This file lives in `oceanfs-durability` because it requires both
//! `SegmentDataStore` (defined in this crate) and `BlobStore` (defined in
//! `oceanfs-storage`). Per ADR-0009, durability depends on storage.

use bytes::Bytes;
use oceanfs_core::SegmentId;
use oceanfs_storage::{BlobStore, Error};

use crate::anti_entropy::SegmentDataStore;

impl SegmentDataStore for BlobStore {
    fn read_segment_data(&self, segment_id: &SegmentId) -> Result<Bytes, Error> {
        self.read_blob(segment_id)?.ok_or(Error::SegmentNotFound(*segment_id))
    }

    fn write_segment_data(&self, segment_id: &SegmentId, data: &[u8]) -> Result<(), Error> {
        self.write_blob(segment_id, data)
    }
}
