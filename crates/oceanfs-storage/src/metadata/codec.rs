//! Public metadata row-key codecs (g8 `metadata-loss-recovery`).
//!
//! The full key-encoding rules stay private to `cf` (the store and its
//! compaction/sealer paths are the only writers). The g8 range stream
//! and rebuild fold only need to RE-DERIVE the object key of a row to
//! hash-filter it against the ring and to guard folds — exposed here so
//! the node layer never has to re-implement the encoding.

use super::cf;

/// Extracts the object key BYTES from any metadata CF row key.
///
/// Works for the objects-CF key (`{bucket}\0{key}`) and for both
/// deletions-CF key shapes (plain tombstone `{bucket}\0{key}` and
/// supersede record keys). Returns `None` for a malformed key (a
/// supersede-shaped key failing its self-check is skipped, never
/// mis-decoded as a plain key).
///
/// # Examples
///
/// ```
/// use oceanfs_storage::metadata::object_key_bytes;
///
/// let row = b"photos\0cat.jpg";
/// assert_eq!(object_key_bytes(row).as_deref(), Some(&b"cat.jpg"[..]));
/// ```
pub fn object_key_bytes(row_key: &[u8]) -> Option<Vec<u8>> {
    match cf::decode_deletions_key(row_key) {
        Some(cf::DeletionsKey::Plain { key, .. })
        | Some(cf::DeletionsKey::Supersede { key, .. }) => Some(key.into_bytes()),
        None => None,
    }
}
