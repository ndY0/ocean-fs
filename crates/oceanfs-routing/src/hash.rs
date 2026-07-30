//! Key hashing — SHA-256 for ring positioning.

use sha2::{Digest, Sha256};

/// Hashes a key with SHA-256, producing a 32-byte ring position.
///
/// # Examples
///
/// ```
/// use oceanfs_routing::hash_key;
///
/// let hash = hash_key(b"photos/cat.jpg");
/// assert_eq!(hash.len(), 32);
/// ```
pub fn hash_key(key: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(key);
    let result = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

/// Hashes a node identifier for virtual node placement on the ring.
pub(crate) fn hash_node(node_id: &str, vnode_index: u32) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(node_id.as_bytes());
    hasher.update(b":");
    hasher.update(vnode_index.to_le_bytes());
    let result = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn same_key_produces_same_hash() {
        let h1 = hash_key(b"hello");
        let h2 = hash_key(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_keys_produce_different_hashes() {
        let h1 = hash_key(b"hello");
        let h2 = hash_key(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn node_hashes_are_unique_per_index() {
        let h1 = hash_node("node-1", 0);
        let h2 = hash_node("node-1", 1);
        assert_ne!(h1, h2);
    }
}
