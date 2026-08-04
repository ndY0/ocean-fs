//! Key hashing for routing.
//!
//! `HashKey` is a pre-computed key hash that flows through all routing
//! layers. Computed once at the HTTP entry point and passed through
//! routing, metadata lookup, and segment operations — never re-hashed.
//!
//! Note: `HashOutput` has moved to `oceanfs-hash` per ADR-0008. It is
//! still re-exported from this module's parent for backward compatibility.

/// A pre-computed key hash that flows through all routing layers.
///
/// Computed once at the HTTP entry point and passed through routing,
/// metadata lookup, and segment operations — never re-hashed.
///
/// # Examples
///
/// ```
/// use oceanfs_core::HashKey;
///
/// let hash_key = HashKey::from_bytes([0u8; 32]);
/// assert_eq!(hash_key.as_bytes().len(), 32);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HashKey([u8; 32]);

impl HashKey {
    /// Creates a `HashKey` from pre-computed SHA-256 hash bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn hash_key_from_bytes_and_as_bytes() {
        let bytes = [0xAAu8; 32];
        let key = HashKey::from_bytes(bytes);
        assert_eq!(key.as_bytes(), &bytes);
    }
}
