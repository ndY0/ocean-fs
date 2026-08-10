//! The `HashOutput` type — a 256-bit BLAKE3 hash.
//!
//! Used throughout the system as the object-content checksum, segment
//! checksum, and Merkle tree node hash. Defined in `oceanfs-core` as
//! a foundational type; `oceanfs-hash` re-exports it.

use std::fmt;

/// A 256-bit BLAKE3 hash output (32 bytes).
///
/// Used as the object-content checksum, segment checksum, and Merkle tree
/// node hash throughout the system.
///
/// # Examples
///
/// ```
/// use oceanfs_core::HashOutput;
///
/// let hash = HashOutput::from_bytes([0u8; 32]);
/// let hex = hash.to_hex();
/// assert_eq!(hex.len(), 64);
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct HashOutput([u8; 32]);

impl HashOutput {
    /// Creates a `HashOutput` from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the hash as a byte slice.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the hash as a lowercase hexadecimal string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Display for HashOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

// ---------------------------------------------------------------------------
// Hex encoding helper
// ---------------------------------------------------------------------------

/// Hex encoding for HashOutput.
mod hex {
    pub(super) const CHARS: &[u8; 16] = b"0123456789abcdef";

    pub(super) fn encode(bytes: [u8; 32]) -> String {
        let mut out = String::with_capacity(64);
        for byte in bytes {
            out.push(CHARS[(byte >> 4) as usize] as char);
            out.push(CHARS[(byte & 0x0f) as usize] as char);
        }
        out
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
    fn hash_output_to_hex_is_64_chars() {
        let hash = HashOutput::from_bytes([0xabu8; 32]);
        let hex = hash.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_output_to_hex_is_lowercase() {
        let hash = HashOutput::from_bytes([0xFFu8; 32]);
        let hex = hash.to_hex();
        assert_eq!(hex, "ff".repeat(32));
    }

    #[test]
    fn hash_output_as_bytes_returns_32_bytes() {
        let bytes = [42u8; 32];
        let hash = HashOutput::from_bytes(bytes);
        assert_eq!(hash.as_bytes(), &bytes);
    }

    #[test]
    fn hash_output_display_is_hex() {
        let hash = HashOutput::from_bytes([0x12u8; 32]);
        let displayed = hash.to_string();
        assert_eq!(displayed.len(), 64);
        assert_eq!(displayed, "12".repeat(32));
    }
}
