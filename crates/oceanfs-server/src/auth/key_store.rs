//! File-based access key store.
//!
//! Loads access keys and their corresponding secret keys from a
//! TOML file. Used by [`SigV4Verifier`](super::sigv4::SigV4Verifier) to look up credentials
//! during signature verification.

use std::{collections::HashMap, path::Path};

use serde::Deserialize;

/// Credentials for an access key.
#[derive(Debug, Clone, Deserialize)]
pub struct Credentials {
    /// The access key ID (e.g., "AKIAIOSFODNN7EXAMPLE").
    pub access_key: String,
    /// The corresponding secret key.
    pub secret_key: String,
    /// Optional human-readable name for the principal.
    #[serde(default)]
    pub description: String,
}

/// A file-based store mapping access keys to credentials.
///
/// Loaded from a TOML file with format:
///
/// ```toml
/// [[keys]]
/// access_key = "AKIAIOSFODNN7EXAMPLE"
/// secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
/// description = "admin"
/// ```
#[derive(Debug, Clone)]
pub struct KeyStore {
    keys: HashMap<String, Credentials>,
}

impl KeyStore {
    /// Loads access keys from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or is not
    /// valid TOML.
    pub fn load(path: &Path) -> Result<Self, KeyStoreError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| KeyStoreError::Io(e.to_string()))?;

        #[derive(Deserialize)]
        struct KeysFile {
            keys: Vec<Credentials>,
        }

        let keys_file: KeysFile =
            toml::from_str(&content).map_err(|e| KeyStoreError::Parse(e.to_string()))?;

        let mut map = HashMap::new();
        for cred in keys_file.keys {
            map.insert(cred.access_key.clone(), cred);
        }

        Ok(Self { keys: map })
    }

    /// Looks up credentials for the given access key.
    ///
    /// Returns `None` if the access key is not found.
    pub fn lookup(&self, access_key: &str) -> Option<&Credentials> {
        self.keys.get(access_key)
    }

    /// Returns the number of keys in the store.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns `true` if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// Errors that can occur when loading the key store.
#[derive(Debug, thiserror::Error)]
pub enum KeyStoreError {
    /// I/O error reading the keys file.
    #[error("I/O error: {0}")]
    Io(String),

    /// TOML parse error.
    #[error("parse error: {0}")]
    Parse(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn key_store_lookup_found() {
        let mut map = HashMap::new();
        map.insert(
            "AKI".into(),
            Credentials {
                access_key: "AKI".into(),
                secret_key: "secret1".into(),
                description: String::new(),
            },
        );
        let store = KeyStore { keys: map };
        let cred = store.lookup("AKI").unwrap();
        assert_eq!(cred.secret_key, "secret1");
    }

    #[test]
    fn key_store_lookup_not_found() {
        let store = KeyStore { keys: HashMap::new() };
        assert!(store.lookup("UNKNOWN").is_none());
    }

    #[test]
    fn key_store_len() {
        let mut map = HashMap::new();
        map.insert(
            "a".into(),
            Credentials {
                access_key: "a".into(),
                secret_key: "s".into(),
                description: String::new(),
            },
        );
        map.insert(
            "b".into(),
            Credentials {
                access_key: "b".into(),
                secret_key: "t".into(),
                description: String::new(),
            },
        );
        let store = KeyStore { keys: map };
        assert_eq!(store.len(), 2);
    }
}
