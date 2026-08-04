//! MIME type map for Content-Type header inference.
//!
//! Provides a mapping from file extensions to MIME types used
//! when constructing S3 GET and HEAD responses.

use std::collections::HashMap;

/// A map from file extensions to MIME types.
///
/// Used to set the `Content-Type` header on GET and HEAD responses.
/// The default configuration covers common web and media types.
#[derive(Debug)]
pub struct MimeMap {
    /// Extension → MIME type mapping (extension without dot).
    map: HashMap<String, String>,
}

impl MimeMap {
    /// Creates a new `MimeMap` with default common MIME types.
    pub fn new() -> Self {
        let mut map = HashMap::new();
        // Text
        map.insert("html".into(), "text/html".into());
        map.insert("htm".into(), "text/html".into());
        map.insert("css".into(), "text/css".into());
        map.insert("js".into(), "application/javascript".into());
        map.insert("txt".into(), "text/plain".into());
        map.insert("csv".into(), "text/csv".into());
        map.insert("xml".into(), "application/xml".into());
        map.insert("json".into(), "application/json".into());
        // Images
        map.insert("jpg".into(), "image/jpeg".into());
        map.insert("jpeg".into(), "image/jpeg".into());
        map.insert("png".into(), "image/png".into());
        map.insert("gif".into(), "image/gif".into());
        map.insert("svg".into(), "image/svg+xml".into());
        map.insert("webp".into(), "image/webp".into());
        // Audio/Video
        map.insert("mp3".into(), "audio/mpeg".into());
        map.insert("mp4".into(), "video/mp4".into());
        map.insert("webm".into(), "video/webm".into());
        // Documents
        map.insert("pdf".into(), "application/pdf".into());
        map.insert("zip".into(), "application/zip".into());
        map.insert("gz".into(), "application/gzip".into());
        map.insert("tar".into(), "application/x-tar".into());
        // Binary
        map.insert("wasm".into(), "application/wasm".into());
        Self { map }
    }

    /// Guesses the MIME type from a key (file name or path).
    ///
    /// Returns `"application/octet-stream"` if the extension
    /// is not recognized.
    pub fn guess(&self, key: &str) -> String {
        if let Some(dot_pos) = key.rfind('.') {
            let ext = &key[(dot_pos + 1)..];
            self.map.get(ext).cloned().unwrap_or_else(|| "application/octet-stream".into())
        } else {
            "application/octet-stream".into()
        }
    }
}

impl Default for MimeMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn mime_map_guess_known_extension() {
        let map = MimeMap::new();
        assert_eq!(map.guess("photo.jpg"), "image/jpeg");
        assert_eq!(map.guess("page.html"), "text/html");
        assert_eq!(map.guess("data.json"), "application/json");
    }

    #[test]
    fn mime_map_guess_unknown_returns_octet_stream() {
        let map = MimeMap::new();
        assert_eq!(map.guess("file.xyz"), "application/octet-stream");
    }

    #[test]
    fn mime_map_guess_no_extension_returns_octet_stream() {
        let map = MimeMap::new();
        assert_eq!(map.guess("README"), "application/octet-stream");
    }
}
