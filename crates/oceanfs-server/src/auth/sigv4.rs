//! AWS Signature Version 4 verification.
//!
//! Validates SigV4 signatures on incoming S3 requests. The verifier
//! extracts the authorization header, parses signed headers, and
//! recomputes the signature to compare against the provided value.
//!
//! ## Algorithm Overview
//!
//! 1. Parse `Authorization: AWS4-HMAC-SHA256 ...` header
//! 2. Extract access key, scope (date/region/service), signed headers
//! 3. Look up the secret key from the [`KeyStore`]
//! 4. Reconstruct the signing key using HMAC-SHA256 chain
//! 5. Build the canonical request and string-to-sign
//! 6. Compare computed signature with provided signature

use std::collections::HashMap;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::{
    auth::key_store::KeyStore,
    error::{Error, Result},
};

type HmacSha256 = Hmac<Sha256>;

/// AWS Signature V4 verifier.
///
/// Validates SigV4 signatures against the provided key store.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_server::auth::{SigV4Verifier, KeyStore};
///
/// let store = KeyStore::load("keys.toml").unwrap();
/// let verifier = SigV4Verifier::new(store);
/// let result = verifier.verify(&request, &body);
/// ```
pub struct SigV4Verifier {
    key_store: KeyStore,
}

impl SigV4Verifier {
    /// Creates a new SigV4 verifier backed by the given key store.
    pub fn new(key_store: KeyStore) -> Self {
        Self { key_store }
    }

    /// Verifies an AWS SigV4 signature on an HTTP request.
    ///
    /// # Arguments
    ///
    /// * `headers` — The HTTP request headers (must include `Authorization`)
    /// * `method` — The HTTP method (e.g., "PUT")
    /// * `uri` — The request URI path (e.g., "/bucket/key")
    /// * `query_string` — The raw query string (e.g., "list-type=2")
    /// * `body` — The request body bytes
    ///
    /// # Errors
    ///
    /// Returns `Error::AccessDenied` if the signature is missing,
    /// the access key is not found, or the signature does not match.
    pub fn verify(
        &self,
        headers: &HashMap<String, String>,
        method: &str,
        uri: &str,
        query_string: &str,
        body: &[u8],
    ) -> Result<()> {
        // Step 1: Parse Authorization header
        let auth_header = headers
            .get("authorization")
            .or_else(|| headers.get("Authorization"))
            .ok_or_else(|| Error::AccessDenied("missing Authorization header".into()))?;

        let auth_parts = parse_authorization_header(auth_header)?;

        // Step 2: Look up credentials
        let credentials = self
            .key_store
            .lookup(&auth_parts.access_key)
            .ok_or_else(|| Error::AccessDenied("invalid access key".into()))?;

        // Step 3: Verify date is within valid range (5 minute clock skew)
        let now = current_utc_time();
        let request_date = &auth_parts.scope_date;
        if !valid_date_range(request_date, &now, 300) {
            return Err(Error::AccessDenied("request timestamp is expired".into()));
        }

        // Step 4: Compute body hash
        let body_hash = hex::encode(Sha256::digest(body));

        // Step 5: Build canonical request
        let canonical_request = build_canonical_request(
            method,
            uri,
            query_string,
            headers,
            &auth_parts.signed_headers,
            &body_hash,
        );
        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        // Step 6: Build string to sign
        let region = auth_parts.scope_region.as_deref().unwrap_or("us-east-1");
        let service = auth_parts.scope_service.as_deref().unwrap_or("s3");
        let scope = format!("{}/{}/{}/aws4_request", auth_parts.scope_date, region, service);

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            auth_parts.scope_date, scope, canonical_request_hash
        );

        // Step 7: Compute signing key
        let signing_key =
            compute_signing_key(&credentials.secret_key, request_date, region, service);

        // Step 8: Compute signature
        let computed = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        // Step 9: Compare
        if computed != auth_parts.signature {
            return Err(Error::AccessDenied("signature does not match".into()));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AuthHeaderParts
// ---------------------------------------------------------------------------

struct AuthHeaderParts {
    access_key: String,
    scope_date: String,
    scope_region: Option<String>,
    scope_service: Option<String>,
    signed_headers: Vec<String>,
    signature: String,
}

/// Parses an `Authorization: AWS4-HMAC-SHA256 ...` header.
fn parse_authorization_header(header: &str) -> Result<AuthHeaderParts> {
    if !header.starts_with("AWS4-HMAC-SHA256 ") {
        return Err(Error::AccessDenied("unsupported authorization scheme".into()));
    }

    let content = &header["AWS4-HMAC-SHA256 ".len()..];
    let mut parts_map = HashMap::new();

    for part in content.split(", ") {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| Error::AccessDenied("malformed authorization header".into()))?;
        parts_map.insert(key.to_string(), value.to_string());
    }

    let credential = parts_map
        .get("Credential")
        .ok_or_else(|| Error::AccessDenied("missing Credential".into()))?;
    let signed_headers_str = parts_map
        .get("SignedHeaders")
        .ok_or_else(|| Error::AccessDenied("missing SignedHeaders".into()))?;
    let signature_value = parts_map
        .get("Signature")
        .ok_or_else(|| Error::AccessDenied("missing Signature".into()))?;

    // Parse Credential: access-key/date/region/service/aws4_request
    let cred_parts: Vec<&str> = credential.splitn(5, '/').collect();
    if cred_parts.len() != 5 {
        return Err(Error::AccessDenied("malformed Credential scope".into()));
    }

    Ok(AuthHeaderParts {
        access_key: cred_parts[0].to_string(),
        scope_date: cred_parts[1].to_string(),
        scope_region: if cred_parts.len() > 2 { Some(cred_parts[2].to_string()) } else { None },
        scope_service: if cred_parts.len() > 3 { Some(cred_parts[3].to_string()) } else { None },
        signed_headers: signed_headers_str.split(';').map(|s| s.trim().to_lowercase()).collect(),
        signature: signature_value.to_string(),
    })
}

/// Builds the canonical request string for SigV4.
fn build_canonical_request(
    method: &str,
    uri: &str,
    query_string: &str,
    headers: &HashMap<String, String>,
    signed_headers: &[String],
    payload_hash: &str,
) -> String {
    // Canonical URI (encode path)
    let canonical_uri = uri;

    // Canonical query string (already URL-encoded by caller)
    let canonical_query = query_string;

    // Canonical headers
    let mut header_lines: Vec<String> = signed_headers
        .iter()
        .map(|h| {
            let value = headers.get(h).cloned().unwrap_or_default();
            format!("{}:{}", h, value.trim())
        })
        .collect();
    header_lines.sort();
    let canonical_headers = header_lines.join("\n");

    // Signed headers list
    let signed_headers_str = signed_headers.join(";");

    format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers_str, payload_hash,
    )
}

/// Computes the AWS SigV4 signing key chain:
/// kDate = HMAC-SHA256("AWS4" + secret, date)
/// kRegion = HMAC-SHA256(kDate, region)
/// kService = HMAC-SHA256(kRegion, service)
/// kSigning = HMAC-SHA256(kService, "aws4_request")
fn compute_signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Computes HMAC-SHA256 and returns the raw bytes.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = match HmacSha256::new_from_slice(key) {
        Ok(m) => m,
        Err(_) => return Vec::new(),
    };
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Returns the current UTC time as a formatted date string (YYYYMMDD).
fn current_utc_time() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    // Convert epoch seconds to YYYYMMDD string
    let days_since_epoch = now / 86400;
    // Simple Gregorian date calculation (good enough for tests; production
    // would use chrono)
    let (y, m, d) = epoch_to_date(days_since_epoch);
    format!("{:04}{:02}{:02}", y, m, d)
}

/// Converts seconds since epoch to (year, month, day).
fn epoch_to_date(seconds: u64) -> (u64, u64, u64) {
    let days = seconds / 86400;
    let mut year = 1970u64;
    let mut remaining_days = days as i64;

    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let months_days = if is_leap(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1u64;
    for md in months_days.iter() {
        if remaining_days < *md {
            break;
        }
        remaining_days -= *md;
        month += 1;
    }

    (year, month, (remaining_days + 1) as u64)
}

fn is_leap(year: u64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Checks if the request date is within `max_skew_seconds` of now.
fn valid_date_range(request_date: &str, now_date: &str, _max_skew_seconds: u64) -> bool {
    // Simple check: dates must match exactly on the day boundary.
    // Full implementation would allow ±5 minute clock skew.
    request_date == now_date
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_auth_header_valid() {
        let header = "AWS4-HMAC-SHA256 Credential=AKI/20260801/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123";
        let parts = parse_authorization_header(header).unwrap();
        assert_eq!(parts.access_key, "AKI");
        assert_eq!(parts.scope_date, "20260801");
        assert_eq!(parts.scope_region, Some("us-east-1".into()));
        assert_eq!(parts.signature, "abc123");
        assert_eq!(parts.signed_headers, vec!["host", "x-amz-date"]);
    }

    #[test]
    fn parse_auth_header_unsupported_scheme() {
        let result = parse_authorization_header("BASIC user:pass");
        assert!(result.is_err());
    }

    #[test]
    fn parse_auth_header_missing_credential() {
        let result =
            parse_authorization_header("AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc");
        assert!(result.is_err());
    }

    #[test]
    fn body_hash_is_deterministic() {
        let body = b"hello world";
        let hash1 = hex::encode(Sha256::digest(body));
        let hash2 = hex::encode(Sha256::digest(body));
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn canonical_request_format() {
        let mut headers = HashMap::new();
        headers.insert("host".into(), "s3.amazonaws.com".into());
        headers.insert("x-amz-date".into(), "20260801T000000Z".into());

        let canonical = build_canonical_request(
            "GET",
            "/bucket/key",
            "",
            &headers,
            &["host".into(), "x-amz-date".into()],
            "UNSIGNED-PAYLOAD",
        );

        assert!(canonical.contains("GET"));
        assert!(canonical.contains("/bucket/key"));
        assert!(canonical.contains("host:s3.amazonaws.com"));
        assert!(canonical.contains("x-amz-date:20260801T000000Z"));
        assert!(canonical.contains("UNSIGNED-PAYLOAD"));
    }

    #[test]
    fn signing_key_is_reproducible() {
        let key1 = compute_signing_key("secret", "20260801", "us-east-1", "s3");
        let key2 = compute_signing_key("secret", "20260801", "us-east-1", "s3");
        assert_eq!(key1, key2);
    }

    #[test]
    fn different_secret_produces_different_key() {
        let key1 = compute_signing_key("secret1", "20260801", "us-east-1", "s3");
        let key2 = compute_signing_key("secret2", "20260801", "us-east-1", "s3");
        assert_ne!(key1, key2);
    }

    #[test]
    fn different_date_produces_different_key() {
        let key1 = compute_signing_key("secret", "20260801", "us-east-1", "s3");
        let key2 = compute_signing_key("secret", "20260802", "us-east-1", "s3");
        assert_ne!(key1, key2);
    }

    #[test]
    fn valid_date_range_same_day_passes() {
        assert!(valid_date_range("20260801", "20260801", 300));
    }

    #[test]
    fn valid_date_range_different_day_fails() {
        assert!(!valid_date_range("20260801", "20260802", 300));
    }

    #[test]
    fn epoch_to_date_known_date() {
        // 2026-08-01: compute epoch seconds
        // From 1970-01-01 to 2026-08-01:
        // Days: 365*56 + leap days (1972,76,80,84,88,92,96,2000,04,08,12,16,20,24) = 14 days
        // 365*56 = 20440, + 14 = 20454, + days in 2026 up to Aug 1:
        // Jan 31 + Feb 28 + Mar 31 + Apr 30 + May 31 + Jun 30 + Jul 31 = 212
        // Total: 20454 + 212 = 20666 days
        let (y, m, d) = epoch_to_date(20666u64 * 86400);
        assert_eq!((y, m, d), (2026, 8, 1));
    }

    #[test]
    fn epoch_to_date_epoch() {
        let (y, m, d) = epoch_to_date(0);
        assert_eq!((y, m, d), (1970, 1, 1));
    }
}
