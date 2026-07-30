---
feature: "Authentication & mTLS"
epic: "phase-5-s3-api"
status: proposed
priority: medium
owner: ""
dependencies:
  - feature: s3-http-handlers
    reason: Authentication middleware wraps S3 handlers
  - feature: connection-pool-grpc
    reason: mTLS shares TLS configuration with gRPC pool
adr: []
perf: []
created: 2026-07-30
updated: 2026-07-30
---

# Authentication & mTLS

## Summary

Implement authentication for the S3 HTTP API and mutual TLS for internal
node-to-node gRPC communication in `oceanfs-server`. S3 clients authenticate
via AWS Signature V4 (configurable). Internal nodes authenticate via mTLS with
shared or per-node certificates. This is a foundational security layer; the
implementation is a placeholder framework initially, with full multi-tenancy
deferred.

## Scope

### In Scope
- AWS Signature V4 verification for S3 API requests (configurable enable/disable)
- `Authorization` header parsing: extract access key, signed headers, signature
- Signature verification: reconstruct signing key, compare signatures
- Access key → bucket policy mapping (simple file-based key store initially)
- mTLS configuration for gRPC: server cert + client CA, permissive or strict mode
- TLS config shared between HTTP server (optional TLS) and gRPC connections
- `AuthMiddleware`: axum/tower middleware layer that runs before handlers
- Config flags: `s3_auth_enabled`, `mtls_enabled`, `tls_cert_path`, `tls_key_path`
- Anonymous access mode (auth disabled) for development
- Unit tests for signature V4 verification, mTLS handshake

### Out of Scope
- Full IAM-style multi-tenancy (future work, spec §16)
- OIDC/LDAP integration
- Token-based authentication
- Per-bucket access control policies
- Certificate rotation automation

## Crate Impact

| Crate | Change |
|---|---|
| `oceanfs-core` | New types: `AuthConfig`, `AccessKey`, `Credentials` |
| `oceanfs-server` | New modules: `auth/sigv4.rs`, `auth/middleware.rs`, `auth/key_store.rs` |
| `oceanfs-network` | Updated TLS module: `tls.rs` → shared mTLS config |

## Interface (Public API)

- `pub struct AuthConfig` — `s3_auth_enabled: bool`, `mtls_enabled: bool`, `tls_cert_path: Option<PathBuf>`, `tls_key_path: Option<PathBuf>`, `client_ca_path: Option<PathBuf>`, `access_keys_path: Option<PathBuf>`
- `pub(crate) struct SigV4Verifier` — `pub(crate) fn verify(&self, request: &http::Request<Body>, secret_key: &str) -> Result<()>`
- `pub(crate) struct KeyStore` — `pub(crate) fn load(path: &Path) -> Result<Self>`, `pub(crate) fn lookup(&self, access_key: &AccessKey) -> Option<Credentials>`
- `pub(crate) struct AuthMiddleware` — tower `Layer` that enforces S3 auth on configured routes

## Data Flow

```
Authenticated S3 request:
  PUT /{bucket}/{key}
  Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260730/...
    → AuthMiddleware intercepts
      ├─ auth disabled? → pass through
      ├─ auth enabled:
      │    ├─ Parse Authorization header → extract access_key, scope, signed_headers, signature
      │    ├─ KeyStore::lookup(access_key) → Some(credentials) or 403
      │    ├─ SigV4Verifier::verify(request, secret_key)
      │    │    ├─ Reconstruct signing key from secret + date + region + service
      │    │    ├─ Build canonical request → hash → string_to_sign
      │    │    └─ Compare computed signature with provided
      │    │         ├─ Match → proceed to handler
      │    │         └─ Mismatch → 403 Forbidden (SignatureDoesNotMatch)
      │    └─ 403 if key not found (InvalidAccessKeyId)
      └─ Continue to S3 handler

Internal gRPC mTLS:
  Node A → Node B:
    ├─ Client TLS config: load client cert + key, trust server CA
    ├─ Server TLS config: load server cert + key, require client cert
    ├─ Both nodes present certificates signed by shared CA → handshake succeeds
    └─ Unauthorized node (no cert or wrong CA) → connection refused
```

## Definition of Done

- [ ] **Code:** `cargo build --all-targets` succeeds in affected crates
- [ ] **Tests:** Unit tests: valid SigV4 signature passes, tampered body fails, expired timestamp fails, wrong secret key fails, anonymous mode passes through all requests, mTLS handshake between two nodes with valid certs succeeds, mTLS with wrong CA fails
- [ ] **Coverage:** `cargo tarpaulin --fail-under 80` on `oceanfs-server`
- [ ] **Lint:** `cargo clippy -- -D warnings` passes
- [ ] **Docs:** `#![deny(missing_docs)]` passes; `AuthConfig` documented
- [ ] **ADR:** N/A
- [ ] **Perf:** N/A (auth is not on the data hot path after verification; signature verification is CPU-bound but once per request)
- [ ] **Integration:** `tests/auth_sigv4.rs`: signed S3 request (using aws-sigv4 crate) → passes auth → reaches handler; unsigned request → 403; `tests/mtls.rs`: two nodes with mTLS enabled → gRPC call succeeds; one node without cert → connection refused
- [ ] **Manual:** Example in docs: generate self-signed certs, configure mTLS, verify connection
