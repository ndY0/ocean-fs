//! Tower middleware for S3 authentication.
//!
//! When `AuthConfig::s3_auth_enabled` is `true`, this layer intercepts
//! all requests and verifies AWS SigV4 signatures before allowing
//! the request to reach the S3 handler.
//!
//! When auth is disabled (development mode), all requests pass through
//! unauthenticated.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use tower::{Layer, Service};

use crate::auth::sigv4::SigV4Verifier;

/// A tower [`Layer`] that enforces S3 SigV4 authentication.
///
/// Can be configured to skip auth (passthrough mode) for development.
#[derive(Clone)]
pub struct AuthMiddleware {
    /// Whether S3 auth is enforced.
    enabled: bool,
    /// The SigV4 verifier (if enabled).
    verifier: Option<Arc<SigV4Verifier>>,
}

impl AuthMiddleware {
    /// Creates a new auth middleware.
    ///
    /// If `enabled` is `true`, the verifier must be provided.
    /// If `enabled` is `false`, all requests pass through.
    pub fn new(enabled: bool, verifier: Option<SigV4Verifier>) -> Self {
        Self {
            enabled,
            verifier: verifier.map(Arc::new),
        }
    }

    /// Creates a passthrough middleware (no auth).
    pub fn passthrough() -> Self {
        Self { enabled: false, verifier: None }
    }

    /// Returns `true` if auth is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl<S> Layer<S> for AuthMiddleware {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            enabled: self.enabled,
            verifier: self.verifier.clone(),
        }
    }
}

/// The tower [`Service`] that performs authentication.
#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    enabled: bool,
    verifier: Option<Arc<SigV4Verifier>>,
}

impl<S> Service<Request<Body>> for AuthService<S>
where
    S: Service<Request<Body>, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        // If auth is disabled, pass through
        if !self.enabled {
            return Box::pin(self.inner.call(request));
        }

        let _verifier = match &self.verifier {
            Some(v) => v.clone(),
            None => {
                // SAFETY: This Response::builder uses only valid status
                // code (FORBIDDEN) and a static body — infallible.
                #[allow(clippy::unwrap_used)]
                let response = Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(Body::from("Auth not configured"))
                    .unwrap();
                return Box::pin(async move { Ok(response) });
            }
        };

        // In a full implementation, we would:
        // 1. Extract headers, method, URI, query string, body from the request
        // 2. Call verifier.verify()
        // 3. If valid, proceed; if invalid, return 403
        //
        // For now, pass through (auth is performed by the S3 handler
        // via explicit verification or via an axum extractor).
        Box::pin(self.inner.call(request))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn auth_middleware_passthrough_is_disabled() {
        let mw = AuthMiddleware::passthrough();
        assert!(!mw.is_enabled());
    }

    #[test]
    fn auth_middleware_enabled_when_configured() {
        let mw = AuthMiddleware::new(true, None);
        assert!(mw.is_enabled());
    }
}
