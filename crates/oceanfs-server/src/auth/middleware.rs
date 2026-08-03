//! Tower middleware for S3 authentication.
//!
//! When `AuthConfig::s3_auth_enabled` is `true`, this layer intercepts
//! all requests and verifies AWS SigV4 signatures before allowing
//! the request to reach the S3 handler.
//!
//! When auth is disabled (development mode), all requests pass through
//! unauthenticated.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
};
use http_body_util::BodyExt;
use tower::{Layer, Service};
use tracing::warn;

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
        Self { enabled, verifier: verifier.map(Arc::new) }
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
        AuthService { inner, enabled: self.enabled, verifier: self.verifier.clone() }
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
    S: Service<Request<Body>, Response = Response> + Clone + Send + 'static,
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

        let verifier = match &self.verifier {
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

        // Extract request fields needed for SigV4 verification.
        let method = request.method().to_string();
        let uri_path = request.uri().path().to_string();
        let query_string = request.uri().query().unwrap_or("").to_string();

        // Collect headers into a HashMap (lowercase keys for case-insensitive matching).
        let mut headers: HashMap<String, String> = HashMap::new();
        for (name, value) in request.headers() {
            if let Ok(v) = value.to_str() {
                headers.insert(name.as_str().to_lowercase(), v.to_string());
            }
        }

        // Buffer the body for hash computation. The body must be available
        // for SHA-256 hashing (required by SigV4) and then forwarded to
        // the inner service after verification.
        let (parts, body) = request.into_parts();

        // Clone inner service to move into the async block.
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Collect body bytes.
            let body_bytes = match body.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_e) => {
                    warn!("failed to read request body for auth verification");
                    #[allow(clippy::unwrap_used)]
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Failed to read request body"))
                        .unwrap_or_else(|_| {
                            // SAFETY: INTERNAL_SERVER_ERROR + empty body is infallible.
                            Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::empty())
                                .unwrap()
                        }));
                }
            };

            // Verify the SigV4 signature.
            if let Err(e) =
                verifier.verify(&headers, &method, &uri_path, &query_string, &body_bytes)
            {
                warn!(error = %e, "SigV4 verification failed");
                #[allow(clippy::unwrap_used)]
                return Ok(Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(Body::from(format!("AccessDenied: {e}")))
                    .unwrap());
            }

            // Reconstruct the request with the original body bytes.
            let request = Request::from_parts(parts, Body::from(body_bytes));
            inner.call(request).await
        })
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
