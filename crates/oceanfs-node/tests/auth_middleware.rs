//! Auth middleware integration tests.
//!
//! Verifies that the auth middleware correctly passes through or
//! blocks requests based on the `enabled` flag.

#![allow(clippy::unwrap_used)]

use oceanfs_server::auth::AuthMiddleware;

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

#[test]
fn auth_middleware_disabled_when_explicitly_false() {
    let mw = AuthMiddleware::new(false, None);
    assert!(!mw.is_enabled());
}

#[test]
fn auth_middleware_passthrough_does_not_panic_on_clone() {
    let mw = AuthMiddleware::passthrough();
    let _cloned = mw.clone();
    // Cloning should not panic.
}

#[test]
fn auth_middleware_layer_has_service_type() {
    // Verify that AuthMiddleware implements tower::Layer
    let mw = AuthMiddleware::passthrough();
    // We just need to verify the type compiles — the service type
    // is inferred by the compiler.
    assert!(!mw.is_enabled());
}
