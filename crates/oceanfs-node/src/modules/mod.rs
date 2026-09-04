//! Composition-root subsystem builders (c1–c5 decomposition).
//!
//! The 2026-09 review program decomposes `Node::start()` into plain module
//! builders — one function per subsystem bundle, each returning a typed
//! struct that owns its components (see
//! `docs/features/refactoring/composition-root-decomposition/`). No DI
//! framework: the explicit construction graph stays visible at the call
//! site, per architecture.md §4.1.

pub(crate) mod storage;
