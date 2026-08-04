//! Build script for oceanfs-cache.
//!
//! Generates gRPC client and server stubs for cache invalidation
//! services owned by this crate.
//!
//! Uses `extern_path` to reference message types generated in oceanfs-core
//! so that tonic-build emits absolute `::oceanfs_core::proto::*` paths.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let workspace_root =
        crate_dir.parent().and_then(|p| p.parent()).expect("cannot find workspace root");
    let proto_dir = workspace_root.join("proto");

    let out_dir = PathBuf::from("src/generated");
    std::fs::create_dir_all(&out_dir)?;

    let protos = &[proto_dir.join("oceanfs/cache.proto")];

    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_build::configure()
        .extern_path(".oceanfs.common", "::oceanfs_core::proto::common")
        .out_dir(&out_dir)
        .build_server(true)
        .build_client(true)
        .compile_protos(protos, &[proto_dir])?;

    Ok(())
}
