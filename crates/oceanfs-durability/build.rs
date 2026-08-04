//! Build script for oceanfs-durability.
//!
//! Generates gRPC client and server stubs for healing and scrub RPCs
//! owned by this crate.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let workspace_root =
        crate_dir.parent().and_then(|p| p.parent()).expect("cannot find workspace root");
    let proto_dir = workspace_root.join("proto");

    let out_dir = PathBuf::from("src/generated");
    std::fs::create_dir_all(&out_dir)?;

    let protos = &[proto_dir.join("oceanfs/healing.proto"), proto_dir.join("oceanfs/scrub.proto")];

    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_build::configure()
        .extern_path(".oceanfs.common", "::oceanfs_core::proto::common")
        .extern_path(".oceanfs.segment", "::oceanfs_core::proto::segment")
        .extern_path(".oceanfs.membership", "::oceanfs_core::proto::membership")
        .out_dir(&out_dir)
        .build_server(true)
        .build_client(true)
        .compile_protos(protos, &[proto_dir])?;

    Ok(())
}
