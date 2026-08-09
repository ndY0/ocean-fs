//! Build script for oceanfs-durability.
//!
//! Generates gRPC client and server stubs for healing and scrub RPCs
//! owned by this crate.

#![allow(clippy::expect_used, clippy::needless_borrows_for_generic_args)]

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let workspace_root =
        crate_dir.parent().and_then(|p| p.parent()).expect("cannot find workspace root");
    let proto_dir = workspace_root.join("proto");

    let out_dir = PathBuf::from("src/generated");
    std::fs::create_dir_all(&out_dir)?;

    // Step 1: Generate hinted_handoff proto types first.
    let hint_protos = &[proto_dir.join("oceanfs/hinted_handoff.proto")];
    for proto in hint_protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_build::configure()
        .extern_path(".oceanfs.common", "::oceanfs_core::proto::common")
        .out_dir(&out_dir)
        .build_server(false)
        .build_client(false)
        .bytes(&["."])
        .compile_protos(hint_protos, &[proto_dir.as_path()])?;

    // Step 2: Generate healing and scrub proto types, using extern_path for
    // hinted_handoff to avoid name collision with our source module.
    let svc_protos =
        &[proto_dir.join("oceanfs/healing.proto"), proto_dir.join("oceanfs/scrub.proto")];
    for proto in svc_protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_build::configure()
        .extern_path(".oceanfs.common", "::oceanfs_core::proto::common")
        .extern_path(".oceanfs.segment", "::oceanfs_core::proto::segment")
        .extern_path(".oceanfs.membership", "::oceanfs_core::proto::membership")
        .extern_path(".oceanfs.hinted_handoff", "crate::hinted_handoff_rpc")
        .out_dir(&out_dir)
        .build_server(true)
        .build_client(true)
        .bytes(&["."])
        .compile_protos(svc_protos, &[proto_dir])?;

    Ok(())
}
