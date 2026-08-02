//! Build script for oceanfs-core.
//!
//! Generates protobuf Rust types for message definitions shared across
//! crates: common, segment, and membership message types.
//!
//! Service definitions (gossip, storage, healing, cache) are generated
//! in oceanfs-network's build.rs with extern_path mappings pointing
//! back to the modules generated here.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let workspace_root = crate_dir.parent().and_then(|p| p.parent()).expect("cannot find workspace root");
    let proto_dir = workspace_root.join("proto");

    let out_dir = PathBuf::from("src/generated");
    std::fs::create_dir_all(&out_dir)?;

    let protos = &[
        proto_dir.join("oceanfs/common.proto"),
        proto_dir.join("oceanfs/segment.proto"),
        proto_dir.join("oceanfs/membership.proto"),
    ];

    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_build::configure()
        .out_dir(&out_dir)
        .build_server(false)
        .build_client(false)
        .compile_protos(protos, &[proto_dir])?;

    Ok(())
}
