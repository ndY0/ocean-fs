//! Build script for oceanfs-core.
//!
//! Generates protobuf Rust types for message definitions shared across
//! crates: common, segment, and membership message types.
//!
//! Service definitions (gossip, storage, healing, cache) are generated
//! in oceanfs-network's build.rs.
//!
//! ## Regeneration
//!
//! The generated files are checked into version control. To regenerate
//! them (after changing .proto files), run:
//!
//! ```sh
//! OCEANFS_REGENERATE_PROTO=1 cargo build -p oceanfs-core
//! ```

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("OCEANFS_REGENERATE_PROTO").is_err() {
        println!("cargo:warning=skipping proto generation (set OCEANFS_REGENERATE_PROTO=1 to regenerate)");
        return Ok(());
    }

    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let workspace_root = crate_dir.parent().and_then(|p| p.parent()).expect("cannot find workspace root");
    let proto_dir = workspace_root.join("proto");
    let out_dir = PathBuf::from("src/generated");
    std::fs::create_dir_all(&out_dir)?;

    tonic_build::configure()
        .out_dir(&out_dir)
        .build_server(false)
        .build_client(false)
        .compile_protos(
            &[
                proto_dir.join("oceanfs/common.proto"),
                proto_dir.join("oceanfs/segment.proto"),
                proto_dir.join("oceanfs/membership.proto"),
            ],
            &[proto_dir],
        )?;

    Ok(())
}
