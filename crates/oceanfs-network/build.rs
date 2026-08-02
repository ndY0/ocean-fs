//! Build script for oceanfs-network.
//!
//! Generates gRPC client and server stubs for all OceanFS RPC services:
//! segment, gossip, healing, scrub, and cache.
//!
//! Message types used by these services are generated in oceanfs-core's
//! build.rs (common, segment, membership).
//!
//! ## Regeneration
//!
//! The generated files are checked into version control. To regenerate
//! them (after changing .proto files), run:
//!
//! ```sh
//! OCEANFS_REGENERATE_PROTO=1 cargo build -p oceanfs-network
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
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                proto_dir.join("oceanfs/storage.proto"),
                proto_dir.join("oceanfs/gossip.proto"),
                proto_dir.join("oceanfs/healing.proto"),
                proto_dir.join("oceanfs/cache.proto"),
                proto_dir.join("oceanfs/scrub.proto"),
            ],
            &[proto_dir],
        )?;

    Ok(())
}
