//! Integration test: boot refusal over a **pre-pool** data directory
//! (ADR-0031 D3).
//!
//! A directory written by a pre-pools-era node contains `pool_id`-less
//! Seal records in the event WAL and/or v2 checkpoints. There is no
//! migration and no continued decode: `Node::start` must refuse with an
//! explicit "unsupported pre-pool data directory" error — never a silent
//! start from an older snapshot, an empty registry, or a truncation of
//! the "torn tail" (the pre-pool shape is a complete, well-formed
//! record).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use oceanfs_core::NodeConfig;
use oceanfs_node::Node;

/// A bootable pools-enabled node config (ADR-0031 f1 topology: one data
/// — id 0 — one wal, one metadata, one hints pool on sibling roots).
fn node_config(tmp: &tempfile::TempDir) -> NodeConfig {
    fn pool(
        name: &str,
        role: oceanfs_core::PoolRole,
        root: std::path::PathBuf,
    ) -> oceanfs_core::StoragePoolConfig {
        oceanfs_core::StoragePoolConfig {
            name: name.into(),
            role,
            root,
            weight: None,
            tech: Default::default(),
            health: Default::default(),
        }
    }
    NodeConfig {
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),      // ephemeral port
        grpc_listen_addr: "127.0.0.1:0".into(), // ephemeral port
        storage: oceanfs_core::StorageConfig {
            pools: vec![
                pool("data-0", oceanfs_core::PoolRole::Data, tmp.path().join("pool-data")),
                pool("wal-0", oceanfs_core::PoolRole::Wal, tmp.path().join("pool-wal")),
                pool("meta-0", oceanfs_core::PoolRole::Metadata, tmp.path().join("pool-meta")),
                pool("hints-0", oceanfs_core::PoolRole::Hints, tmp.path().join("pool-hints")),
            ],
            missing_root_policy: oceanfs_core::MissingRootPolicy::Fatal,
        },
        ..NodeConfig::default()
    }
}

/// The node opens its event WAL at `{wal pool root}/event-wal` (the
/// role-pinned path resolved by `pool_paths` — the config-level
/// `event_wal_dir` is always overridden).
fn event_wal_dir(tmp: &tempfile::TempDir) -> std::path::PathBuf {
    let dir = tmp.path().join("pool-wal").join("event-wal");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Crafts a full pre-pool Seal record: the 48-byte payload WITHOUT the
/// pool-id flag/bytes that every pre-pools-era writer produced (magic
/// "EVL\1", record version 1, kind 1 = Seal, CRC over header + payload).
fn craft_prepool_seal_record() -> Vec<u8> {
    let mut payload = Vec::with_capacity(48);
    payload.push(2); // tier byte: Standard
    payload.push(4); // ec_k
    payload.push(2); // ec_m
    payload.push(0); // flags — pre-pool records have no pool-id bit
    payload.extend_from_slice(&[0xAB; 32]); // merkle_root
    payload.extend_from_slice(&3u32.to_le_bytes()); // data_wal_pos.file_seq
    payload.extend_from_slice(&4096u64.to_le_bytes()); // data_wal_pos.offset
    let mut buf = Vec::with_capacity(28 + payload.len() + 4);
    buf.extend_from_slice(b"EVL\x01");
    buf.push(1); // version
    buf.push(1); // kind = Seal
    buf.extend_from_slice(&[0u8; 2]); // reserved
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&[0xAA; 16]); // segment_id
    buf.extend_from_slice(&payload);
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// Crafts a minimal v2 (pre-pool) checkpoint: magic "CHK\1", version
/// byte 2, covered pos, zero entries, CRC over the preceding bytes.
fn craft_prepool_v2_checkpoint() -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"CHK\x01");
    buf.push(2); // pre-pool checkpoint version
    buf.extend_from_slice(&3u32.to_le_bytes()); // covered file_seq
    buf.extend_from_slice(&4096u64.to_le_bytes()); // covered offset
    buf.extend_from_slice(&0u32.to_le_bytes()); // entry_count = 0
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// A pre-pool Seal record in the event WAL refuses boot at open: the
/// node must surface the explicit pre-pool error (never truncate the
/// "torn tail" and never boot silently over pre-pool data).
#[tokio::test]
async fn prepool_seal_record_refuses_node_start() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(event_wal_dir(&tmp).join("evl_00000000.log"), craft_prepool_seal_record())
        .unwrap();

    let err = match Node::start(node_config(&tmp)).await {
        Ok(_) => panic!("pre-pool log must refuse boot"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported pre-pool data directory"),
        "the boot error must carry the explicit pre-pool message: {msg}"
    );
    // The pre-pool record must NOT have been truncated away by the
    // open-time torn-tail self-heal.
    let survived = std::fs::read(tmp.path().join("pool-wal/event-wal/evl_00000000.log")).unwrap();
    assert_eq!(survived.len(), craft_prepool_seal_record().len(), "pre-pool record survives");
}

/// A v2 (pre-pool) checkpoint refuses boot at startup recovery: the
/// node must not fall back to an older snapshot or an empty registry
/// over a pre-pool directory.
#[tokio::test]
async fn prepool_v2_checkpoint_refuses_node_start() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        event_wal_dir(&tmp).join("checkpoint-00000003-4096"),
        craft_prepool_v2_checkpoint(),
    )
    .unwrap();

    let err = match Node::start(node_config(&tmp)).await {
        Ok(_) => panic!("v2 checkpoint must refuse boot"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported pre-pool data directory"),
        "the boot error must carry the explicit pre-pool message: {msg}"
    );
}
