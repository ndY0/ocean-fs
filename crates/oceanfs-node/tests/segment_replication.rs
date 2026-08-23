//! Integration test (sealed-segment-replication DoD): the multi-node
//! durability test that should have existed since phase 2.
//!
//! A 3-node local cluster (legacy mode, RF=3): concurrent PUTs on the
//! owner (A) pack multiple objects per small segment (32 KiB bodies into
//! 64 KiB-target segments — so mid-segment objects exist, the case the
//! old offset-0 fragment never covered). After seals fire, A's segment
//! replicator pushes each sealed segment's full data section to the
//! segment's ring replicas (B and C). The test then:
//!   1. asserts B's and C's segment stores actually HOLD the sealed
//!      segments (filesystem-level: the `.dat` files exist);
//!   2. deletes A's `.dat` files (the disk-death scenario);
//!   3. reads every object back THROUGH A: its local reads fail, the
//!      gRPC fallback serves the bytes from the replicas — hashes must
//!      match.
//!
//! Before this feature, step 3 failed for every mid-segment object (the
//! only replicas ever written were offset-0 fragments of the first
//! object per segment).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use oceanfs_core::NodeConfig;
use oceanfs_node::Node;

/// Reserves `n` distinct free TCP ports by binding `n` listeners at once
/// (ephemeral ports are handed out sequentially — binding one at a time
/// and dropping lets the OS reuse the same port for the next call).
/// The listeners are held open while the ports are collected, then
/// dropped; the nodes bind the ports a moment later (a tiny race, safe
/// under `--test-threads=1`).
fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> =
        (0..n).map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0")).collect();
    let ports = listeners.iter().map(|l| l.local_addr().expect("local addr").port()).collect();
    drop(listeners);
    ports
}

/// One cluster node's fixed addresses (membership plane needs a stable
/// address the seeds point at; gRPC is announced via gossip).
struct NodeAddrs {
    grpc: String,
    membership: String,
}

/// Boots a node with the given id and seed (empty = first node).
///
/// `fast_gc` enables compaction-friendly timings (1 s tombstone TTL, 1 s
/// GC interval, low compact threshold) — used by the compaction-variant
/// test so a DELETE → tombstone-expiry → compaction cycle runs in
/// seconds instead of the 3-day default TTL.
async fn boot_node(
    id: &str,
    seed: Option<&str>,
    addrs: &NodeAddrs,
    fast_gc: bool,
) -> (Node, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = NodeConfig {
        node_id: id.to_string(),
        data_dir: tmp.path().join("data"),
        listen_addr: "127.0.0.1:0".into(),
        grpc_listen_addr: addrs.grpc.clone(),
        membership_listen_addr: addrs.membership.clone(),
        gossip: oceanfs_core::GossipConfig {
            // Fast convergence for the test.
            interval_ms: 250,
            suspicion_timeout_ms: 60_000,
            failure_timeout_ms: 120_000,
            seed_nodes: seed.map(|s| vec![s.to_string()]).unwrap_or_default(),
            ..Default::default()
        },
        gc_interval_sec: if fast_gc { 1 } else { 3600 },
        tombstone_ttl_sec: if fast_gc { 1 } else { 259200 },
        // Compaction fires when liveness_ratio < threshold. The test
        // deletes half of each packed segment (2 objects/segment → 1
        // dead → ratio 0.5), so the threshold must be above 0.5 for the
        // partially-dead segments to qualify.
        gc_compact_threshold: if fast_gc { 0.6 } else { 0.5 },
        ..NodeConfig::default()
    };
    let node = Node::start(config).await.expect("node boots");
    (node, tmp)
}

/// Waits until the node's ring view contains all 3 cluster nodes (the
/// membership plane converges via gossip; the ring is updated from
/// membership events).
async fn wait_for_cluster_convergence(node: &Node) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let ring_nodes = node.segment_replicator().ring_node_count();
        if ring_nodes >= 3 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster must converge to 3 nodes within 30s (ring has {ring_nodes})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Waits until the node's segment directory contains `expected` `.dat`
/// files (replication landed on this replica's store).
async fn wait_for_segment_files(dir: &std::path::Path, expected: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let count = std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".dat"))
                    .count()
            })
            .unwrap_or(0);
        if count >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica store must receive {expected} segments within 60s (has {count})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// PUTs `body` under `key` on the node's S3 API; asserts 200.
async fn put(client: &reqwest::Client, addr: std::net::SocketAddr, key: &str, body: &[u8]) {
    let resp = client
        .put(format!("http://{addr}/durability/{key}"))
        .body(body.to_vec())
        .send()
        .await
        .expect("PUT must succeed");
    assert_eq!(resp.status(), 200, "PUT {key} returns 200");
}

/// GETs `key` and returns the body (the caller asserts status).
async fn get(client: &reqwest::Client, addr: std::net::SocketAddr, key: &str) -> reqwest::Response {
    client
        .get(format!("http://{addr}/durability/{key}"))
        .send()
        .await
        .expect("GET must reach the node")
}

/// DELETEs `key` on the node's S3 API; asserts 204.
async fn delete(client: &reqwest::Client, addr: std::net::SocketAddr, key: &str) {
    let resp = client
        .delete(format!("http://{addr}/durability/{key}"))
        .send()
        .await
        .expect("DELETE must reach the node");
    assert_eq!(resp.status(), 204, "DELETE {key} returns 204");
}

/// Lists the `.dat` segment ids present in a node's segment directory.
fn segment_ids(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".dat"))
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn data_survives_owner_disk_death_via_segment_replicas() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();
    // ---- Boot the 3-node cluster (A = seed/owner; B, C join via A) ----
    // Reserve all 6 data-plane ports at once so they are distinct.
    let ports = free_ports(6);
    let a_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[0]),
        membership: format!("127.0.0.1:{}", ports[1]),
    };
    let b_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[2]),
        membership: format!("127.0.0.1:{}", ports[3]),
    };
    let c_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[4]),
        membership: format!("127.0.0.1:{}", ports[5]),
    };
    let (node_a, tmp_a) = boot_node("node-a", None, &a_addrs, false).await;
    let (node_b, tmp_b) = boot_node("node-b", Some(&a_addrs.membership), &b_addrs, false).await;
    let (node_c, tmp_c) = boot_node("node-c", Some(&a_addrs.membership), &c_addrs, false).await;

    // Convergence before writes: the replicator's target derivation needs
    // the full ring, and the write path needs quorum (RF=3).
    wait_for_cluster_convergence(&node_a).await;
    wait_for_cluster_convergence(&node_b).await;
    wait_for_cluster_convergence(&node_c).await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr_a = node_a.server_addr();

    // ---- PUT 8 × 32 KiB objects CONCURRENTLY ----
    // 32 KiB bodies land in the Small tier (64 KiB target) — concurrent
    // writers share the active segment, so 2 objects per segment and the
    // second object of each is MID-SEGMENT (offset > 0): the exact case
    // the offset-0 fragment never covered.
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let keys: Vec<String> = (0..8).map(|i| format!("obj-{i:02}")).collect();
    let mut handles = Vec::new();
    for key in &keys {
        let client = client.clone();
        let body = body.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            put(&client, addr_a, &key, &body).await;
        }));
    }
    for h in handles {
        h.await.expect("PUT task");
    }

    // ---- Wait for seals + replication to land on B and C ----
    // The owner sealed N small segments; each was pushed to the segment's
    // ring replicas (B and C). Assert the replicas' stores actually hold
    // the .dat files.
    let segments_dir_a = tmp_a.path().join("data/segments");
    let segments_dir_b = tmp_b.path().join("data/segments");
    let segments_dir_c = tmp_c.path().join("data/segments");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let owner_segment_count = loop {
        let count = std::fs::read_dir(&segments_dir_a)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".dat"))
                    .count()
            })
            .unwrap_or(0);
        if count >= 2 {
            break count;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "owner must seal at least 2 segments within 60s (has {count})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert!(
        owner_segment_count <= 8,
        "8 × 32 KiB bodies into 64 KiB segments → at most 8 segments (got {owner_segment_count})"
    );
    // The replicator must also have drained (its needs set is empty).
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if node_a.segment_replicator().needs_len() == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replicator must drain its needs set within 60s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // The replicas must physically hold the data.
    wait_for_segment_files(&segments_dir_b, owner_segment_count).await;
    wait_for_segment_files(&segments_dir_c, owner_segment_count).await;

    // ---- Kill A's data (the disk-death scenario) ----
    for entry in std::fs::read_dir(&segments_dir_a).expect("read owner segments") {
        let path = entry.expect("dir entry").path();
        if path.to_string_lossy().ends_with(".dat") {
            std::fs::remove_file(&path).expect("remove owner .dat");
        }
    }

    // ---- Read every object back THROUGH A ----
    // A's local reads now fail; the read path must fall back to the
    // segment's ring replicas (B/C) and serve byte-identical data.
    for key in &keys {
        let resp = get(&client, addr_a, key).await;
        assert_eq!(resp.status(), 200, "GET {key} through the owner must succeed from replicas");
        let got = resp.bytes().await.expect("read body");
        assert_eq!(
            &got[..],
            &body[..],
            "object {key} must be byte-identical after the owner's data died"
        );
    }

    node_a.shutdown().await.expect("node A shutdown");
    node_b.shutdown().await.expect("node B shutdown");
    node_c.shutdown().await.expect("node C shutdown");
}

/// GAP-1 closure (g3 `loss-announcement` Option A — owner-authoritative
/// compaction propagation): after GC compaction rewrites ONLY the owner's
/// metadata, every node must converge on the repacked segment so reads
/// work through ALL THREE nodes — not just the owner.
///
/// Before this feature, the backbone's compaction-variant test
/// deliberately scoped DOWN read-availability because a GET routed to B
/// (or C) used ITS metadata, which still referenced the original segment
/// — and B/C's own GC had compacted that original away. Result: reads
/// referenced a segment that existed nowhere (500). This test asserts
/// the fix: after DELETE + compaction + remap propagation, every object
/// is byte-identical when read through A, B, AND C.
#[tokio::test]
async fn compacted_segments_are_readable_from_every_node() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(6);
    let a_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[0]),
        membership: format!("127.0.0.1:{}", ports[1]),
    };
    let b_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[2]),
        membership: format!("127.0.0.1:{}", ports[3]),
    };
    let c_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[4]),
        membership: format!("127.0.0.1:{}", ports[5]),
    };
    let (node_a, tmp_a) = boot_node("node-a", None, &a_addrs, true).await;
    let (node_b, tmp_b) = boot_node("node-b", Some(&a_addrs.membership), &b_addrs, true).await;
    let (node_c, tmp_c) = boot_node("node-c", Some(&a_addrs.membership), &c_addrs, true).await;

    wait_for_cluster_convergence(&node_a).await;
    wait_for_cluster_convergence(&node_b).await;
    wait_for_cluster_convergence(&node_c).await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr_a = node_a.server_addr();
    let addr_b = node_b.server_addr();
    let addr_c = node_c.server_addr();

    // ---- PUT 8 × 32 KiB objects concurrently (packed into segments) ----
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let keys: Vec<String> = (0..8).map(|i| format!("obj-{i:02}")).collect();
    let mut handles = Vec::new();
    for key in &keys {
        let client = client.clone();
        let body = body.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            put(&client, addr_a, &key, &body).await;
        }));
    }
    for h in handles {
        h.await.expect("PUT task");
    }

    // ---- Wait for initial seals + replication to land on B/C ----
    let segments_dir_a = tmp_a.path().join("data/segments");
    let segments_dir_b = tmp_b.path().join("data/segments");
    let segments_dir_c = tmp_c.path().join("data/segments");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let initial_ids = loop {
        let ids = segment_ids(&segments_dir_a);
        if ids.len() >= 2 {
            break ids;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "owner must seal at least 2 segments within 60s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    wait_for_segment_files(&segments_dir_b, initial_ids.len()).await;
    wait_for_segment_files(&segments_dir_c, initial_ids.len()).await;
    // The owner's replicator drained (all pushes acked).
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if node_a.segment_replicator().needs_len() == 0 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "replicator must drain before compaction");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ---- Sanity: reads work through all three nodes BEFORE deletion ----
    for key in &keys {
        for addr in [addr_a, addr_b, addr_c] {
            let resp = get(&client, addr, key).await;
            assert_eq!(resp.status(), 200, "pre-delete GET {key} via {addr}");
        }
    }

    // ---- DELETE half the objects (tombstones replicate to B/C) ----
    for key in keys.iter().take(4) {
        delete(&client, addr_a, key).await;
    }

    // ---- Wait for GC compaction to produce the repacked segments ----
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let on_a = segment_ids(&segments_dir_a);
        let repacked: Vec<String> =
            on_a.iter().filter(|id| !initial_ids.contains(id)).cloned().collect();
        let on_b = segment_ids(&segments_dir_b);
        let on_c = segment_ids(&segments_dir_c);
        if !repacked.is_empty() && repacked.iter().all(|id| on_b.contains(id) && on_c.contains(id))
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "all repacked segments must be replicated to B and C within 60s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ---- GAP-1 closure: the remap must have propagated. ----
    // Every node's metadata must now reference the repacked segments for
    // the surviving objects; reads through A, B, AND C must return
    // byte-identical data. Wait for convergence (the remap push + the
    // receiver-side re-point are async). Only the SURVIVING keys are
    // polled (the deleted 4 must 404 — never 200).
    let surviving_keys: Vec<String> = keys.iter().skip(4).cloned().collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut all_readable = true;
        for key in &surviving_keys {
            for addr in [addr_a, addr_b, addr_c] {
                let resp = get(&client, addr, key).await;
                if resp.status() != 200 {
                    all_readable = false;
                }
            }
        }
        if all_readable {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "surviving objects must be readable through A, B, C after compaction remap"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // The surviving objects (the 4 NOT deleted) are byte-identical on
    // every node. The deleted 4 return 404 everywhere (their tombstones
    // replicated).
    let deleted_keys: Vec<String> = keys.iter().take(4).cloned().collect();
    for key in &keys {
        for addr in [addr_a, addr_b, addr_c] {
            let resp = get(&client, addr, key).await;
            if deleted_keys.contains(key) {
                assert_eq!(
                    resp.status(),
                    404,
                    "deleted {key} must 404 via {addr} (tombstone replicated)"
                );
            } else {
                assert_eq!(
                    resp.status(),
                    200,
                    "surviving {key} must be readable via {addr} after compaction"
                );
                let got = resp.bytes().await.expect("read body");
                assert_eq!(
                    &got[..],
                    &body[..],
                    "surviving {key} must be byte-identical via {addr}"
                );
            }
        }
    }

    node_a.shutdown().await.expect("node A shutdown");
    node_b.shutdown().await.expect("node B shutdown");
    node_c.shutdown().await.expect("node C shutdown");
}
/// seals a NEW repacked segment OUTSIDE the write-path seal worker — the
/// compactor's `with_segment_sealed_notifier` hook must enqueue it so the
/// replicator fans it out to the segment's ring replicas. Without that
/// hook, post-compaction objects silently have ZERO replicas.
///
/// Flow: concurrent PUTs pack objects into segments → seals + replication
/// land on B/C → DELETE half the objects → tombstone TTL (1 s) expires →
/// GC (1 s interval) compacts the partially-dead segments into new
/// segments → EVERY repacked segment is replicated to B and C (the
/// feature's guarantee, asserted here).
///
/// Read-availability after compaction is DELIBERATELY not asserted —
/// DEFERRED to g3/g4 (GAP-1 in the feature doc): compaction remaps only
/// the OWNER's metadata; a GET routed to B/C uses THEIR metadata, which
/// still references the originals that their own GC may have compacted
/// away — a read can reference a segment that exists nowhere.
#[tokio::test]
async fn repacked_segments_are_replicated_to_ring_replicas() {
    let _guard = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let ports = free_ports(6);
    let a_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[0]),
        membership: format!("127.0.0.1:{}", ports[1]),
    };
    let b_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[2]),
        membership: format!("127.0.0.1:{}", ports[3]),
    };
    let c_addrs = NodeAddrs {
        grpc: format!("127.0.0.1:{}", ports[4]),
        membership: format!("127.0.0.1:{}", ports[5]),
    };
    // Fast GC: 1 s tombstone TTL, 1 s GC interval, 0.6 compact threshold
    // (above 0.5 so partially-dead packed segments qualify — see
    // `boot_node`'s `fast_gc` config).
    let (node_a, tmp_a) = boot_node("node-a", None, &a_addrs, true).await;
    let (node_b, tmp_b) = boot_node("node-b", Some(&a_addrs.membership), &b_addrs, true).await;
    let (node_c, tmp_c) = boot_node("node-c", Some(&a_addrs.membership), &c_addrs, true).await;

    wait_for_cluster_convergence(&node_a).await;
    wait_for_cluster_convergence(&node_b).await;
    wait_for_cluster_convergence(&node_c).await;

    let client =
        reqwest::Client::builder().timeout(Duration::from_secs(15)).build().expect("client");
    let addr_a = node_a.server_addr();

    // ---- PUT 8 × 32 KiB objects concurrently (packed into segments) ----
    let body: Vec<u8> = (0..32 * 1024).map(|i| (i % 251) as u8).collect();
    let keys: Vec<String> = (0..8).map(|i| format!("obj-{i:02}")).collect();
    let mut handles = Vec::new();
    for key in &keys {
        let client = client.clone();
        let body = body.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            put(&client, addr_a, &key, &body).await;
        }));
    }
    for h in handles {
        h.await.expect("PUT task");
    }

    // ---- Wait for initial seals + replication to land on B/C ----
    let segments_dir_a = tmp_a.path().join("data/segments");
    let segments_dir_b = tmp_b.path().join("data/segments");
    let segments_dir_c = tmp_c.path().join("data/segments");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let initial_ids = loop {
        let ids = segment_ids(&segments_dir_a);
        if ids.len() >= 2 {
            break ids;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "owner must seal at least 2 segments within 60s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    wait_for_segment_files(&segments_dir_b, initial_ids.len()).await;
    wait_for_segment_files(&segments_dir_c, initial_ids.len()).await;

    // ---- DELETE half the objects (tombstones) ----
    for key in keys.iter().take(4) {
        delete(&client, addr_a, key).await;
    }

    // ---- Wait for GC compaction to produce the repacked segments ----
    // Compaction repacks EVERY partially-dead segment (4 here) into NEW
    // segment ids; each must be replicated to B and C (the compactor's
    // enqueue hook is what makes this happen). Wait for ALL of them.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let all_repacked = loop {
        let on_a = segment_ids(&segments_dir_a);
        let repacked: Vec<String> =
            on_a.iter().filter(|id| !initial_ids.contains(id)).cloned().collect();
        let on_b = segment_ids(&segments_dir_b);
        let on_c = segment_ids(&segments_dir_c);
        if !repacked.is_empty() && repacked.iter().all(|id| on_b.contains(id) && on_c.contains(id))
        {
            break repacked;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "all repacked segments must be replicated to B and C within 60s \
             (A has {repacked:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    // Compaction repacked at least one segment. The fully-live originals
    // (no dead bytes) are NOT repacked — they remain on A alongside the
    // repacked ones, so only the repacked count is asserted here (their
    // B/C replication was already verified by the loop above).
    assert!(!all_repacked.is_empty(), "at least one segment must be repacked");

    // NOTE: read-availability assertions after compaction are DELIBERATELY
    // ABSENT — see GAP-1 in the feature doc. Compaction remaps only the
    // OWNER's metadata; a GET routed to B/C (by object-key hash) uses
    // THEIR metadata, which still references the original segment — and
    // B/C's own GC may have compacted that original away. The read then
    // 500s on a segment that exists nowhere. Closing this requires g3/g4
    // metadata-remap propagation. THIS test pins the feature's guarantee:
    // the compactor enqueue hook fires and every repacked segment IS
    // replicated to B and C (before this feature they had ZERO replicas).

    node_a.shutdown().await.expect("node A shutdown");
    node_b.shutdown().await.expect("node B shutdown");
    node_c.shutdown().await.expect("node C shutdown");
}
