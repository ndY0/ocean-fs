//! Compression roundtrip — per-bucket chunk compression.
//!
//! Verifies that a bucket opted into compression via the admin policy
//! endpoint stores chunks compressed and serves them back byte-identical:
//! PUT a body that compresses well, GET it back, and confirm the bytes
//! match and the accel compress/decompress counters moved.

use e2e::harness::{config_standard, NodeProcess};

#[tokio::test]
async fn compression_roundtrip_preserves_bytes() {
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    let bucket = "compress-bucket";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    // Opt the bucket into compression (Auto tier → zstd on CPU).
    let policy = br#"{"compression": {"tier": "Auto", "level": 3}}"#;
    let policy_resp = node
        .put(&format!("/admin/buckets/{bucket}/policy"), policy)
        .await
        .expect("set compression policy");
    assert_eq!(policy_resp.status(), 200, "policy endpoint should accept the policy");

    // A highly-compressible body (repeated pattern) large enough to
    // reach the segment path (above the inline threshold) and well above
    // the min-chunk skip threshold.
    let payload: Vec<u8> = {
        let block = b"The quick brown fox jumps over the lazy dog. ";
        let mut v = Vec::with_capacity(1 << 20);
        for _ in 0..(1 << 20) / block.len() {
            v.extend_from_slice(block);
        }
        v
    };
    let key = "compressible.bin";
    let put_resp = node.put(&format!("/{bucket}/{key}"), &payload).await.expect("PUT");
    assert_eq!(put_resp.status(), 200, "PUT should succeed");

    let get_resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET");
    assert_eq!(get_resp.status(), 200, "GET should succeed");
    let body = get_resp.bytes().await.expect("read GET body");
    assert_eq!(&body[..], &payload[..], "compressed roundtrip must return the original bytes");

    // The compress/decompress counters must have moved — the write path
    // compressed through the accel dispatcher, the read path decompressed.
    let metrics = node
        .get("/admin/metrics")
        .await
        .expect("GET metrics")
        .text()
        .await
        .expect("read metrics body");
    let compress_count = metrics
        .lines()
        .find_map(|l| l.strip_prefix("accel_compress_duration_seconds_count "))
        .map(|v| v.parse::<f64>().unwrap_or(0.0))
        .unwrap_or(0.0);
    let decompress_count = metrics
        .lines()
        .find_map(|l| l.strip_prefix("accel_decompress_duration_seconds_count "))
        .map(|v| v.parse::<f64>().unwrap_or(0.0))
        .unwrap_or(0.0);
    assert!(
        compress_count > 0.0,
        "accel_compress_duration_seconds_count must be > 0 after a compressed PUT (got {compress_count})"
    );
    assert!(
        decompress_count > 0.0,
        "accel_decompress_duration_seconds_count must be > 0 after a compressed GET (got {decompress_count})"
    );

    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn uncompressed_bucket_default_does_not_compress() {
    // Default bucket policy (compression tier None) must leave the
    // compress counters untouched — baselines stay comparable.
    let node = NodeProcess::spawn(&config_standard()).await.expect("spawn node");

    let bucket = "plain-bucket";
    node.put(&format!("/{bucket}"), &[]).await.expect("create bucket");

    let payload = vec![0xABu8; 8192];
    let key = "plain.bin";
    let put_resp = node.put(&format!("/{bucket}/{key}"), &payload).await.expect("PUT");
    assert_eq!(put_resp.status(), 200, "PUT should succeed");
    let get_resp = node.get(&format!("/{bucket}/{key}")).await.expect("GET");
    assert_eq!(get_resp.status(), 200, "GET should succeed");
    let body = get_resp.bytes().await.expect("read GET body");
    assert_eq!(&body[..], &payload[..], "uncompressed roundtrip must match");

    let metrics = node
        .get("/admin/metrics")
        .await
        .expect("GET metrics")
        .text()
        .await
        .expect("read metrics body");
    let compress_count = metrics
        .lines()
        .find_map(|l| l.strip_prefix("accel_compress_duration_seconds_count "))
        .map(|v| v.parse::<f64>().unwrap_or(0.0))
        .unwrap_or(0.0);
    assert_eq!(compress_count, 0.0, "default bucket must not compress (count={compress_count})");

    node.shutdown().await.expect("shutdown");
}
