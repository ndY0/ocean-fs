# OceanFS Benchmarks

Criterion benchmarks for hot-path functions in the OceanFS storage engine.

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench --manifest-path benches/Cargo.toml

# Run a specific benchmark file
cargo bench --bench ec_benchmark
cargo bench --bench hash_benchmark
cargo bench --bench storage_benchmark

# Run with specific criteria
cargo bench --bench ec_benchmark -- --sample-size 50 --measurement-time 10
```

## Updating Baselines

```bash
# Save current results as the new baseline
cargo bench --bench ec_benchmark -- --save-baseline main

# Compare against baseline (regression detection)
cargo bench --bench ec_benchmark -- --baseline main
```

## Benchmarks

### `ec_benchmark.rs`
Erasure coding encode/decode performance.

| Benchmark | Description |
|---|---|
| `gf_mul` | GF(2^8) multiplication warmup |
| `ec_encode/k4_m2_64k` | Encode: k=4, m=2, 64 KB stripe |
| `ec_encode/k8_m4_64k` | Encode: k=8, m=4, 64 KB stripe |
| `ec_encode/k16_m8_64k` | Encode: k=16, m=8, 64 KB stripe |
| `ec_encode/k4_m2_4k` | Encode: k=4, m=2, 4 KB stripe (small segment) |
| `ec_decode/k4_m2_64k_recover1` | Decode: recover 1 missing shard, k=4, m=2 |
| `ec_decode/k8_m4_64k_recover2` | Decode: recover 2 missing shards, k=8, m=4 |

### `hash_benchmark.rs`
BLAKE3 hashing throughput.

| Benchmark | Description |
|---|---|
| `blake3/1kb` | Hash 1 KB (inline blob) |
| `blake3/64kb` | Hash 64 KB (stripe size) |
| `blake3/1mb` | Hash 1 MB (medium blob) |
| `blake3/100mb` | Hash 100 MB (large blob) |
| `blake3_streaming_1mb` | Streaming hash, 4 KB chunks × 256 |

### `storage_benchmark.rs`
Storage engine operation throughput.

| Benchmark | Description |
|---|---|
| `wal/append_1000x4kb` | Simulated WAL append: 1000 × 4 KB entries |
| `metadata_get_1000` | Simulated metadata get: 1000 lookups |
| `segment_index_lookup_1000` | Simulated segment index binary search: 1000 lookups |
| `segment/read_write_4mb` | Simulated segment read/write: 4 MB copy |

## CI Regression Detection

CI runs `cargo bench --no-run` to verify benchmarks compile. For full regression detection, use a GPU-capable CI runner or a dedicated benchmark server.
