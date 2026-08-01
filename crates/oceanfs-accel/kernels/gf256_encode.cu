// GF(2^8) encode kernel for OceanFS.
//
// One thread per output byte. Each thread computes one byte of one parity
// shard by accumulating GF(2^8) products of data bytes with precomputed
// split-table lookup tables.
//
// Compile to PTX with:
//   nvcc -ptx --gpu-architecture=compute_75 gf256_encode.cu -o gf256_encode.ptx
//
// Replace compute_75 with your GPU's compute capability.
// RTX 2060 = 7.5, A100 = 8.0, H100 = 9.0.

// GF(2^8) multiply via split-table: result = lo[b & 0xF] ^ hi[b >> 4]
// The tables are precomputed on the CPU for each encoding matrix coefficient
// and uploaded to GPU global memory before kernel launch.
//
// Tables layout: lo_tables[row][col][16] then hi_tables[row][col][16]
// Total: 32 * k * m bytes.

extern "C" __global__ void gf256_encode(
    // Input: k data shards, each with `num_bytes` bytes, laid out consecutively.
    // data[col * num_bytes + pos] = byte at position `pos` in data shard `col`.
    const unsigned char* __restrict__ data,

    // Output: m parity shards, same layout.
    // parity[row * num_bytes + pos] = computed parity byte.
    unsigned char* __restrict__ parity,

    // Precomputed split-tables: lo_tables flattened then hi_tables flattened.
    // For coefficient at (row, col): lo = tables[row * k * 16 + col * 16 + nibble]
    //                                hi = tables[row * k * 16 + col * 16 + nibble + k*m*16]
    const unsigned char* __restrict__ tables,

    // Number of data shards.
    int k,

    // Number of parity shards to compute.
    int m,

    // Size of each shard in bytes.
    int num_bytes
) {
    // Global thread index: one thread per byte position
    int tid = blockIdx.x * blockDim.x + threadIdx.x;

    if (tid >= num_bytes) return;

    int pos = tid;

    // For each parity shard, accumulate the dot product
    for (int row = 0; row < m; row++) {
        unsigned char acc = 0;

        for (int col = 0; col < k; col++) {
            // Index into the tables: lo_tables[row][col] is at offset:
            //   row * k * 16 + col * 16
            int table_base = row * k * 16 + col * 16;

            // Load data byte
            unsigned char b = data[col * num_bytes + pos];

            if (b != 0) {
                unsigned char lo_nibble = b & 0x0F;
                unsigned char hi_nibble = b >> 4;

                // lo_table lookup
                unsigned char lo = tables[table_base + lo_nibble];
                // hi_table lookup (stored after all lo_tables)
                unsigned char hi = tables[k * m * 16 + table_base + hi_nibble];

                acc ^= lo ^ hi;
            }
        }

        parity[row * num_bytes + pos] = acc;
    }
}
