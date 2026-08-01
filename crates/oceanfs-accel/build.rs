//! Build script for oceanfs-accel.
//!
//! - `isa-l` feature: links against system `libisal` via pkg-config.
//! - `cuda` feature: compiles CUDA kernels to PTX with `nvcc`.

#![allow(clippy::expect_used)]

#[cfg(any(feature = "isa-l", feature = "cuda"))]
use std::process::Command;

/// Runs `pkg-config` and returns the flags for the given library.
#[cfg(feature = "isa-l")]
fn pkg_config(lib: &str, flag: &str) -> Result<String, String> {
    let output = Command::new("pkg-config")
        .arg(flag)
        .arg(lib)
        .output()
        .map_err(|e| format!("failed to run pkg-config: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("pkg-config {flag} {lib} failed: {stderr}"));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
    // --- ISA-L linking (gated on feature isa-l) ---
    #[cfg(feature = "isa-l")]
    {
        match pkg_config("libisal", "--libs") {
            Ok(libs) => {
                // pkg-config returns something like "-lisal"
                // Pass through to the linker
                for flag in libs.split_whitespace() {
                    if let Some(lib) = flag.strip_prefix("-l") {
                        println!("cargo:rustc-link-lib={lib}");
                    } else if let Some(path) = flag.strip_prefix("-L") {
                        println!("cargo:rustc-link-search=native={path}");
                    }
                }
            }
            Err(e) => {
                panic!(
                    "ISA-L feature enabled but libisal not found via pkg-config: {e}\n\
                     Install libisal-dev: sudo apt install libisal-dev"
                );
            }
        }

        println!("cargo:isa-l=enabled");
        println!("cargo:rerun-if-env-changed=ISA_L_PATH");
    }

    #[cfg(not(feature = "isa-l"))]
    {
        println!("cargo:isa-l=disabled");
    }

    // --- CUDA kernel compilation + nvCOMP linking (gated on feature cuda) ---
    #[cfg(feature = "cuda")]
    {
        // Link nvCOMP (GPU-accelerated LZ4/zstd compression)
        // Installed at /usr/local/lib by nvCOMP 4.0 SDK
        println!("cargo:rustc-link-search=native=/usr/local/lib");
        println!("cargo:rustc-link-lib=nvcomp");
        println!("cargo:rustc-link-lib=nvcomp_cpu");
        // Also link CUDA runtime for raw stream operations
        println!("cargo:rustc-link-search=native=/usr/local/cuda/lib64");
        println!("cargo:rustc-link-lib=cudart");

        // --- CUDA kernel compilation ---
        let kernel_dir = "kernels";
        let kernel_src = format!("{kernel_dir}/gf256_encode.cu");
        let ptx_out = format!("{kernel_dir}/gf256_encode.ptx");

        // Detect GPU compute capability from nvidia-smi or use default
        let cc = detect_compute_capability();

        println!("cargo:rerun-if-changed={kernel_src}");
        println!("cargo:rerun-if-env-changed=CUDA_COMPUTE_CAP");

        let status = Command::new("nvcc")
            .args([
                "-ptx",
                &format!("--gpu-architecture=compute_{cc}"),
                "-o",
                &ptx_out,
                &kernel_src,
            ])
            .status()
            .expect("nvcc not found — install nvidia-cuda-toolkit: sudo apt install nvidia-cuda-toolkit");

        if !status.success() {
            panic!("CUDA kernel compilation failed (nvcc exited with {status})");
        }

        // Tell cargo where to find the PTX file for include_str!
        println!("cargo:rerun-if-changed={ptx_out}");
    }
}

#[cfg(feature = "cuda")]
fn detect_compute_capability() -> String {
    // Try nvidia-smi first
    if let Ok(output) = Command::new("nvidia-smi")
        .args(["--query-gpu=compute_cap", "--format=csv,noheader"])
        .output()
    {
        let cap = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !cap.is_empty() {
            let clean = cap.replace('.', "");
            eprintln!("cargo:warning=Detected GPU compute capability: sm_{clean}");
            return clean;
        }
    }

    // Environment override
    if let Ok(cap) = std::env::var("CUDA_COMPUTE_CAP") {
        eprintln!("cargo:warning=Using CUDA_COMPUTE_CAP={cap}");
        return cap;
    }

    // Default: Maxwell (5.0) — widely compatible
    "50".into()
}
