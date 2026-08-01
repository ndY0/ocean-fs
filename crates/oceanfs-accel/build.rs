//! Build script for oceanfs-accel.
//!
//! - `isa-l` feature: links against system `libisal` via pkg-config.
//! - `cuda` feature: probes for CUDA toolkit + nvCOMP; sets cfg flags
//!   when absent so Rust code can degrade gracefully. The checked-in
//!   PTX (`kernels/gf256_encode.ptx`) is always used; nvcc recompilation
//!   is a local-only optimization for the host GPU's compute capability.

#![allow(clippy::expect_used)]

#[cfg(feature = "cuda")]
use std::path::Path;
#[cfg(any(feature = "cuda", feature = "isa-l"))]
use std::process::Command;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the given command is found in PATH.
#[cfg(feature = "cuda")]
fn command_exists(cmd: &str) -> bool {
    Command::new("which").arg(cmd).output().map(|o| o.status.success()).unwrap_or(false)
}

/// Returns `true` if the given shared library exists at one of the
/// standard system paths.
#[cfg(feature = "cuda")]
fn lib_exists(lib: &str) -> bool {
    let name = format!("lib{lib}.so");
    for dir in ["/usr/lib", "/usr/local/lib", "/usr/lib/x86_64-linux-gnu"] {
        if Path::new(dir).join(&name).exists() {
            return true;
        }
    }
    false
}

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

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    // Declare custom cfgs — always emitted so test files referencing
    // no_cuda_toolkit / no_nvcomp don't fail other feature builds.
    println!("cargo:rustc-check-cfg=cfg(no_cuda_toolkit)");
    println!("cargo:rustc-check-cfg=cfg(no_nvcomp)");

    // --- ISA-L linking (gated on feature isa-l) ---
    #[cfg(feature = "isa-l")]
    {
        match pkg_config("libisal", "--libs") {
            Ok(libs) => {
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

    // --- CUDA toolkit + nvCOMP probing (gated on feature cuda) ---
    #[cfg(feature = "cuda")]
    {
        // ---- CUDA toolkit (nvcc + cudart) ----
        let has_cuda = command_exists("nvcc") && lib_exists("cudart");

        if has_cuda {
            // Recompile PTX for host GPU's compute capability (optimization).
            // The checked-in PTX targets sm_50 which is always available.
            let kernel_dir = "kernels";
            let kernel_src = format!("{kernel_dir}/gf256_encode.cu");
            let ptx_out = format!("{kernel_dir}/gf256_encode.ptx");

            let cc = detect_compute_capability();

            println!("cargo:rerun-if-changed={kernel_src}");
            println!("cargo:rerun-if-env-changed=CUDA_COMPUTE_CAP");

            if let Ok(status) = Command::new("nvcc")
                .args([
                    "-ptx",
                    &format!("--gpu-architecture=compute_{cc}"),
                    "-o",
                    &ptx_out,
                    &kernel_src,
                ])
                .status()
            {
                if !status.success() {
                    eprintln!(
                        "cargo:warning=CUDA kernel recompilation failed; using checked-in PTX"
                    );
                }
            }

            println!("cargo:rerun-if-changed={ptx_out}");

            // Link CUDA runtime for raw stream operations (cudaStreamCreate etc).
            // cudarc links its own cudart; this covers our direct extern "C" usage.
            println!("cargo:rustc-link-search=native=/usr/local/cuda/lib64");
            println!("cargo:rustc-link-lib=cudart");
        } else {
            eprintln!("cargo:warning=CUDA toolkit not found — cuda feature active but GPU kernels unavailable");
            println!("cargo:rustc-cfg=no_cuda_toolkit");
        }

        // ---- nvCOMP (GPU compression library) ----
        let has_nvcomp = lib_exists("nvcomp") && lib_exists("nvcomp_cpu");

        if has_nvcomp {
            println!("cargo:rustc-link-search=native=/usr/local/lib");
            println!("cargo:rustc-link-lib=nvcomp");
            println!("cargo:rustc-link-lib=nvcomp_cpu");
        } else {
            eprintln!("cargo:warning=nvCOMP not found — GPU compression unavailable");
            println!("cargo:rustc-cfg=no_nvcomp");
        }

        println!("cargo:rerun-if-env-changed=CUDA_COMPUTE_CAP");
    }
}

#[cfg(feature = "cuda")]
fn detect_compute_capability() -> String {
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

    if let Ok(cap) = std::env::var("CUDA_COMPUTE_CAP") {
        eprintln!("cargo:warning=Using CUDA_COMPUTE_CAP={cap}");
        return cap;
    }

    "50".into()
}
