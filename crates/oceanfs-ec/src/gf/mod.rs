//! Galois Field GF(2^8) arithmetic.
//!
//! Uses log/exp tables for fast multiplication and division.
//! Addition is XOR. The primitive polynomial is 0x11D
//! (x^8 + x^4 + x^3 + x^2 + 1).
//!
//! ## SIMD Acceleration (x86_64)
//!
//! On x86_64, run-time CPU feature detection selects the fastest available
//! SIMD path for batched GF(2^8) multiplication:
//!
//! - **GFNI** (VGF2P8MULB): 64 bytes/instruction with AVX-512, 32 with AVX2.
//!   Single-instruction GF(2^8) multiply — no table lookups. ~8.7× portable.
//! - **AVX-512** (VPSHUFB split-table): 64 elements/instruction, ~4× portable
//! - **AVX2** (VPSHUFB split-table): 32 elements/instruction, ~2× portable
//! - **SSE4.1** (PSHUFB split-table): 16 elements/instruction, ~1.5× portable
//!
//! The SIMD code lives in the `simd_x86` module. Use [`gf_mul_simd`] to
//! multiply a byte slice by a single coefficient using the fastest available
//! SIMD path.

#[cfg(target_arch = "x86_64")]
mod simd_x86;

#[cfg(target_arch = "x86_64")]
pub use simd_x86::{gf_mul_simd, gf_mul_simd_unchecked, GfSimdLevel};

/// GF(2^8) element (0..255).
pub type Gf8 = u8;

/// Primitive polynomial: x^8 + x^4 + x^3 + x^2 + 1
const PRIMITIVE_POLY: u16 = 0x11D;

/// Log table: log[alpha^i] = i for i in 0..255.
/// alpha = 2 is the primitive element.
#[cfg_attr(tarpaulin, ignore)]
static LOG_TABLE: [u8; 256] = build_log_table();
/// Exp table: exp[i] = alpha^i.
#[cfg_attr(tarpaulin, ignore)]
static EXP_TABLE: [u8; 512] = build_exp_table();

#[cfg_attr(tarpaulin, ignore)]
const fn build_log_table() -> [u8; 256] {
    let mut log = [0u8; 256];
    let mut x: u16 = 1;
    let mut i: u16 = 0;
    while i < 255 {
        log[x as usize] = i as u8;
        x <<= 1;
        if x >= 256 {
            x ^= PRIMITIVE_POLY;
        }
        i += 1;
    }
    log
}

#[cfg_attr(tarpaulin, ignore)]
const fn build_exp_table() -> [u8; 512] {
    let mut exp = [0u8; 512];
    let mut x: u16 = 1;
    let mut i: u16 = 0;
    while i < 511 {
        exp[i as usize] = x as u8;
        x <<= 1;
        if x >= 256 {
            x ^= PRIMITIVE_POLY;
        }
        i += 1;
    }
    exp[255] = exp[0]; // alpha^255 = 1
    exp
}

/// Adds two GF(2^8) elements (XOR).
///
/// Force-inlined to ensure the compiler can fold multiple XORs into
/// a single SIMD operation when vectorizing the Cauchy encode loop.
#[inline(always)]
pub fn gf_add(a: Gf8, b: Gf8) -> Gf8 {
    a ^ b
}

/// Multiplies two GF(2^8) elements.
///
/// Force-inlined to allow the compiler to see through the log/exp table
/// lookup and potentially vectorize when LTO is enabled.
#[inline(always)]
pub fn gf_mul(a: Gf8, b: Gf8) -> Gf8 {
    if a == 0 || b == 0 {
        0
    } else {
        let sum = LOG_TABLE[a as usize] as u16 + LOG_TABLE[b as usize] as u16;
        EXP_TABLE[sum as usize]
    }
}

/// Divides a / b in GF(2^8).
///
/// Force-inlined per perf guideline §10.1 — the scalar division path
/// is used in the decode matrix inversion where every cycle counts.
///
/// # Panics
///
/// Panics if `b` is zero (division by zero in GF(2^8)).
#[inline(always)]
pub fn gf_div(a: Gf8, b: Gf8) -> Gf8 {
    if a == 0 {
        0
    } else if b == 0 {
        panic!("division by zero in GF(2^8)")
    } else {
        let diff = LOG_TABLE[a as usize] as i16 - LOG_TABLE[b as usize] as i16;
        let idx = if diff < 0 { diff + 255 } else { diff } as usize;
        EXP_TABLE[idx]
    }
}

/// Computes the multiplicative inverse a^(-1) in GF(2^8).
///
/// # Panics
///
/// Panics if `a` is zero (zero has no multiplicative inverse).
#[inline]
pub fn gf_inv(a: Gf8) -> Gf8 {
    if a == 0 {
        panic!("inverse of zero in GF(2^8)")
    }
    gf_div(1, a)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn add_is_xor() {
        assert_eq!(gf_add(0x53, 0xCA), 0x53 ^ 0xCA);
    }

    #[test]
    fn mul_commutative() {
        for a in 1..=255u8 {
            for b in 1..=255u8 {
                assert_eq!(gf_mul(a, b), gf_mul(b, a), "a={a}, b={b}");
            }
        }
    }

    #[test]
    fn mul_distributive() {
        let a = 0x12;
        let b = 0x34;
        let c = 0x56;
        assert_eq!(gf_mul(a, gf_add(b, c)), gf_add(gf_mul(a, b), gf_mul(a, c)));
    }

    #[test]
    fn mul_identity() {
        for a in 1..=255u8 {
            assert_eq!(gf_mul(a, 1), a);
        }
    }

    #[test]
    fn mul_zero() {
        for a in 0..=255u8 {
            assert_eq!(gf_mul(a, 0), 0);
        }
    }

    #[test]
    fn inv_times_self_is_one() {
        for a in 1..=255u8 {
            assert_eq!(gf_mul(a, gf_inv(a)), 1, "a={a}");
        }
    }

    #[test]
    fn div_undoes_mul() {
        for a in 1..=255u8 {
            for b in 1..=255u8 {
                assert_eq!(gf_div(gf_mul(a, b), b), a, "a={a}, b={b}");
            }
        }
    }

    #[test]
    fn div_zero_numerator_is_zero() {
        for b in 1..=255u8 {
            assert_eq!(gf_div(0, b), 0, "b={b}");
        }
    }

    #[test]
    fn add_zero_is_identity() {
        for a in 0..=255u8 {
            assert_eq!(gf_add(a, 0), a);
        }
    }

    #[test]
    fn mul_associative() {
        let a = 0x12;
        let b = 0x34;
        let c = 0x56;
        assert_eq!(gf_mul(gf_mul(a, b), c), gf_mul(a, gf_mul(b, c)));
    }

    #[test]
    fn add_associative() {
        let a = 0x12;
        let b = 0x34;
        let c = 0x56;
        assert_eq!(gf_add(gf_add(a, b), c), gf_add(a, gf_add(b, c)));
    }

    #[test]
    #[should_panic(expected = "division by zero")]
    fn div_by_zero_panics() {
        gf_div(1, 0);
    }

    #[test]
    #[should_panic(expected = "inverse of zero")]
    fn inv_of_zero_panics() {
        gf_inv(0);
    }
}
