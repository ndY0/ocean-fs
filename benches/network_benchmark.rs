//! Network socket option benchmarks.
//!
//! Measures the CPU cost of applying socket options (`TCP_QUICKACK`,
//! `SO_BUSY_POLL`, `SO_REUSEPORT`) on a socket. The kernel
//! processing time for these advisory hints is negligible, but this
//! confirms the overhead is bounded.
//!
//! Run with:
//!   cargo bench --bench network_benchmark

use criterion::{criterion_group, criterion_main, Criterion};
use oceanfs_network::{set_busy_poll, set_quickack, set_reuseport};

fn create_socket() -> socket2::Socket {
    socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
        .unwrap()
}

fn bench_set_quickack(c: &mut Criterion) {
    c.bench_function("socket_opts/set_quickack", |b| {
        let socket = create_socket();
        b.iter(|| {
            set_quickack(&socket).unwrap();
        });
    });
}

fn bench_set_busy_poll(c: &mut Criterion) {
    c.bench_function("socket_opts/set_busy_poll_50us", |b| {
        let socket = create_socket();
        b.iter(|| {
            set_busy_poll(&socket, 50).unwrap();
        });
    });
}

fn bench_set_reuseport(c: &mut Criterion) {
    c.bench_function("socket_opts/set_reuseport", |b| {
        let socket = create_socket();
        b.iter(|| {
            set_reuseport(&socket).unwrap();
        });
    });
}

fn bench_all_opts(c: &mut Criterion) {
    c.bench_function("socket_opts/all_three", |b| {
        let socket = create_socket();
        b.iter(|| {
            set_quickack(&socket).unwrap();
            set_busy_poll(&socket, 50).unwrap();
            set_reuseport(&socket).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_set_quickack,
    bench_set_busy_poll,
    bench_set_reuseport,
    bench_all_opts,
);
criterion_main!(benches);
