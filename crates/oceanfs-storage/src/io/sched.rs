//! Background thread scheduling hints (Linux only).
//!
//! Provides `ioprio_set(IOPRIO_CLASS_IDLE)` and `SCHED_IDLE` scheduling
//! for background task threads. These hints prevent GC, scrub, heal, and
//! anti-entropy threads from competing with client-facing I/O and CPU.
//!
//! All functions are `#[cfg(target_os = "linux")]`-gated with no-op
//! fallbacks on non-Linux platforms per performance guideline §10.6.
//!
//! # Capability Requirements
//!
//! - `ioprio_set` requires no special privileges.
//! - `SCHED_IDLE` requires `CAP_SYS_NICE`; if not held, the call fails
//!   with `EPERM` and the system logs an info message and continues.

/// Applies `IOPRIO_CLASS_IDLE` I/O scheduling to the calling thread.
///
/// Threads with `IOPRIO_CLASS_IDLE` only receive I/O bandwidth when no
/// other thread wants it — preventing background scans from competing
/// with client I/O for NVMe command slots.
///
/// No-op on non-Linux platforms.
pub fn apply_background_io_class(thread_name: &str) {
    #[cfg(target_os = "linux")]
    {
        // Linux ioprio constants (from <linux/ioprio.h>):
        // IOPRIO_WHO_PROCESS = 1
        // IOPRIO_CLASS_SHIFT = 13
        // IOPRIO_CLASS_IDLE = 3
        // IOPRIO_PRIO_VALUE(class, data) = ((class) << 13) | (data)
        const IOPRIO_WHO_PROCESS: i32 = 1;
        const IOPRIO_CLASS_IDLE: i32 = 3;
        const IOPRIO_CLASS_SHIFT: i32 = 13;
        let prio = (IOPRIO_CLASS_IDLE & 0x07) << IOPRIO_CLASS_SHIFT;

        // SAFETY: `syscall(SYS_ioprio_set)` sets the I/O scheduling class
        // for the calling thread. This is a per-thread hint — it cannot
        // cause UB or affect other processes.
        #[allow(unsafe_code)]
        let ret = unsafe {
            libc::syscall(
                libc::SYS_ioprio_set,
                IOPRIO_WHO_PROCESS as libc::c_long,
                0i64,
                prio as libc::c_long,
            )
        } as i32;
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            tracing::warn!(
                thread = thread_name,
                error = %err,
                "failed to set IOPRIO_CLASS_IDLE for background thread"
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = thread_name;
    }
}

/// Applies `SCHED_IDLE` CPU scheduling to the calling thread.
///
/// Threads with `SCHED_IDLE` only execute when no other thread wants the
/// CPU — they literally run in idle CPU time. Requires `CAP_SYS_NICE`
/// capability; gracefully degrades on `EPERM` with an info log message.
///
/// No-op on non-Linux platforms.
pub fn apply_background_cpu_sched(thread_name: &str) {
    #[cfg(target_os = "linux")]
    {
        // SCHED_IDLE = 5 (from <linux/sched.h>)
        const SCHED_IDLE: i32 = 5;

        let param = libc::sched_param { sched_priority: 0 };
        // SAFETY: `sched_setscheduler` sets the scheduling policy for
        // the calling thread (pid=0). `SCHED_IDLE` is a per-thread
        // policy that only runs when no other thread is runnable.
        #[allow(unsafe_code)]
        let ret = unsafe { libc::sched_setscheduler(0, SCHED_IDLE, &param) };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EPERM) {
                tracing::info!(
                    thread = thread_name,
                    "SCHED_IDLE not available (CAP_SYS_NICE required); \
                     background thread will use normal CPU scheduling"
                );
            } else {
                tracing::warn!(
                    thread = thread_name,
                    error = %err,
                    "failed to set SCHED_IDLE for background thread"
                );
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = thread_name;
    }
}
