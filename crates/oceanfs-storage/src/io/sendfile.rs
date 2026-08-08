//! Zero-copy response body for segment data.
//!
//! When serving segment data read via mmap, the `Bytes` payload shares the
//! mmap region's `Arc` — no data copy. The axum/hyper HTTP layer sends
//! these bytes to the socket via `write(2)`.
//!
//! For true kernel-space `sendfile(2)`, deploy OceanFS behind nginx or
//! varnish with `sendfile on; aio threads;`. This is the standard
//! object-store deployment pattern (MinIO, Ceph RGW all recommend it).
//!
//! Per performance guideline §3.6.
//!
//! # Feature gate
//!
//! This module is feature-gated behind `sendfile` in `Cargo.toml`
//! because it depends on `http-body` and `http` crates.

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body::Body;

/// A response body backed by segment data as `Bytes`.
///
/// Wraps segment data for efficient serving via axum. When the
/// `Bytes` was sliced from an mmap region (via `SegmentFileCache`),
/// the data is zero-copy from the kernel page cache.
///
/// For sendfile(2) acceleration, deploy nginx in front of OceanFS
/// — true kernel-space disk→socket copy requires the socket fd,
/// which axum/hyper does not expose to the application layer.
///
/// # Examples
///
/// ```ignore
/// use oceanfs_storage::io::SegmentFileBody;
/// use bytes::Bytes;
///
/// let data = Bytes::from_static(b"segment payload");
/// let body = SegmentFileBody::new(data, 0, 14);
/// // Use with axum: Body::new(body)
/// ```
pub struct SegmentFileBody {
    /// The segment data, potentially mmap-backed (zero-copy).
    data: Bytes,
    /// Whether the data has been yielded in poll_frame.
    yielded: bool,
}

impl SegmentFileBody {
    /// Creates a new segment-backed response body.
    ///
    /// `data` is the blob data. When backed by `SegmentFileCache` mmap,
    /// `data` was sliced from `Arc<Mmap>` — no heap allocation.
    pub fn new(data: Bytes, _offset: u64, _length: u64) -> Self {
        Self { data, yielded: false }
    }

    /// Returns the total content length in bytes.
    pub fn content_length(&self) -> u64 {
        self.data.len() as u64
    }
}

impl Body for SegmentFileBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.yielded || self.data.is_empty() {
            return Poll::Ready(None);
        }

        self.yielded = true;
        let frame = http_body::Frame::data(self.data.clone());
        Poll::Ready(Some(Ok(frame)))
    }

    fn is_end_stream(&self) -> bool {
        self.yielded || self.data.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let mut hint = http_body::SizeHint::new();
        hint.set_exact(self.data.len() as u64);
        hint
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use http_body::Body as _;

    use super::*;

    #[tokio::test]
    async fn body_yields_single_frame() {
        let data = Bytes::from_static(b"hello sendfile");
        let mut body = SegmentFileBody::new(data.clone(), 0, data.len() as u64);

        let frame = body.frame().await.unwrap().unwrap();
        let frame_data = frame.into_data().unwrap();
        assert_eq!(&frame_data[..], b"hello sendfile");

        let frame2 = body.frame().await;
        assert!(frame2.is_none());
    }

    #[tokio::test]
    async fn empty_body_yields_none() {
        let data = Bytes::new();
        let mut body = SegmentFileBody::new(data.clone(), 0, 0);
        let frame = body.frame().await;
        assert!(frame.is_none());
    }

    #[test]
    fn size_hint_is_exact() {
        let data = Bytes::from_static(&[0u8; 1024]);
        let body = SegmentFileBody::new(data, 0, 1024);
        assert_eq!(body.size_hint().exact(), Some(1024));
    }

    #[test]
    fn content_length_matches_data() {
        let data = Bytes::from_static(b"abc");
        let body = SegmentFileBody::new(data, 10, 3);
        assert_eq!(body.content_length(), 3);
    }
}
