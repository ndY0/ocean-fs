//! Stripe layout, batching, and parallel encode/decode.
//!
//! Splits segment data into independent stripes and processes them
//! in parallel using rayon.

mod batch;
mod layout;
mod parallel;

pub use batch::StripeBatch;
pub use layout::StripeLayout;
pub use parallel::{ParallelDecoder, ParallelEncoder};
