//! Adapters for `&[u8]` backends.

mod adapter;

pub use adapter::{SliceSink, SliceSource};
