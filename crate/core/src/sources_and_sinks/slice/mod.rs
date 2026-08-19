//! `&[u8]`/`&mut [u8]` backend: adapts borrowed slices into the
//! driver's [`Source`](crate::Source)/[`Sink`](crate::Sink) traits, for
//! the shortest possible path between a codec and bytes you already
//! have in memory.

mod adapter;

pub use adapter::{SliceSink, SliceSource};
