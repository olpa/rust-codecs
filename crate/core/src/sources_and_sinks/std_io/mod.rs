//! `std::io` backend: adapts into the driver's [`Source`](crate::Source)/
//! [`Sink`](crate::Sink) traits, and the [`CodecReader`]/[`CodecWriter`]
//! wrappers built on top of them.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSource, StdSink};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};
