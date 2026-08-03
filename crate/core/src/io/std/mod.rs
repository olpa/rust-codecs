//! `std::io` backend: bridges into the driver's [`Source`](crate::io::Source)/
//! [`Sink`](crate::io::Sink) traits, and the [`CodecReader`]/[`CodecWriter`]
//! wrappers built on top of them.

mod bridge;
mod stream;

pub use bridge::{StdSource, StdSink};
pub use stream::{CodecReader, CodecWriter};
