//! `embedded_io` backend: bridges into the driver's [`Source`](crate::Source)/
//! [`Sink`](crate::Sink) traits, and the [`CodecReader`]/
//! [`CodecWriter`] wrappers built on top of them.

mod bridge;
mod stream;

pub use bridge::{EmbeddedSource, EmbeddedSink};
pub use stream::{CodecReader, CodecWriter, EmbeddedError};
