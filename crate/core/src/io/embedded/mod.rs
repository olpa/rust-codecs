//! `embedded_io` backend: bridges into the driver's [`Source`](crate::io::Source)/
//! [`Sink`](crate::io::Sink) traits, and the [`EmbeddedCodecReader`]/
//! [`EmbeddedCodecWriter`] wrappers built on top of them.

mod bridge;
mod stream;

pub use bridge::{EmbeddedSource, EmbeddedSink};
pub use stream::{EmbeddedCodecReader, EmbeddedCodecWriter, EmbeddedError};
