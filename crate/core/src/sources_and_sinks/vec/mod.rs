//! `Vec<u8>` backend: bridges an owned vector into the driver's
//! [`Source`](crate::Source)/[`Sink`](crate::Sink) traits. Fully
//! in-memory, so there's no `std::io`/`embedded_io`-style stream wrapper
//! to build on top — [`stream_to_stream`](crate::stream_to_stream) is
//! the entry point.

mod bridge;

pub use bridge::{VecSource, VecSink};
