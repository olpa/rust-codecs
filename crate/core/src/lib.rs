#![cfg_attr(not(feature = "std"), no_std)]
//! Core crate for RustCodecs: the [`Codec`] trait, its vocabulary, and
//! the stream adapters a codec crate and its clients build on.
//!
//! - [`Codec`]: implement this to add a codec.
//! - [`Progress`], [`Drain`], [`Error`], [`ErrorKind`]: the vocabulary
//!   [`Codec`]'s methods speak in. The contract in one sentence: every
//!   call fully consumes its input, fully fills its output, or ends
//!   the stream.
//! - [`Carry`]: helper for codecs with a minimum atomic output unit,
//!   letting an emitted unit span output buffers.
//! - [`Source`]/[`Sink`]/[`stream_to_stream`]: the lending stream
//!   contract, independent of any particular byte transport.
//! - [`sources_and_sinks`]: concrete `Source`/`Sink` backends —
//!   `std::io`, `embedded_io`, and `Vec<u8>`.

#[cfg(feature = "alloc")]
extern crate alloc;

mod vocabulary;
pub use vocabulary::{Codec, Drain, Error, ErrorKind, Progress};

mod carry;
pub use carry::Carry;

mod transfer;
mod driver;

mod chain;
pub use chain::Chain;

mod stream_to_stream;
pub use stream_to_stream::{stream_to_stream, DriveError, Source, Sink, Totals};

pub mod sources_and_sinks;

mod codecs;
#[allow(unused_imports)]
pub use codecs::*;
