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
//!
//! # Examples
//!
//! ## Wrapping an output
//!
//! Goal: write a JSON string, escaping its content, with the
//! surrounding quotes written literally. [`json_enc`](json::json_enc)
//! escapes the content; the quotes go straight to the base writer.
//!
//! ```
//! use std::io::Write;
//!
//! use rust_codecs_core::json::json_enc;
//! use rust_codecs_core::sources_and_sinks::std_io::CodecWriter;
//!
//! // Arrange
//! let scratch_buffer = vec![0u8; 64];
//! let mut codec_writer = CodecWriter::new(Vec::new(), json_enc(), scratch_buffer);
//!
//! // Act
//! write!(codec_writer.get_mut(), r#"""#).unwrap();
//! write!(codec_writer, r#""\"#).unwrap();
//! let mut base_writer = codec_writer.finish().unwrap();
//! write!(base_writer, r#"""#).unwrap();
//!
//! // Assert
//! assert_eq!(base_writer, br#""\"\\""#);
//! ```

#[cfg(feature = "alloc")]
extern crate alloc;

mod vocabulary;
pub use vocabulary::{Codec, Drain, Error, ErrorKind, Progress};

mod carry;
pub use carry::Carry;

mod pump;
pub use pump::{stream_to_stream, DriveError, Sink, Source, Totals};

mod chain;
pub use chain::Chain;

pub mod sources_and_sinks;

mod codecs;
#[allow(unused_imports)]
pub use codecs::*;
