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
//! Write a JSON string, escaping its content with
//! [`json_enc`](json::json_enc), while the surrounding quotes go
//! straight through to the base writer, unescaped.
//!
//! ```
//! use std::io::Write;
//!
//! use rust_codecs_core::json::json_enc;
//! use rust_codecs_core::sources_and_sinks::std_io::CodecWriter;
//!
//! // Arrange
//! let json_codec = json_enc();
//! let scratch_buffer = vec![0u8; 64];
//! let mut base_writer: Vec<u8> = Vec::new(); // implements std::io::Write
//!
//! // Act
//! write!(base_writer, r#"""#).unwrap();
//!
//! let mut codec_writer = CodecWriter::new(base_writer, json_codec, scratch_buffer);
//! write!(codec_writer, r#""\"#).unwrap();
//!
//! let mut base_writer = codec_writer.finish().unwrap();
//! write!(base_writer, r#"""#).unwrap();
//!
//! // Assert
//! let output = base_writer;
//! assert_eq!(output, br#""\"\\""#);
//! ```
//!
//! ## Wrapping an input
//!
//! Decode a base64 stream, this time through `embedded_io` streams and
//! wrappers instead of `std::io`.
//!
//! ```
//! use embedded_io::Read;
//!
//! use rust_codecs_core::base64::base64_dec;
//! use rust_codecs_core::sources_and_sinks::embedded_io::CodecReader;
//!
//! // Arrange
//! let base64_codec = base64_dec();
//! let scratch_buffer = [0u8; 64];
//! let mut read_buffer = [0u8; 4];
//!
//! // Act
//! let input: &[u8] = b"8J+mgA==";
//! let mut codec_reader = CodecReader::new(input, base64_codec, scratch_buffer);
//! // `read_exact` is `embedded_io::Read`'s, imported above
//! codec_reader.read_exact(&mut read_buffer).unwrap();
//!
//! // Assert
//! let text = std::str::from_utf8(&read_buffer).unwrap();
//! assert_eq!(text, "🦀");
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
