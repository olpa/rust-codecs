#![cfg_attr(not(feature = "std"), no_std)]
//! Core crate for RustCodecs: the [`Codec`] trait, its vocabulary, and
//! the stream adapters a codec crate and its clients build on.
//!
//! - [`Codec`]: implement this for a whole-stream codec. Every ordinary
//!   codec is automatically a [`EndCapableCodec`] too, for input-side
//!   drivers that can use an in-band end.
//! - [`Progress`], [`EndCapableProgress`], [`Drain`], [`Error`],
//!   [`ErrorKind`]: the vocabulary these traits' methods speak in. The
//!   contract in one sentence: every call fully consumes its input,
//!   fully fills its output, or (for a `EndCapableCodec`) ends the
//!   stream in-band.
//! - [`Carry`]: helper for codecs that write output in multi-byte
//!   chunks, letting an emitted chunk span output buffers.
//! - [`Source`]/[`Sink`]/[`stream_to_stream`]: the lending stream
//!   contract, independent of any particular byte transport.
//! - [`sources_and_sinks`]: concrete `Source`/`Sink` backends —
//!   `std::io`, `embedded_io`, and `Vec<u8>`.
//! - [`Pump`]/[`sources_and_sinks::shared_io`]: the reusable core
//!   behind this crate's own `CodecReader`/`CodecWriter` wrappers, for
//!   building an incremental `Read`/`Write`-style wrapper over a
//!   `Source`/`Sink` backend of your own.
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
//! # #[cfg(feature = "json")]
//! # {
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
//! # }
//! ```
//!
//! ## Wrapping an input
//!
//! Decode a base64 stream, this time through `embedded_io` streams and
//! wrappers instead of `std::io`.
//!
//! ```
//! # #[cfg(all(feature = "base64", feature = "embedded-io"))]
//! # {
//! use embedded_io::Read;
//!
//! use rust_codecs_core::base64_dec::base64_dec;
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
//! # }
//! ```
//!
//! ## Any stream to any stream
//!
//! [`stream_to_stream`] applies a codec straight from a [`Source`] to a
//! [`Sink`], with no reader/writer wrapper in between. Here the source
//! borrows a `&str`'s bytes and the sink grows a `Vec<u8>`.
//!
//! ```
//! # #[cfg(feature = "rot13")]
//! # {
//! use rust_codecs_core::rot13::rot13;
//! use rust_codecs_core::sources_and_sinks::slice::SliceSource;
//! use rust_codecs_core::sources_and_sinks::vec::VecSink;
//! use rust_codecs_core::stream_to_stream;
//!
//! // Arrange
//! let rot13_codec = rot13();
//! let mut sink = VecSink::default();
//!
//! // Act
//! let input: &str = "hello";
//! let mut source = SliceSource::new(input.as_bytes());
//!
//! stream_to_stream(&mut source, rot13_codec, &mut sink).unwrap();
//!
//! // Assert
//! let output = String::from_utf8(sink.into_inner()).unwrap();
//! assert_eq!(output, "uryyb");
//! # }
//! ```
//!
//! Notes:
//!
//! - This slice-to-`Vec` wiring is a simplified re-implementation of
//!   [`encode_str`](sources_and_sinks::vec::encode_str)/
//!   [`encode_string`](sources_and_sinks::vec::encode_string); reach for
//!   those instead of hand-rolling this pattern.
//! - Side usage: with the [`identity`](identity::identity) codec,
//!   [`stream_to_stream`] becomes a generic "copy" function between
//!   streams that are otherwise incompatible, for example `std::io`
//!   and `embedded_io`.
//!
//! ## Chain of codecs
//!
//! [`Chain`] composes two codecs into one.
//!
//! TODO: implement example gzip + base64 after we have gzip (from `compcol`).
//!
//! ```ignore
//! // TODO: gzip_enc() doesn't exist yet.
//! use rust_codecs_core::base64_enc::base64_enc;
//! use rust_codecs_core::gzip::gzip_enc;
//! use rust_codecs_core::Chain;
//! use rust_codecs_core::sources_and_sinks::slice::SliceSource;
//! use rust_codecs_core::sources_and_sinks::vec::VecSink;
//! use rust_codecs_core::stream_to_stream;
//!
//! let chain = Chain::new(gzip_enc(), base64_enc(), vec![0u8; 64]);
//! let mut source = SliceSource::new(b"hello");
//! let mut sink = VecSink::default();
//! stream_to_stream(&mut source, chain, &mut sink).unwrap();
//! ```
//!
//! This looks equivalent to wrapping the input in one codec and the
//! output in the other, then running `std::io::copy` between them —
//! and for a well-behaved, complete stream it is. See [`Chain`]'s docs
//! for where that equivalence breaks down.
//!
//! ## Parsing using early-stop codecs
//!
//! A [`EndCapableCodec`] does not have to run through
//! [`stream_to_stream`] end to end. It can also power a small
//! hand-written parser, driven one step at a time. The full source for
//! this example lives in `core/tests/early_stop_input.rs`, which
//! tokenizes input made of plain text with quoted strings inside it.
//!
//! The codec at the center of that test, `QuoteEnd`, copies bytes
//! through unchanged until it meets a `"`. It treats that quote as an
//! in-band end, but doesn't consume the quote byte itself.
//!
//! Two ways of moving through the input show up side by side:
//!
//! - Inside a span of plain text, or inside a quoted string, the
//!   codec does the reading. `encode_string` runs it over a
//!   [`Source`] through [`stream_to_stream`], and the source's
//!   current position moves forward as a side effect.
//! - The quote character itself has no codec behind it. The driver loop
//!   reads it with plain [`Source`] calls, `chunk()` and `consume()`,
//!   advancing the source's position by hand.
//!
//! ```text
//! while source.chunk().unwrap().is_some() {
//!     state = match state {
//!         ...
//!         State::String => {
//!             let text = encode_string(source, quote_end()).unwrap();
//!             tokens.push(("string", text));
//!             State::QuoteThenTopLevel
//!         }
//!         State::QuoteThenString | State::QuoteThenTopLevel => {
//!             let chunk = source.chunk().unwrap().unwrap();
//!             assert_eq!(chunk[0], b'"');
//!             source.consume(1);
//!             ...
//!         }
//!     };
//! }
//! ```
//!
//! See `tokenize_string_array_literal` in
//! `core/tests/early_stop_input.rs` for the full working version.
//!
//! # A note on `base64` and `json`
//!
//! These two codecs live in this crate as a chicken-and-egg bootstrap:
//! the [`Codec`] trait needed real implementations to shake out its
//! design before this crate had any users, and there was no separate
//! codec crate yet to put them in. Long-term they belong in their own
//! crates outside `rust-codecs-core`, each depending on this crate
//! rather than living inside it.

#[cfg(feature = "alloc")]
extern crate alloc;

mod protocol;
pub use protocol::{
    Codec, Drain, DrainCodec, EndCapableCodec, EndCapableProgress, Error, ErrorKind, Progress,
    Sink, Source,
};

mod carry;
pub use carry::{Carry, CarryError};

mod step;

mod stream;
pub use stream::{stream_to_stream, DriveError, Pump, Totals};

mod chain;
pub use chain::Chain;

pub mod sources_and_sinks;

mod codecs;
#[allow(unused_imports)]
pub use codecs::*;

#[cfg(test)]
mod feature_coverage {
    // A plain `cargo test` silently skips whatever optional features
    // aren't enabled — no error, just fewer tests run. Surface that
    // instead of letting it pass quietly: this test only actually runs
    // (and only then needs to pass) under `--all-features`; otherwise
    // it's skipped as "ignored" with the reason below, which shows up
    // in the summary line without failing the narrower run.
    #[test]
    #[cfg_attr(
        not(all(
            feature = "std",
            feature = "base64",
            feature = "json",
            feature = "embedded-io"
        )),
        ignore = "some optional features are disabled — run `cargo test --all-features`"
    )]
    #[allow(clippy::assertions_on_constants)]
    fn all_optional_features_are_enabled() {
        assert!(cfg!(feature = "std"), "feature \"std\" is off");
        assert!(cfg!(feature = "base64"), "feature \"base64\" is off");
        assert!(cfg!(feature = "json"), "feature \"json\" is off");
        assert!(
            cfg!(feature = "embedded-io"),
            "feature \"embedded-io\" is off"
        );
    }
}
