//! A proof of concept: parse using early-stop codecs.
//!
//! [`QuoteEnd`] copies bytes unchanged until it finds a `"`. It treats
//! the quote as an in-band end. It does not consume the quote itself.
//!
//! This example is simple, so it does not handle escapes.

#![cfg(feature = "alloc")]

use core::convert::Infallible;

use rust_codecs_core::sources_and_sinks::slice::SliceSource;
use rust_codecs_core::sources_and_sinks::vec::VecSink;
use rust_codecs_core::{
    stream_to_stream, BoundaryAwareCodec, BoundaryAwareProgress, DrainCodec, DrainProgress,
    DriveError, Error, Source,
};
use std::mem::MaybeUninit;

struct QuoteEnd;

impl DrainCodec for QuoteEnd {
    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }
}

impl BoundaryAwareCodec for QuoteEnd {
    fn process(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        let quote_pos = input.iter().position(|&b| b == b'"');
        let available = quote_pos.unwrap_or(input.len());
        let n = available.min(output.len());
        output[..n].write_copy_of_slice(&input[..n]);
        if n < available {
            // Output ran out before the quote, or before the end of input.
            Ok(BoundaryAwareProgress::OutputFilled { consumed: n })
        } else if quote_pos.is_some() {
            // Reached the quote. Leave it unconsumed; the driver handles it.
            Ok(BoundaryAwareProgress::Boundary {
                consumed: n,
                written: n,
            })
        } else {
            // Consumed all input. No quote found.
            Ok(BoundaryAwareProgress::InputConsumed { written: n })
        }
    }
}

fn quote_end() -> QuoteEnd {
    QuoteEnd
}

/// Run `codec` over the rest of `source`. Collect the output into a
/// `String`. Unlike `rust_codecs_core::sources_and_sinks::vec::encode_string`,
/// which builds its own `SliceSource` from a borrowed `&str`, this
/// drives an existing, shared `source`, so it does not own it or its
/// read position.
fn drive_to_string<S: Source>(
    source: &mut S,
    codec: impl BoundaryAwareCodec,
) -> Result<String, DriveError<S::Error, Infallible>> {
    let mut sink = VecSink::default();
    stream_to_stream(source, codec, &mut sink)?;
    Ok(String::from_utf8(sink.into_inner()).unwrap())
}

/// What the tokenizer below expects next.
///
/// The opening quote and the closing quote each get their own state,
/// instead of one `Quote` state parameterized by what follows. Each
/// name says where that state leads.
#[derive(Clone, Copy, PartialEq)]
enum State {
    /// Plain text outside quotes.
    Span,
    /// The opening quote. It leads into a string.
    QuoteThenString,
    /// Plain text inside quotes.
    String,
    /// The closing quote. It leads into a span.
    QuoteThenSpan,
}

/// The tokenizing loop. Each iteration drives [`State`] forward one
/// step. `Span` and `String` scan text with [`drive_to_string`]. Each
/// `Quote*` state consumes the delimiter itself.
///
/// There is no manual position tracking. Each step reads `source`
/// starting right where the previous step stopped.
#[test]
fn tokenize_string_array_literal() {
    let input = br#"let a = ["s1", "s2", "s3"];"#;
    let mut source = SliceSource::new(input);
    let mut tokens: Vec<(&str, String)> = Vec::new();
    let mut state = State::Span;

    while source.chunk().unwrap().is_some() {
        state = match state {
            State::Span => {
                let text = drive_to_string(&mut source, quote_end()).unwrap();
                tokens.push(("span", text));
                State::QuoteThenString
            }
            State::String => {
                let text = drive_to_string(&mut source, quote_end()).unwrap();
                tokens.push(("string", text));
                State::QuoteThenSpan
            }
            State::QuoteThenString | State::QuoteThenSpan => {
                let chunk = source.chunk().unwrap().unwrap();
                assert_eq!(chunk[0], b'"');
                source.consume(1);
                tokens.push(("quote", "\"".to_string()));
                if state == State::QuoteThenString {
                    State::String
                } else {
                    State::Span
                }
            }
        };
    }

    let expected: Vec<(&str, String)> = vec![
        ("span", "let a = [".to_string()),
        ("quote", "\"".to_string()),
        ("string", "s1".to_string()),
        ("quote", "\"".to_string()),
        ("span", ", ".to_string()),
        ("quote", "\"".to_string()),
        ("string", "s2".to_string()),
        ("quote", "\"".to_string()),
        ("span", ", ".to_string()),
        ("quote", "\"".to_string()),
        ("string", "s3".to_string()),
        ("quote", "\"".to_string()),
        ("span", "];".to_string()),
    ];
    assert_eq!(tokens, expected);
}
