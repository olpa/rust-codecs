//! [`QuoteEnd`] copies bytes through unchanged until it sees a `"`,
//! which it treats as a terminator it will not consume itself (no
//! escape handling, for simplicity) — an in-band `End` a tokenizer
//! drives through `stream_to_stream`, one quote-delimited segment at
//! a time.

#![cfg(feature = "alloc")]

use core::convert::Infallible;

use rust_codecs_core::sources_and_sinks::slice::SliceSource;
use rust_codecs_core::sources_and_sinks::vec::VecSink;
use rust_codecs_core::{
    stream_to_stream, BoundaryAwareCodec, BoundaryAwareProgress, DrainProgress, DrainCodec, DriveError,
    Error, Source,
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
            // Output ran out before reaching the quote (or the end of input).
            Ok(BoundaryAwareProgress::OutputFilled { consumed: n })
        } else if quote_pos.is_some() {
            // Reached the quote; it's left unconsumed for the driver to deal with.
            Ok(BoundaryAwareProgress::Boundary {
                consumed: n,
                written: n,
            })
        } else {
            // Consumed all of input; no quote in sight.
            Ok(BoundaryAwareProgress::InputConsumed { written: n })
        }
    }
}

/// Build a fresh [`QuoteEnd`]. Even though it happens to hold no
/// state, call sites that hand a codec to `stream_to_stream` — which
/// takes it by value and consumes it — should still go through a
/// constructor rather than writing the unit struct's name directly,
/// the same as every other codec in this crate (e.g. `rot13()`).
fn quote_end() -> QuoteEnd {
    QuoteEnd
}

/// Run `codec` over the remaining bytes of `source`, collecting its
/// output into a `String` — the shared-`Source` counterpart to
/// `rust_codecs_core::sources_and_sinks::vec::encode_string`, which
/// only ever reads from a borrowed `&str` of its own.
fn encode_string<S: Source>(
    source: &mut S,
    codec: impl BoundaryAwareCodec,
) -> Result<String, DriveError<S::Error, Infallible>> {
    let mut sink = VecSink::default();
    stream_to_stream(source, codec, &mut sink)?;
    Ok(String::from_utf8(sink.into_inner()).unwrap())
}

/// What the tokenizer below expects to find next: plain text outside
/// quotes, plain text inside them, or one of the two quote marks in
/// between — kept as separate states, rather than one `Quote`
/// parameterized by what follows, since the opening quote (before a
/// string) and the closing quote (before a span) lead somewhere
/// different; each is named for where it leads.
#[derive(Clone, Copy, PartialEq)]
enum State {
    Span,
    QuoteThenString,
    String,
    QuoteThenSpan,
}

/// The tokenizing loop: drive [`State`] forward one step per
/// iteration — `Span`/`String` scan text with [`encode_string`],
/// either `Quote*` state consumes the delimiter itself — with
/// `source` picking up exactly where each step left off, since
/// nothing here ever takes ownership of it.
#[test]
fn tokenize_string_array_literal() {
    let input = br#"let a = ["s1", "s2", "s3"];"#;
    let mut source = SliceSource::new(input);
    let mut tokens: Vec<(&str, String)> = Vec::new();
    let mut state = State::Span;

    while source.chunk().unwrap().is_some() {
        state = match state {
            State::Span => {
                let text = encode_string(&mut source, quote_end()).unwrap();
                tokens.push(("span", text));
                State::QuoteThenString
            }
            State::String => {
                let text = encode_string(&mut source, quote_end()).unwrap();
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
