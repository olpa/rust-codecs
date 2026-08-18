//! Demonstrates driving a [`EndCapableCodec`] directly, call by call,
//! instead of through `stream_to_stream`'s internal pump loop — and
//! what an early-stop input (in-band `End`) looks like from that
//! level.
//!
//! [`QuoteEnd`] copies bytes through unchanged until it sees a `"`,
//! which it treats as a terminator it will not consume itself (no
//! escape handling, for simplicity). Driven directly, that lets the
//! same codec instance be reused across multiple quote-delimited
//! segments of one input — something `Pump` doesn't allow, since it
//! latches permanently after the first `End`. The driver (this test)
//! is responsible for skipping the quote byte itself between calls.
//!
//! The last test stresses the same tokenizer over a [`CodecSource`],
//! a base64 decoder wrapped around [`TwoByteSource`], turning the
//! codec's own output into a `Source`.

use core::convert::Infallible;

use rust_codecs_core::sources_and_sinks::slice::SliceSource;
use rust_codecs_core::sources_and_sinks::vec::VecSink;
use rust_codecs_core::{
    stream_to_stream, Drain, DrainCodec, DriveError, Error, Source, EndCapableCodec,
    EndCapableProgress,
};

struct QuoteEnd;

impl DrainCodec for QuoteEnd {
    fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

impl EndCapableCodec for QuoteEnd {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<EndCapableProgress, Error> {
        let quote_pos = input.iter().position(|&b| b == b'"');
        let available = quote_pos.unwrap_or(input.len());
        let n = available.min(output.len());
        output[..n].copy_from_slice(&input[..n]);
        if n < available {
            // Output ran out before reaching the quote (or the end of input).
            Ok(EndCapableProgress::OutputFilled { consumed: n })
        } else if quote_pos.is_some() {
            // Reached the quote; it's left unconsumed for the driver to deal with.
            Ok(EndCapableProgress::End {
                consumed: n,
                written: n,
            })
        } else {
            // Consumed all of input; no quote in sight.
            Ok(EndCapableProgress::InputConsumed { written: n })
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

#[test]
fn drives_three_segments_across_two_early_stops() {
    let input = b"let s = \"Hello, world!\";".to_vec();
    let mut pos = 0;
    let mut codec = quote_end();
    let mut output = [0u8; 64];

    // First call: copies everything up to (not including) the opening
    // quote, then gets stuck — End, with the quote itself still
    // sitting unconsumed at the front of what's left.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    let EndCapableProgress::End { consumed, written } = progress else {
        panic!("expected End, got {progress:?}");
    };
    assert_eq!(&output[..written], b"let s = ");
    pos += consumed;
    assert_eq!(input[pos], b'"');

    // Confirm it's actually stuck: calling again with the quote still
    // at the front makes zero progress on either side.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    assert_eq!(
        progress,
        EndCapableProgress::End {
            consumed: 0,
            written: 0
        }
    );

    // The driver's move: skip the delimiter itself, since the codec
    // never will.
    pos += 1;

    // Second call: same codec instance, now past the opening quote —
    // copies the quoted content up to the closing quote, then gets
    // stuck again the same way.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    let EndCapableProgress::End { consumed, written } = progress else {
        panic!("expected End, got {progress:?}");
    };
    assert_eq!(&output[..written], b"Hello, world!");
    pos += consumed;
    assert_eq!(input[pos], b'"');

    // Same check at the closing quote: stuck again, zero progress.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    assert_eq!(
        progress,
        EndCapableProgress::End {
            consumed: 0,
            written: 0
        }
    );

    pos += 1;

    // Third call: nothing left but the trailing `;` and no more
    // quotes — an ordinary InputConsumed, no early stop this time.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    let EndCapableProgress::InputConsumed { written } = progress else {
        panic!("expected InputConsumed, got {progress:?}");
    };
    assert_eq!(&output[..written], b";");
}

/// A small tokenizer built on the same early-stop pattern above, but
/// driving `QuoteEnd` through `stream_to_stream` instead of raw
/// `process()` calls.
///
/// The outer loop scans byte by byte for the next quote itself (no
/// codec involved for that part) — collecting whatever came before it
/// as a `"span"` token and the quote itself as a `"quote"` token. Once
/// inside a quoted span, `QuoteEnd` takes over via `stream_to_stream`,
/// copying bytes into a `Vec` sink until the closing quote, whose byte
/// count consumed is read back off `Totals` to advance the outer
/// scan — the closing quote itself is then asserted and pushed the
/// same way as the opening one.
#[test]
fn tokenizes_a_string_array_literal() {
    let input = br#"let a = ["s1", "s2", "s3"];"#;
    let mut pos = 0;
    let mut tokens: Vec<(&str, String)> = Vec::new();

    while pos < input.len() {
        // Byte-by-byte scan for the next quote — plain Rust, no codec.
        let start = pos;
        while pos < input.len() && input[pos] != b'"' {
            pos += 1;
        }
        if pos > start {
            let text = String::from_utf8(input[start..pos].to_vec()).unwrap();
            tokens.push(("span", text));
        }
        if pos == input.len() {
            break; // Ran off the end without finding another quote.
        }

        // Opening quote.
        tokens.push(("quote", "\"".to_string()));
        pos += 1;

        // Hand the quoted span to QuoteEnd, driven through
        // stream_to_stream instead of by hand.
        let mut source = SliceSource::new(&input[pos..]);
        let mut sink = VecSink::default();
        let totals = stream_to_stream(&mut source, quote_end(), &mut sink).unwrap();
        let string_token = String::from_utf8(sink.into_inner()).unwrap();
        tokens.push(("string", string_token));
        pos += totals.consumed;

        // Closing quote: QuoteEnd left it unconsumed, exactly like the
        // raw process() calls above did.
        assert_eq!(input[pos], b'"');
        tokens.push(("quote", "\"".to_string()));
        pos += 1;
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

/// A [`Source`] over a `&[u8]` that hands out at most 2 bytes per
/// `chunk()` call, instead of the whole remaining slice at once like
/// [`SliceSource`] does — used below to check that `QuoteEnd` doesn't
/// care how narrow its window onto the input is.
struct TwoByteSource<'a> {
    input: &'a [u8],
    pos: usize,
    buf: [u8; 2],
    buf_len: usize,
    buf_pos: usize,
}

impl<'a> TwoByteSource<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            pos: 0,
            buf: [0; 2],
            buf_len: 0,
            buf_pos: 0,
        }
    }
}

impl Source for TwoByteSource<'_> {
    type Error = Infallible;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        if self.buf_pos == self.buf_len {
            let n = (self.input.len() - self.pos).min(2);
            self.buf[..n].copy_from_slice(&self.input[self.pos..self.pos + n]);
            self.pos += n;
            self.buf_len = n;
            self.buf_pos = 0;
        }
        Ok((self.buf_pos < self.buf_len).then_some(&self.buf[self.buf_pos..self.buf_len]))
    }

    fn consume(&mut self, amount: usize) {
        self.buf_pos += amount;
    }
}

/// Run `codec` over the remaining bytes of `source`, collecting its
/// output into a `String` — the shared-`Source` counterpart to
/// `rust_codecs_core::sources_and_sinks::vec::encode_string`, which
/// only ever reads from a borrowed `&str` of its own.
fn encode_string<S: Source>(
    source: &mut S,
    codec: impl EndCapableCodec,
) -> Result<String, DriveError<S::Error, Infallible>> {
    let mut sink = VecSink::default();
    stream_to_stream(source, codec, &mut sink)?;
    Ok(String::from_utf8(sink.into_inner()).unwrap())
}

/// What [`tokenize_string_array_literal`] expects to find next: plain
/// text outside quotes, plain text inside them, or one of the two
/// quote marks in between — kept as separate states, rather than one
/// `Quote` parameterized by what follows, since the opening quote
/// (before a string) and the closing quote (before a span) lead
/// somewhere different; each is named for where it leads.
#[derive(Clone, Copy, PartialEq)]
enum State {
    Span,
    QuoteThenString,
    String,
    QuoteThenSpan,
}

/// The tokenizing loop shared by every test below that reads through a
/// [`Source`] rather than indexing a `&[u8]` directly: drive [`State`]
/// forward one step per iteration — `Span`/`String` scan text with
/// [`encode_string`], either `Quote*` state consumes the delimiter
/// itself — with `source` picking up exactly where each step left
/// off, since nothing here ever takes ownership of it.
fn tokenize_string_array_literal<S: Source>(source: &mut S) -> Vec<(&'static str, String)>
where
    S::Error: core::fmt::Debug,
{
    let mut tokens: Vec<(&str, String)> = Vec::new();
    let mut state = State::Span;

    while source.chunk().unwrap().is_some() {
        state = match state {
            State::Span => {
                let text = encode_string(source, quote_end()).unwrap();
                tokens.push(("span", text));
                State::QuoteThenString
            }
            State::String => {
                let text = encode_string(source, quote_end()).unwrap();
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

    tokens
}

/// The token stream every test below expects, decoded from `let a =
/// ["s1", "s2", "s3"];` regardless of which `Source` produced those
/// bytes.
fn assert_string_array_literal_tokens(tokens: Vec<(&str, String)>) {
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

/// Same tokenizer as [`tokenizes_a_string_array_literal`], same input
/// value, but reading through one [`TwoByteSource`] for the *whole*
/// input — created once, before the loop even starts — instead of a
/// fresh [`SliceSource`] per quoted span. The outer scan and every
/// `stream_to_stream` call take turns advancing it: `stream_to_stream`
/// borrows it just for the quoted content, and because it only ever
/// takes `&mut Source` (never owns it), control returns to the outer
/// scan afterward with the source picking up exactly where `QuoteEnd`
/// left off — the closing quote still sitting unconsumed at the
/// front, ready for `read_span`'s next `chunk()` call to see. Nothing
/// about `Source`/`stream_to_stream` needed to change to make this
/// work.
#[test]
fn tokenizes_a_string_array_literal_from_a_two_byte_source() {
    let input = br#"let a = ["s1", "s2", "s3"];"#;
    let mut source = TwoByteSource::new(input);
    let tokens = tokenize_string_array_literal(&mut source);
    assert_string_array_literal_tokens(tokens);
}

/// The same tokenizer and expected output again, but now `source`
/// isn't reading plaintext at all — [`CodecSource`] decodes base64 on
/// the fly, through a 2-byte scratch buffer, over a [`TwoByteSource`]
/// that hands out the *encoded* bytes 2 at a time. Two narrow windows
/// stacked on each other: `TwoByteSource` stresses `Base64Dec`'s own
/// input handling, and `CodecSource`'s 2-byte decode buffer (smaller
/// than `Base64Dec`'s 3-byte atomic output group) stresses its
/// `Carry`. Neither `read_span` nor `tokenize_string_array_literal`
/// know or care that they're reading through a codec instead of raw
/// bytes.
#[test]
fn tokenizes_a_string_array_literal_from_a_base64_decoded_two_byte_source() {
    use rust_codecs_core::base64::base64_dec;

    let encoded = b"bGV0IGEgPSBbInMxIiwgInMyIiwgInMzIl07";
    let mut source: CodecSource<_, _, 2> =
        CodecSource::new(TwoByteSource::new(encoded), base64_dec());
    let tokens = tokenize_string_array_literal(&mut source);
    assert_string_array_literal_tokens(tokens);
}

/// Wraps a [`Source`] with a codec, decoding through an owned,
/// fixed-size scratch buffer of `N` bytes — turning the codec's output
/// into a `Source` in its own right. Built on [`Pump`]/`pump_read`,
/// the same pieces `std_io`/`embedded_io`'s own `CodecReader` uses
/// internally to do the equivalent for `std::io::Read`/
/// `embedded_io::Read`; see `CREATING-IO-BACKENDS.md` for the general
/// pattern.
struct CodecSource<I: Source, C: EndCapableCodec, const N: usize> {
    inner: I,
    pump: rust_codecs_core::Pump<C>,
    buf: [u8; N],
    pos: usize,
    len: usize,
}

impl<I: Source, C: EndCapableCodec, const N: usize> CodecSource<I, C, N> {
    fn new(inner: I, codec: C) -> Self {
        Self {
            inner,
            pump: rust_codecs_core::Pump::new(codec),
            buf: [0; N],
            pos: 0,
            len: 0,
        }
    }
}

impl<I: Source, C: EndCapableCodec, const N: usize> Source for CodecSource<I, C, N> {
    type Error = rust_codecs_core::DriveError<I::Error, Infallible>;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        if self.pos == self.len {
            self.len = rust_codecs_core::sources_and_sinks::shared_io::pump_read(
                &mut self.pump,
                &mut self.inner,
                &mut self.buf,
                rust_codecs_core::sources_and_sinks::shared_io::ReadGranularity::FillBuffer,
            )?;
            self.pos = 0;
        }
        Ok((self.pos < self.len).then_some(&self.buf[self.pos..self.len]))
    }

    fn consume(&mut self, amount: usize) {
        self.pos += amount;
    }
}
