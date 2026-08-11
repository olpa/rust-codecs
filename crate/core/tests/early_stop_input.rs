//! Demonstrates driving a [`TerminatingCodec`] directly, call by call,
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

use core::convert::Infallible;

use rust_codecs_core::sources_and_sinks::slice::SliceSource;
use rust_codecs_core::sources_and_sinks::vec::VecSink;
use rust_codecs_core::{
    stream_to_stream, Drain, DrainCodec, Error, Source, TerminatingCodec, TerminatingProgress,
};

struct QuoteEnd;

impl DrainCodec for QuoteEnd {
    fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

impl TerminatingCodec for QuoteEnd {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<TerminatingProgress, Error> {
        let quote_pos = input.iter().position(|&b| b == b'"');
        let available = quote_pos.unwrap_or(input.len());
        let n = available.min(output.len());
        output[..n].copy_from_slice(&input[..n]);
        if n < available {
            // Output ran out before reaching the quote (or the end of input).
            Ok(TerminatingProgress::OutputFilled { consumed: n })
        } else if quote_pos.is_some() {
            // Reached the quote; it's left unconsumed for the driver to deal with.
            Ok(TerminatingProgress::End { consumed: n, written: n })
        } else {
            // Consumed all of input; no quote in sight.
            Ok(TerminatingProgress::InputConsumed { written: n })
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
    let TerminatingProgress::End { consumed, written } = progress else {
        panic!("expected End, got {progress:?}");
    };
    assert_eq!(&output[..written], b"let s = ");
    pos += consumed;
    assert_eq!(input[pos], b'"');

    // Confirm it's actually stuck: calling again with the quote still
    // at the front makes zero progress on either side.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    assert_eq!(progress, TerminatingProgress::End { consumed: 0, written: 0 });

    // The driver's move: skip the delimiter itself, since the codec
    // never will.
    pos += 1;

    // Second call: same codec instance, now past the opening quote —
    // copies the quoted content up to the closing quote, then gets
    // stuck again the same way.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    let TerminatingProgress::End { consumed, written } = progress else {
        panic!("expected End, got {progress:?}");
    };
    assert_eq!(&output[..written], b"Hello, world!");
    pos += consumed;
    assert_eq!(input[pos], b'"');

    // Same check at the closing quote: stuck again, zero progress.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    assert_eq!(progress, TerminatingProgress::End { consumed: 0, written: 0 });

    pos += 1;

    // Third call: nothing left but the trailing `;` and no more
    // quotes — an ordinary InputConsumed, no early stop this time.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    let TerminatingProgress::InputConsumed { written } = progress else {
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
        Self { input, pos: 0, buf: [0; 2], buf_len: 0, buf_pos: 0 }
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

/// How [`read_span`] stopped: at a quote (left unconsumed, exactly
/// like `QuoteEnd` leaves it), or because the source ran out.
enum SpanEnd {
    Quote(String),
    Eof(String),
}

/// The outer scan from [`tokenizes_a_string_array_literal`], rewritten
/// to read through [`Source::chunk`]/[`Source::consume`] instead of
/// indexing a `&[u8]` directly — so it can share one `Source` instance
/// with the `stream_to_stream` calls that handle the quoted spans,
/// instead of each side getting its own fresh slice.
fn read_span<S: Source>(source: &mut S) -> Result<SpanEnd, S::Error> {
    let mut span = Vec::new();
    loop {
        let Some(chunk) = source.chunk()? else {
            return Ok(SpanEnd::Eof(String::from_utf8(span).unwrap()));
        };
        match chunk.iter().position(|&b| b == b'"') {
            Some(quote_pos) => {
                span.extend_from_slice(&chunk[..quote_pos]);
                source.consume(quote_pos);
                return Ok(SpanEnd::Quote(String::from_utf8(span).unwrap()));
            }
            None => {
                let n = chunk.len();
                span.extend_from_slice(chunk);
                source.consume(n);
            }
        }
    }
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
    let mut tokens: Vec<(&str, String)> = Vec::new();

    loop {
        let span = match read_span(&mut source).unwrap() {
            SpanEnd::Quote(span) => span,
            SpanEnd::Eof(span) => {
                if !span.is_empty() {
                    tokens.push(("span", span));
                }
                break;
            }
        };
        if !span.is_empty() {
            tokens.push(("span", span));
        }

        // Opening quote: read_span left it unconsumed.
        let chunk = source.chunk().unwrap().unwrap();
        assert_eq!(chunk[0], b'"');
        source.consume(1);
        tokens.push(("quote", "\"".to_string()));

        // Same source, handed to stream_to_stream just for this span.
        let mut sink = VecSink::default();
        stream_to_stream(&mut source, quote_end(), &mut sink).unwrap();
        let string_token = String::from_utf8(sink.into_inner()).unwrap();
        tokens.push(("string", string_token));

        // Closing quote: QuoteEnd left it unconsumed too.
        let chunk = source.chunk().unwrap().unwrap();
        assert_eq!(chunk[0], b'"');
        source.consume(1);
        tokens.push(("quote", "\"".to_string()));
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
