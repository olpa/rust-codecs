//! Demonstrates driving a [`Codec`] directly, call by call, instead of
//! through `stream_to_stream`'s internal pump loop — and what an
//! early-stop input (in-band `StreamEnd`) looks like from that level.
//!
//! [`QuoteEnd`] copies bytes through unchanged until it sees a `"`,
//! which it treats as a terminator it will not consume itself (no
//! escape handling, for simplicity). Driven directly, that lets the
//! same codec instance be reused across multiple quote-delimited
//! segments of one input — something `Pump` doesn't allow, since it
//! latches permanently after the first `StreamEnd`. The driver (this
//! test) is responsible for skipping the quote byte itself between
//! calls.

use rust_codecs_core::{Codec, Drain, Error, Progress};

struct QuoteEnd;

impl Codec for QuoteEnd {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        let quote_pos = input.iter().position(|&b| b == b'"');
        let available = quote_pos.unwrap_or(input.len());
        let n = available.min(output.len());
        output[..n].copy_from_slice(&input[..n]);
        if n < available {
            // Output ran out before reaching the quote (or the end of input).
            Ok(Progress::OutputFilled { consumed: n })
        } else if quote_pos.is_some() {
            // Reached the quote; it's left unconsumed for the driver to deal with.
            Ok(Progress::StreamEnd { consumed: n, written: n })
        } else {
            // Consumed all of input; no quote in sight.
            Ok(Progress::InputConsumed { written: n })
        }
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

#[test]
fn drives_three_segments_across_two_early_stops() {
    let input = b"let s = \"Hello, world!\";".to_vec();
    let mut pos = 0;
    let mut codec = QuoteEnd;
    let mut output = [0u8; 64];

    // First call: copies everything up to (not including) the opening
    // quote, then gets stuck — StreamEnd, with the quote itself still
    // sitting unconsumed at the front of what's left.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    let Progress::StreamEnd { consumed, written } = progress else {
        panic!("expected StreamEnd, got {progress:?}");
    };
    assert_eq!(&output[..written], b"let s = ");
    pos += consumed;
    assert_eq!(input[pos], b'"');

    // Confirm it's actually stuck: calling again with the quote still
    // at the front makes zero progress on either side.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    assert_eq!(progress, Progress::StreamEnd { consumed: 0, written: 0 });

    // The driver's move: skip the delimiter itself, since the codec
    // never will.
    pos += 1;

    // Second call: same codec instance, now past the opening quote —
    // copies the quoted content up to the closing quote, then gets
    // stuck again the same way.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    let Progress::StreamEnd { consumed, written } = progress else {
        panic!("expected StreamEnd, got {progress:?}");
    };
    assert_eq!(&output[..written], b"Hello, world!");
    pos += consumed;
    assert_eq!(input[pos], b'"');

    // Same check at the closing quote: stuck again, zero progress.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    assert_eq!(progress, Progress::StreamEnd { consumed: 0, written: 0 });

    pos += 1;

    // Third call: nothing left but the trailing `;` and no more
    // quotes — an ordinary InputConsumed, no early stop this time.
    let progress = codec.process(&input[pos..], &mut output).unwrap();
    let Progress::InputConsumed { written } = progress else {
        panic!("expected InputConsumed, got {progress:?}");
    };
    assert_eq!(&output[..written], b";");
}
