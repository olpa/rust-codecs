use core::convert::Infallible;

use crate::sources_and_sinks::slice::SliceSink;
use crate::stream::{Pump, PumpDrainEnd, PumpStepEnd};
use crate::{DriveError, EndCapableCodec, Source};

/// Drive `pump` against `input`, filling `buf` with transformed bytes —
/// the transport-independent core of a `Read::read` impl, matching
/// what `std_io`/`embedded_io`'s own `CodecReader` calls internally.
/// Returns as soon as one pull from `input` yields output, instead of
/// chasing a full `buf` — so `read()` never blocks past what a single
/// pull from `input` already blocked on. A step that pulls input but
/// produces no output yet (a codec buffering several input bytes
/// before it can emit anything, e.g. `base64_dec`) doesn't count:
/// looping past it is required to avoid `read()` returning `Ok(0)`
/// before genuine end-of-stream, which callers would misread as EOF.
/// Ends the codec's stream (running `finish`) once `input` reports
/// exhaustion, so the caller never has to invoke `finish` itself.
pub fn pump_read<I: Source, C: EndCapableCodec>(
    pump: &mut Pump<C>,
    input: &mut I,
    buf: &mut [u8],
) -> Result<usize, DriveError<I::Error, Infallible>> {
    if buf.is_empty() || pump.is_done() {
        return Ok(0);
    }
    let mut output = SliceSink::new(buf);
    let source_exhausted = loop {
        let step = pump.transfer_step(input, &mut output)?;
        if step.end == PumpStepEnd::SourceExhausted {
            break true;
        }
        if step.end != PumpStepEnd::Progressed || output.written() > 0 {
            break false;
        }
    };
    if source_exhausted {
        let drained = pump.finish_to(&mut output).map_err(widen)?;
        // Filling this caller-provided read buffer is normal partial-read
        // progress, not an I/O failure. `finish_to` records that condition
        // in its successful result so the next `read` can resume finalizing.
        debug_assert!(matches!(
            drained.end,
            PumpDrainEnd::Done | PumpDrainEnd::SinkExhausted
        ));
    }
    Ok(output.written())
}

/// `finish_to`'s error is `DriveError<Infallible, Infallible>` (its
/// input side never runs and its output side is the same `SliceSink`
/// as `transfer_from`'s); widen it to line up with `pump_read`'s
/// return type so both calls share one `?`-friendly error type.
fn widen<T>(error: DriveError<Infallible, Infallible>) -> DriveError<T, Infallible> {
    match error {
        DriveError::Source(never) | DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => DriveError::Codec(error),
        DriveError::SinkExhausted => DriveError::SinkExhausted,
        DriveError::NoProgress => DriveError::NoProgress,
    }
}

#[cfg(test)]
mod tests {
    use core::convert::Infallible;

    use super::pump_read;
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::{Codec, Drain, DrainCodec, Error, Progress, Pump, Source};

    struct Trailer {
        position: usize,
    }

    impl DrainCodec for Trailer {
        fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            const BYTES: &[u8] = b"final";
            let written = (BYTES.len() - self.position).min(output.len());
            output[..written].copy_from_slice(&BYTES[self.position..self.position + written]);
            self.position += written;
            if self.position == BYTES.len() {
                Ok(Drain::Done { written })
            } else {
                Ok(Drain::OutputFilled)
            }
        }
    }

    impl Codec for Trailer {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            Ok(Progress::InputConsumed { written: 0 })
        }
    }

    #[test]
    fn finalization_resumes_after_filling_each_read_buffer() {
        let mut source = SliceSource::new(&[]);
        let mut pump = Pump::new(Trailer { position: 0 });
        let mut collected = [0; 5];
        let mut position = 0;

        while position < collected.len() {
            let end = (position + 2).min(collected.len());
            let written = pump_read(&mut pump, &mut source, &mut collected[position..end]).unwrap();
            assert!(written > 0);
            position += written;
        }

        assert_eq!(&collected, b"final");
        assert_eq!(pump_read(&mut pump, &mut source, &mut [0; 2]).unwrap(), 0);
    }

    /// A byte-identical copy codec with no feature-gated dependency
    /// (unlike `crate::identity::identity()`), so these tests don't
    /// need the `identity` feature enabled.
    struct PassThrough;

    impl DrainCodec for PassThrough {
        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    impl Codec for PassThrough {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
            let n = input.len().min(output.len());
            output[..n].copy_from_slice(&input[..n]);
            if n == input.len() {
                Ok(Progress::InputConsumed { written: n })
            } else {
                Ok(Progress::OutputFilled { consumed: n })
            }
        }
    }

    /// A `Source` that never reveals more than `chunk_size` bytes from
    /// one `chunk()` call, no matter how much of `bytes` remains —
    /// stands in for a wrapped reader whose own `read()` returns in
    /// bounded pieces (like a terminal line at a time), proving
    /// `pump_read` returns after a single such pull instead of chasing
    /// a full buffer.
    struct LimitedChunkSource<'a> {
        bytes: &'a [u8],
        pos: usize,
        chunk_size: usize,
    }

    impl Source for LimitedChunkSource<'_> {
        type Error = Infallible;

        fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
            let end = (self.pos + self.chunk_size).min(self.bytes.len());
            Ok((self.pos < self.bytes.len()).then_some(&self.bytes[self.pos..end]))
        }

        fn consume(&mut self, amount: usize) {
            self.pos += amount;
        }
    }

    #[test]
    fn single_read_returns_after_one_source_pull() {
        // 9 bytes available in 3-byte pulls, into a buffer big enough
        // for all of them: `pump_read` must return after the first
        // pull alone, not chase a full buffer.
        let mut source = LimitedChunkSource {
            bytes: b"abcdefghi",
            pos: 0,
            chunk_size: 3,
        };
        let mut pump = Pump::new(PassThrough);
        let mut buf = [0u8; 9];

        let written = pump_read(&mut pump, &mut source, &mut buf).unwrap();

        assert_eq!(written, 3);
        assert_eq!(&buf[..3], b"abc");
        assert_eq!(source.pos, 3);
    }
}
