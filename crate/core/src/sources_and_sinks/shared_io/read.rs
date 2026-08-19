use core::convert::Infallible;

use crate::stream::{DrainEnd, Pump, PumpEnd, StepEnd};
use crate::sources_and_sinks::slice::SliceSink;
use crate::{DriveError, Source, EndCapableCodec};

/// How much a single [`pump_read`] call (and so a single `Read::read`
/// call on `CodecReader`/`BufReadCodecReader`) pulls from the wrapped
/// source before returning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadGranularity {
    /// Keep pulling more input until the caller's buffer is full or
    /// the source is exhausted — best throughput, the default. Right
    /// for bulk/piped transfers, where nothing is waiting on any one
    /// chunk in particular.
    #[default]
    FillBuffer,
    /// Return as soon as one pull from the wrapped source made any
    /// progress, instead of chasing a full buffer. Matches the wrapped
    /// source's own read granularity, so `read()` never blocks past
    /// what a single inner read already blocked on.
    ///
    /// This is the interactive-application setting: use it whenever a
    /// handler downstream of this reader should see each unit of input
    /// (a typed terminal line, a network datagram) as soon as it
    /// arrives, rather than only once enough of them have accumulated
    /// to fill some buffer it has no visibility into.
    SingleRead,
}

/// Drive `pump` against `input`, filling `buf` with transformed bytes —
/// the transport-independent core of a `Read::read` impl, matching
/// what `std_io`/`embedded_io`'s own `CodecReader` calls internally.
/// Ends the codec's stream (running `finish`) once `input` reports
/// exhaustion, so the caller never has to invoke `finish` itself.
pub fn pump_read<I: Source, C: EndCapableCodec>(
    pump: &mut Pump<C>,
    input: &mut I,
    buf: &mut [u8],
    granularity: ReadGranularity,
) -> Result<usize, DriveError<I::Error, Infallible>> {
    if buf.is_empty() || pump.is_done() {
        return Ok(0);
    }
    let mut output = SliceSink::new(buf);
    let source_exhausted = match granularity {
        ReadGranularity::FillBuffer => {
            pump.transfer_from(input, &mut output)?.end == PumpEnd::SourceExhausted
        }
        ReadGranularity::SingleRead => {
            pump.transfer_step(input, &mut output)?.end == StepEnd::SourceExhausted
        }
    };
    if source_exhausted {
        let drained = pump.finish_to(&mut output).map_err(widen)?;
        // Filling this caller-provided read buffer is normal partial-read
        // progress, not an I/O failure. `finish_to` records that condition
        // in its successful result so the next `read` can resume finalizing.
        debug_assert!(matches!(drained.end, DrainEnd::Done | DrainEnd::SinkExhausted));
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

    use super::{pump_read, ReadGranularity};
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
            let written = pump_read(
                &mut pump,
                &mut source,
                &mut collected[position..end],
                ReadGranularity::FillBuffer,
            )
            .unwrap();
            assert!(written > 0);
            position += written;
        }

        assert_eq!(&collected, b"final");
        assert_eq!(
            pump_read(&mut pump, &mut source, &mut [0; 2], ReadGranularity::FillBuffer).unwrap(),
            0
        );
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
    /// bounded pieces (like a terminal line at a time), the case
    /// `ReadGranularity::SingleRead` exists for.
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
        // for all of them: FillBuffer keeps pulling until the buffer
        // (or the source) is exhausted, but SingleRead must return
        // after the first pull alone.
        let mut source = LimitedChunkSource { bytes: b"abcdefghi", pos: 0, chunk_size: 3 };
        let mut pump = Pump::new(PassThrough);
        let mut buf = [0u8; 9];

        let written =
            pump_read(&mut pump, &mut source, &mut buf, ReadGranularity::SingleRead).unwrap();

        assert_eq!(written, 3);
        assert_eq!(&buf[..3], b"abc");
        assert_eq!(source.pos, 3);
    }

    #[test]
    fn fill_buffer_coalesces_every_source_pull() {
        // Same source/buffer as `single_read_returns_after_one_source_pull`,
        // but under the default granularity: one `pump_read` call
        // drains the whole source across multiple `chunk()` pulls.
        let mut source = LimitedChunkSource { bytes: b"abcdefghi", pos: 0, chunk_size: 3 };
        let mut pump = Pump::new(PassThrough);
        let mut buf = [0u8; 9];

        let written =
            pump_read(&mut pump, &mut source, &mut buf, ReadGranularity::FillBuffer).unwrap();

        assert_eq!(written, 9);
        assert_eq!(&buf, b"abcdefghi");
        assert_eq!(source.pos, 9);
    }
}
