use core::convert::Infallible;

use crate::sources_and_sinks::slice::SliceSink;
use crate::stream::{Pump, PumpDrainEnd, PumpStepEnd};
use crate::{DriveError, EndSignallingCodec, Source};

/// Drive `pump` against `input`, filling `buf` with transformed bytes —
/// the transport-independent core of a `Read::read` impl.
///
/// - Returns as soon as one pull from `input` yields output, instead of
///   chasing a full `buf` — so `read()` never blocks past what a
///   single pull from `input` already blocked on.
/// - Loops past a step that pulls input but produces no output yet (a
///   codec buffering several input bytes before it can emit anything,
///   e.g. `base64_dec`): counting that as a stopping point would make
///   `read()` return `Ok(0)`, which callers would misread as EOF
///   before the stream has genuinely ended.
/// - Ends the codec's stream (running `finish`) once `input` reports
///   exhaustion. It's an extra responsibility, but there is no good
///   way to let the caller do so.
pub fn end_signalling_pump_read<I: Source, C: EndSignallingCodec>(
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
/// as `transfer_from`'s); widen it to line up with `end_signalling_pump_read`'s
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
    use core::mem::MaybeUninit;

    use core::convert::Infallible;

    use super::end_signalling_pump_read;
    use crate::identity::identity;
    use crate::{
        Codec, Drain, DrainCodec, EndSignallingCodec, EndSignallingProgress, Error, Progress, Pump,
        Source,
    };

    /// A `Source` over a byte slice, yielding it in fixed-size pulls
    /// no matter how much of `bytes` remains — stands in for a wrapped
    /// reader whose own `read()` returns in bounded pieces (like a
    /// terminal line at a time).
    struct ChunkedSource<'a> {
        bytes: &'a [u8],
        pos: usize,
        chunk_size: usize,
    }

    impl Source for ChunkedSource<'_> {
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
    fn smoke_test() {
        let mut source = ChunkedSource {
            bytes: b"ok",
            pos: 0,
            chunk_size: 1,
        };
        let mut pump = Pump::new(identity());
        let mut buf = [0u8; 8];

        let mut read = || {
            let n = end_signalling_pump_read(&mut pump, &mut source, &mut buf).unwrap();
            buf[..n].to_vec()
        };

        assert_eq!(read(), b"o");
        assert_eq!(read(), b"k");
        assert_eq!(read(), b"");
        assert_eq!(read(), b"");
        assert_eq!(read(), b"");
    }

    /// Buffers one byte silently, then emits both held bytes together
    /// on the next `process` call. It helps to test the interaction
    /// when the codec must consume several input bytes before it can
    /// produce any output.
    ///
    /// To simplify the codec logic, the input buffer must be always
    /// exactly one byte.
    #[derive(Default)]
    struct CanProgressWithoutOutput {
        held: Option<u8>,
    }

    impl DrainCodec for CanProgressWithoutOutput {
        fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }

        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
            match self.held.take() {
                None => Ok(Drain::Done { written: 0 }),
                Some(held) => {
                    output[0].write(held);
                    Ok(Drain::Done { written: 1 })
                }
            }
        }
    }

    impl Codec for CanProgressWithoutOutput {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            debug_assert_eq!(input.len(), 1);
            match self.held.take() {
                None => {
                    self.held = Some(input[0]);
                    Ok(Progress::InputConsumed { written: 0 })
                }
                Some(held) => {
                    output[0].write(held);
                    output[1].write(input[0]);
                    Ok(Progress::InputConsumed { written: 2 })
                }
            }
        }
    }

    #[test]
    fn loops_past_a_step_that_consumes_input_but_writes_nothing_yet() {
        let mut source = ChunkedSource {
            bytes: b"ok",
            pos: 0,
            chunk_size: 1,
        };
        let mut pump = Pump::new(CanProgressWithoutOutput::default());
        let mut buf = [0u8; 8];

        let mut read = || {
            let n = end_signalling_pump_read(&mut pump, &mut source, &mut buf).unwrap();
            buf[..n].to_vec()
        };

        // The first pull only consumes 'o' and writes nothing — a
        // buggy `end_signalling_pump_read` that stopped there would
        // return `b""` here instead of looping to the next pull.
        assert_eq!(read(), b"ok");
        assert_eq!(read(), b"");
        assert_eq!(read(), b"");
    }

    #[test]
    fn drains_pending_codec_state_before_reporting_done() {
        let mut source = ChunkedSource {
            bytes: b"odd",
            pos: 0,
            chunk_size: 1,
        };
        let mut pump = Pump::new(CanProgressWithoutOutput::default());
        let mut buf = [0u8; 8];

        let mut read = || {
            let n = end_signalling_pump_read(&mut pump, &mut source, &mut buf).unwrap();
            buf[..n].to_vec()
        };

        // 'o' and 'd' pair up and come out together; the trailing 'd'
        // is left held when the source is exhausted, so `finish` must
        // flush it instead of silently dropping it.
        assert_eq!(read(), b"od");
        assert_eq!(read(), b"d");
        assert_eq!(read(), b"");
    }

    /// A codec with nothing to process, only a multi-byte trailer to
    /// emit from `finish` — stands in for a format whose finalization
    /// (a checksum, a footer) is bigger than one `read()` buffer.
    struct EmitsTrailerOnFinish {
        position: usize,
    }

    impl DrainCodec for EmitsTrailerOnFinish {
        fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }

        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
            const TRAILER: &[u8] = b"final";
            let n = (TRAILER.len() - self.position).min(output.len());
            output[..n].write_copy_of_slice(&TRAILER[self.position..self.position + n]);
            self.position += n;
            if self.position == TRAILER.len() {
                Ok(Drain::Done { written: n })
            } else {
                Ok(Drain::OutputFilled)
            }
        }
    }

    impl Codec for EmitsTrailerOnFinish {
        fn process(
            &mut self,
            _input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            Ok(Progress::InputConsumed { written: 0 })
        }
    }

    #[test]
    fn resumes_a_partial_finish_on_the_next_read() {
        use crate::sources_and_sinks::slice::SliceSource;

        let mut source = SliceSource::new(b"");
        let mut pump = Pump::new(EmitsTrailerOnFinish { position: 0 });
        let mut buf = [0u8; 2];

        let mut read = || {
            let n = end_signalling_pump_read(&mut pump, &mut source, &mut buf).unwrap();
            buf[..n].to_vec()
        };

        // Each call hits `SinkExhausted` (the 2-byte buffer can't hold
        // all of "final" at once) before the trailer is fully drained
        // — the next `read` must pick up where the last one left off,
        // not restart or skip ahead.
        assert_eq!(read(), b"fi");
        assert_eq!(read(), b"na");
        assert_eq!(read(), b"l");
        assert_eq!(read(), b"");
        assert_eq!(read(), b"");
    }

    /// An `EndSignallingCodec` that copies bytes 1:1 but ends its stream
    /// after `limit` bytes, like a self-describing format with an
    /// in-band terminator.
    struct EarlyEnd {
        limit: usize,
        done: usize,
    }

    impl DrainCodec for EarlyEnd {
        fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }

        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    impl EndSignallingCodec for EarlyEnd {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<EndSignallingProgress, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].write_copy_of_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(EndSignallingProgress::End {
                    consumed: n,
                    written: n,
                })
            } else if n == input.len() {
                Ok(EndSignallingProgress::InputConsumed { written: n })
            } else {
                Ok(EndSignallingProgress::OutputFilled { consumed: n })
            }
        }
    }

    #[test]
    fn stops_at_in_band_end_without_touching_the_source_again() {
        use crate::sources_and_sinks::slice::SliceSource;

        let mut source = SliceSource::new(b"Hello World");
        let mut pump = Pump::new(EarlyEnd { limit: 3, done: 0 });
        let mut buf = [0u8; 8];

        let n = end_signalling_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"Hel");

        // `pump.is_done()` must short-circuit before ever calling
        // `source.chunk()` again — if it didn't, `source.consumed()`
        // would advance past 3.
        let n = end_signalling_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(n, 0);
        assert_eq!(source.consumed(), 3);
    }
}
