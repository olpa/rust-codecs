use core::convert::Infallible;

use crate::pump::{DrainEnd, Pump, PumpEnd};
use crate::sources_and_sinks::slice::SliceSink;
use crate::{DriveError, Source, TerminatingCodec};

/// Drive `pump` against `input`, filling `buf` with transformed bytes —
/// the transport-independent core of a `Read::read` impl, matching
/// what `std_io`/`embedded_io`'s own `CodecReader` calls internally.
/// Ends the codec's stream (running `finish`) once `input` reports
/// exhaustion, so the caller never has to invoke `finish` itself.
pub fn pump_read<I: Source, C: TerminatingCodec>(
    pump: &mut Pump<C>,
    input: &mut I,
    buf: &mut [u8],
) -> Result<usize, DriveError<I::Error, Infallible>> {
    if buf.is_empty() || pump.is_done() {
        return Ok(0);
    }
    let mut output = SliceSink::new(buf);
    let moved = pump.transfer_from(input, &mut output)?;
    if moved.end == PumpEnd::SourceExhausted {
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
    use super::pump_read;
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::{Codec, Drain, DrainCodec, Error, Progress, Pump};

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
}
