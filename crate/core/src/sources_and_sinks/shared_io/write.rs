// The functions here are trivial.
// - Technical goal: a caller forwards execution to one of them, so
//   that the body of a caller (a `Write::write`/`finish`/`flush`
//   method) is only one line.
// - Reason: implementors of an io backend don't need to learn the
//   gory details of what to call in which order, and don't need to
//   copy-paste the drain/finalize/sync sequencing across backends.

use core::convert::Infallible;

use crate::sources_and_sinks::slice::SliceSource;
use crate::stream::{Pump, PumpDrain};
use crate::{Codec, DriveError, Sink};

/// Drive `pump` from `buf`, writing transformed bytes into `output`.
/// The transport-independent core of a `Write::write` impl.
/// Returns the number of bytes consumed from `buf`.
pub fn pump_write<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
    buf: &[u8],
) -> Result<usize, DriveError<Infallible, O::Error>> {
    let mut input = SliceSource::new(buf);
    // FIXME: A commit error can occur after input was consumed.
    // Track Write::write failure handling: https://github.com/olpa/rust-codecs/issues/20
    pump.transfer_from(&mut input, output)?;
    Ok(input.consumed())
}

/// Drain `pump`'s trailing output into `output`, then finalize
/// `output` itself. The transport-independent core of a `finish`
/// method that consumes the wrapper and hands back its endpoint.
pub fn pump_finish<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
) -> Result<(), DriveError<Infallible, O::Error>> {
    match pump.finish_to(output)? {
        PumpDrain::Done { .. } => {
            output.finish().map_err(DriveError::Sink)?;
            Ok(())
        }
        PumpDrain::SinkExhausted { .. } => Err(DriveError::SinkExhausted),
    }
}

/// Flush the output endpoint.
pub fn pump_flush<O: Sink>(output: &mut O) -> Result<(), DriveError<Infallible, O::Error>> {
    output.flush().map_err(DriveError::Sink)
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;

    use super::super::sink::ScratchSink;
    use super::super::test_support::{EmitsTrailerOnFinish, SliceWriter};
    use super::{pump_finish, pump_flush, pump_write};
    use crate::sources_and_sinks::slice::SliceSink;
    use crate::stream::Pump;
    use crate::{Codec, DrainCodec, DrainProgress, DriveError, Error, Progress};

    #[test]
    fn reports_sink_exhausted_instead_of_silently_truncating_the_trailer() {
        let mut pump = Pump::new(EmitsTrailerOnFinish::new(b"YQ=="));
        let mut buf = [0u8; 1];
        let mut sink = SliceSink::new(&mut buf);

        // Regression: `pump_finish` discarded `finish_to`'s result
        // and always returned `Ok(())`. The trailer is 4 bytes, the
        // sink holds 1. Draining did not finish. `pump_finish` must
        // report that, not return success.
        let result = pump_finish(&mut pump, &mut sink);
        assert!(matches!(result, Err(DriveError::SinkExhausted)));
    }

    /// A codec that holds output:
    /// - For each input byte, adds `HOLD` of 'X' to hold.
    /// - Each call releases one held 'X', if any.
    ///
    /// To simplify, requires exactly 1 byte of input and 1 byte of
    /// output per call.
    #[derive(Default)]
    struct HoldsProducedOutput {
        hold_left: usize,
    }

    const HOLD: usize = 3;

    impl DrainCodec for HoldsProducedOutput {
        // The test never calls `finish`. A stub is enough to satisfy
        // `Codec: DrainCodec`.
        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl Codec for HoldsProducedOutput {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            debug_assert_eq!(input.len(), 1);
            debug_assert_eq!(output.len(), 1);
            if self.hold_left > 0 {
                output[0].write(b'X');
                self.hold_left -= 1;
                return Ok(Progress::OutputFilled { consumed: 0 });
            }
            self.hold_left = HOLD;
            Ok(Progress::InputConsumed { written: 0 })
        }
    }

    #[test]
    fn flush_delivers_already_produced_output_without_finalizing_the_codec() {
        let mut pump = Pump::new(HoldsProducedOutput::default());
        let mut bytes = *b"aaaaaaaa";
        {
            let mut sink = ScratchSink::new(SliceWriter::new(&mut bytes), [0u8; 1]).unwrap();
            pump_write(&mut pump, &mut sink, b"a").unwrap();

            // Regression: `pump_flush` only flushes the transport.
            // It must also drain the codec's held output, without
            // finalizing it.
            pump_flush(&mut sink).unwrap();
        }
        assert_eq!(bytes, *b"XXXaaaaa");
    }
}
