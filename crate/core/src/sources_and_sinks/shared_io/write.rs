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
/// Returns the number of bytes consumed from `buf`.
///
/// This is the transport-independent core of a `Write::write` impl.
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

/// Drain the bytes that the codec produced but did not write yet
/// into `output`. Then flush `output`. The codec stream does not
/// end.
///
/// This is the transport-independent core of a `Write::flush` impl.
///
/// # Errors
///
/// - If `output` is full before the codec is done, the function
///   returns `DriveError::SinkExhausted`.
/// - The function returns all errors from `pump` and from `output`
///   without change.
pub fn pump_flush<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
) -> Result<(), DriveError<Infallible, O::Error>> {
    match pump.flush_to(output)? {
        PumpDrain::Done { .. } => output.flush().map_err(DriveError::Sink),
        PumpDrain::SinkExhausted { .. } => Err(DriveError::SinkExhausted),
    }
}

/// Drain all the bytes that the codec must still write into
/// `output`. Then finish `output`. The codec stream ends. The
/// function does not close `output`.
///
/// This is the transport-independent core of a `finish` method.
/// That method consumes the wrapper and gives back its endpoint.
///
/// # Errors
///
/// - If `output` is full before the codec is done, the function
///   returns `DriveError::SinkExhausted`.
/// - The function returns all errors from `pump` and from `output`
///   without change.
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

#[cfg(test)]
mod tests {
    use super::{pump_finish, pump_flush, pump_write};
    use crate::codecs::test_support::HoldsOutput;
    use crate::sources_and_sinks::slice::SliceSink;
    use crate::stream::Pump;
    use crate::DriveError;

    #[test]
    fn reports_sink_exhausted_instead_of_silently_truncating_the_trailer() {
        let mut pump = Pump::new(HoldsOutput {
            trailer: b"YQ==",
            ..Default::default()
        });
        let mut buf = [0u8; 1];
        let mut sink = SliceSink::new(&mut buf);

        // Regression: `pump_finish` discarded `finish_to`'s result
        // and always returned `Ok(())`. The trailer is 4 bytes, the
        // sink holds 1. Draining did not finish. `pump_finish` must
        // report that, not return success.
        let result = pump_finish(&mut pump, &mut sink);
        assert!(matches!(result, Err(DriveError::SinkExhausted)));
    }

    #[test]
    fn flush_delivers_already_produced_output_without_finalizing_the_codec() {
        let mut pump = Pump::new(HoldsOutput {
            per_input: 3,
            trailer: b"F",
            ..Default::default()
        });
        let mut bytes = *b"aaaaaaaa";
        let (consumed, written) = {
            let mut sink = SliceSink::new(&mut bytes);
            let consumed = pump_write(&mut pump, &mut sink, b"a").unwrap();
            (consumed, sink.written())
        };
        // The codec holds its output, so the write delivers nothing
        // yet. If not, the flush below proves nothing.
        assert_eq!(consumed, 1);
        assert_eq!(written, 0);
        assert_eq!(bytes, *b"aaaaaaaa");

        {
            // The write delivered no bytes, so a new sink can start
            // at the same position.
            let mut sink = SliceSink::new(&mut bytes);

            // Regression: `pump_flush` only flushed the transport.
            // It must also drain the codec's held output, without
            // finalizing it.
            pump_flush(&mut pump, &mut sink).unwrap();
        }
        assert_eq!(bytes, *b"XXXaaaaa");
    }
}
