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
    use super::pump_finish;
    use super::super::test_support::EmitsTrailerOnFinish;
    use crate::sources_and_sinks::slice::SliceSink;
    use crate::stream::Pump;
    use crate::DriveError;

    #[test]
    fn reports_sink_exhausted_instead_of_silently_truncating_the_trailer() {
        let mut pump = Pump::new(EmitsTrailerOnFinish::new(b"YQ=="));
        let mut buf = [0u8; 1];
        let mut sink = SliceSink::new(&mut buf);

        // Regression: `pump_finish` used to discard `finish_to`'s
        // `PumpDrain` and always return `Ok(())`. The trailer is 4
        // bytes ("YQ=="), but the sink holds only 1, so draining did
        // not complete — `pump_finish` must report that, not silently
        // succeed after writing only "Y".
        let result = pump_finish(&mut pump, &mut sink);
        assert!(matches!(result, Err(DriveError::SinkExhausted)));
    }
}
