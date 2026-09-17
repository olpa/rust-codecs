// The functions here are trivial.
// - Technical goal: a caller forwards execution to one of them, so
//   that the body of a caller (a `Write::write`/`finish`/`flush`
//   method) is only one line.
// - Reason: implementors of an io backend don't need to learn the
//   gory details of what to call in which order, and don't need to
//   copy-paste the drain/finalize/sync sequencing across backends.

use core::convert::Infallible;

use crate::sources_and_sinks::slice::SliceSource;
use crate::stream::Pump;
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
    pump.finish_to(output)?;
    output.finish().map_err(DriveError::Sink)?;
    Ok(())
}

/// Flush the output endpoint.
pub fn pump_flush<O: Sink>(output: &mut O) -> Result<(), DriveError<Infallible, O::Error>> {
    output.flush().map_err(DriveError::Sink)
}
