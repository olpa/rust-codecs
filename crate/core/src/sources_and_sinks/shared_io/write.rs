// The functions here are trivial.
// - Technical goal: a caller forwards execution to one of them, so
//   that the codec part of a caller (a `Write::write`/`finish`/`flush`
//   method) is only one line.
// - Reason: implementors of an io backend don't need to learn the
//   gory details of what to call in which order, and don't need to
//   copy-paste the drain/finalize sequencing across backends.

use core::convert::Infallible;

use crate::sources_and_sinks::slice::SliceSource;
use crate::stream::{Pump, PumpDrain};
use crate::{Codec, DriveError, DriveErrorKind, Sink};

/// Drive `pump` from `buf`, writing transformed bytes into `output`.
/// Returns the number of bytes consumed from `buf`.
///
/// This is the transport-independent core of a `Write::write` impl.
///
/// The `Write` contract says: if `write` returns an error, it took no
/// bytes of `buf`. Therefore, if an error happens after the function
/// took bytes of `buf`, then `pump_write` returns the number of bytes
/// taken, and reports the error on the next call. (FIXME: Only codec
/// errors work this way now. The comment in the code lists the other
/// errors.)
pub fn pump_write<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
    buf: &[u8],
) -> Result<usize, DriveError<Infallible, O::Error>> {
    let mut input = SliceSource::new(buf);
    // FIXME: `write` can still return `Err` after it took bytes from
    // `buf`. Only codec errors are latched. These errors are not fixed yet:
    // - A sink error (`spare` or `commit`) after an earlier step
    //   committed output.
    // - A failed commit. The codec has consumed the input of that step.
    //   A retry duplicates output.
    // - `NoProgress` after an earlier step moved bytes.
    // Track Write::write failure handling: https://github.com/olpa/rust-codecs/issues/20
    match pump.transfer_from(&mut input, output) {
        Ok(_) => {}
        Err(error) if input.consumed() > 0 && matches!(error.kind, DriveErrorKind::Codec(_)) => {
            // `Pump::transfer_step` latched the error. The next call returns it.
        }
        Err(error) => return Err(error),
    }
    Ok(input.consumed())
}

/// Flush the codec into `output`. The codec decides how much of its
/// held output to write. The codec stream does not end.
///
/// This is the transport-independent core of a `Write::flush` impl.
///
/// # Errors
///
/// - If `output` is full before the codec is done, the function
///   returns `DriveErrorKind::SinkExhausted`.
/// - The function returns all errors from `pump` and from `output`
///   without change.
pub fn pump_flush<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
) -> Result<(), DriveError<Infallible, O::Error>> {
    match pump.flush_to(output)? {
        PumpDrain::Done => Ok(()),
        PumpDrain::SinkExhausted => Err(pump.drive_error(DriveErrorKind::SinkExhausted)),
    }
}

/// Drain all the bytes that the codec must still write into
/// `output`. The codec stream ends.
///
/// This is the transport-independent core of a `finish` method.
/// That method consumes the wrapper and gives back its endpoint.
///
/// # Errors
///
/// - If `output` is full before the codec is done, the function
///   returns `DriveErrorKind::SinkExhausted`.
/// - The function returns all errors from `pump` and from `output`
///   without change.
pub fn pump_finish<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
) -> Result<(), DriveError<Infallible, O::Error>> {
    match pump.finish_to(output)? {
        PumpDrain::Done => Ok(()),
        PumpDrain::SinkExhausted => Err(pump.drive_error(DriveErrorKind::SinkExhausted)),
    }
}

#[cfg(test)]
mod tests {
    use super::{pump_finish, pump_flush, pump_write};
    use crate::codecs::test_support::HoldsOutput;
    use crate::sources_and_sinks::slice::SliceSink;
    use crate::stream::Pump;
    use crate::{DriveErrorKind, TransferCounts};

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
        let error = result.unwrap_err();
        assert!(matches!(error.kind, DriveErrorKind::SinkExhausted));
        assert_eq!(
            error.moved,
            TransferCounts {
                consumed: 0,
                written: 1,
            }
        );
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

            // `pump_flush` must drain the codec's held output,
            // without finalizing it.
            pump_flush(&mut pump, &mut sink).unwrap();
        }
        assert_eq!(bytes, *b"XXXaaaaa");
    }

    // The codec passes the first 4 bytes and fails on the next ones. The
    // sink is too small for all of `buf`, so the first step commits its
    // 4 bytes before the second step fails. A `Write::write` that
    // returns an error must have taken no bytes. So `pump_write` must
    // report the progress as `Ok(4)`. The latched error comes on the
    // next call.
    #[test]
    fn a_codec_error_after_progress_is_reported_by_the_next_call() {
        use crate::codecs::test_support::FailsAfterInner;
        use crate::identity::identity;

        let mut pump = Pump::new(FailsAfterInner {
            inner: identity(),
            ok_calls: 1,
            calls: 0,
        });
        let mut bytes = [0u8; 4];
        let mut sink = SliceSink::new(&mut bytes);

        let first = pump_write(&mut pump, &mut sink, b"01234567");
        assert_eq!(first.unwrap(), 4);

        let second = pump_write(&mut pump, &mut sink, b"4567").unwrap_err();
        assert!(matches!(second.kind, DriveErrorKind::Codec(_)));
    }

    // The codec passes three 4-byte chunks and fails on the fourth. The
    // failing call moves its 4 bytes too. The error must count the
    // whole stream: 16 bytes in and 16 out, not only the last write. A
    // repeat of the call gives the same totals.
    #[test]
    fn a_codec_error_counts_the_whole_stream() {
        use crate::codecs::test_support::FailsAfterInner;
        use crate::identity::identity;

        let mut pump = Pump::new(FailsAfterInner {
            inner: identity(),
            ok_calls: 3,
            calls: 0,
        });
        let mut bytes = [0u8; 16];
        let mut sink = SliceSink::new(&mut bytes);

        let mut error = None;
        for piece in b"01234567890123456789".chunks(4) {
            if let Err(e) = pump_write(&mut pump, &mut sink, piece) {
                error = Some(e);
                break;
            }
        }

        let error = error.expect("the fourth piece must fail");
        assert!(matches!(error.kind, DriveErrorKind::Codec(_)));
        assert_eq!(
            error.moved,
            TransferCounts {
                consumed: 16,
                written: 16,
            }
        );

        let again = pump_write(&mut pump, &mut sink, b"6789").unwrap_err();
        assert!(matches!(again.kind, DriveErrorKind::Codec(_)));
        assert_eq!(again.moved, error.moved);
    }
}
