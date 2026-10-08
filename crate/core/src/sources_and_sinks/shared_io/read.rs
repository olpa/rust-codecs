use core::convert::Infallible;

use crate::sources_and_sinks::slice::SliceSink;
use crate::stream::{Pump, PumpTransfer};
use crate::{BoundaryAwareCodec, DriveError, DriveErrorKind, Source, TransferCounts};

/// This is the transport-independent core of a `Read::read`
/// implementation. In the normal case, one pull from `input` makes
/// the codec produce output.
///
/// If the codec produces no output, the function pulls again.
/// It repeats until the codec produces output, or `input` ends.
/// Rationale: `Ok(0)` here would wrongly signal EOF to the caller.
///
/// Once `input` ends, or the codec reports an in-band end, this
/// function also ends the codec's stream, by running `finish`. This
/// is an extra responsibility for this function. Rationale: No good
/// way exists to let the caller do this instead.
///
/// A codec error after some bytes reached `buf` gives `Ok(n)`. The
/// error comes on the next call. Rationale: `Read::read` must not
/// return `Err` after it read bytes, and POSIX `read(2)` works the
/// same way. `Pump` latches the error, so every later call returns
/// it.
pub fn boundary_aware_pump_read<I: Source, C: BoundaryAwareCodec>(
    pump: &mut Pump<C>,
    input: &mut I,
    buf: &mut [u8],
) -> Result<usize, DriveError<I::Error, Infallible>> {
    if let Some(error) = pump.failure() {
        return Err(DriveError::new(
            DriveErrorKind::Codec(error),
            TransferCounts::default(),
        ));
    }
    if buf.is_empty() || pump.is_done() {
        return Ok(0);
    }
    let mut output = SliceSink::new(buf);
    match read_into(pump, input, &mut output) {
        Ok(()) => Ok(output.written()),
        Err(DriveError {
            kind: DriveErrorKind::Codec(_),
            ..
        }) if output.written() > 0 => Ok(output.written()),
        Err(error) => Err(error),
    }
}

/// The body of [`boundary_aware_pump_read`], without the deferred
/// codec error.
fn read_into<I: Source, C: BoundaryAwareCodec>(
    pump: &mut Pump<C>,
    input: &mut I,
    output: &mut SliceSink<'_>,
) -> Result<(), DriveError<I::Error, Infallible>> {
    let mut moved = TransferCounts::default();
    let input_ended = pump.is_finishing()
        || loop {
            let step = pump
                .transfer_step(input, output)
                .map_err(|error| error.after(moved))?;
            let (PumpTransfer::SourceExhausted(step_moved)
            | PumpTransfer::SinkExhausted(step_moved)
            | PumpTransfer::Progressed(step_moved)
            | PumpTransfer::InputEnded(step_moved)) = step;
            moved.consumed += step_moved.consumed;
            moved.written += step_moved.written;
            match step {
                PumpTransfer::SourceExhausted(_) | PumpTransfer::InputEnded(_) => break true,
                PumpTransfer::Progressed(_) if output.written() == 0 => {}
                PumpTransfer::Progressed(_) | PumpTransfer::SinkExhausted(_) => break false,
            }
        };
    if input_ended {
        // Filling this caller-provided read buffer is normal partial-read
        // progress, not an I/O failure. `finish_to` records that condition
        // in its successful result so the next `read` can resume finalizing.
        pump.finish_to(output)
            .map_err(|error| error.widen_source().after(moved))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use core::convert::Infallible;

    use super::boundary_aware_pump_read;
    use crate::identity::identity;
    use crate::{Pump, Source};

    /// A `Source` over a byte slice, yielding it in fixed-size pulls
    /// no matter how much of `bytes` remains — stands in for a wrapped
    /// reader whose own `read()` returns in bounded pieces (like a
    /// terminal line at a time).
    struct ChunkedSource<'a> {
        bytes: &'a [u8],
        pos: usize,
        chunk_size: usize,
        count_chunk_calls: usize,
    }

    impl Source for ChunkedSource<'_> {
        type Error = Infallible;

        fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
            self.count_chunk_calls += 1;
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
            count_chunk_calls: 0,
        };
        let mut pump = Pump::new(identity());
        let mut buf = [0u8; 8];

        let mut read = || {
            let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
            buf[..n].to_vec()
        };

        assert_eq!(read(), b"o");
        assert_eq!(read(), b"k");
        assert_eq!(read(), b"");
        assert_eq!(read(), b"");
        assert_eq!(read(), b"");
    }

    #[test]
    fn loops_past_a_step_that_consumes_input_but_writes_nothing_yet() {
        use crate::codecs::test_support::HoldsOutput;

        let mut source = ChunkedSource {
            bytes: b"ok",
            pos: 0,
            chunk_size: 1,
            count_chunk_calls: 0,
        };
        // Each input byte makes the codec hold one 'X'. It releases the
        // 'X' on the next call.
        let mut pump = Pump::new(HoldsOutput {
            per_input: 1,
            ..Default::default()
        });
        let mut buf = [0u8; 8];

        let mut read = || {
            let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
            buf[..n].to_vec()
        };

        // The first pull only consumes 'o' and writes nothing — a
        // buggy `boundary_aware_pump_read` that stopped there would
        // return `b""` here instead of looping to the next pull.
        assert_eq!(read(), b"X");
        assert_eq!(read(), b"X");
        assert_eq!(read(), b"");
    }

    #[test]
    fn drains_pending_codec_state_before_reporting_done() {
        use crate::codecs::test_support::HoldsOutput;

        let mut source = ChunkedSource {
            bytes: b"odd",
            pos: 0,
            chunk_size: 1,
            count_chunk_calls: 0,
        };
        let mut pump = Pump::new(HoldsOutput {
            per_input: 1,
            ..Default::default()
        });
        let mut buf = [0u8; 8];

        let mut read = || {
            let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
            buf[..n].to_vec()
        };

        // The codec releases one 'X' per call, one call late. When the
        // source is exhausted, it still holds the 'X' for the last
        // 'd'. `finish` must write it, not silently drop it.
        assert_eq!(read(), b"X");
        assert_eq!(read(), b"X");
        assert_eq!(read(), b"X");
        assert_eq!(read(), b"");
    }

    #[test]
    fn resumes_a_partial_finish_on_the_next_read() {
        use crate::codecs::test_support::HoldsOutput;
        use crate::sources_and_sinks::slice::SliceSource;

        let mut source = SliceSource::new(b"");
        let mut pump = Pump::new(HoldsOutput {
            trailer: b"final",
            ..Default::default()
        });
        let mut buf = [0u8; 2];

        let mut read = || {
            let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
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

    #[test]
    fn resumes_a_partial_finish_without_polling_the_source_again() {
        use crate::codecs::test_support::HoldsOutput;

        let mut source = ChunkedSource {
            bytes: b"x",
            pos: 0,
            chunk_size: 8,
            count_chunk_calls: 0,
        };
        let mut pump = Pump::new(HoldsOutput {
            trailer: b"final",
            ..Default::default()
        });
        let mut buf = [0u8; 2];

        // The 2-byte buffer can't hold all of "final" at once, so the
        // first read only starts draining. By then the source has
        // already reported exhaustion once. A buggy implementation
        // that forgets that would poll it again on each following
        // call.
        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"fi");
        let chunk_calls_once_exhausted = source.count_chunk_calls;

        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"na");
        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"l");
        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(n, 0);
        assert_eq!(source.count_chunk_calls, chunk_calls_once_exhausted);
    }

    #[test]
    fn drains_the_tail_after_an_in_band_end() {
        use crate::codecs::test_support::{EndsAtBar, HoldsOutput};

        let mut source = ChunkedSource {
            bytes: b"ab|cd",
            pos: 0,
            chunk_size: 8,
            count_chunk_calls: 0,
        };
        // For the input "ab", `HoldsOutput` holds "XX". At the in-band
        // end, it did not write "XX" and the trailer yet.
        let mut pump = Pump::new(EndsAtBar {
            inner: HoldsOutput {
                per_input: 1,
                trailer: b"TAIL",
                ..Default::default()
            },
            ..Default::default()
        });
        let mut buf = [0u8; 2];

        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"XX");
        let chunk_calls_at_the_end = source.count_chunk_calls;

        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"TA");
        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"IL");
        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(n, 0);
        let finish_calls_at_eof = pump.get_ref().finish_calls;
        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(n, 0);
        assert_eq!(
            pump.get_ref().finish_calls,
            finish_calls_at_eof,
            "a read after EOF must not call `finish` again"
        );

        assert_eq!(source.pos, 3, "bytes after the marker stay in the source");
        assert_eq!(
            source.count_chunk_calls, chunk_calls_at_the_end,
            "the source must not be polled after the in-band end"
        );
    }

    #[test]
    fn returns_the_bytes_before_a_process_error_then_the_error() {
        use crate::codecs::test_support::{assert_latched, FailsAfterInner, HoldsOutput};
        use crate::sources_and_sinks::slice::SliceSource;

        let mut source = SliceSource::new(b"abc");
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                held: 2,
                ..Default::default()
            },
            ..Default::default()
        });
        let mut buf = [0u8; 8];

        // `Read::read` must not return `Err` after it read bytes, so
        // the bytes come first.
        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"XX");

        for _ in 0..2 {
            let error = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap_err();
            assert_latched(&error.kind);
        }
        // The latched error comes before the empty-buffer `Ok(0)`.
        let error = boundary_aware_pump_read(&mut pump, &mut source, &mut []).unwrap_err();
        assert_latched(&error.kind);
        assert_eq!(pump.get_ref().calls, 1);
    }

    #[test]
    fn returns_the_bytes_before_a_finish_error_then_the_error() {
        use crate::codecs::test_support::{FailsAfterInner, HoldsOutput};
        use crate::sources_and_sinks::slice::SliceSource;
        use crate::{DriveErrorKind, Error, ErrorKind};

        let mut source = SliceSource::new(b"");
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                trailer: b"!",
                ..Default::default()
            },
            ..Default::default()
        });
        let mut buf = [0u8; 8];

        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"!");

        // A buggy read would call `finish` again, because the pump is
        // still in the finishing mode.
        let error = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap_err();
        assert!(matches!(
            error.kind,
            DriveErrorKind::Codec(e) if e == Error::new(ErrorKind::CorruptStream, 0, 0)
        ));
        assert_eq!(pump.get_ref().calls, 1);
    }

    #[test]
    fn a_source_error_is_not_latched() {
        use crate::sources_and_sinks::slice::SliceSource;
        use crate::DriveErrorKind;

        /// Fails the first `chunk` call, like a non-blocking reader
        /// with no data yet. Then delegates to `inner`.
        struct FailsOnce<'a> {
            inner: SliceSource<'a>,
            failed: bool,
        }

        impl Source for FailsOnce<'_> {
            type Error = ();

            fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
                if !self.failed {
                    self.failed = true;
                    return Err(());
                }
                self.inner.chunk().map_err(|never| match never {})
            }

            fn consume(&mut self, amount: usize) {
                self.inner.consume(amount);
            }
        }

        let mut source = FailsOnce {
            inner: SliceSource::new(b"ok"),
            failed: false,
        };
        let mut pump = Pump::new(identity());
        let mut buf = [0u8; 8];

        let error = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap_err();
        assert!(matches!(error.kind, DriveErrorKind::Source(())));
        let n = boundary_aware_pump_read(&mut pump, &mut source, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"ok");
    }
}
