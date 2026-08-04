//! Bufferless codec lifecycle shared by directional frontends.
//!
//! Endpoint adapters retain the buffers dictated by their direction:
//! readers own input storage, writers own output storage, and chunked
//! frontends lend both current windows. This driver owns only codec
//! lifecycle, so using it never introduces a byte copy.

use crate::transfer::{transfer, ProgressStep};
use crate::{Codec, Drain, DriveError, Error, Sink, Source};

/// Exact progress and boundary of one validated `finish` or `flush`
/// call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainStep {
    pub(crate) written: usize,
    pub(crate) end: DrainEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainEnd {
    SinkExhausted,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpTransfer {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: PumpEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpEnd {
    SourceExhausted,
    SinkExhausted,
    StreamEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpDrain {
    pub(crate) written: usize,
    pub(crate) end: DrainEnd,
}

/// Which of the codec's two draining operations `drain_to` should
/// call: `Finish` permanently ends the stream, `Flush` may be
/// followed by further `process` calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainKind {
    Finish,
    Flush,
}

impl DrainKind {
    fn drive<C: Codec>(self, driver: &mut Driver<C>, output: &mut [u8]) -> Result<DrainStep, Error> {
        match self {
            DrainKind::Finish => driver.finish(output),
            DrainKind::Flush => driver.flush(output),
        }
    }
}

/// A bufferless lifecycle wrapper around a codec.
pub(crate) struct Driver<C> {
    codec: C,
    done: bool,
}

impl<C: Codec> Driver<C> {
    pub(crate) fn new(codec: C) -> Self {
        Self { codec, done: false }
    }

    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    pub(crate) fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<ProgressStep, Error> {
        if self.done {
            return Ok(ProgressStep {
                consumed: 0,
                written: 0,
                end: crate::transfer::ProgressEnd::StreamEnd,
            });
        }
        let moved = transfer(&mut self.codec, input, output)?;
        if moved.end == crate::transfer::ProgressEnd::StreamEnd {
            self.done = true;
        }
        Ok(moved)
    }

    /// Drive the codec by repeatedly pulling chunks from `input` and
    /// pushing processed bytes into `output`, until the codec reaches
    /// its stream end or either endpoint has no more room/data to
    /// offer.
    ///
    /// Each loop iteration fetches one chunk from `input`, then one
    /// spare slice from `output`, calls [`Driver::process`] once, then
    /// feeds the result back (`input.consume`, `output.commit`).
    ///
    /// Returns once one of three things happens: the source is
    /// exhausted (`PumpEnd::SourceExhausted`), the sink has no more
    /// spare space (`PumpEnd::SinkExhausted`), or the codec itself
    /// signals the end of the stream (`PumpEnd::StreamEnd`). A call
    /// that moves zero bytes on both sides without ending the stream
    /// is a stall — the sink offered an empty (but `Some`) slice and
    /// the codec couldn't do anything with it — and is reported as
    /// `DriveError::NoProgress`, the same as a codec error, both of
    /// which abort the loop after committing zero bytes to the sink.
    pub(crate) fn transfer_from<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        let mut consumed = 0;
        let mut written = 0;
        loop {
            // Invariants at top of loop: `consumed`/`written` are the
            // committed totals from prior iterations only (this
            // iteration hasn't moved anything yet); the codec hasn't
            // reported `StreamEnd` (that returns immediately); neither
            // `input` nor `output` holds a chunk/spare left over from a
            // prior iteration (each iteration either resolves what it
            // borrowed via `process`, or commits 0 before returning).
            let Some(chunk) = input.chunk().map_err(DriveError::Source)? else {
                return Ok(PumpTransfer { consumed, written, end: PumpEnd::SourceExhausted });
            };
            let Some(spare) = output.spare().map_err(DriveError::Sink)? else {
                return Ok(PumpTransfer { consumed, written, end: PumpEnd::SinkExhausted });
            };
            // A call may make progress with an empty `spare` (e.g. a
            // codec that only ever consumes input, never writing
            // anything), so instead of rejecting an empty slice up
            // front, progress is judged as part of the call's own
            // result, right alongside the error case: zero bytes moved
            // on both sides, without ending the stream, means this
            // pair genuinely can't advance.
            let moved = match self.process(chunk, spare) {
                Ok(moved)
                    if moved.consumed > 0
                        || moved.written > 0
                        || moved.end == crate::transfer::ProgressEnd::StreamEnd =>
                {
                    moved
                }
                Ok(_) => {
                    output.commit(0).map_err(DriveError::Sink)?;
                    return Err(DriveError::NoProgress);
                }
                Err(error) => {
                    output.commit(0).map_err(DriveError::Sink)?;
                    return Err(DriveError::Codec(error));
                }
            };
            // `moved.consumed` may be less than `chunk.len()` (output
            // ran out first); the unconsumed remainder isn't lost —
            // it reappears (overlapping this chunk) on the next
            // `input.chunk()` call.
            input.consume(moved.consumed);
            output.commit(moved.written).map_err(DriveError::Sink)?;
            consumed += moved.consumed;
            written += moved.written;
            if moved.end == crate::transfer::ProgressEnd::StreamEnd {
                return Ok(PumpTransfer { consumed, written, end: PumpEnd::StreamEnd });
            }
        }
    }

    /// Drain the codec's trailing output by repeatedly calling
    /// [`Driver::finish`] against spare space taken from `output`,
    /// until the codec reports `Done`.
    ///
    /// This is the finalizing counterpart to `transfer_from`: it is
    /// called once the source is exhausted, to flush whatever bytes
    /// the codec still owes (e.g. block padding, trailers) with no
    /// further input. It shares its loop with `flush_to` via
    /// `drain_to(output, DrainKind::Finish)`, which selects `finish`
    /// (permanently ends the stream) over `flush` (may be called
    /// again later).
    pub(crate) fn finish_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainKind::Finish)
    }

    #[cfg_attr(not(any(feature = "std", feature = "embedded-io")), allow(dead_code))]
    pub(crate) fn flush_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainKind::Flush)
    }

    /// Shared loop behind `finish_to`/`flush_to`: repeatedly obtain
    /// spare space from `output` and hand it to [`Driver::finish`] (if
    /// `kind` is `DrainKind::Finish`) or [`Driver::flush`] (otherwise),
    /// committing what was written, until the codec reports `Done`.
    ///
    /// When `output.spare()` returns `None`, the sink has no room —
    /// but rather than immediately reporting `SinkExhausted`, one more
    /// `finish`/`flush` call is made against an empty slice (`&mut
    /// []`) to ask the codec whether it was actually done regardless
    /// (e.g. nothing left to write, or it errors/rejects the empty
    /// buffer). If that call reports `Done`, the drain is genuinely
    /// complete; otherwise it really is blocked on sink space, and
    /// `SinkExhausted` is returned. This avoids conflating "sink is
    /// full but the codec had nothing left anyway" with "sink is full
    /// and blocking real progress".
    ///
    /// A call that writes nothing and doesn't reach `Done` is a stall
    /// (`DriveError::NoProgress`), judged as part of the call's own
    /// result right alongside the error case, not by rejecting an
    /// empty spare slice up front — so a codec that happens to have
    /// nothing left to write against an empty buffer still completes
    /// normally. A codec error commits zero bytes to the sink before
    /// propagating.
    fn drain_to<O: Sink>(
        &mut self,
        output: &mut O,
        kind: DrainKind,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        let mut written = 0;
        loop {
            let moved = match output.spare().map_err(DriveError::Sink)? {
                Some(spare) => match kind.drive(self, spare) {
                    Ok(moved) if moved.written > 0 || moved.end == DrainEnd::Done => Ok(moved),
                    Ok(_) => {
                        output.commit(0).map_err(DriveError::Sink)?;
                        return Err(DriveError::NoProgress);
                    }
                    Err(error) => {
                        output.commit(0).map_err(DriveError::Sink)?;
                        return Err(DriveError::Codec(error));
                    }
                },
                None => {
                    let moved = kind.drive(self, &mut []).map_err(DriveError::Codec)?;
                    return Ok(PumpDrain { written, end: if moved.end == DrainEnd::Done {
                        DrainEnd::Done
                    } else {
                        DrainEnd::SinkExhausted
                    }});
                }
            }
            .map_err(DriveError::Codec)?;
            output.commit(moved.written).map_err(DriveError::Sink)?;
            written += moved.written;
            if moved.end == DrainEnd::Done {
                return Ok(PumpDrain { written, end: DrainEnd::Done });
            }
        }
    }

    pub(crate) fn finish(&mut self, output: &mut [u8]) -> Result<DrainStep, Error> {
        if self.done {
            return Ok(DrainStep {
                written: 0,
                end: DrainEnd::Done,
            });
        }
        let output_len = output.len();
        let moved = normalize_drain(self.codec.finish(output), output_len)?;
        if moved.end == DrainEnd::Done {
            self.done = true;
        }
        Ok(moved)
    }

    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) fn flush(&mut self, output: &mut [u8]) -> Result<DrainStep, Error> {
        if self.done {
            return Ok(DrainStep {
                written: 0,
                end: DrainEnd::Done,
            });
        }
        let output_len = output.len();
        normalize_drain(self.codec.flush(output), output_len)
    }
}

/// Turn a raw [`Drain`] from `Codec::finish`/`Codec::flush` into a
/// [`DrainStep`], after checking it against the codec contract
/// (`validated`, which rejects a `Done { written }` that overclaims
/// past `output_len` as `ErrorKind::ContractViolation`).
///
/// `Drain` only tells you *how* the codec stopped (filled the buffer,
/// or actually finished); it doesn't carry the amount written for the
/// filled case, since by contract that must be the whole buffer. This
/// fills that in: `OutputFilled` becomes `written: output_len`, so
/// both variants collapse into the uniform `{ written, end }` shape
/// `Driver::finish`/`Driver::flush` and their callers work with.
fn normalize_drain(
    result: Result<Drain, Error>,
    output_len: usize,
) -> Result<DrainStep, Error> {
    Ok(match result?.validated(output_len)? {
        Drain::OutputFilled => DrainStep {
            written: output_len,
            end: DrainEnd::SinkExhausted,
        },
        Drain::Done { written } => DrainStep {
            written,
            end: DrainEnd::Done,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{DrainEnd, Driver, PumpEnd};
    use crate::transfer::ProgressEnd;
    use crate::{Codec, Drain, DriveError, Error, ErrorKind, Progress, Sink, Source};

    struct Scripted {
        process: Progress,
        drain: Drain,
    }

    impl Codec for Scripted {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            Ok(self.process)
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(self.drain)
        }

        fn flush(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(self.drain)
        }
    }

    /// A `Source` over a plain byte slice.
    struct SliceSource<'a> {
        bytes: &'a [u8],
        pos: usize,
    }

    impl Source for SliceSource<'_> {
        type Error = core::convert::Infallible;

        fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
            Ok((self.pos < self.bytes.len()).then_some(&self.bytes[self.pos..]))
        }

        fn consume(&mut self, amount: usize) {
            self.pos += amount;
        }
    }

    /// A `Sink` that always offers a zero-length slice and never
    /// reports exhaustion — stands in for an endpoint a codec doesn't
    /// actually need output room from.
    struct NullSink;

    impl Sink for NullSink {
        type Error = core::convert::Infallible;

        fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
            Ok(Some(&mut []))
        }

        fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
            assert_eq!(amount, 0, "NullSink never offers room to write into");
            Ok(())
        }
    }

    /// Consumes everything, writes nothing — e.g. a hash/checksum
    /// pass with no output stream at all.
    struct DropEverything;

    impl Codec for DropEverything {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            Ok(Progress::InputConsumed { written: 0 })
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn codec_that_only_consumes_input_needs_no_output_room() {
        let mut input = SliceSource { bytes: b"abcdef", pos: 0 };
        let mut output = NullSink;
        let mut driver = Driver::new(DropEverything);
        let moved = driver.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(moved.consumed, 6);
        assert_eq!(moved.written, 0);
        assert_eq!(moved.end, PumpEnd::SourceExhausted);
    }

    #[test]
    fn a_pair_that_truly_cannot_progress_reports_no_progress() {
        let mut input = SliceSource { bytes: b"x", pos: 0 };
        let mut output = NullSink;
        let mut driver = Driver::new(Scripted {
            process: Progress::OutputFilled { consumed: 0 },
            drain: Drain::Done { written: 0 },
        });
        let error = driver.transfer_from(&mut input, &mut output).unwrap_err();
        assert!(matches!(error, DriveError::NoProgress));
    }

    #[test]
    fn process_uses_the_shared_transfer_boundary() {
        let mut driver = Driver::new(Scripted {
            process: Progress::OutputFilled { consumed: 2 },
            drain: Drain::Done { written: 0 },
        });
        let moved = driver.process(b"abc", &mut [0; 4]).unwrap();
        assert_eq!(moved.consumed, 2);
        assert_eq!(moved.written, 4);
        assert_eq!(moved.end, ProgressEnd::OutputExhausted);
    }

    #[test]
    fn in_band_end_latches_completion() {
        let mut driver = Driver::new(Scripted {
            process: Progress::StreamEnd {
                consumed: 1,
                written: 2,
            },
            drain: Drain::OutputFilled,
        });
        driver.process(b"abc", &mut [0; 4]).unwrap();
        assert!(driver.is_done());
        assert_eq!(driver.finish(&mut []).unwrap().end, DrainEnd::Done);
        let repeated = driver.process(b"trailing", &mut [0; 4]).unwrap();
        assert_eq!(repeated.consumed, 0);
        assert_eq!(repeated.written, 0);
        assert_eq!(repeated.end, ProgressEnd::StreamEnd);
    }

    #[test]
    fn finish_normalizes_output_progress_and_latches_done() {
        let mut filled = Driver::new(Scripted {
            process: Progress::InputConsumed { written: 0 },
            drain: Drain::OutputFilled,
        });
        let moved = filled.finish(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 3);
        assert_eq!(moved.end, DrainEnd::SinkExhausted);
        assert!(!filled.is_done());

        let mut done = Driver::new(Scripted {
            process: Progress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 2 },
        });
        let moved = done.finish(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, DrainEnd::Done);
        assert!(done.is_done());
    }

    #[test]
    fn flush_does_not_end_the_stream() {
        let mut driver = Driver::new(Scripted {
            process: Progress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 2 },
        });
        let moved = driver.flush(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, DrainEnd::Done);
        assert!(!driver.is_done());
    }

    #[test]
    fn drain_overclaims_are_contract_violations() {
        let mut driver = Driver::new(Scripted {
            process: Progress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 4 },
        });
        assert_eq!(
            driver.finish(&mut [0; 3]),
            Err(Error::new(ErrorKind::ContractViolation, 0, 0))
        );
    }
}
