//! Bufferless codec lifecycle, the codec trust boundary it's built on,
//! and the lending stream driver ([`stream_to_stream`]) that ties a
//! codec to a pair of endpoints.
//!
//! Endpoint adapters retain the buffers dictated by their direction:
//! readers own input storage, writers own output storage, and chunked
//! frontends lend both current windows. [`Pump`] owns only codec
//! lifecycle, so using it never introduces a byte copy.
//!
//! [`Codec::process`](crate::Codec::process) reports only the counts
//! not already implied by its outcome: all input was consumed, all
//! output was filled, or the stream ended. `step` validates that
//! report and normalizes it into exact progress on both sides — the
//! trust boundary every `Pump::process` call and [`stream_to_stream`]
//! call goes through.

use crate::{Codec, Drain, Error, Progress};

/// A byte source which lends its current input chunk to the pump.
pub trait Source {
    type Error;

    /// Return the current non-empty chunk, or `None` at end of input.
    ///
    /// "Current" is load-bearing: this is whatever hasn't been
    /// released by `consume` yet, not necessarily fresh bytes. A
    /// caller is never required to consume a whole chunk in one call
    /// (a codec may only take part of it, e.g. when output runs out
    /// first) — the unconsumed remainder is exactly what the next
    /// `chunk()` call returns, so consecutive chunks can overlap.
    /// Implementations must not hand out new bytes ahead of `pos`
    /// until the old ones are released.
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error>;

    /// Release the first `amount` bytes of the current chunk.
    fn consume(&mut self, amount: usize);
}

/// A byte destination which lends writable space to the pump.
pub trait Sink {
    type Error;

    /// Return writable space, or `None` when the destination is full.
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error>;

    /// Commit the first `amount` bytes of the space returned by `spare`.
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error>;

    /// Complete the destination after the codec stream has ended.
    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// How much moved through [`stream_to_stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Totals {
    pub consumed: usize,
    pub written: usize,
}

/// Why [`stream_to_stream`] stopped before the codec finished its stream.
#[derive(Debug)]
pub enum DriveError<EI, EO> {
    Source(EI),
    Sink(EO),
    Codec(crate::Error),
    SinkExhausted,
    /// A call moved zero bytes on both sides without ending the
    /// stream — the pump refuses to spin forever on a stalled
    /// codec/endpoint pair.
    NoProgress,
}

/// Drive one codec between any pair of lending stream adapters.
pub fn stream_to_stream<I, O, C>(
    input: &mut I,
    codec: C,
    output: &mut O,
) -> Result<Totals, DriveError<I::Error, O::Error>>
where
    I: Source,
    O: Sink,
    C: Codec,
{
    let mut pump = Pump::new(codec);
    let mut totals = Totals { consumed: 0, written: 0 };

    let moved = pump.transfer_from(input, output)?;
    totals.consumed += moved.consumed;
    totals.written += moved.written;
    match moved.end {
        PumpEnd::StreamEnd => {
            output.finish().map_err(DriveError::Sink)?;
            return Ok(totals);
        }
        PumpEnd::SinkExhausted => return Err(DriveError::SinkExhausted),
        PumpEnd::SourceExhausted => {}
    }

    let drained = pump.finish_to(output).map_err(|error| match error {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => DriveError::Sink(error),
        DriveError::Codec(error) => DriveError::Codec(error),
        DriveError::SinkExhausted => DriveError::SinkExhausted,
        DriveError::NoProgress => DriveError::NoProgress,
    })?;
    totals.written += drained.written;
    match drained.end {
        DrainEnd::Done => {
            output.finish().map_err(DriveError::Sink)?;
            Ok(totals)
        }
        DrainEnd::SinkExhausted => Err(DriveError::SinkExhausted),
    }
}

/// Why one step between the current input and output windows stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgressEnd {
    /// The complete input window was consumed.
    InputExhausted,
    /// The complete output window was filled.
    OutputExhausted,
    /// The codec ended its stream in-band.
    StreamEnd,
}

/// Exact progress made by one validated [`Codec::process`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProgressStep {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: ProgressEnd,
}

/// Run one step between the current windows until the codec reaches the
/// boundary guaranteed by its contract.
pub(crate) fn step<C: Codec + ?Sized>(
    codec: &mut C,
    input: &[u8],
    output: &mut [u8],
) -> Result<ProgressStep, Error> {
    let input_len = input.len();
    let output_len = output.len();
    let outcome = codec
        .process(input, output)?
        .validated(input_len, output_len)?;

    Ok(match outcome {
        Progress::InputConsumed { written } => ProgressStep {
            consumed: input_len,
            written,
            end: ProgressEnd::InputExhausted,
        },
        Progress::OutputFilled { consumed } => ProgressStep {
            consumed,
            written: output_len,
            end: ProgressEnd::OutputExhausted,
        },
        Progress::StreamEnd { consumed, written } => ProgressStep {
            consumed,
            written,
            end: ProgressEnd::StreamEnd,
        },
    })
}

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
    fn drive<C: Codec>(self, pump: &mut Pump<C>, output: &mut [u8]) -> Result<DrainStep, Error> {
        match self {
            DrainKind::Finish => pump.finish(output),
            DrainKind::Flush => pump.flush(output),
        }
    }
}

/// A bufferless lifecycle wrapper around a codec.
pub(crate) struct Pump<C> {
    codec: C,
    done: bool,
}

impl<C: Codec> Pump<C> {
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
                end: ProgressEnd::StreamEnd,
            });
        }
        let moved = step(&mut self.codec, input, output)?;
        if moved.end == ProgressEnd::StreamEnd {
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
    /// spare slice from `output`, calls [`Pump::process`] once, then
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
                        || moved.end == ProgressEnd::StreamEnd =>
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
            if moved.end == ProgressEnd::StreamEnd {
                return Ok(PumpTransfer { consumed, written, end: PumpEnd::StreamEnd });
            }
        }
    }

    /// Drain the codec's trailing output by repeatedly calling
    /// [`Pump::finish`] against spare space taken from `output`,
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
    /// spare space from `output` and hand it to [`Pump::finish`] (if
    /// `kind` is `DrainKind::Finish`) or [`Pump::flush`] (otherwise),
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

    /// `self.done` is only ever set by [`Pump::process`] reaching a
    /// genuine `Progress::StreamEnd` (contract point 4 — pinned
    /// forever, across every method). Reaching `Drain::Done` here is
    /// governed by point 3 instead: `finish` is idempotent against
    /// repeats of itself, but that doesn't license skipping `process`
    /// or `flush` afterward (point 6), so a `Done` from `self.codec`
    /// is never latched into `self.done` — only reported.
    pub(crate) fn finish(&mut self, output: &mut [u8]) -> Result<DrainStep, Error> {
        if self.done {
            return Ok(DrainStep {
                written: 0,
                end: DrainEnd::Done,
            });
        }
        let output_len = output.len();
        normalize_drain(self.codec.finish(output), output_len)
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
/// `Pump::finish`/`Pump::flush` and their callers work with.
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
    use super::{step, DrainEnd, Pump, ProgressEnd, ProgressStep, PumpEnd};
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
        let mut pump = Pump::new(DropEverything);
        let moved = pump.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(moved.consumed, 6);
        assert_eq!(moved.written, 0);
        assert_eq!(moved.end, PumpEnd::SourceExhausted);
    }

    #[test]
    fn a_pair_that_truly_cannot_progress_reports_no_progress() {
        let mut input = SliceSource { bytes: b"x", pos: 0 };
        let mut output = NullSink;
        let mut pump = Pump::new(Scripted {
            process: Progress::OutputFilled { consumed: 0 },
            drain: Drain::Done { written: 0 },
        });
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert!(matches!(error, DriveError::NoProgress));
    }

    #[test]
    fn process_uses_the_shared_step() {
        let mut pump = Pump::new(Scripted {
            process: Progress::OutputFilled { consumed: 2 },
            drain: Drain::Done { written: 0 },
        });
        let moved = pump.process(b"abc", &mut [0; 4]).unwrap();
        assert_eq!(moved.consumed, 2);
        assert_eq!(moved.written, 4);
        assert_eq!(moved.end, ProgressEnd::OutputExhausted);
    }

    #[test]
    fn in_band_end_latches_completion() {
        let mut pump = Pump::new(Scripted {
            process: Progress::StreamEnd {
                consumed: 1,
                written: 2,
            },
            drain: Drain::OutputFilled,
        });
        pump.process(b"abc", &mut [0; 4]).unwrap();
        assert!(pump.is_done());
        assert_eq!(pump.finish(&mut []).unwrap().end, DrainEnd::Done);
        let repeated = pump.process(b"trailing", &mut [0; 4]).unwrap();
        assert_eq!(repeated.consumed, 0);
        assert_eq!(repeated.written, 0);
        assert_eq!(repeated.end, ProgressEnd::StreamEnd);
    }

    #[test]
    fn finish_normalizes_output_progress_without_latching_done() {
        let mut filled = Pump::new(Scripted {
            process: Progress::InputConsumed { written: 0 },
            drain: Drain::OutputFilled,
        });
        let moved = filled.finish(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 3);
        assert_eq!(moved.end, DrainEnd::SinkExhausted);
        assert!(!filled.is_done());

        // `finish` reaching `Done` is only point-3 self-idempotency,
        // not a point-4 pin — `process` must still be free to run
        // normally afterward (point 6), so `is_done()` stays false and
        // the codec, not a synthetic `StreamEnd`, answers the next
        // `process` call.
        let mut done = Pump::new(Scripted {
            process: Progress::InputConsumed { written: 5 },
            drain: Drain::Done { written: 2 },
        });
        let moved = done.finish(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, DrainEnd::Done);
        assert!(!done.is_done());
        let resumed = done.process(b"abc", &mut [0; 8]).unwrap();
        assert_eq!(resumed.written, 5);
    }

    #[test]
    fn flush_does_not_end_the_stream() {
        let mut pump = Pump::new(Scripted {
            process: Progress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 2 },
        });
        let moved = pump.flush(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, DrainEnd::Done);
        assert!(!pump.is_done());
    }

    #[test]
    fn drain_overclaims_are_contract_violations() {
        let mut pump = Pump::new(Scripted {
            process: Progress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 4 },
        });
        assert_eq!(
            pump.finish(&mut [0; 3]),
            Err(Error::new(ErrorKind::ContractViolation, 0, 0))
        );
    }

    struct Reports(Progress);

    impl Codec for Reports {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            Ok(self.0)
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn input_exhaustion_implies_all_input_was_consumed() {
        let mut codec = Reports(Progress::InputConsumed { written: 2 });
        let mut output = [0; 5];

        assert_eq!(
            step(&mut codec, b"abc", &mut output),
            Ok(ProgressStep {
                consumed: 3,
                written: 2,
                end: ProgressEnd::InputExhausted,
            })
        );
    }

    #[test]
    fn output_exhaustion_implies_all_output_was_written() {
        let mut codec = Reports(Progress::OutputFilled { consumed: 2 });
        let mut output = [0; 5];

        assert_eq!(
            step(&mut codec, b"abc", &mut output),
            Ok(ProgressStep {
                consumed: 2,
                written: 5,
                end: ProgressEnd::OutputExhausted,
            })
        );
    }

    #[test]
    fn stream_end_preserves_both_explicit_counts() {
        let mut codec = Reports(Progress::StreamEnd {
            consumed: 2,
            written: 4,
        });
        let mut output = [0; 5];

        assert_eq!(
            step(&mut codec, b"abc", &mut output),
            Ok(ProgressStep {
                consumed: 2,
                written: 4,
                end: ProgressEnd::StreamEnd
            })
        );
    }

    #[test]
    fn degenerate_windows_remain_well_defined() {
        let mut input_done = Reports(Progress::InputConsumed { written: 0 });
        assert_eq!(
            step(&mut input_done, b"", &mut []),
            Ok(ProgressStep {
                consumed: 0,
                written: 0,
                end: ProgressEnd::InputExhausted,
            })
        );

        let mut output_done = Reports(Progress::OutputFilled { consumed: 0 });
        assert_eq!(
            step(&mut output_done, b"abc", &mut []),
            Ok(ProgressStep {
                consumed: 0,
                written: 0,
                end: ProgressEnd::OutputExhausted,
            })
        );
    }

    #[test]
    fn overclaims_are_rejected_at_the_shared_boundary() {
        let violation = Error::new(ErrorKind::ContractViolation, 0, 0);

        let mut input_done = Reports(Progress::InputConsumed { written: 6 });
        assert_eq!(
            step(&mut input_done, b"abc", &mut [0; 5]),
            Err(violation)
        );

        let mut output_done = Reports(Progress::OutputFilled { consumed: 4 });
        assert_eq!(
            step(&mut output_done, b"abc", &mut [0; 5]),
            Err(violation)
        );

        let mut ended = Reports(Progress::StreamEnd {
            consumed: 4,
            written: 6,
        });
        assert_eq!(step(&mut ended, b"abc", &mut [0; 5]), Err(violation));
    }

    struct Fails;

    impl Codec for Fails {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn codec_errors_are_preserved() {
        assert_eq!(
            step(&mut Fails, b"abc", &mut [0; 5]),
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        );
    }
}
