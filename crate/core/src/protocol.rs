//! The [`Codec`]/[`TerminatingCodec`] traits and the vocabulary their
//! methods speak in: [`Progress`], [`TerminatingProgress`], [`Drain`],
//! [`Error`]. See `CREATING-CODECS.md` for how to write a codec.

// ----
// Progress
// ----

/// Progress of one [`Codec::process`] call. Every variant states an
/// invariant a driver can rely on without inspecting byte counts —
/// "made no progress and can't say why" is not expressible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// All of `input` was consumed; `written` bytes were produced
    /// (possibly zero, when everything went into internal buffering).
    /// The driver's move: supply more input, or `finish`.
    InputConsumed { written: usize },
    /// All of `output` was filled; `consumed` bytes of input were
    /// taken (possibly zero, when output pending from an earlier call
    /// filled the buffer by itself). The driver's move: drain the
    /// output and call again.
    OutputFilled { consumed: usize },
}

/// Progress of one [`TerminatingCodec::process`] call: everything
/// [`Progress`] can report, plus an in-band end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminatingProgress {
    /// All of `input` was consumed; `written` bytes were produced
    /// (possibly zero, when everything went into internal buffering).
    /// The driver's move: supply more input, or `finish`.
    InputConsumed { written: usize },
    /// All of `output` was filled; `consumed` bytes of input were
    /// taken (possibly zero, when output pending from an earlier call
    /// filled the buffer by itself). The driver's move: drain the
    /// output and call again.
    OutputFilled { consumed: usize },
    /// The stream ended in-band (self-terminating format): nothing
    /// more will ever be produced, and input past the stream's end was
    /// left unconsumed. Neither side is necessarily "full".
    End { consumed: usize, written: usize },
}

impl From<Progress> for TerminatingProgress {
    fn from(progress: Progress) -> Self {
        match progress {
            Progress::InputConsumed { written } => Self::InputConsumed { written },
            Progress::OutputFilled { consumed } => Self::OutputFilled { consumed },
        }
    }
}

// ----
// Draining
// ----

/// Progress of one [`DrainCodec::finish`] or [`DrainCodec::flush`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drain {
    /// All of `output` was filled and there is more to come — call
    /// again with a fresh (or drained) buffer.
    OutputFilled,
    /// Everything owed was delivered; the final `written` bytes (at
    /// most the output's length) landed in this call. For `finish`
    /// this is the end of the stream; for `flush`, the sync point.
    Done { written: usize },
}

// ----
// Errors and validation
// ----

/// What kind of failure a codec reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The encoded stream is malformed.
    Corrupt,
    /// The stream ended somewhere it shouldn't have. Either direction:
    /// too little data — `finish` was called while the codec still
    /// needed more input to complete a unit — or too much — a
    /// downstream codec in a composition (e.g. `second` in
    /// [`Chain`](crate::Chain)) ended its stream while bytes an
    /// upstream codec had already handed it were still unconsumed,
    /// which would otherwise be silently lost. Neither case has a
    /// reasonable recovery beyond surfacing it: there's no more input
    /// to give in the first, and no way to un-lose the bytes in the
    /// second.
    UnexpectedEnd,
    /// The codec's internal carry buffer couldn't hold an atomic
    /// output unit — a codec bug (the carry is sized statically to the
    /// codec's largest unit), or a format whose units are unbounded.
    BufferOverrun,
    /// The codec reported byte counts exceeding the buffers it was
    /// given — a codec bug, caught at the driver's trust boundary (see
    /// [`Progress::validated`]/[`Drain::validated`]) before it can
    /// corrupt positions, panic on a later slice, or make an adapter
    /// break its host contract (`std::io::Read` must never report more
    /// bytes than the buffer holds). The error's `consumed`/`written`
    /// are zero: the reported counts are exactly what can't be
    /// trusted.
    ContractViolation,
}

/// A codec failure, carrying how far the call got before failing so
/// the caller knows which bytes were already accounted for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    /// Bytes of `input` consumed by this call before the failure.
    pub consumed: usize,
    /// Bytes written to `output` by this call before the failure.
    pub written: usize,
}

impl Error {
    pub fn new(kind: ErrorKind, consumed: usize, written: usize) -> Self {
        Self {
            kind,
            consumed,
            written,
        }
    }

    /// Check the progress carried by an error against the buffers used by
    /// the failing call. Error progress crosses the same trust boundary as
    /// successful progress and must not be allowed to advance endpoints
    /// beyond the slices the codec received.
    pub fn validated(self, input_len: usize, output_len: usize) -> Result<Self, Self> {
        if self.consumed <= input_len && self.written <= output_len {
            Ok(self)
        } else {
            Err(Self::new(ErrorKind::ContractViolation, 0, 0))
        }
    }
}

impl Progress {
    /// Check the reported byte counts against the buffer sizes the
    /// call was actually given, turning a lying codec into
    /// [`ErrorKind::ContractViolation`] instead of letting bogus
    /// counts corrupt driver state. Every driver in this crate applies
    /// this at its codec trust boundary; a driver of your own should
    /// too.
    pub fn validated(self, input_len: usize, output_len: usize) -> Result<Progress, Error> {
        let honest = match self {
            Progress::InputConsumed { written } => written <= output_len,
            Progress::OutputFilled { consumed } => consumed <= input_len,
        };
        if honest {
            Ok(self)
        } else {
            Err(Error::new(ErrorKind::ContractViolation, 0, 0))
        }
    }
}

impl TerminatingProgress {
    /// The [`Progress::validated`] counterpart for
    /// [`TerminatingCodec::process`].
    pub fn validated(
        self,
        input_len: usize,
        output_len: usize,
    ) -> Result<TerminatingProgress, Error> {
        let honest = match self {
            TerminatingProgress::InputConsumed { written } => written <= output_len,
            TerminatingProgress::OutputFilled { consumed } => consumed <= input_len,
            TerminatingProgress::End { consumed, written } => {
                consumed <= input_len && written <= output_len
            }
        };
        if honest {
            Ok(self)
        } else {
            Err(Error::new(ErrorKind::ContractViolation, 0, 0))
        }
    }
}

impl Drain {
    /// The [`Progress::validated`] counterpart for `finish`/`flush`.
    pub fn validated(self, output_len: usize) -> Result<Drain, Error> {
        match self {
            Drain::Done { written } if written > output_len => {
                Err(Error::new(ErrorKind::ContractViolation, 0, 0))
            }
            honest => Ok(honest),
        }
    }
}

// ----
// Codec traits
// ----

/// Lifecycle operations shared by [`Codec`] and [`TerminatingCodec`]:
/// signalling end-of-input and reaching a sync point.
pub trait DrainCodec {
    /// Signal "no more input is coming": flush any buffered state and,
    /// for formats with one, write the trailer/checksum. Call
    /// repeatedly (draining `output` between calls) until it reports
    /// [`Drain::Done`]. Idempotent once `Done`: see [`Codec`] contract
    /// point 3. For a [`TerminatingCodec`], pinned to reporting `Done`
    /// forever once `process` has reported
    /// [`TerminatingProgress::End`]. Must always resolve to
    /// `OutputFilled`, `Done`, or `Err`: see [`Codec`] contract point 7.
    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error>;

    /// Drain any bytes this codec is withholding to a sync boundary,
    /// *without* ending the stream — unlike `finish`, the stream
    /// continues afterward. Only meaningful for codecs that buffer
    /// output for a format-defined in-band sync marker
    /// (deflate/zlib/gzip do); the default owes nothing. Idempotent
    /// once `Done`: see [`Codec`] contract point 3. Must always resolve
    /// to `OutputFilled`, `Done`, or `Err`: see [`Codec`] contract
    /// point 7. Calling this after `finish`: see [`Codec`] contract
    /// point 6.
    fn flush(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

/// A stateful, whole-stream byte-rewrite transform: a call never
/// declares the stream finished. End of input is supplied out of band
/// by calling `finish`.
///
/// # The contract
///
/// 1)
/// - a call fully consumes its input,
/// - or it fully fills its output.
///
/// A codec that simply ran out of input to consume reports that via
/// `InputConsumed`.
///
/// 2)
/// Each call addresses `input` and `output` from byte 0 of the slices
/// it was given — there is no notion of a "leftover" position carried
/// over from the previous call's buffers. If a call didn't consume
/// all of `input`, those unconsumed bytes are gone once the call
/// returns: it is the caller's job to fold them into the front of
/// whatever `input` it passes next, if it wants them processed at
/// all.
///
/// 3)
/// `finish` and `flush` are each idempotent once *that same method*
/// reaches [`Drain::Done`]: calling `finish` again after `finish` was
/// `Done` (or `flush` again after `flush` was `Done`), with no
/// intervening `process` call supplying new input, must report
/// `Drain::Done { written: 0 }` again rather than repeating whatever
/// format-level side effect produced that `Done` — no second trailer,
/// no second sync marker. A caller is free to call `finish`/`flush`
/// again after `Done` (e.g. to resume a call that was interrupted
/// elsewhere in a composition) and must see a no-op, not a repeat.
///
/// `finish` and `flush` are independent of each other, though: a
/// `Done` from one does not make the other idempotent. `flush`
/// reaching `Done` only means a sync point was reached — the stream
/// stays open — so a `finish` called afterward (with no new `process`
/// input) does its real work, producing the actual trailer, not a
/// no-op.
///
/// 4)
/// A call that returns `Err` does not leave the codec in a defined
/// failure state. A later call is not required to keep failing — it
/// may make ordinary progress, as if the error had never happened. A
/// caller that wants "this codec is dead after an error" semantics
/// has to enforce that itself; the contract doesn't give it that for
/// free.
///
/// 5)
/// Calling `process` or `flush` after `finish` is not defined by this
/// contract. A codec with no real trailer/terminal state is free to
/// just keep processing as if `finish` had never been called; one
/// with a hard terminal state (already wrote a final checksum,
/// already closed out the format) should report an `Err` instead of
/// doing something silently wrong. Either is a valid `Codec` impl; a
/// caller can't rely on which one it's talking to.
///
/// 6)
/// `finish` and `flush` must always resolve one of three ways, the
/// `Drain` counterpart of point 1: [`Drain::OutputFilled`],
/// [`Drain::Done`], or `Err`. `Drain::OutputFilled` carries no count —
/// unlike `Progress::OutputFilled`, it isn't a partial-progress
/// report, it commits to having filled the *entire* non-empty
/// `output` it was given. There's no fourth option where a call
/// stalls, reporting `OutputFilled` without having actually written
/// all of a non-empty buffer.
///
/// # Why full consumption, not partial — a codec must always make progress
///
/// A tempting alternative: let one call report partial progress on
/// *both* sides at once — consume some input, write some output, and
/// leave the rest for later. This sounds more flexible, but it causes
/// a problem.
///
/// Imagine an encoder that needs several more input bytes, or several
/// more bytes of free output space, before it can produce anything.
/// What happens when neither side has enough room to make progress?
///
/// If the caller has to handle this, it must call `process` again,
/// hoping that more input or output space will be available next
/// time. But the caller has no way to know how many retries are
/// normal, and how many mean the codec is stuck or broken.
///
/// This is not an imaginary edge case: a [`Source`](crate::Source)
/// reading from a network socket may hand over data one byte at a
/// time, so the caller could easily need many retries just to gather
/// enough input for the codec to do anything.
///
/// Beyond that corner case, there is a broader question of where the
/// complexity should live: on the codec side or on the caller side.
/// Putting the burden on the codec has two benefits:
///
/// - The caller's code stays less complicated. Handling partial
///   progress on both sides at once tends to produce messy code,
///   based on experience.
/// - This matters beyond this crate: we expect people to build on
///   top of the `Codec` trait not only new codecs, but also wrappers
///   around custom [`Source`](crate::Source)s and
///   [`Sink`](crate::Sink)s — and that caller-side code deserves to
///   stay less complicated just as much as codec implementations do.
///
/// # Creating a codec
///
/// See `CREATING-CODECS.md` for how to write one.
///
pub trait Codec: DrainCodec {
    /// Push input bytes and pull output bytes.
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error>;
}

/// A stateful byte-rewrite transform that may additionally recognize
/// its logical stream ending inside an input slice — a self-terminating
/// format saying "I am finished" mid-stream, not a failure or premature
/// cancellation.
///
/// Accepted by input-side drivers ([`CodecReader`](crate::sources_and_sinks::std_io::CodecReader),
/// [`stream_to_stream`](crate::stream_to_stream)), which can expose
/// transformed bytes only up to the reported end and leave the rest of
/// the source available to whatever comes next. Not accepted by
/// [`CodecWriter`](crate::sources_and_sinks::std_io::CodecWriter):
/// `Write` cannot represent a successful permanent short write.
///
/// Every [`Codec`] is automatically a `TerminatingCodec` that never
/// returns `End` — see the blanket implementation below. The
/// conversion is one-way: a `TerminatingCodec` cannot be used where
/// `Codec` is required, because it cannot promise to consume the whole
/// logical input stream. One concrete type also cannot independently
/// implement both traits with different behavior — a format offering
/// ordinary and terminating modes should expose distinct types.
///
/// # The contract
///
/// Same as [`Codec`]'s points 1–6 (`process` may additionally resolve
/// to [`TerminatingProgress::End`], in which case: `consumed` input
/// bytes belong to the ended stream, `written` output bytes were
/// produced by this call, `input[consumed..]` was not consumed and
/// belongs to whatever follows the terminated stream, and the codec
/// will never produce more output. A codec may consume or leave
/// unconsumed the delimiter that establishes the boundary; that
/// behavior is format-specific and must be documented by the codec.
///
/// Once `process` reaches `End`, the stream has ended for good: every
/// later call, of any of the three methods, must keep reporting that,
/// forever, for the rest of the codec's lifetime — `process` answers
/// `End { consumed: 0, written: 0 }` again on any `input`,
/// `finish`/`flush` answer `Drain::Done { written: 0 }`. None of them
/// re-run whatever ended the stream. A caller (or a combinator built on
/// top of `TerminatingCodec`) is free to call any of the three again
/// after `End` and must see a no-op every time, not a second ending.
/// This is the one case where `finish` and `flush` stop being
/// independent of each other: past `End`, both are pinned to `Done {
/// written: 0 }` together, not just idempotent against repeats of
/// themselves.
///
/// A codec intended to yield several tokens or boundaries from one
/// instance is not a `TerminatingCodec` in the usual driver sense —
/// drivers like [`Pump`](crate::pump::Pump) latch permanently after the
/// first `End`. Driving `process` directly, call by call, still allows
/// reusing one instance across multiple logical streams (see
/// `core/tests/early_stop_input.rs`); a driver that shouldn't latch has
/// to be written with that in mind.
///
/// This crate does not impose one universal meaning on EOF before a
/// terminating codec reports `End`. If the in-band terminator is
/// required, `finish` should return an `UnexpectedEnd` error when EOF
/// arrives first; if both an in-band terminator and source EOF are
/// valid endings, `finish` may drain buffered output and return `Done`.
/// Each `TerminatingCodec` should document which rule it uses.
///
/// # Naming the operation
///
/// Both `Codec` and `TerminatingCodec` deliberately name their method
/// `process`. Because of the blanket implementation, a direct method
/// call on an ordinary concrete codec can be ambiguous when both traits
/// are in scope. Use Rust's fully qualified syntax in that case:
///
/// ```ignore
/// Codec::process(&mut codec, input, output);
/// TerminatingCodec::process(&mut codec, input, output);
/// ```
pub trait TerminatingCodec: DrainCodec {
    /// Push input bytes and pull output bytes, possibly recognizing the
    /// stream's in-band end. Pinned to reporting `End` forever once
    /// it's reached. Calling this after `finish` is unsupported.
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<TerminatingProgress, Error>;
}

impl<C: Codec> TerminatingCodec for C {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<TerminatingProgress, Error> {
        Codec::process(self, input, output).map(Into::into)
    }
}

// ----
// Allocation-backed delegation
// ----

// Mirrors std's `impl<R: Read + ?Sized> Read for Box<R>`: lets a `Box<dyn
// Codec>` (or a boxed concrete codec) stand in anywhere a `Codec` is
// expected, e.g. to build a runtime-determined chain of codecs.
#[cfg(feature = "alloc")]
use alloc::boxed::Box;

#[cfg(feature = "alloc")]
impl<C: DrainCodec + ?Sized> DrainCodec for Box<C> {
    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        (**self).finish(output)
    }

    fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        (**self).flush(output)
    }
}

#[cfg(feature = "alloc")]
impl<C: Codec + ?Sized> Codec for Box<C> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        (**self).process(input, output)
    }
}

// ----
// Tests
// ----

#[cfg(test)]
mod tests {
    use super::{Drain, Error, ErrorKind, Progress, TerminatingProgress};

    const CV: Error = Error {
        kind: ErrorKind::ContractViolation,
        consumed: 0,
        written: 0,
    };

    #[test]
    fn validated_accepts_honest_counts() {
        assert!(Progress::InputConsumed { written: 4 }
            .validated(10, 4)
            .is_ok());
        assert!(Progress::OutputFilled { consumed: 10 }
            .validated(10, 4)
            .is_ok());
        assert!(TerminatingProgress::End {
            consumed: 0,
            written: 0
        }
        .validated(0, 0)
        .is_ok());
        assert!(Drain::OutputFilled.validated(0).is_ok());
        assert!(Drain::Done { written: 4 }.validated(4).is_ok());
    }

    #[test]
    fn validated_rejects_overclaimed_counts() {
        assert_eq!(
            Progress::InputConsumed { written: 5 }.validated(10, 4),
            Err(CV)
        );
        assert_eq!(
            Progress::OutputFilled { consumed: 11 }.validated(10, 4),
            Err(CV)
        );
        assert_eq!(
            TerminatingProgress::End {
                consumed: 11,
                written: 0
            }
            .validated(10, 4),
            Err(CV)
        );
        assert_eq!(
            TerminatingProgress::End {
                consumed: 0,
                written: 5
            }
            .validated(10, 4),
            Err(CV)
        );
        assert_eq!(Drain::Done { written: 5 }.validated(4), Err(CV));
    }
}
