//! The vocabulary and contracts for codecs and I/O backends.

use core::mem::MaybeUninit;

// ----
// I/O backend contracts
// ----

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
    ///
    /// Never fails. This is mostly accounting, not I/O; any fallible
    /// work belongs in `chunk`, not here.
    fn consume(&mut self, amount: usize);
}

/// A byte destination which lends writable space to the pump.
pub trait Sink {
    type Error;

    /// Return writable space, or `None` when the destination is full.
    ///
    /// The returned bytes are not necessarily initialized — a `Sink`
    /// backed by growable storage (e.g. `VecSink`) may lend spare
    /// capacity straight from the allocator. A codec must only ever
    /// write to this space, never read from it, and must not claim any
    /// byte as written to `commit` that it did not itself initialize.
    ///
    /// A caller is never required to commit any of it before calling
    /// `spare` again — an uncommitted call may simply be re-issued,
    /// returning the same (or an equivalent) span.
    fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error>;

    /// Commit the first `amount` bytes of the space returned by `spare`.
    ///
    /// Can fail. Unlike `Source::consume`, I/O is possible here.
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error>;

    /// Complete the destination after the codec stream has ended.
    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Sync the destination at a sync point, without ending it — e.g.
    /// forward to the underlying transport's own `flush`. Unlike
    /// `finish`, more writes may follow.
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ----
// Codec traits
// ----

/// Operations shared by [`Codec`] and [`EndCapableCodec`], for two
/// things: telling the codec that input has ended, and reaching a
/// sync point.
pub trait DrainCodec {
    /// Tell the codec that no more input will come. The codec flushes
    /// any buffered state. If the format has a trailer or a checksum,
    /// the codec writes it now.
    ///
    /// One call may not be enough. If `output` is too small to hold
    /// everything the codec owes, `finish` returns
    /// `Drain::OutputFilled` instead of `Drain::Done`. Call `finish`
    /// again, and drain `output` between calls, until it returns
    /// [`Drain::Done`]. A driver that calls `finish` only once, and
    /// treats `OutputFilled` as success, will silently cut off the
    /// end of the stream: part of the trailer, checksum, or padding
    /// will be missing.
    ///
    /// Once `finish` returns `Done`, it is idempotent: see [`Codec`]
    /// contract point 3. For an [`EndCapableCodec`], once `process`
    /// has returned [`EndCapableProgress::End`], `finish` always
    /// returns `Done` after that, forever. `finish` must always
    /// return one of three results: `OutputFilled`, `Done`, or `Err`.
    /// See [`Codec`] contract point 6.
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error>;

    /// Release any bytes the codec is holding back, up to a sync
    /// boundary. This does not end the stream: unlike `finish`, the
    /// stream stays open, and more calls to `process` may follow.
    ///
    /// This method matters only for codecs that buffer output for a
    /// sync marker defined by the format — deflate, zlib, and gzip do
    /// this. The default implementation owes nothing, so it does
    /// nothing.
    ///
    /// Once `flush` returns `Done`, it is idempotent: see [`Codec`]
    /// contract point 3. `flush` must always return one of three
    /// results: `OutputFilled`, `Done`, or `Err`. See [`Codec`]
    /// contract point 6. For what happens when `flush` is called
    /// after `finish`, see [`Codec`] contract point 5.
    fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

/// A stateful transform that rewrites a whole stream of bytes. A call
/// to `process` never declares the stream finished. To signal the
/// end of input, call `finish` separately.
///
/// # The contract
///
/// 1) Each call to `process` does one of two things:
/// - it consumes all of `input`, or
/// - it fills all of `output`.
///
/// If the codec runs out of input before `output` is full, it
/// reports this with `InputConsumed`.
///
/// 2)
/// Each call reads `input` and writes `output` starting at byte 0 of
/// the slices it was given. The codec does not remember a "leftover"
/// position from the previous call. If a call did not consume all of
/// `input`, the caller must retain those bytes and supply them again
/// if it wants them processed. It may pass a suffix of the same
/// buffer; copying the bytes is not required.
///
/// 3)
/// `finish` and `flush` are each idempotent, but only against repeats
/// of themselves, and only once they reach [`Drain::Done`]. Suppose
/// `finish` returns `Done`, and no call to `process` supplies new
/// input in between. Then a later call to `finish` must return
/// `Drain::Done { written: 0 }` again. It must not repeat the
/// format-level effect that produced the first `Done`: no second
/// trailer, no second sync marker. The same rule applies to `flush`
/// against itself. A caller may call `finish` or `flush` again after
/// `Done` — for example, to resume a call that was interrupted
/// elsewhere in a larger composition — and must see a no-op, not a
/// repeat.
///
/// `finish` and `flush` are independent of each other. A `Done` from
/// one does not make the other idempotent. When `flush` reaches
/// `Done`, it means only that the codec reached a sync point; the
/// stream stays open. So if `finish` is then called, with no new
/// `process` input in between, it does its real work: it produces
/// the actual trailer, not a no-op.
///
/// 4)
/// A call that returns `Err` does not put the codec into a defined
/// failure state. A later call is not required to keep failing. It
/// may make ordinary progress, as if the error had never happened.
/// If a caller wants a codec to stay dead after an error, the caller
/// must enforce that itself. The contract does not give this for
/// free.
///
/// 5)
/// This contract does not define what happens when `process` or
/// `flush` is called after `finish`. A codec with no real trailer
/// and no terminal state may keep processing, as if `finish` had
/// never been called. A codec with a hard terminal state — for
/// example, one that already wrote a final checksum and closed out
/// the format — should return `Err` instead of doing something
/// silently wrong. Both behaviors are valid `Codec` implementations.
/// A caller cannot rely on which one it is talking to.
///
/// 6)
/// `finish` and `flush` must always return one of three results:
/// [`Drain::OutputFilled`], [`Drain::Done`], or `Err`. This is the
/// `Drain` counterpart of point 1. `Drain::OutputFilled` carries no
/// count. Unlike `Progress::OutputFilled`, it is not a
/// partial-progress report — it means the call filled the entire
/// non-empty `output` it was given. There is no fourth option where
/// a call stalls: reporting `OutputFilled` without having actually
/// written all of a non-empty buffer.
///
/// # Why full consumption, not partial — a codec must always make progress
///
/// One alternative design would let a single call report partial
/// progress on both sides at once: consume some input, write some
/// output, and leave the rest for later. This sounds more flexible,
/// but it causes a problem.
///
/// Imagine an encoder that needs several more input bytes, or
/// several more bytes of free output space, before it can produce
/// anything. What happens when neither side has enough room for it
/// to make progress?
///
/// If the caller had to handle this case, it would call `process`
/// again, and hope that more input or output space is available
/// next time. But the caller would have no way to know how many
/// retries are normal, and how many mean the codec is stuck or
/// broken.
///
/// This is not a rare edge case. A [`Source`] that reads from a
/// network socket may hand over data one byte at a time. In that
/// case, the caller could need many retries just to gather enough
/// input for the codec to do anything.
///
/// There is also a broader question: where should this complexity
/// live, on the codec side or on the caller side? Putting it on the
/// codec side has two benefits.
///
/// - The caller's code stays simpler. Handling partial progress on
///   both sides at once tends to produce messy code.
/// - This benefit reaches beyond this crate. People will build on
///   top of the `Codec` trait not only new codecs, but also wrappers
///   around custom [`Source`]s and [`Sink`]s. That caller-side code
///   deserves to stay simple, just as much as codec implementations
///   do.
///
/// # Creating a codec
///
/// See `CREATING-CODECS.md` for how to write one.
///
pub trait Codec: DrainCodec {
    /// Push input bytes and pull output bytes.
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error>;
}

/// A stateful transform that rewrites bytes and may also recognize
/// its own logical end inside an input slice. This is a
/// self-terminating format saying "I am finished" in the middle of
/// the stream. It is not a failure, and not an early cancellation.
///
/// Input-side drivers accept this trait:
/// [`CodecReader`](crate::sources_and_sinks::std_io::CodecReader) and
/// [`stream_to_stream`](crate::stream_to_stream). They can expose the
/// transformed bytes up to the reported end, and leave the rest of
/// the source available to whatever comes next.
/// [`CodecWriter`](crate::sources_and_sinks::std_io::CodecWriter) does
/// not accept this trait, because `Write` cannot represent a
/// successful permanent short write.
///
/// Every [`Codec`] is automatically an `EndCapableCodec` that never
/// returns `End` — see the blanket implementation below. This
/// conversion goes one way only: an `EndCapableCodec` cannot be used
/// where `Codec` is required, because it cannot promise to consume
/// the whole logical input stream. One concrete type also cannot
/// implement both traits with different behavior. A format that
/// offers both an ordinary mode and a terminating mode should expose
/// two distinct types.
///
/// # The contract
///
/// This trait follows [`Codec`]'s points 1–6, with one addition:
/// `process` may also resolve to [`EndCapableProgress::End`]. When it
/// does:
/// - the `consumed` input bytes belong to the ended stream,
/// - the `written` output bytes were produced by this call,
/// - `input[consumed..]` was not consumed; it belongs to whatever
///   follows the terminated stream, and
/// - the codec will never produce more output.
///
/// A codec may consume the delimiter that marks the boundary, or
/// leave it unconsumed. This behavior is specific to each format, and
/// the codec must document it.
///
/// Once `process` reaches `End`, the stream has ended for good. Every
/// later call, of any of the three methods, must keep reporting that,
/// forever, for the rest of the codec's lifetime:
/// - `process` answers `End { consumed: 0, written: 0 }` again, on
///   any `input`;
/// - `finish` and `flush` answer `Drain::Done { written: 0 }`.
///
/// None of these calls re-run whatever ended the stream. A caller —
/// or a combinator built on top of `EndCapableCodec` — may call any
/// of the three again after `End`, and must see a no-op every time,
/// not a second ending. This is the one case where `finish` and
/// `flush` stop being independent of each other: past `End`, both
/// are pinned to `Done { written: 0 }` together, not just idempotent
/// against repeats of themselves.
///
/// A codec meant to yield several tokens or boundaries from one
/// instance is not an `EndCapableCodec` in the usual driver sense.
/// Drivers such as [`Pump`](crate::stream::Pump) latch permanently
/// after the first `End`. Driving `process` directly, call by call,
/// still allows reusing one instance across multiple logical streams
/// (see `core/tests/early_stop_input.rs`). A driver that must not
/// latch has to be written with that in mind.
///
/// This crate does not impose one universal meaning on EOF before a
/// terminating codec reports `End`. If the in-band terminator is
/// required, `finish` should return an `UnexpectedEnd` error when EOF
/// arrives first. If both an in-band terminator and source EOF are
/// valid endings, `finish` may drain buffered output and return
/// `Done`. Each `EndCapableCodec` should document which rule it
/// uses.
///
/// # Naming the operation
///
/// Both `Codec` and `EndCapableCodec` deliberately name their method
/// `process`. Because of the blanket implementation, a direct method
/// call on an ordinary concrete codec can be ambiguous when both
/// traits are in scope. In that case, use Rust's fully qualified
/// syntax:
///
/// ```text
/// Codec::process(&mut codec, input, output);
/// EndCapableCodec::process(&mut codec, input, output);
/// ```
pub trait EndCapableCodec: DrainCodec {
    /// Push input bytes and pull output bytes. This may also
    /// recognize the stream's in-band end. Once it reports `End`, it
    /// keeps reporting `End` forever. Calling this after `finish` is
    /// not supported.
    fn process(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<EndCapableProgress, Error>;
}

impl<C: Codec> EndCapableCodec for C {
    fn process(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<EndCapableProgress, Error> {
        Codec::process(self, input, output).map(Into::into)
    }
}

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

/// Progress of one [`EndCapableCodec::process`] call: everything
/// [`Progress`] can report, plus an in-band end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndCapableProgress {
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

impl From<Progress> for EndCapableProgress {
    fn from(progress: Progress) -> Self {
        match progress {
            Progress::InputConsumed { written } => Self::InputConsumed { written },
            Progress::OutputFilled { consumed } => Self::OutputFilled { consumed },
        }
    }
}

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

impl EndCapableProgress {
    /// The [`Progress::validated`] counterpart for
    /// [`EndCapableCodec::process`].
    pub fn validated(
        self,
        input_len: usize,
        output_len: usize,
    ) -> Result<EndCapableProgress, Error> {
        let honest = match self {
            EndCapableProgress::InputConsumed { written } => written <= output_len,
            EndCapableProgress::OutputFilled { consumed } => consumed <= input_len,
            EndCapableProgress::End { consumed, written } => {
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
// Boxing support
// ----

// Mirrors std's `impl<R: Read + ?Sized> Read for Box<R>`: lets a `Box<dyn
// Codec>` (or a boxed concrete codec) stand in anywhere a `Codec` is
// expected, e.g. to build a runtime-determined chain of codecs.
#[cfg(feature = "alloc")]
use alloc::boxed::Box;

#[cfg(feature = "alloc")]
impl<C: DrainCodec + ?Sized> DrainCodec for Box<C> {
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        (**self).finish(output)
    }

    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        (**self).flush(output)
    }
}

#[cfg(feature = "alloc")]
impl<C: Codec + ?Sized> Codec for Box<C> {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        (**self).process(input, output)
    }
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

// ----
// Tests
// ----

#[cfg(test)]
mod tests {
    use super::{Drain, EndCapableProgress, Error, ErrorKind, Progress};

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
        assert!(EndCapableProgress::End {
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
            EndCapableProgress::End {
                consumed: 11,
                written: 0
            }
            .validated(10, 4),
            Err(CV)
        );
        assert_eq!(
            EndCapableProgress::End {
                consumed: 0,
                written: 5
            }
            .validated(10, 4),
            Err(CV)
        );
        assert_eq!(Drain::Done { written: 5 }.validated(4), Err(CV));
    }
}
