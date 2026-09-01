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

/// Operations that tell a codec that input has ended or request a
/// sync point. Both [`Codec`] and [`EndSignallingCodec`] use these
/// operations.
pub trait DrainCodec {
    /// Tell the codec that no more input will come. The codec flushes
    /// any buffered state. If the format has a trailer or a checksum,
    /// the codec writes it now.
    ///
    /// One call may not be enough. If `output` is too small to hold
    /// all pending output, `finish` returns
    /// `Drain::OutputFilled` instead of `Drain::Done`. Call `finish`
    /// again, and drain `output` between calls, until it returns
    /// [`Drain::Done`]. If a driver treats `OutputFilled` as success,
    /// it truncates the stream. Part of the trailer, checksum, or
    /// padding will be missing.
    ///
    /// Once `finish` returns `Done`, it is idempotent: see [`Codec`]
    /// contract point 3. `finish` must always return one of three
    /// results: `OutputFilled`, `Done`, or `Err`. See [`Codec`]
    /// contract point 6. An [`EndSignallingCodec`] driver must not
    /// forward this call after `process` has returned
    /// [`EndSignallingProgress::End`]; see that trait's contract.
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error>;

    /// Write all buffered output up to a sync boundary. This does not
    /// end the stream. More calls to `process` may follow.
    ///
    /// Override this method only when the format defines a sync
    /// marker and the codec buffers output for it. Deflate, zlib, and
    /// gzip are examples. The default implementation has no pending
    /// output and does nothing.
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

/// A stateful transform for a complete byte stream. `process` never
/// declares the stream complete. Call `finish` to signal the end of
/// input.
///
/// # The contract
///
/// 1) Each successful call to `process` does one of two things:
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
/// `finish` and `flush` become idempotent separately after they return
/// [`Drain::Done`]. Suppose `finish` returns `Done` and no later call
/// to `process` supplies new input. Each later call to `finish` must
/// return `Drain::Done { written: 0 }`. It must not write another
/// trailer. The same rule applies to repeated `flush` calls: they must
/// not write another sync marker. This permits a caller to repeat an
/// operation after an interruption in a larger composition.
///
/// A completed `flush` does not complete `finish`. `Done` from
/// `flush` means only that the codec reached a sync point; the stream
/// stays open. If `finish` is called afterward, with no new `process`
/// input in between, it still does its real work and produces the
/// actual trailer. The reverse order, calling `flush` after `finish`,
/// is covered by point 5 and is not generally defined.
///
/// 4)
/// The codec state after `Err` is not defined. A later call can fail
/// again or make normal progress. A caller that requires a permanent
/// failure state must implement that state itself.
///
/// The error's `consumed` and `written` fields report the exact input
/// and output prefixes already processed by that call. Both counts
/// must fit within the slices passed to the call, just like counts in
/// a successful result.
///
/// 5)
/// This contract does not define a call to `process` or `flush` after
/// `finish`. A codec without a trailer or terminal state can continue
/// to process input. A codec that has closed its format, for example
/// by writing a final checksum, should return `Err`. Both behaviors
/// are valid. A caller must not depend on either behavior.
///
/// 6)
/// `finish` and `flush` must always return one of three results:
/// [`Drain::OutputFilled`], [`Drain::Done`], or `Err`. This is the
/// `Drain` counterpart of point 1. `Drain::OutputFilled` carries no
/// count. Unlike `Progress::OutputFilled`, it is not a
/// partial-progress report. It means that the call filled the entire
/// non-empty `output` slice. A call must not report `OutputFilled`
/// without filling a non-empty output slice.
///
/// Requiring one side to complete prevents ambiguous zero-progress
/// stalls and keeps drivers simple. Codecs that need larger input or
/// output units must buffer those units internally. The implementation
/// patterns and fuller rationale are in `CREATING-CODECS.md`.
///
/// # Creating a codec
///
/// See `CREATING-CODECS.md` for how to write one.
///
pub trait Codec: DrainCodec {
    /// Push input bytes and pull output bytes.
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error>;
}

/// A stateful byte transform that can recognize its logical end
/// inside an input slice. This is an in-band end of a self-terminating
/// format. It is not an error or a cancellation.
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
/// Every [`Codec`] is automatically an `EndSignallingCodec` that never
/// returns `End`; see the blanket implementation below. An
/// `EndSignallingCodec` cannot be used where `Codec` is required. It
/// cannot promise to consume the complete logical input stream. One
/// concrete type also cannot implement both traits with different
/// behavior. A format that has an ordinary mode and a terminating
/// mode should provide two distinct types.
///
/// # The contract
///
/// This trait follows [`Codec`]'s points 1–6, with one addition: a
/// successful `process` call may also resolve to
/// [`EndSignallingProgress::End`]. When it does:
/// - the `consumed` input bytes belong to the ended stream,
/// - the `written` output bytes were produced by this call,
/// - `input[consumed..]` was not consumed; it belongs to whatever
///   follows the terminated stream, and
/// - the current logical driving operation is complete.
///
/// A codec may consume the delimiter that marks the boundary, or
/// leave it unconsumed. This behavior is specific to each format, and
/// the codec must document it.
///
/// Once `process` reaches `End`, a driver must stop driving the
/// current logical stream. It must not forward later `process`,
/// `finish`, or `flush` calls to the codec as part of that stream.
/// Lifecycle wrappers such as [`Pump`](crate::stream::Pump) latch the
/// signal and answer later calls themselves with zero-progress
/// terminal results.
///
/// This trait does not define what direct codec calls do
/// after `End`. A concrete codec may document that its instance can
/// be reused for another logical stream; another may have entered a
/// permanently terminal internal state. Generic code must rely on
/// neither behavior.
///
/// EOF before `End` does not have one required meaning. If the format
/// requires an in-band terminator, `finish` should return an
/// `UnexpectedEnd` error when EOF occurs first. If either the
/// terminator or EOF can end the stream, `finish` may write buffered
/// output and return `Done`. Each `EndSignallingCodec` should document
/// its rule.
///
/// # Naming the operation
///
/// Both `Codec` and `EndSignallingCodec` deliberately name their method
/// `process`. Because of the blanket implementation, a direct method
/// call on an ordinary concrete codec can be ambiguous when both
/// traits are in scope. In that case, use Rust's fully qualified
/// syntax:
///
/// ```text
/// Codec::process(&mut codec, input, output);
/// EndSignallingCodec::process(&mut codec, input, output);
/// ```
pub trait EndSignallingCodec: DrainCodec {
    /// Push input bytes and pull output bytes. This may also
    /// recognize the current logical stream's in-band end. Calling
    /// this after `finish` or after it has returned `End` is not
    /// specified by this trait.
    fn process(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<EndSignallingProgress, Error>;
}

impl<C: Codec> EndSignallingCodec for C {
    fn process(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<EndSignallingProgress, Error> {
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

/// Progress of one [`EndSignallingCodec::process`] call: everything
/// [`Progress`] can report, plus an in-band end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndSignallingProgress {
    /// All of `input` was consumed; `written` bytes were produced
    /// (possibly zero, when everything went into internal buffering).
    /// The driver's move: supply more input, or `finish`.
    InputConsumed { written: usize },
    /// All of `output` was filled; `consumed` bytes of input were
    /// taken (possibly zero, when output pending from an earlier call
    /// filled the buffer by itself). The driver's move: drain the
    /// output and call again.
    OutputFilled { consumed: usize },
    /// The current logical stream ended in-band. Input past its end
    /// was left unconsumed, and neither side is necessarily "full".
    End { consumed: usize, written: usize },
}

impl From<Progress> for EndSignallingProgress {
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

impl EndSignallingProgress {
    /// The [`Progress::validated`] counterpart for
    /// [`EndSignallingCodec::process`].
    pub fn validated(
        self,
        input_len: usize,
        output_len: usize,
    ) -> Result<EndSignallingProgress, Error> {
        let honest = match self {
            EndSignallingProgress::InputConsumed { written } => written <= output_len,
            EndSignallingProgress::OutputFilled { consumed } => consumed <= input_len,
            EndSignallingProgress::End { consumed, written } => {
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
    use super::{Drain, EndSignallingProgress, Error, ErrorKind, Progress};

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
        assert!(EndSignallingProgress::End {
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
            EndSignallingProgress::End {
                consumed: 11,
                written: 0
            }
            .validated(10, 4),
            Err(CV)
        );
        assert_eq!(
            EndSignallingProgress::End {
                consumed: 0,
                written: 5
            }
            .validated(10, 4),
            Err(CV)
        );
        assert_eq!(Drain::Done { written: 5 }.validated(4), Err(CV));
    }
}
