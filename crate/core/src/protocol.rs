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

/// Shared supertrait with [`Self::sync_flush`] and [`Self::finish`].
///
/// This trait is a technical artifact, not a standalone abstraction.
/// Write code against [`Codec`] or [`BoundaryAwareCodec`] instead.
pub trait DrainCodec {
    /// Deflate, zlib, and similar codecs support this: they write
    /// buffered output and a sync marker. Most codecs do not need
    /// `sync_flush`.
    fn sync_flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }

    /// Tell the codec that no more input will come. The codec flushes
    /// any buffered state. If the format has a trailer or a checksum,
    /// the codec writes it now.
    ///
    /// One call may not be enough. If `output` is too small to hold
    /// all pending output, `finish` returns [`Drain::OutputFilled`]
    /// instead of [`Drain::Done`]. Call `finish` repeatedly until it
    /// returns [`Drain::Done`].
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error>;
}

/// A stateful transform for a complete chunked stream.
///
/// See `CREATING-CODECS.md` for how to write one.
pub trait Codec: DrainCodec {
    /// Transform one chunk of input into output.
    ///
    /// 1) Each successful call does one of two things:
    /// - it consumes **all** of `input`, reporting [`Progress::InputConsumed`], or
    /// - it fills **all** of `output`, reporting [`Progress::OutputFilled`].
    ///
    /// 2) Each call reads `input` and writes `output` starting at byte 0 of
    /// the slices it was given.
    ///
    /// The codec does not remember a "leftover" position from the previous call.
    /// If a call did not consume all of `input`, the caller must retain those
    /// bytes and supply them again.
    ///
    /// 3) The codec state after `Err` is not defined. A later call can fail
    /// again or make normal progress.
    ///
    /// 4) This contract does not define a call to `process` or
    /// [`sync_flush`](DrainCodec::sync_flush) after `finish`. Two
    /// behaviors are valid:
    /// - a codec without a trailer or terminal state may continue to
    ///   process input, or
    /// - a codec that has closed its format, for example by writing a
    ///   final checksum, may return `Err`.
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error>;
}

/// A stateful transform that can recognize the logical end of its input
/// inside a byte stream.
///
/// It leaves the rest of the source available to whatever comes next.
///
/// Every [`Codec`] is automatically a `BoundaryAwareCodec` that never
/// returns [`Boundary`](BoundaryAwareProgress::Boundary); see the
/// blanket implementation.
pub trait BoundaryAwareCodec: DrainCodec {
    /// This trait follows [`Codec::process`]'s contract, with one addition:
    /// a successful `process` call may also resolve to
    /// [`BoundaryAwareProgress::Boundary`].
    ///
    /// Once `process` reaches [`Boundary`](BoundaryAwareProgress::Boundary), a
    /// driver must stop driving the current logical stream. Lifecycle
    /// wrappers such as [`Pump`](crate::stream::Pump) latch the signal
    /// and answer later calls themselves with zero-progress terminal
    /// results.
    ///
    /// This trait does not define what direct codec calls do after
    /// [`Boundary`](BoundaryAwareProgress::Boundary). A concrete codec may
    /// document that its instance can be reused for another logical
    /// stream; another may have entered a permanently terminal internal
    /// state.
    ///
    /// If EOF arrives before `process` reports
    /// [`Boundary`](BoundaryAwareProgress::Boundary), `finish` decides what that
    /// means. If the format requires an in-band terminator, `finish` may
    /// report [`ErrorKind::UnexpectedEnd`]. If the format treats EOF
    /// itself as a valid end, `finish` may instead return
    /// [`Done`](Drain::Done).
    fn process(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error>;
}

impl<C: Codec> BoundaryAwareCodec for C {
    fn process(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
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

/// Progress of one [`BoundaryAwareCodec::process`] call: everything
/// [`Progress`] can report, plus an in-band end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryAwareProgress {
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
    Boundary { consumed: usize, written: usize },
}

impl From<Progress> for BoundaryAwareProgress {
    fn from(progress: Progress) -> Self {
        match progress {
            Progress::InputConsumed { written } => Self::InputConsumed { written },
            Progress::OutputFilled { consumed } => Self::OutputFilled { consumed },
        }
    }
}

/// Progress of one [`DrainCodec::finish`] or [`DrainCodec::sync_flush`]
/// call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drain {
    /// All of `output` was filled and there is more to come — call
    /// again with a fresh (or drained) buffer.
    OutputFilled,
    /// Everything owed was delivered; the final `written` bytes (at
    /// most the output's length) landed in this call. For `finish`
    /// this is the end of the stream; for `sync_flush`, the sync
    /// point.
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

impl BoundaryAwareProgress {
    /// The [`Progress::validated`] counterpart for
    /// [`BoundaryAwareCodec::process`].
    pub fn validated(
        self,
        input_len: usize,
        output_len: usize,
    ) -> Result<BoundaryAwareProgress, Error> {
        let honest = match self {
            BoundaryAwareProgress::InputConsumed { written } => written <= output_len,
            BoundaryAwareProgress::OutputFilled { consumed } => consumed <= input_len,
            BoundaryAwareProgress::Boundary { consumed, written } => {
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
    fn sync_flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        (**self).sync_flush(output)
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        (**self).finish(output)
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
    use super::{BoundaryAwareProgress, Drain, Error, ErrorKind, Progress};

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
        assert!(BoundaryAwareProgress::Boundary {
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
            BoundaryAwareProgress::Boundary {
                consumed: 11,
                written: 0
            }
            .validated(10, 4),
            Err(CV)
        );
        assert_eq!(
            BoundaryAwareProgress::Boundary {
                consumed: 0,
                written: 5
            }
            .validated(10, 4),
            Err(CV)
        );
        assert_eq!(Drain::Done { written: 5 }.validated(4), Err(CV));
    }
}
