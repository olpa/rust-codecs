//! The vocabulary and contracts for codecs and I/O backends.

use core::mem::MaybeUninit;

// ----
// I/O backend contracts
// ----

/// A byte source that lends its input in chunks.
pub trait Source {
    type Error;

    /// Return the current non-empty chunk, or `None` at end of input.
    ///
    /// A caller may consume only part of the chunk; the next call
    /// returns at least the rest, possibly with more data appended.
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error>;

    /// Consume the first `amount` bytes of the current chunk.
    ///
    /// Returns no error; it only updates accounting. Any fallible
    /// work belongs in `chunk`.
    ///
    /// A caller must not consume more bytes than the current chunk
    /// holds; behavior on violation is implementation-defined (may
    /// panic).
    fn consume(&mut self, amount: usize);
}

/// A byte destination that lends writable space to the caller.
pub trait Sink {
    type Error;

    /// Return writable space, or `None` when the destination is full.
    ///
    /// A caller may call this again without committing anything; the
    /// same span may come back again, so bytes written but not
    /// committed may be overwritten.
    fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error>;

    /// Commit the first `amount` bytes of the space returned by `spare`.
    ///
    /// Returns an error on failure. Unlike `Source::consume`, I/O is
    /// possible here.
    ///
    /// A caller must not commit a byte it did not initialize, nor
    /// commit more bytes than the space returned by `spare` holds;
    /// behavior on violation is implementation-defined (may panic).
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error>;

    /// Complete the destination after the codec stream has ended.
    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Flush this sink, ensuring that all intermediately buffered
    /// contents reach their destination.
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
    fn sync_flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }

    /// Tell the codec that no more input will come. The codec flushes
    /// any buffered state. If the format has a trailer or a checksum,
    /// the codec writes it now.
    ///
    /// One call may not be enough. If `output` is too small to hold
    /// all pending output, `finish` returns [`DrainProgress::OutputFilled`]
    /// instead of [`DrainProgress::Done`]. Call `finish` repeatedly until it
    /// returns [`DrainProgress::Done`].
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error>;
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
    /// [`Done`](DrainProgress::Done).
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

/// Progress of one [`Codec::process`] call.
///
/// A call may satisfy both conditions at once: it consumes all of
/// `input` and fills all of `output` on the same call. The choice
/// between the two variants is then not defined. The caller must
/// accept either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// The call consumed all of `input` and produced `written` bytes.
    /// `written` may be zero.
    InputConsumed { written: usize },
    /// The call filled all of `output` and took `consumed` bytes of
    /// input. `consumed` may be zero.
    OutputFilled { consumed: usize },
}

/// Progress of one [`BoundaryAwareCodec::process`] call: everything
/// [`Progress`] can report, plus [`Boundary`](BoundaryAwareProgress::Boundary),
/// an in-band signal that the logical stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryAwareProgress {
    /// The call consumed all of `input` and produced `written` bytes.
    /// `written` may be zero.
    InputConsumed { written: usize },
    /// The call filled all of `output` and took `consumed` bytes of
    /// input. `consumed` may be zero.
    OutputFilled { consumed: usize },
    /// The current logical stream ended in-band. `consumed` and
    /// `written` need not reach the buffer lengths.
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
pub enum DrainProgress {
    /// The call filled all of `output`. More output is pending.
    OutputFilled,
    /// The call delivered the last `written` bytes.
    Done { written: usize },
}

impl Progress {
    /// Check the reported byte counts against the buffer sizes the
    /// call was given. This turns a lying codec into
    /// [`ErrorKind::ByteCountClaim`].
    pub fn validated(self, input_len: usize, output_len: usize) -> Result<Progress, Error> {
        let honest = match self {
            Progress::InputConsumed { written } => written <= output_len,
            Progress::OutputFilled { consumed } => consumed <= input_len,
        };
        if honest {
            Ok(self)
        } else {
            Err(Error::new(ErrorKind::ByteCountClaim, 0, 0))
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
            Err(Error::new(ErrorKind::ByteCountClaim, 0, 0))
        }
    }
}

impl DrainProgress {
    /// The [`Progress::validated`] counterpart for `finish`/`flush`.
    pub fn validated(self, output_len: usize) -> Result<DrainProgress, Error> {
        match self {
            DrainProgress::Done { written } if written > output_len => {
                Err(Error::new(ErrorKind::ByteCountClaim, 0, 0))
            }
            honest => Ok(honest),
        }
    }
}

/// Byte counts produced by a codec call or accumulated across a transfer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransferCounts {
    pub consumed: usize,
    pub written: usize,
}

// ----
// Boxing support
// ----

// Lets a `Box<dyn Codec>` stand in anywhere a `Codec` is expected.
#[cfg(feature = "alloc")]
use alloc::boxed::Box;

#[cfg(feature = "alloc")]
impl<C: DrainCodec + ?Sized> DrainCodec for Box<C> {
    fn sync_flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        (**self).sync_flush(output)
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
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
    CorruptStream,
    /// The stream ended where it should not have.
    UnexpectedEnd,
    /// The codec's internal carry buffer couldn't hold an atomic data unit.
    CodecBufferTooSmall,
    /// The codec reported byte counts exceeding the buffers it was
    /// given. The error's `consumed`/`written` can't be trusted, so
    /// validation (such as [`Progress::validated`]) zeroes them.
    ByteCountClaim,
}

/// A codec failure.
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

    /// The [`Progress::validated`] counterpart for a failing call.
    pub fn validated(self, input_len: usize, output_len: usize) -> Result<Self, Self> {
        if self.consumed <= input_len && self.written <= output_len {
            Ok(self)
        } else {
            Err(Self::new(ErrorKind::ByteCountClaim, 0, 0))
        }
    }
}
