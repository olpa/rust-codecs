//! The [`Codec`] trait and the vocabulary its methods speak in:
//! [`Outcome`], [`Drain`], [`Error`]. See `CREATING-CODECS.md` for how
//! to write a codec.

/// Outcome of one [`Codec::process`] call. Every variant states an
/// invariant a driver can rely on without inspecting byte counts —
/// "made no progress and can't say why" is not expressible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
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
    StreamEnd { consumed: usize, written: usize },
}

/// Outcome of one [`Codec::finish`] or [`Codec::flush`] call.
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

/// What kind of failure a codec reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The encoded stream is malformed.
    Corrupt,
    /// The stream was cut off mid-symbol: `finish` was called while
    /// the codec still needed more input to complete a unit.
    UnexpectedEnd,
    /// The codec's internal carry buffer couldn't hold an atomic
    /// output unit — a codec bug (the carry is sized statically to the
    /// codec's largest unit), or a format whose units are unbounded.
    BufferOverrun,
    /// The codec reported byte counts exceeding the buffers it was
    /// given — a codec bug, caught at the driver's trust boundary (see
    /// [`Outcome::validated`]/[`Drain::validated`]) before it can
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
        Self { kind, consumed, written }
    }
}

impl Outcome {
    /// Check the reported byte counts against the buffer sizes the
    /// call was actually given, turning a lying codec into
    /// [`ErrorKind::ContractViolation`] instead of letting bogus
    /// counts corrupt driver state. Every driver in this crate applies
    /// this at its codec trust boundary; a driver of your own should
    /// too.
    pub fn validated(self, input_len: usize, output_len: usize) -> Result<Outcome, Error> {
        let honest = match self {
            Outcome::InputConsumed { written } => written <= output_len,
            Outcome::OutputFilled { consumed } => consumed <= input_len,
            Outcome::StreamEnd { consumed, written } => {
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
    /// The [`Outcome::validated`] counterpart for `finish`/`flush`.
    pub fn validated(self, output_len: usize) -> Result<Drain, Error> {
        match self {
            Drain::Done { written } if written > output_len => {
                Err(Error::new(ErrorKind::ContractViolation, 0, 0))
            }
            honest => Ok(honest),
        }
    }
}

/// A stateful byte-rewrite transform. See `CREATING-CODECS.md` for how
/// to write one.
///
/// The contract, in one sentence: **every call fully consumes its
/// input, fully fills its output, or ends the stream** — a codec never
/// declines a buffer as too small. Codecs with a minimum atomic output
/// unit uphold this with a [`Carry`](crate::Carry): write what fits,
/// hold the tail, deliver it first on the next call. Consequently any
/// non-empty buffer works on either side, no matter how small.
///
/// Degenerate buffers: with empty `input`, `process` drains pending
/// output (if any) and reports `InputConsumed`; with empty `output`,
/// `OutputFilled` is trivially true — drivers should avoid the
/// empty-output call, since it can't progress.
pub trait Codec {
    /// Push input bytes and pull output bytes.
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error>;

    /// Signal "no more input is coming": flush any buffered state and,
    /// for formats with one, write the trailer/checksum. Call
    /// repeatedly (draining `output` between calls) until it reports
    /// [`Drain::Done`].
    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error>;

    /// Drain any bytes this codec is withholding to a sync boundary,
    /// *without* ending the stream — unlike `finish`, the stream
    /// continues afterward. Only meaningful for codecs that buffer
    /// output for a format-defined in-band sync marker
    /// (deflate/zlib/gzip do); the default owes nothing.
    fn flush(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::{Drain, Error, ErrorKind, Outcome};

    const CV: Error = Error { kind: ErrorKind::ContractViolation, consumed: 0, written: 0 };

    #[test]
    fn validated_accepts_honest_counts() {
        assert!(Outcome::InputConsumed { written: 4 }.validated(10, 4).is_ok());
        assert!(Outcome::OutputFilled { consumed: 10 }.validated(10, 4).is_ok());
        assert!(Outcome::StreamEnd { consumed: 0, written: 0 }.validated(0, 0).is_ok());
        assert!(Drain::OutputFilled.validated(0).is_ok());
        assert!(Drain::Done { written: 4 }.validated(4).is_ok());
    }

    #[test]
    fn validated_rejects_overclaimed_counts() {
        assert_eq!(Outcome::InputConsumed { written: 5 }.validated(10, 4), Err(CV));
        assert_eq!(Outcome::OutputFilled { consumed: 11 }.validated(10, 4), Err(CV));
        assert_eq!(Outcome::StreamEnd { consumed: 11, written: 0 }.validated(10, 4), Err(CV));
        assert_eq!(Outcome::StreamEnd { consumed: 0, written: 5 }.validated(10, 4), Err(CV));
        assert_eq!(Drain::Done { written: 5 }.validated(4), Err(CV));
    }
}

// Mirrors std's `impl<R: Read + ?Sized> Read for Box<R>`: lets a `Box<dyn
// Codec>` (or a boxed concrete codec) stand in anywhere a `Codec` is
// expected, e.g. to build a runtime-determined chain of codecs.
impl<C: Codec + ?Sized> Codec for Box<C> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
        (**self).process(input, output)
    }

    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        (**self).finish(output)
    }

    fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        (**self).flush(output)
    }
}
