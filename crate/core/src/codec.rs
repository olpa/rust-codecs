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
