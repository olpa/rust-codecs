//! Adapters that turn `compcol` encoders and decoders into
//! [`BoundaryAwareCodec`]s.
//!
//! Behind the `compcol` feature flag.
//!
//! | Adapter                                   | Wraps                | Implements             |
//! |-------------------------------------------|----------------------|------------------------|
//! | [`CompcolEncoderAsBoundaryAwareCodec`]    | a `compcol::Encoder` | [`BoundaryAwareCodec`] |
//! | [`CompcolDecoderAsBoundaryAwareCodec`]    | a `compcol::Decoder` | [`BoundaryAwareCodec`] |
//!
//! To get a plain [`Codec`](crate::Codec) from either, wrap it in
//! [`BoundaryAwareCodecAsCodec`](crate::boundary_as_codec::BoundaryAwareCodecAsCodec).
//!
//! The algorithms come from your own dependency on `compcol`, for
//! example `compcol = { version = "0.7.8", features = ["gzip"] }`.
//!
//! # Example
//!
//! ```
//! # #[cfg(feature = "alloc")]
//! # {
//! use rust_codecs_core::boundary_as_codec::BoundaryAwareCodecAsCodec;
//! use rust_codecs_core::compcol::{
//!     CompcolDecoderAsBoundaryAwareCodec, CompcolEncoderAsBoundaryAwareCodec, FlushMode,
//! };
//! use rust_codecs_core::sources_and_sinks::slice::SliceSource;
//! use rust_codecs_core::sources_and_sinks::vec::VecSink;
//! use rust_codecs_core::stream_to_stream;
//!
//! // Compress
//! let encoder = BoundaryAwareCodecAsCodec::new(
//!     CompcolEncoderAsBoundaryAwareCodec::new(compcol::gzip::Encoder::new(), FlushMode::Skip),
//! );
//! let mut compressed = VecSink::default();
//! stream_to_stream(&mut SliceSource::new(b"hello, hello"), encoder, &mut compressed).unwrap();
//! let compressed = compressed.into_inner();
//!
//! // Decompress
//! let decoder = BoundaryAwareCodecAsCodec::new(
//!     CompcolDecoderAsBoundaryAwareCodec::new(compcol::gzip::Decoder::new()),
//! );
//! let mut plain = VecSink::default();
//! stream_to_stream(&mut SliceSource::new(&compressed), decoder, &mut plain).unwrap();
//! assert_eq!(plain.into_inner(), b"hello, hello");
//! # }
//! ```
//!
//! # Flush mode of the encoder
//!
//! [`CompcolEncoderAsBoundaryAwareCodec::new`] takes a [`FlushMode`]:
//!
//! - `Compcol(Flush::Sync)` and `Compcol(Flush::Full)` write a sync
//!   marker on every `flush`, even with no new input. For deflate it is
//!   5 bytes.
//! - `Compcol(Flush::Full)` also resets the history, which costs
//!   compression ratio.
//! - `Skip` keeps the best ratio, but a reader of the output cannot
//!   decode the latest data until `finish`.
//!
//! # At and after a boundary (decoder)
//!
//! A compressed stream marks its own end. When the decoder reaches it,
//! `process_bounded` returns
//! [`Boundary`](BoundaryAwareProgress::Boundary) and leaves the rest
//! of the input in the source.
//!
//! For gzip, build the decoder with `with_multi_member(false)`.
//! Otherwise compcol decodes concatenated members as one stream.
//!
//! After a `Boundary`:
//! 1. Call `finish` until [`Done`](DrainProgress::Done).
//! 2. Call `reset()` to prepare for the next logical stream.
//!
//! # After `finish` and after an error
//!
//! - After the first `finish` call, `process_bounded` and `flush` fail
//!   with [`ErrorKind::ContractViolation`]. When `finish` reports
//!   [`Done`](DrainProgress::Done), a new `finish` returns
//!   `Done { written: 0 }`.
//! - After an `Err`, compcol calls the inner codec poisoned. So the
//!   adapter latches the error. Later calls of `process_bounded`,
//!   `flush` and `finish` return the same kind and subcode, with
//!   `consumed` and `written` zero. They do not call the inner codec.
//! - `reset()` resets the inner codec and the adapter, and removes the
//!   latched error. The instance is then ready for the next logical
//!   stream.
//! - `clear_error()` removes only the latched error. It is for an error
//!   that the inner codec can recover from in place. In compcol 0.7.8,
//!   this is `compcol::Error::NeedDictionary` only: set the dictionary
//!   through `get_mut()`, call `clear_error()`, and call again.
//!
//! # Errors
//!
//! The kind of [`Error`] for each compcol error:
//!
//! | Kind                | `compcol::Error` |
//! |---------------------|------------------|
//! | `CorruptStream`     | `Corrupt`, `UnexpectedEnd`, `BadHeader`, `InvalidBlockType`, `InvalidHuffmanTree`, `InvalidDistance`, `ChecksumMismatch`, `TrailerMismatch` |
//! | `Unsupported`       | `Unsupported`, `NeedDictionary` |
//! | `BehindLimits`      | `OutputTooSmall`, `OutputLimitExceeded` |
//! | `ContractViolation` | a variant that this version does not know |
//!
//! The adapter's own checks give `ContractViolation`: a status that the
//! compcol contract excludes, a call that moves no bytes and does not
//! end the work, a call after `finish`.
//!
//! The subcode is opaque and not part of the API.
//!
//! For the original compcol error behind the latched error, call
//! `compcol_error()` on the adapter. Details such as the wanted dictionary stay in the inner
//! codec, for example `get_ref().dictionary_id()` of a zlib decoder.
//!
//! # Access to the inner codec
//!
//! `get_mut()` is for configuration: dictionaries, limits, and
//! `compcol::Decoder::discard_output`. Do not drive the codec through
//! it (`encode`/`decode`, `finish`, `flush`, `reset`): the state of the
//! adapter would no longer match. Use the adapter methods instead.
//!
//! # Performance
//!
//! compcol writes into `&mut [u8]`, but the output of our codecs is
//! `&mut [MaybeUninit<u8>]`. Each adapter call zero-fills its output
//! once before it calls compcol. See
//! <https://github.com/olpa/rust-codecs/issues/19>.

use core::mem::MaybeUninit;

use ::compcol::{
    Decoder, Encoder, Error as CompcolError, Flush as CompcolFlush, Progress as CompcolProgress,
    Status as CompcolStatus,
};

use crate::step::FinishPhase;
use crate::uninit::zero_init_mut;
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainCodec, DrainProgress, Error, ErrorKind,
};

/// Return type of a single compcol `encode`/`decode`/`finish`/`flush` call.
pub(crate) type CompcolResult = Result<(CompcolProgress, CompcolStatus), CompcolError>;

// ----
// Flush mode
// ----

/// What [`CompcolEncoderAsBoundaryAwareCodec`] does on [`DrainCodec::flush`].
/// See the module docs for the trade-offs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMode {
    /// `flush` writes nothing and returns `Done { written: 0 }`. The
    /// encoder keeps its pending bytes until `finish`.
    Skip,
    /// `flush` calls `compcol::Encoder::flush` with this mode.
    Compcol(CompcolFlush),
}

// ----
// Shared adapter core
// ----

// Atomic units
//
// An atomic unit is the smallest piece that a codec reads or writes in
// one step, for example a 4-byte header or a 5-byte sync marker. A codec
// that gets only part of a unit must keep that part in its own state,
// or it makes no progress.
//
// The adapter has no carry buffer. If the inner codec makes no progress,
// the adapter fails with `ErrorKind::ContractViolation`.
//
// The codecs of compcol 0.7.8 do not cause this error. Each of them takes
// partial input units into its own state, and keeps output that does not
// fit for the next call. So each call with a non-empty output moves at
// least one byte or ends the work. The error can come from other
// `Encoder` or `Decoder` implementations, or from later compcol versions.

/// The errors of the adapter. Private to this module.
impl Error {
    // Opaque subcodes, not part of the API.
    /// The compcol codec returned a status that its contract excludes.
    const SUBCODE_UNEXPECTED_STATUS: usize = 0x102;
    /// The compcol codec moved no bytes and did not end the work.
    const SUBCODE_NO_PROGRESS: usize = 0x103;

    /// Convert a compcol error. See "Errors" in the module docs.
    fn from_compcol(error: CompcolError, consumed: usize, written: usize) -> Self {
        let (kind, codec_subcode) = match error {
            CompcolError::Corrupt => (ErrorKind::CorruptStream, 1),
            CompcolError::UnexpectedEnd => (ErrorKind::CorruptStream, 2),
            CompcolError::OutputTooSmall => (ErrorKind::BehindLimits, 3),
            CompcolError::BadHeader => (ErrorKind::CorruptStream, 4),
            CompcolError::InvalidBlockType => (ErrorKind::CorruptStream, 5),
            CompcolError::InvalidHuffmanTree => (ErrorKind::CorruptStream, 6),
            CompcolError::InvalidDistance => (ErrorKind::CorruptStream, 7),
            CompcolError::ChecksumMismatch => (ErrorKind::CorruptStream, 8),
            CompcolError::TrailerMismatch => (ErrorKind::CorruptStream, 9),
            CompcolError::Unsupported => (ErrorKind::Unsupported, 10),
            CompcolError::OutputLimitExceeded => (ErrorKind::BehindLimits, 11),
            CompcolError::NeedDictionary => (ErrorKind::Unsupported, 12),
            // `CompcolError` is `#[non_exhaustive]`, so the compiler
            // requires this arm. It is for a variant that a later
            // compcol version adds.
            _ => (ErrorKind::ContractViolation, 0),
        };
        Self {
            kind,
            codec_subcode,
            consumed,
            written,
        }
    }

    /// An error of the kind `ContractViolation` that the adapter makes
    /// itself.
    fn contract_violation(codec_subcode: usize, consumed: usize, written: usize) -> Self {
        Self {
            kind: ErrorKind::ContractViolation,
            codec_subcode,
            consumed,
            written,
        }
    }

    /// The error for a compcol call that moved no bytes and did not end
    /// the work. compcol 0.7.8 codecs never cause it. See "Atomic units"
    /// above.
    fn no_progress(status: CompcolStatus, consumed: usize, written: usize) -> Self {
        let codec_subcode = if status == CompcolStatus::OutputFull {
            Self::SUBCODE_NO_PROGRESS
        } else {
            Self::SUBCODE_UNEXPECTED_STATUS
        };
        Self::contract_violation(codec_subcode, consumed, written)
    }
}

/// Fail if a compcol call claims more bytes than it received.
/// `consumed` and `written` are the counts before the call.
fn check_counts(
    progress: CompcolProgress,
    input_len: usize,
    output_len: usize,
    consumed: usize,
    written: usize,
) -> Result<(), Error> {
    if progress.consumed > input_len || progress.written > output_len {
        return Err(Error::contract_violation(
            Error::SUBCODE_BYTE_COUNT_CLAIM,
            consumed,
            written,
        ));
    }
    Ok(())
}

/// The latched error of the adapter.
///
/// After `Err`, compcol calls the inner codec poisoned. So the adapter
/// does not call it again. It returns this error instead.
#[derive(Debug, Clone, Copy)]
enum Failure {
    /// The inner codec returned this error.
    Compcol(CompcolError),
    /// The adapter made the error itself.
    Adapter {
        kind: ErrorKind,
        codec_subcode: usize,
    },
}

// ----
// Encoder and decoder behind one trait
// ----

mod sealed {
    use super::CompcolResult;

    /// What an encoder or a decoder adds to the adapter. The adapter
    /// calls the inner compcol codec only through this trait, so one
    /// adapter serves both.
    ///
    /// `Encoding` and `Decoding` implement it. The trait is sealed:
    /// other crates cannot name or implement it.
    pub trait CompcolKind<I> {
        /// `encode` or `decode`.
        fn step(&mut self, inner: &mut I, input: &[u8], output: &mut [u8]) -> CompcolResult;

        /// Write the output that the codec holds. If there is nothing to
        /// flush, write nothing and return `InputEmpty`.
        fn flush(&mut self, inner: &mut I, output: &mut [u8]) -> CompcolResult;

        fn finish(&mut self, inner: &mut I, output: &mut [u8]) -> CompcolResult;

        fn reset(&mut self, inner: &mut I);
    }
}

use sealed::CompcolKind;

/// The result of a flush that writes nothing. `drain_loop` turns it
/// into `Done { written: 0 }`.
const NOTHING_TO_FLUSH: CompcolResult = Ok((
    CompcolProgress {
        consumed: 0,
        written: 0,
    },
    CompcolStatus::InputEmpty,
));

/// What an encoder adds to the adapter: its [`FlushMode`]. The kind of
/// [`CompcolEncoderAsBoundaryAwareCodec`].
#[derive(Debug, Clone)]
pub struct Encoding {
    flush_mode: FlushMode,
}

impl<E: Encoder> CompcolKind<E> for Encoding {
    fn step(&mut self, inner: &mut E, input: &[u8], output: &mut [u8]) -> CompcolResult {
        inner.encode(input, output)
    }

    fn flush(&mut self, inner: &mut E, output: &mut [u8]) -> CompcolResult {
        match self.flush_mode {
            FlushMode::Skip => NOTHING_TO_FLUSH,
            FlushMode::Compcol(mode) => inner.flush(output, mode),
        }
    }

    fn finish(&mut self, inner: &mut E, output: &mut [u8]) -> CompcolResult {
        inner.finish(output)
    }

    fn reset(&mut self, inner: &mut E) {
        inner.reset();
    }
}

/// What a decoder adds to the adapter: nothing. The kind of
/// [`CompcolDecoderAsBoundaryAwareCodec`].
#[derive(Debug, Clone)]
pub struct Decoding;

impl<D: Decoder> CompcolKind<D> for Decoding {
    fn step(&mut self, inner: &mut D, input: &[u8], output: &mut [u8]) -> CompcolResult {
        inner.decode(input, output)
    }

    /// compcol decoders have no flush method.
    fn flush(&mut self, _inner: &mut D, _output: &mut [u8]) -> CompcolResult {
        NOTHING_TO_FLUSH
    }

    fn finish(&mut self, inner: &mut D, output: &mut [u8]) -> CompcolResult {
        inner.finish(output)
    }

    fn reset(&mut self, inner: &mut D) {
        inner.reset();
    }
}

// ----
// Adapter
// ----

/// A compcol encoder or decoder as a [`BoundaryAwareCodec`].
///
/// Use it as [`CompcolEncoderAsBoundaryAwareCodec`] or
/// [`CompcolDecoderAsBoundaryAwareCodec`].
#[derive(Debug, Clone)]
pub struct CompcolAsBoundaryAwareCodec<I, K> {
    inner: I,
    /// [`Encoding`] or [`Decoding`].
    kind: K,
    /// How far the adapter is in `finish`.
    finish_phase: FinishPhase,
    /// The latched error, if any.
    failure: Option<Failure>,
}

impl<I, K> CompcolAsBoundaryAwareCodec<I, K> {
    fn from_parts(inner: I, kind: K) -> Self {
        Self {
            inner,
            kind,
            finish_phase: FinishPhase::Open,
            failure: None,
        }
    }

    /// The compcol error behind the latched error. `None` if there is
    /// no latched error, or if the adapter made the error itself.
    pub fn compcol_error(&self) -> Option<CompcolError> {
        match self.failure {
            Some(Failure::Compcol(error)) => Some(error),
            Some(Failure::Adapter { .. }) | None => None,
        }
    }

    /// The inner compcol codec.
    pub fn get_ref(&self) -> &I {
        &self.inner
    }

    /// The inner compcol codec, for configuration only. See
    /// "Access to the inner codec" in the module docs.
    pub fn get_mut(&mut self) -> &mut I {
        &mut self.inner
    }

    /// Unwrap the inner compcol codec.
    pub fn into_inner(self) -> I {
        self.inner
    }

    /// Remove the latched error. Keep the state of the inner codec.
    ///
    /// This is a backdoor for an error that the inner codec can
    /// recover from in place, such as `compcol::Error::NeedDictionary`.
    /// See "After `finish` and after an error" in the module docs.
    ///
    /// `reset()` cannot do this work, because it also resets the inner
    /// codec. For example, a zlib decoder that waits for a dictionary
    /// accepts `set_dictionary` only in that state. After `reset()`, the
    /// state is gone.
    pub fn clear_error(&mut self) {
        self.failure = None;
    }

    /// Return the latched error, if any, before a call to the inner
    /// codec.
    ///
    /// The counts are zero: the call that returns the error moves no
    /// bytes.
    fn check_not_failed(&self) -> Result<(), Error> {
        match self.failure {
            Some(Failure::Compcol(error)) => Err(Error::from_compcol(error, 0, 0)),
            Some(Failure::Adapter {
                kind,
                codec_subcode,
            }) => Err(Error {
                kind,
                codec_subcode,
                consumed: 0,
                written: 0,
            }),
            None => Ok(()),
        }
    }

    /// Convert a compcol error, latch it, and return it. Later calls
    /// return it and do not call the inner codec. `compcol_error()`
    /// gives the original compcol error.
    fn latch_compcol_error(
        &mut self,
        error: CompcolError,
        consumed: usize,
        written: usize,
    ) -> Error {
        self.failure = Some(Failure::Compcol(error));
        Error::from_compcol(error, consumed, written)
    }

    /// Latch an error that the adapter makes itself, and return it.
    /// Later calls return it and do not call the inner codec. No compcol
    /// error is behind it.
    fn latch_adapter_error(&mut self, error: Error) -> Error {
        self.failure = Some(Failure::Adapter {
            kind: error.kind,
            codec_subcode: error.codec_subcode,
        });
        error
    }
}

impl<I, K: CompcolKind<I>> CompcolAsBoundaryAwareCodec<I, K> {
    /// Reset the inner codec and the adapter for a new stream.
    ///
    /// It calls `reset` of the inner compcol codec. compcol keeps the
    /// configuration, such as a dictionary or a limit. Then it resets
    /// the adapter: it removes the latched error, and makes
    /// `process_bounded` and `flush` work again after `finish`.
    pub fn reset(&mut self) {
        self.kind.reset(&mut self.inner);
        self.clear_error();
        self.finish_phase = FinishPhase::Open;
    }

    /// Repeat `step` (`encode` or `decode`) until `input` is consumed,
    /// `output` is full, or the stream ends. The stream end gives
    /// `Boundary`.
    fn transfer_loop(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        // An empty output is legal for our codecs: `Chain` and `Pump` pass
        // one when their output is full. Some compcol codecs fail on an
        // empty output (`lzx` gives `UnexpectedEnd` while it holds
        // decoded bytes), so do not call them. `OutputFilled` makes the
        // caller call again with room.
        if output.is_empty() {
            return Ok(BoundaryAwareProgress::OutputFilled { consumed: 0 });
        }
        let output = zero_init_mut(output);
        let (mut consumed, mut written) = (0, 0);
        loop {
            let rest_in = &input[consumed..];
            let rest_out = &mut output[written..];
            let (in_len, out_len) = (rest_in.len(), rest_out.len());
            let (progress, status) = self
                .kind
                .step(&mut self.inner, rest_in, rest_out)
                .map_err(|error| self.latch_compcol_error(error, consumed, written))?;
            check_counts(progress, in_len, out_len, consumed, written)
                .map_err(|error| self.latch_adapter_error(error))?;
            consumed += progress.consumed;
            written += progress.written;

            if status == CompcolStatus::StreamEnd {
                return Ok(BoundaryAwareProgress::Boundary { consumed, written });
            }
            // The order of `OutputFilled` and `InputConsumed` is not
            // important.
            if written == output.len() {
                return Ok(BoundaryAwareProgress::OutputFilled { consumed });
            }
            if consumed == input.len() {
                return Ok(BoundaryAwareProgress::InputConsumed { written });
            }
            if progress.consumed == 0 && progress.written == 0 {
                return Err(self.latch_adapter_error(Error::no_progress(status, consumed, written)));
            }
        }
    }

    /// Repeat `step` (`flush` or `finish`) until it has no more output,
    /// or `output` is full.
    fn drain_loop(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        mut step: impl FnMut(&mut K, &mut I, &mut [u8]) -> CompcolResult,
    ) -> Result<DrainProgress, Error> {
        if output.is_empty() {
            return Ok(DrainProgress::OutputFilled);
        }
        let output = zero_init_mut(output);
        let mut written = 0;
        loop {
            let rest_out = &mut output[written..];
            let out_len = rest_out.len();
            let (progress, status) = step(&mut self.kind, &mut self.inner, rest_out)
                .map_err(|error| self.latch_compcol_error(error, 0, written))?;
            check_counts(progress, 0, out_len, 0, written)
                .map_err(|error| self.latch_adapter_error(error))?;
            written += progress.written;

            if matches!(status, CompcolStatus::InputEmpty | CompcolStatus::StreamEnd) {
                return Ok(DrainProgress::Done { written });
            }
            if written == output.len() {
                return Ok(DrainProgress::OutputFilled);
            }
            if progress.written == 0 {
                return Err(self.latch_adapter_error(Error::no_progress(status, 0, written)));
            }
        }
    }
}

impl<I, K: CompcolKind<I>> DrainCodec for CompcolAsBoundaryAwareCodec<I, K> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.check_not_failed()?;
        self.finish_phase.check_open()?;
        self.drain_loop(output, K::flush)
    }

    /// If `finish` reported `Done`, a new call writes no bytes and
    /// reports `Done` again. It does not call the inner codec.
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.check_not_failed()?;
        if self.finish_phase.is_finished() {
            return Ok(DrainProgress::Done { written: 0 });
        }
        self.finish_phase.start_finish();
        let progress = self.drain_loop(output, K::finish)?;
        if matches!(progress, DrainProgress::Done { .. }) {
            self.finish_phase.finish_done();
        }
        Ok(progress)
    }
}

impl<I, K: CompcolKind<I>> BoundaryAwareCodec for CompcolAsBoundaryAwareCodec<I, K> {
    fn process_bounded(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        self.check_not_failed()?;
        self.finish_phase.check_open()?;
        self.transfer_loop(input, output)
    }
}

// ----
// Encoder adapter
// ----

/// A `compcol::Encoder` as a [`BoundaryAwareCodec`].
///
/// # Example
///
/// ```
/// # use rust_codecs_core::compcol::{CompcolEncoderAsBoundaryAwareCodec, FlushMode};
/// let codec = CompcolEncoderAsBoundaryAwareCodec::new(
///     compcol::gzip::Encoder::new(),
///     FlushMode::Skip,
/// );
/// ```
pub type CompcolEncoderAsBoundaryAwareCodec<E> = CompcolAsBoundaryAwareCodec<E, Encoding>;

impl<E> CompcolEncoderAsBoundaryAwareCodec<E> {
    /// Wrap `inner`. `flush_mode` decides what [`DrainCodec::flush`] does.
    pub fn new(inner: E, flush_mode: FlushMode) -> Self {
        Self::from_parts(inner, Encoding { flush_mode })
    }

    /// The current flush mode.
    pub fn flush_mode(&self) -> FlushMode {
        self.kind.flush_mode
    }

    /// Change the flush mode between calls.
    pub fn set_flush_mode(&mut self, flush_mode: FlushMode) {
        self.kind.flush_mode = flush_mode;
    }
}

// ----
// Decoder adapter
// ----

/// A `compcol::Decoder` as a [`BoundaryAwareCodec`].
///
/// At the end of the compressed stream it reports
/// [`Boundary`](BoundaryAwareProgress::Boundary) and leaves the rest of
/// the input in the source. To decode the next stream: call `finish`
/// until [`Done`](DrainProgress::Done), then `reset()`.
///
/// For gzip, build the decoder with `with_multi_member(false)`.
/// Otherwise compcol decodes concatenated members as one stream.
///
/// # Example
///
/// ```
/// # use rust_codecs_core::compcol::CompcolDecoderAsBoundaryAwareCodec;
/// let codec = CompcolDecoderAsBoundaryAwareCodec::new(
///     compcol::gzip::Decoder::new().with_multi_member(false),
/// );
/// ```
pub type CompcolDecoderAsBoundaryAwareCodec<D> = CompcolAsBoundaryAwareCodec<D, Decoding>;

impl<D> CompcolDecoderAsBoundaryAwareCodec<D> {
    /// Wrap `inner`.
    pub fn new(inner: D) -> Self {
        Self::from_parts(inner, Decoding)
    }
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use alloc::vec;
    use alloc::vec::Vec;

    use ::compcol::{gzip, zlib, Flush};

    use super::*;
    use crate::codecs::test_support::SplitSource;
    use crate::sources_and_sinks::vec::VecSink;
    use crate::stream_to_stream;
    use crate::uninit::as_uninit_mut;

    fn sample() -> Vec<u8> {
        let mut data = Vec::new();
        for i in 0..2000u32 {
            data.extend_from_slice(b"compcol adapter ");
            data.extend_from_slice(&i.to_le_bytes());
        }
        data
    }

    /// Run `codec` over `input` with `stream_to_stream`. The source
    /// gives at most `chunk` bytes at a time. Return the output and the
    /// number of consumed bytes.
    fn run(codec: &mut dyn BoundaryAwareCodec, input: &[u8], chunk: usize) -> (Vec<u8>, usize) {
        let mut source = SplitSource {
            bytes: input,
            pos: 0,
            chunk_size: chunk,
        };
        let mut sink = VecSink::with_growth(Vec::new(), chunk);
        let counts = stream_to_stream(&mut source, codec, &mut sink).unwrap();
        (sink.into_inner(), counts.consumed)
    }

    fn compress(encoder: impl Encoder, data: &[u8]) -> Vec<u8> {
        let mut codec = CompcolEncoderAsBoundaryAwareCodec::new(encoder, FlushMode::Skip);
        run(&mut codec, data, 4096).0
    }

    fn own_error(kind: ErrorKind, codec_subcode: usize, consumed: usize, written: usize) -> Error {
        Error {
            kind,
            codec_subcode,
            consumed,
            written,
        }
    }

    // ----
    // 1. Round trip
    // ----

    #[test]
    fn round_trip_through_all_adapters_with_small_and_large_buffers() {
        let data = sample();
        for chunk in [1, 7, 4096] {
            let mut enc =
                CompcolEncoderAsBoundaryAwareCodec::new(gzip::Encoder::new(), FlushMode::Skip);
            let (compressed, _) = run(&mut enc, &data, chunk);
            assert!(compressed.len() < data.len() / 4, "chunk {chunk}");

            let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(gzip::Decoder::new());
            let (plain, _) = run(&mut dec, &compressed, chunk);
            assert_eq!(plain, data, "chunk {chunk}");
        }
    }

    // ----
    // 2. Two gzip members
    // ----

    #[test]
    fn boundary_aware_decoder_reads_members_one_by_one() {
        let first = compress(gzip::Encoder::new(), b"first member");
        let second = compress(gzip::Encoder::new(), b"second member");
        let mut input = first.clone();
        input.extend_from_slice(&second);
        input.extend_from_slice(b"tail");

        for chunk in [1, 4096] {
            let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(
                gzip::Decoder::new().with_multi_member(false),
            );
            let (one, consumed) = run(&mut dec, &input, chunk);
            assert_eq!(one, b"first member", "chunk {chunk}");
            assert_eq!(consumed, first.len(), "chunk {chunk}");

            dec.reset();
            let rest = &input[consumed..];
            let (two, consumed) = run(&mut dec, rest, chunk);
            assert_eq!(two, b"second member", "chunk {chunk}");
            assert_eq!(&rest[consumed..], b"tail", "chunk {chunk}");
        }
    }

    // ----
    // 3. NeedDictionary
    // ----

    #[test]
    fn need_dictionary_recovers_through_get_mut() {
        let dictionary = b"a preset dictionary for zlib";
        let compressed = compress(
            zlib::Encoder::new().with_dictionary(dictionary),
            b"a preset dictionary helps",
        );

        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(zlib::Decoder::new());
        let mut out = [0u8; 64];
        let error = dec
            .process_bounded(&compressed, as_uninit_mut(&mut out))
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(error.codec_subcode, 12);
        assert_eq!(dec.compcol_error(), Some(::compcol::Error::NeedDictionary));
        assert!(dec.get_ref().dictionary_id().is_some());

        dec.get_mut().set_dictionary(dictionary).unwrap();
        let rest = &compressed[error.consumed..];
        assert_eq!(
            dec.process_bounded(rest, as_uninit_mut(&mut out)),
            Err(own_error(ErrorKind::Unsupported, 12, 0, 0)),
            "the error stays latched until clear_error"
        );
        dec.clear_error();
        assert_eq!(dec.compcol_error(), None);
        let progress = dec.process_bounded(rest, as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            progress,
            BoundaryAwareProgress::Boundary {
                consumed: rest.len(),
                written: 25
            }
        );
        assert_eq!(&out[..25], b"a preset dictionary helps");
    }

    // ----
    // 4. After finish
    // ----

    #[test]
    fn calls_after_finish_fail_until_reset() {
        let after_finish = own_error(
            ErrorKind::ContractViolation,
            Error::SUBCODE_AFTER_FINISH,
            0,
            0,
        );
        let compressed = compress(gzip::Encoder::new(), b"once");
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(gzip::Decoder::new());
        run(&mut dec, &compressed, 64);

        let mut out = [0u8; 64];
        assert_eq!(
            dec.process_bounded(b"x", as_uninit_mut(&mut out)),
            Err(after_finish)
        );
        assert_eq!(dec.flush(as_uninit_mut(&mut out)), Err(after_finish));
        assert_eq!(
            dec.finish(as_uninit_mut(&mut out)),
            Ok(DrainProgress::Done { written: 0 })
        );
        assert_eq!(dec.compcol_error(), None);

        dec.reset();
        assert_eq!(run(&mut dec, &compressed, 64).0, b"once");
    }

    // ----
    // 5. Flush modes (encoder)
    // ----

    #[test]
    fn flush_sync_writes_a_decodable_prefix() {
        let mut enc = CompcolEncoderAsBoundaryAwareCodec::new(
            zlib::Encoder::new(),
            FlushMode::Compcol(Flush::Sync),
        );
        let mut buf = vec![0u8; 256];
        let mut compressed = Vec::new();
        let input = b"hello, sync";
        match enc.process_bounded(input, as_uninit_mut(&mut buf)).unwrap() {
            BoundaryAwareProgress::InputConsumed { written } => {
                compressed.extend_from_slice(&buf[..written]);
            }
            other => panic!("unexpected: {other:?}"),
        }
        loop {
            match enc.flush(as_uninit_mut(&mut buf)).unwrap() {
                DrainProgress::OutputFilled => compressed.extend_from_slice(&buf),
                DrainProgress::Done { written } => {
                    compressed.extend_from_slice(&buf[..written]);
                    break;
                }
            }
        }

        // The sync-flushed prefix is decodable: process_bounded decodes the
        // input and returns either Boundary or InputConsumed.
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(zlib::Decoder::new());
        let mut out = [0u8; 64];
        let progress = dec
            .process_bounded(&compressed, as_uninit_mut(&mut out))
            .unwrap();
        let written = match progress {
            BoundaryAwareProgress::Boundary { written, .. } => written,
            BoundaryAwareProgress::InputConsumed { written } => written,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(&out[..written], input as &[u8]);
    }
}
