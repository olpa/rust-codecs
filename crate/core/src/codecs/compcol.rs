//! Adapters that turn `compcol` encoders and decoders
//! (<https://docs.rs/compcol/>) into codecs of this crate.
//!
//! Behind the `compcol` feature flag.
//!
//! # Which adapter
//!
//! | Adapter                                   | Wraps                | Implements             |
//! |-------------------------------------------|----------------------|------------------------|
//! | [`CompcolEncoderAsCodec`]                 | a `compcol::Encoder` | [`Codec`]              |
//! | [`CompcolDecoderAsCodec`]                 | a `compcol::Decoder` | [`Codec`]              |
//! | [`CompcolDecoderAsBoundaryAwareCodec`]    | a `compcol::Decoder` | [`BoundaryAwareCodec`] |
//!
//! A compressed stream marks its own end. Choose the decoder adapter
//! by what can follow that end:
//!
//! - If data can follow the stream (the next gzip member, the next
//!   file of an archive, a protocol message), use
//!   [`CompcolDecoderAsBoundaryAwareCodec`]. It reports
//!   [`Boundary`](BoundaryAwareProgress::Boundary) and leaves the rest
//!   of the input in the source.
//! - [`CompcolDecoderAsCodec`] promises that the input is one stream
//!   and nothing follows it. Data after the end breaks this promise and
//!   gives [`ErrorKind::ContractViolation`].
//!
//! The algorithms come from your own dependency on `compcol`, for
//! example `compcol = { version = "0.7.8", features = ["gzip"] }`.
//!
//! # Example
//!
//! ```
//! # #[cfg(feature = "alloc")]
//! # {
//! use rust_codecs_core::compcol::{CompcolDecoderAsCodec, CompcolEncoderAsCodec, FlushMode};
//! use rust_codecs_core::sources_and_sinks::slice::SliceSource;
//! use rust_codecs_core::sources_and_sinks::vec::VecSink;
//! use rust_codecs_core::stream_to_stream;
//!
//! // Compress
//! let encoder = CompcolEncoderAsCodec::new(compcol::gzip::Encoder::new(), FlushMode::Skip);
//! let mut compressed = VecSink::default();
//! stream_to_stream(&mut SliceSource::new(b"hello, hello"), encoder, &mut compressed).unwrap();
//! let compressed = compressed.into_inner();
//!
//! // Decompress
//! let decoder = CompcolDecoderAsCodec::new(compcol::gzip::Decoder::new());
//! let mut plain = VecSink::default();
//! stream_to_stream(&mut SliceSource::new(&compressed), decoder, &mut plain).unwrap();
//! assert_eq!(plain.into_inner(), b"hello, hello");
//! # }
//! ```
//!
//! # Flush mode of the encoder
//!
//! The library cannot know the right flush for the application, so
//! [`CompcolEncoderAsCodec::new`] takes a [`FlushMode`]:
//!
//! - `Compcol(Flush::Sync)` and `Compcol(Flush::Full)` write a sync
//!   marker on every `flush`, even with no new input. For deflate it is
//!   5 bytes. [`CodecWriter`](crate::sources_and_sinks::std_io::CodecWriter)
//!   calls `flush` from `Write::flush`, so frequent `Write::flush` calls
//!   make the output larger.
//! - `Compcol(Flush::Full)` also resets the history, which costs
//!   compression ratio. It makes a point where a reader can start
//!   decoding.
//! - `Skip` keeps the best ratio, but a reader of the output cannot
//!   decode the latest data until `finish`.
//!
//! The decoder adapters have no mode. Their `flush` only writes decoded
//! bytes that the decoder already holds.
//!
//! # After `finish` and after an error
//!
//! - When `finish` reports [`Done`](DrainProgress::Done), the adapter is
//!   finished. Then `process`, `process_bounded` and `flush` fail with
//!   [`ErrorKind::ContractViolation`]. A second `finish` returns
//!   `Done { written: 0 }`.
//! - `reset()` resets the inner codec and the adapter. The instance is
//!   then ready for the next logical stream.
//! - After an `Err`, compcol calls the codec poisoned. `reset()`
//!   recovers it. `compcol::Error::NeedDictionary` is the exception: set
//!   the dictionary through `inner_mut()` and call again.
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
//! The adapter's own checks give `ContractViolation` (data after the
//! end, a status that the compcol contract excludes, a call after
//! `finish`) or `BehindLimits` (an output too small for one step of the
//! codec).
//!
//! The subcode is opaque and not part of the API.
//!
//! For the original compcol error, call `compcol_error()` on the
//! adapter. Details such as the wanted dictionary stay in the inner
//! codec, for example `inner().dictionary_id()` of a zlib decoder.
//!
//! # Access to the inner codec
//!
//! `inner_mut()` is for configuration: dictionaries, limits, and
//! `compcol::Decoder::discard_output`. Do not drive the codec through
//! it (`encode`, `decode`, `finish`, `flush`, `reset`): the state of the
//! adapter would no longer match. Use the adapter methods instead.
//!
//! # Performance
//!
//! compcol writes into `&mut [u8]`, but the output of our codecs is
//! `&mut [MaybeUninit<u8>]`. Each adapter call zero-fills its output
//! once before it calls compcol. See
//! <https://github.com/olpa/rust-codecs/issues/19>.
//!
//! # Limitation
//!
//! If the output is too small for one atomic step of the compcol codec,
//! the adapter fails with [`ErrorKind::BehindLimits`].
//! Real compcol codecs write byte by byte, so this happens only with
//! unusual `Encoder` or `Decoder` implementations. A carry buffer in the adapter is planned.

use core::mem::MaybeUninit;

use ::compcol::{Decoder, Encoder, Flush, Status};

use crate::uninit::zero_init_mut;
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, Error, ErrorKind,
    Progress,
};

// ----
// Subcodes
// ----

// Opaque numbers, not part of the API. The numbers of the compcol
// errors are written in `kind_and_subcode`.

/// The output is too small for one step of the codec. The same number
/// as `compcol::Error::OutputTooSmall`.
const SUBCODE_OUTPUT_TOO_SMALL: usize = 3;
/// [`CompcolDecoderAsCodec`] found data after the end of the stream.
const SUBCODE_DATA_AFTER_END: usize = 0x101;
/// The compcol codec returned a status that its contract excludes, for
/// example `StreamEnd` from `encode`.
const SUBCODE_UNEXPECTED_STATUS: usize = 0x102;

/// Map a compcol error to the kind and subcode of our [`Error`].
fn kind_and_subcode(error: ::compcol::Error) -> (ErrorKind, usize) {
    use ::compcol::Error as E;
    use ErrorKind::*;
    match error {
        E::Corrupt => (CorruptStream, 1),
        E::UnexpectedEnd => (CorruptStream, 2),
        E::OutputTooSmall => (BehindLimits, SUBCODE_OUTPUT_TOO_SMALL),
        E::BadHeader => (CorruptStream, 4),
        E::InvalidBlockType => (CorruptStream, 5),
        E::InvalidHuffmanTree => (CorruptStream, 6),
        E::InvalidDistance => (CorruptStream, 7),
        E::ChecksumMismatch => (CorruptStream, 8),
        E::TrailerMismatch => (CorruptStream, 9),
        E::Unsupported => (Unsupported, 10),
        E::OutputLimitExceeded => (BehindLimits, 11),
        E::NeedDictionary => (Unsupported, 12),
        // `compcol::Error` is `#[non_exhaustive]`.
        _ => (ContractViolation, 0),
    }
}

// ----
// Flush mode
// ----

/// What [`CompcolEncoderAsCodec`] does on [`DrainCodec::flush`]. See
/// the module docs for the trade-offs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMode {
    /// `flush` writes nothing and returns `Done { written: 0 }`. The
    /// encoder keeps its pending bytes until `finish`.
    Skip,
    /// `flush` calls `compcol::Encoder::flush` with this mode.
    Compcol(Flush),
}

// ----
// The loops shared by the adapters
// ----

/// The state that all three adapters share.
#[derive(Debug, Clone, Copy, Default)]
struct AdapterState {
    finished: bool,
    compcol_error: Option<::compcol::Error>,
}

/// A failure of an adapter call, before it is reported.
struct Failure {
    error: Error,
    /// The compcol error behind `error`, or `None` if the adapter
    /// itself detected the failure.
    compcol: Option<::compcol::Error>,
}

impl Failure {
    fn own(kind: ErrorKind, codec_subcode: usize, consumed: usize, written: usize) -> Self {
        Self {
            error: Error {
                kind,
                codec_subcode,
                consumed,
                written,
            },
            compcol: None,
        }
    }

    fn compcol(error: ::compcol::Error, consumed: usize, written: usize) -> Self {
        let (kind, codec_subcode) = kind_and_subcode(error);
        Self {
            error: Error {
                kind,
                codec_subcode,
                consumed,
                written,
            },
            compcol: Some(error),
        }
    }

    fn after_finish() -> Self {
        Self::own(
            ErrorKind::ContractViolation,
            Error::SUBCODE_AFTER_FINISH,
            0,
            0,
        )
    }
}

impl AdapterState {
    /// Record the compcol error behind a failure and return our error.
    fn report(&mut self, failure: Failure) -> Error {
        self.compcol_error = failure.compcol;
        failure.error
    }

    fn check_not_finished(&self) -> Result<(), Failure> {
        if self.finished {
            Err(Failure::after_finish())
        } else {
            Ok(())
        }
    }
}

/// How a run of `encode` or `decode` calls ended.
enum Outcome {
    InputConsumed { written: usize },
    OutputFilled { consumed: usize },
    StreamEnd { consumed: usize, written: usize },
}

type CompcolResult = Result<(::compcol::Progress, Status), ::compcol::Error>;

/// Check that the inner codec did not claim more bytes than it got.
fn check_counts(
    progress: ::compcol::Progress,
    input_len: usize,
    output_len: usize,
    consumed: usize,
    written: usize,
) -> Result<(), Failure> {
    if progress.consumed <= input_len && progress.written <= output_len {
        Ok(())
    } else {
        Err(Failure::own(
            ErrorKind::ContractViolation,
            Error::SUBCODE_BYTE_COUNT_CLAIM,
            consumed,
            written,
        ))
    }
}

/// Call `encode` or `decode` until the input is consumed, the output is
/// full, or the stream ends. The order of the checks matters: see the
/// comments.
fn run_process(
    input: &[u8],
    output: &mut [u8],
    mut call: impl FnMut(&[u8], &mut [u8]) -> CompcolResult,
) -> Result<Outcome, Failure> {
    if output.is_empty() {
        return Ok(Outcome::OutputFilled { consumed: 0 });
    }
    let (mut c, mut w) = (0, 0);
    loop {
        let rest_in = &input[c..];
        let rest_out = &mut output[w..];
        let (in_len, out_len) = (rest_in.len(), rest_out.len());
        let (progress, status) = call(rest_in, rest_out).map_err(|e| Failure::compcol(e, c, w))?;
        check_counts(progress, in_len, out_len, c, w)?;
        c += progress.consumed;
        w += progress.written;

        if status == Status::StreamEnd {
            return Ok(Outcome::StreamEnd {
                consumed: c,
                written: w,
            });
        }
        // Full output first: a decoder can consume all input and still
        // hold decoded bytes.
        if w == output.len() {
            return Ok(Outcome::OutputFilled { consumed: c });
        }
        if c == input.len() && status == Status::InputEmpty {
            return Ok(Outcome::InputConsumed { written: w });
        }
        if progress.consumed == 0 && progress.written == 0 {
            return Err(no_progress(status, c, w));
        }
    }
}

/// Call `finish` or `flush` until it reports `done`, or the output is
/// full.
fn run_drain(
    output: &mut [u8],
    done: Status,
    unexpected: Status,
    mut call: impl FnMut(&mut [u8]) -> CompcolResult,
) -> Result<DrainProgress, Failure> {
    let mut w = 0;
    loop {
        let rest_out = &mut output[w..];
        let out_len = rest_out.len();
        let (progress, status) = call(rest_out).map_err(|e| Failure::compcol(e, 0, w))?;
        check_counts(progress, 0, out_len, 0, w)?;
        w += progress.written;

        if status == done {
            return Ok(DrainProgress::Done { written: w });
        }
        if w == output.len() {
            return Ok(DrainProgress::OutputFilled);
        }
        if status == unexpected {
            return Err(Failure::own(
                ErrorKind::ContractViolation,
                SUBCODE_UNEXPECTED_STATUS,
                0,
                w,
            ));
        }
        if progress.written == 0 {
            return Err(no_progress(status, 0, w));
        }
    }
}

/// The call moved no bytes and did not finish the work.
fn no_progress(status: Status, consumed: usize, written: usize) -> Failure {
    if status == Status::OutputFull {
        // TODO: a carry buffer in the adapter would make every output
        // size legal. See "Limitation" in the module docs.
        Failure::own(
            ErrorKind::BehindLimits,
            SUBCODE_OUTPUT_TOO_SMALL,
            consumed,
            written,
        )
    } else {
        Failure::own(
            ErrorKind::ContractViolation,
            SUBCODE_UNEXPECTED_STATUS,
            consumed,
            written,
        )
    }
}

/// Drain the decoded bytes that a decoder holds: `decode` with an empty
/// input.
fn decoder_flush<D: Decoder>(inner: &mut D, output: &mut [u8]) -> Result<DrainProgress, Failure> {
    match run_process(&[], output, |i, o| inner.decode(i, o))? {
        Outcome::InputConsumed { written } | Outcome::StreamEnd { written, .. } => {
            Ok(DrainProgress::Done { written })
        }
        Outcome::OutputFilled { .. } => Ok(DrainProgress::OutputFilled),
    }
}

/// `finish` of all three adapters.
fn adapter_finish(
    state: &mut AdapterState,
    output: &mut [MaybeUninit<u8>],
    finish: impl FnMut(&mut [u8]) -> CompcolResult,
) -> Result<DrainProgress, Error> {
    if state.finished {
        return Ok(DrainProgress::Done { written: 0 });
    }
    let output = zero_init_mut(output);
    match run_drain(output, Status::StreamEnd, Status::InputEmpty, finish) {
        Ok(progress) => {
            if let DrainProgress::Done { .. } = progress {
                state.finished = true;
            }
            Ok(progress)
        }
        Err(failure) => Err(state.report(failure)),
    }
}

// ----
// The adapters
// ----

macro_rules! common_methods {
    ($name:ident, $param:ident) => {
        impl<$param> $name<$param> {
            /// The compcol error behind the last `Err` of this adapter.
            ///
            /// `None` if the last `Err` came from the adapter itself, or
            /// if there was no `Err` since `new` or `reset()`.
            pub fn compcol_error(&self) -> Option<::compcol::Error> {
                self.state.compcol_error
            }

            /// The inner compcol codec.
            pub fn inner(&self) -> &$param {
                &self.inner
            }

            /// The inner compcol codec, for configuration only. See
            /// "Access to the inner codec" in the module docs.
            pub fn inner_mut(&mut self) -> &mut $param {
                &mut self.inner
            }

            /// Unwrap the inner compcol codec.
            pub fn into_inner(self) -> $param {
                self.inner
            }
        }
    };
}

/// A `compcol::Encoder` as a [`Codec`].
#[derive(Debug, Clone)]
pub struct CompcolEncoderAsCodec<E> {
    inner: E,
    flush_mode: FlushMode,
    state: AdapterState,
}

common_methods!(CompcolEncoderAsCodec, E);

impl<E> CompcolEncoderAsCodec<E> {
    /// Wrap `inner`. `flush_mode` decides what [`DrainCodec::flush`]
    /// does. See the module docs.
    pub fn new(inner: E, flush_mode: FlushMode) -> Self {
        Self {
            inner,
            flush_mode,
            state: AdapterState::default(),
        }
    }

    /// The current flush mode.
    pub fn flush_mode(&self) -> FlushMode {
        self.flush_mode
    }

    /// Change the flush mode between calls. Do not change it while
    /// `flush` reports [`DrainProgress::OutputFilled`].
    pub fn set_flush_mode(&mut self, flush_mode: FlushMode) {
        self.flush_mode = flush_mode;
    }
}

impl<E: Encoder> CompcolEncoderAsCodec<E> {
    /// Reset the inner encoder and the adapter for a new stream. The
    /// flush mode stays.
    pub fn reset(&mut self) {
        self.inner.reset();
        self.state = AdapterState::default();
    }
}

impl<E: Encoder> DrainCodec for CompcolEncoderAsCodec<E> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let result = self
            .state
            .check_not_finished()
            .and_then(|()| match self.flush_mode {
                FlushMode::Skip => Ok(DrainProgress::Done { written: 0 }),
                FlushMode::Compcol(mode) => {
                    let output = zero_init_mut(output);
                    let inner = &mut self.inner;
                    run_drain(output, Status::InputEmpty, Status::StreamEnd, |o| {
                        inner.flush(o, mode)
                    })
                }
            });
        result.map_err(|failure| self.state.report(failure))
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let inner = &mut self.inner;
        adapter_finish(&mut self.state, output, |o| inner.finish(o))
    }
}

impl<E: Encoder> Codec for CompcolEncoderAsCodec<E> {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let result = self.state.check_not_finished().and_then(|()| {
            let output = zero_init_mut(output);
            let inner = &mut self.inner;
            match run_process(input, output, |i, o| inner.encode(i, o))? {
                Outcome::InputConsumed { written } => Ok(Progress::InputConsumed { written }),
                Outcome::OutputFilled { consumed } => Ok(Progress::OutputFilled { consumed }),
                // `encode` does not end the stream.
                Outcome::StreamEnd { consumed, written } => Err(Failure::own(
                    ErrorKind::ContractViolation,
                    SUBCODE_UNEXPECTED_STATUS,
                    consumed,
                    written,
                )),
            }
        });
        result.map_err(|failure| self.state.report(failure))
    }
}

/// A `compcol::Decoder` as a [`Codec`].
///
/// It promises that the input is one stream and nothing follows it.
/// Data after the end fails with subcode `0x101`. If data can
/// follow the stream, use [`CompcolDecoderAsBoundaryAwareCodec`].
#[derive(Debug, Clone)]
pub struct CompcolDecoderAsCodec<D> {
    inner: D,
    state: AdapterState,
}

common_methods!(CompcolDecoderAsCodec, D);

impl<D> CompcolDecoderAsCodec<D> {
    /// Wrap `inner`.
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            state: AdapterState::default(),
        }
    }
}

impl<D: Decoder> CompcolDecoderAsCodec<D> {
    /// Reset the inner decoder and the adapter for a new stream.
    pub fn reset(&mut self) {
        self.inner.reset();
        self.state = AdapterState::default();
    }
}

impl<D: Decoder> DrainCodec for CompcolDecoderAsCodec<D> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let result = self
            .state
            .check_not_finished()
            .and_then(|()| decoder_flush(&mut self.inner, zero_init_mut(output)));
        result.map_err(|failure| self.state.report(failure))
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let inner = &mut self.inner;
        adapter_finish(&mut self.state, output, |o| inner.finish(o))
    }
}

impl<D: Decoder> Codec for CompcolDecoderAsCodec<D> {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let result = self.state.check_not_finished().and_then(|()| {
            let output = zero_init_mut(output);
            let inner = &mut self.inner;
            match run_process(input, output, |i, o| inner.decode(i, o))? {
                Outcome::InputConsumed { written } => Ok(Progress::InputConsumed { written }),
                Outcome::OutputFilled { consumed } => Ok(Progress::OutputFilled { consumed }),
                Outcome::StreamEnd { consumed, written } if consumed == input.len() => {
                    Ok(Progress::InputConsumed { written })
                }
                Outcome::StreamEnd { consumed, written } => Err(Failure::own(
                    ErrorKind::ContractViolation,
                    SUBCODE_DATA_AFTER_END,
                    consumed,
                    written,
                )),
            }
        });
        result.map_err(|failure| self.state.report(failure))
    }
}

/// A `compcol::Decoder` as a [`BoundaryAwareCodec`].
///
/// At the end of the compressed stream it reports
/// [`Boundary`](BoundaryAwareProgress::Boundary) and leaves the rest of
/// the input in the source. To decode the next stream: call `finish`
/// until [`Done`](DrainProgress::Done), then `reset()`.
///
/// For gzip, build the decoder with `with_multi_member(false)`.
/// Otherwise compcol decodes concatenated members as one stream.
#[derive(Debug, Clone)]
pub struct CompcolDecoderAsBoundaryAwareCodec<D> {
    inner: D,
    state: AdapterState,
}

common_methods!(CompcolDecoderAsBoundaryAwareCodec, D);

impl<D> CompcolDecoderAsBoundaryAwareCodec<D> {
    /// Wrap `inner`.
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            state: AdapterState::default(),
        }
    }
}

impl<D: Decoder> CompcolDecoderAsBoundaryAwareCodec<D> {
    /// Reset the inner decoder and the adapter for a new stream.
    pub fn reset(&mut self) {
        self.inner.reset();
        self.state = AdapterState::default();
    }
}

impl<D: Decoder> DrainCodec for CompcolDecoderAsBoundaryAwareCodec<D> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let result = self
            .state
            .check_not_finished()
            .and_then(|()| decoder_flush(&mut self.inner, zero_init_mut(output)));
        result.map_err(|failure| self.state.report(failure))
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let inner = &mut self.inner;
        adapter_finish(&mut self.state, output, |o| inner.finish(o))
    }
}

impl<D: Decoder> BoundaryAwareCodec for CompcolDecoderAsBoundaryAwareCodec<D> {
    fn process_bounded(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        let result = self.state.check_not_finished().and_then(|()| {
            let output = zero_init_mut(output);
            let inner = &mut self.inner;
            Ok(
                match run_process(input, output, |i, o| inner.decode(i, o))? {
                    Outcome::InputConsumed { written } => {
                        BoundaryAwareProgress::InputConsumed { written }
                    }
                    Outcome::OutputFilled { consumed } => {
                        BoundaryAwareProgress::OutputFilled { consumed }
                    }
                    Outcome::StreamEnd { consumed, written } => {
                        BoundaryAwareProgress::Boundary { consumed, written }
                    }
                },
            )
        });
        result.map_err(|failure| self.state.report(failure))
    }
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use alloc::collections::VecDeque;
    use alloc::vec;
    use alloc::vec::Vec;

    use ::compcol::limit::LimitedDecoder;
    use ::compcol::{gzip, zlib};

    use super::*;
    use crate::uninit::as_uninit_mut;

    fn sample() -> Vec<u8> {
        let mut data = Vec::new();
        for i in 0..2000u32 {
            data.extend_from_slice(b"compcol adapter ");
            data.extend_from_slice(&i.to_le_bytes());
        }
        data
    }

    /// How a drive ended, and where in the input.
    #[derive(Debug, PartialEq)]
    struct Drive {
        output: Vec<u8>,
        consumed: usize,
        boundary: bool,
    }

    /// Feed `input` in chunks of `chunk` bytes into an output buffer of
    /// `chunk` bytes. Stop at the end of the input or at a boundary.
    /// Then call `finish` until `Done`.
    fn drive(
        codec: &mut impl BoundaryAwareCodec,
        input: &[u8],
        chunk: usize,
    ) -> Result<Drive, Error> {
        let mut output = Vec::new();
        let mut buf = vec![0u8; chunk];
        let mut pos = 0;
        let mut boundary = false;
        loop {
            let end = (pos + chunk).min(input.len());
            match codec.process_bounded(&input[pos..end], as_uninit_mut(&mut buf))? {
                BoundaryAwareProgress::InputConsumed { written } => {
                    output.extend_from_slice(&buf[..written]);
                    pos = end;
                    if pos == input.len() {
                        break;
                    }
                }
                BoundaryAwareProgress::OutputFilled { consumed } => {
                    output.extend_from_slice(&buf);
                    pos += consumed;
                }
                BoundaryAwareProgress::Boundary { consumed, written } => {
                    output.extend_from_slice(&buf[..written]);
                    pos += consumed;
                    boundary = true;
                    break;
                }
            }
        }
        loop {
            match codec.finish(as_uninit_mut(&mut buf))? {
                DrainProgress::OutputFilled => output.extend_from_slice(&buf),
                DrainProgress::Done { written } => {
                    output.extend_from_slice(&buf[..written]);
                    break;
                }
            }
        }
        Ok(Drive {
            output,
            consumed: pos,
            boundary,
        })
    }

    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        let mut enc = CompcolEncoderAsCodec::new(gzip::Encoder::new(), FlushMode::Skip);
        drive(&mut enc, data, 4096).unwrap().output
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
            let mut enc = CompcolEncoderAsCodec::new(gzip::Encoder::new(), FlushMode::Skip);
            let compressed = drive(&mut enc, &data, chunk).unwrap().output;
            assert!(compressed.len() < data.len() / 4, "chunk {chunk}");

            let mut dec = CompcolDecoderAsCodec::new(gzip::Decoder::new());
            let plain = drive(&mut dec, &compressed, chunk).unwrap();
            assert_eq!(plain.output, data, "chunk {chunk}");

            let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(gzip::Decoder::new());
            let plain = drive(&mut dec, &compressed, chunk).unwrap();
            assert_eq!(plain.output, data, "chunk {chunk}");
        }
    }

    #[test]
    fn round_trip_with_stream_to_stream() {
        use crate::sources_and_sinks::slice::SliceSource;
        use crate::sources_and_sinks::vec::VecSink;
        use crate::stream_to_stream;

        let data = sample();
        let enc = CompcolEncoderAsCodec::new(zlib::Encoder::new(), FlushMode::Skip);
        let mut compressed = VecSink::default();
        stream_to_stream(&mut SliceSource::new(&data), enc, &mut compressed).unwrap();
        let compressed = compressed.into_inner();

        let dec = CompcolDecoderAsCodec::new(zlib::Decoder::new());
        let mut plain = VecSink::default();
        stream_to_stream(&mut SliceSource::new(&compressed), dec, &mut plain).unwrap();
        assert_eq!(plain.into_inner(), data);
    }

    // ----
    // 2. Two gzip members
    // ----

    #[test]
    fn boundary_aware_decoder_reads_members_one_by_one() {
        let first = gzip_compress(b"first member");
        let second = gzip_compress(b"second member");
        let mut input = first.clone();
        input.extend_from_slice(&second);
        input.extend_from_slice(b"tail");

        for chunk in [1, 4096] {
            let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(
                gzip::Decoder::new().with_multi_member(false),
            );
            let one = drive(&mut dec, &input, chunk).unwrap();
            assert_eq!(one.output, b"first member", "chunk {chunk}");
            assert!(one.boundary);
            assert_eq!(one.consumed, first.len());

            dec.reset();
            let rest = &input[one.consumed..];
            let two = drive(&mut dec, rest, chunk).unwrap();
            assert_eq!(two.output, b"second member", "chunk {chunk}");
            assert!(two.boundary);
            assert_eq!(&rest[two.consumed..], b"tail");
        }
    }

    // ----
    // 3. Data after end
    // ----

    #[test]
    fn plain_decoder_rejects_data_after_end_in_the_same_chunk() {
        let mut enc = CompcolEncoderAsCodec::new(zlib::Encoder::new(), FlushMode::Skip);
        let mut input = drive(&mut enc, b"hello", 64).unwrap().output;
        let stream_len = input.len();
        input.push(b'x');

        let mut dec = CompcolDecoderAsCodec::new(zlib::Decoder::new());
        let mut out = [0u8; 64];
        let error = dec.process(&input, as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(
            error,
            own_error(
                ErrorKind::ContractViolation,
                SUBCODE_DATA_AFTER_END,
                stream_len,
                5
            )
        );
        assert_eq!(dec.compcol_error(), None);
    }

    #[test]
    fn plain_decoder_rejects_data_after_end_in_a_later_chunk() {
        let mut enc = CompcolEncoderAsCodec::new(zlib::Encoder::new(), FlushMode::Skip);
        let input = drive(&mut enc, b"hello", 64).unwrap().output;

        let mut dec = CompcolDecoderAsCodec::new(zlib::Decoder::new());
        let mut out = [0u8; 64];
        let progress = dec.process(&input, as_uninit_mut(&mut out)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 5 });
        assert_eq!(&out[..5], b"hello");

        let progress = dec.process(b"", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 0 });

        let error = dec.process(b"x", as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(
            error,
            own_error(ErrorKind::ContractViolation, SUBCODE_DATA_AFTER_END, 0, 0)
        );
    }

    // ----
    // 4. Flush modes
    // ----

    #[test]
    fn flush_skip_writes_nothing() {
        let mut enc = CompcolEncoderAsCodec::new(zlib::Encoder::new(), FlushMode::Skip);
        let mut out = [0u8; 256];
        enc.process(b"hello", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            enc.flush(as_uninit_mut(&mut out)),
            Ok(DrainProgress::Done { written: 0 })
        );
    }

    #[test]
    fn flush_sync_writes_a_decodable_prefix() {
        for chunk in [1, 256] {
            let mut enc =
                CompcolEncoderAsCodec::new(zlib::Encoder::new(), FlushMode::Compcol(Flush::Sync));
            let mut buf = vec![0u8; chunk];
            let mut compressed = Vec::new();
            let mut pos = 0;
            let input = b"hello, sync";
            while pos < input.len() {
                match enc.process(&input[pos..], as_uninit_mut(&mut buf)).unwrap() {
                    Progress::InputConsumed { written } => {
                        compressed.extend_from_slice(&buf[..written]);
                        pos = input.len();
                    }
                    Progress::OutputFilled { consumed } => {
                        compressed.extend_from_slice(&buf);
                        pos += consumed;
                    }
                }
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

            let mut dec = CompcolDecoderAsCodec::new(zlib::Decoder::new());
            let mut out = [0u8; 64];
            let progress = dec.process(&compressed, as_uninit_mut(&mut out)).unwrap();
            assert_eq!(
                progress,
                Progress::InputConsumed {
                    written: input.len()
                },
                "chunk {chunk}"
            );
            assert_eq!(&out[..input.len()], input);
        }
    }

    // ----
    // 5. Error mapping
    // ----

    #[test]
    fn corrupt_input_keeps_the_compcol_error() {
        let mut enc = CompcolEncoderAsCodec::new(zlib::Encoder::new(), FlushMode::Skip);
        let mut input = drive(&mut enc, b"hello", 64).unwrap().output;
        *input.last_mut().unwrap() ^= 0xFF; // the Adler-32 trailer

        let mut dec = CompcolDecoderAsCodec::new(zlib::Decoder::new());
        let error = drive(&mut dec, &input, 64).unwrap_err();
        assert_eq!(error.kind, ErrorKind::CorruptStream);
        assert_eq!(error.codec_subcode, 8);
        assert_eq!(
            dec.compcol_error(),
            Some(::compcol::Error::ChecksumMismatch)
        );
    }

    #[test]
    fn output_limit_is_behind_limits() {
        let input = gzip_compress(&sample());
        let mut dec = CompcolDecoderAsCodec::new(LimitedDecoder::new(gzip::Decoder::new(), 100));
        let error = drive(&mut dec, &input, 4096).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BehindLimits);
        assert_eq!(error.codec_subcode, 11);
        assert_eq!(
            dec.compcol_error(),
            Some(::compcol::Error::OutputLimitExceeded)
        );
    }

    // ----
    // 6. NeedDictionary
    // ----

    #[test]
    fn need_dictionary_recovers_through_inner_mut() {
        let dictionary = b"a preset dictionary for zlib";
        let mut enc = CompcolEncoderAsCodec::new(
            zlib::Encoder::new().with_dictionary(dictionary),
            FlushMode::Skip,
        );
        let input = drive(&mut enc, b"a preset dictionary helps", 64)
            .unwrap()
            .output;

        let mut dec = CompcolDecoderAsCodec::new(zlib::Decoder::new());
        let mut out = [0u8; 64];
        let error = dec.process(&input, as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(error.codec_subcode, 12);
        assert_eq!(dec.compcol_error(), Some(::compcol::Error::NeedDictionary));
        assert!(dec.inner().dictionary_id().is_some());

        dec.inner_mut().set_dictionary(dictionary).unwrap();
        let rest = &input[error.consumed..];
        let progress = dec.process(rest, as_uninit_mut(&mut out)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 25 });
        assert_eq!(&out[..25], b"a preset dictionary helps");
    }

    // ----
    // 7. After finish
    // ----

    #[test]
    fn calls_after_finish_fail_until_reset() {
        let after_finish = own_error(
            ErrorKind::ContractViolation,
            Error::SUBCODE_AFTER_FINISH,
            0,
            0,
        );
        let mut enc = CompcolEncoderAsCodec::new(gzip::Encoder::new(), FlushMode::Skip);
        let first = drive(&mut enc, b"once", 64).unwrap().output;

        let mut out = [0u8; 64];
        assert_eq!(
            enc.process(b"x", as_uninit_mut(&mut out)),
            Err(after_finish)
        );
        assert_eq!(enc.flush(as_uninit_mut(&mut out)), Err(after_finish));
        assert_eq!(
            enc.finish(as_uninit_mut(&mut out)),
            Ok(DrainProgress::Done { written: 0 })
        );
        assert_eq!(enc.compcol_error(), None);

        enc.reset();
        let second = drive(&mut enc, b"once", 64).unwrap().output;
        assert_eq!(first, second);
    }

    // ----
    // 8. Statuses that real codecs do not produce
    // ----

    /// A compcol codec that returns scripted results and writes nothing.
    #[derive(Default)]
    struct Scripted {
        results: VecDeque<CompcolResult>,
    }

    impl Scripted {
        fn new(results: impl IntoIterator<Item = CompcolResult>) -> Self {
            Self {
                results: results.into_iter().collect(),
            }
        }

        fn next(&mut self) -> CompcolResult {
            self.results.pop_front().expect("unexpected call")
        }
    }

    impl ::compcol::Encoder for Scripted {
        fn encode(&mut self, _: &[u8], _: &mut [u8]) -> CompcolResult {
            self.next()
        }
        fn finish(&mut self, _: &mut [u8]) -> CompcolResult {
            self.next()
        }
        fn reset(&mut self) {}
        fn flush(&mut self, _: &mut [u8], _: Flush) -> CompcolResult {
            self.next()
        }
    }

    impl ::compcol::Decoder for Scripted {
        fn decode(&mut self, _: &[u8], _: &mut [u8]) -> CompcolResult {
            self.next()
        }
        fn finish(&mut self, _: &mut [u8]) -> CompcolResult {
            self.next()
        }
        fn reset(&mut self) {}
        fn discard_output(&mut self, _: &[u8], _: usize) -> CompcolResult {
            unimplemented!()
        }
    }

    fn step(consumed: usize, written: usize, status: Status) -> CompcolResult {
        Ok((::compcol::Progress { consumed, written }, status))
    }

    #[test]
    fn empty_output_reports_output_filled_without_calling() {
        let mut enc = CompcolEncoderAsCodec::new(Scripted::default(), FlushMode::Skip);
        assert_eq!(
            enc.process(b"abc", &mut []),
            Ok(Progress::OutputFilled { consumed: 0 })
        );
    }

    #[test]
    fn partial_output_full_calls_again() {
        let script = [
            step(1, 2, Status::OutputFull),
            step(2, 3, Status::InputEmpty),
        ];
        let mut enc = CompcolEncoderAsCodec::new(Scripted::new(script), FlushMode::Skip);
        let mut out = [0u8; 8];
        assert_eq!(
            enc.process(b"abc", as_uninit_mut(&mut out)),
            Ok(Progress::InputConsumed { written: 5 })
        );
    }

    #[test]
    fn no_progress_with_output_full_is_behind_limits() {
        let script = [
            step(3, 4, Status::OutputFull),
            step(0, 0, Status::OutputFull),
        ];
        let mut enc = CompcolEncoderAsCodec::new(Scripted::new(script), FlushMode::Skip);
        let mut out = [0u8; 6];
        assert_eq!(
            enc.process(b"abcdef", as_uninit_mut(&mut out)),
            Err(own_error(
                ErrorKind::BehindLimits,
                SUBCODE_OUTPUT_TOO_SMALL,
                3,
                4
            ))
        );
        assert_eq!(enc.compcol_error(), None);
    }

    #[test]
    fn no_progress_with_input_empty_is_unexpected_status() {
        let script = [step(0, 0, Status::InputEmpty)];
        let mut dec = CompcolDecoderAsCodec::new(Scripted::new(script));
        let mut out = [0u8; 6];
        assert_eq!(
            dec.process(b"abc", as_uninit_mut(&mut out)),
            Err(own_error(
                ErrorKind::ContractViolation,
                SUBCODE_UNEXPECTED_STATUS,
                0,
                0
            ))
        );
    }

    #[test]
    fn stream_end_from_encode_is_unexpected_status() {
        let script = [step(1, 1, Status::StreamEnd)];
        let mut enc = CompcolEncoderAsCodec::new(Scripted::new(script), FlushMode::Skip);
        let mut out = [0u8; 6];
        assert_eq!(
            enc.process(b"abc", as_uninit_mut(&mut out)),
            Err(own_error(
                ErrorKind::ContractViolation,
                SUBCODE_UNEXPECTED_STATUS,
                1,
                1
            ))
        );
    }

    #[test]
    fn input_empty_from_finish_is_unexpected_status() {
        let script = [step(0, 2, Status::InputEmpty)];
        let mut enc = CompcolEncoderAsCodec::new(Scripted::new(script), FlushMode::Skip);
        let mut out = [0u8; 6];
        assert_eq!(
            enc.finish(as_uninit_mut(&mut out)),
            Err(own_error(
                ErrorKind::ContractViolation,
                SUBCODE_UNEXPECTED_STATUS,
                0,
                2
            ))
        );
    }

    #[test]
    fn overclaimed_counts_are_a_contract_violation() {
        let script = [
            step(1, 1, Status::OutputFull),
            step(0, 99, Status::OutputFull),
        ];
        let mut enc = CompcolEncoderAsCodec::new(Scripted::new(script), FlushMode::Skip);
        let mut out = [0u8; 6];
        assert_eq!(
            enc.process(b"abc", as_uninit_mut(&mut out)),
            Err(own_error(
                ErrorKind::ContractViolation,
                Error::SUBCODE_BYTE_COUNT_CLAIM,
                1,
                1
            ))
        );
    }

    #[test]
    fn stream_end_during_decoder_flush_is_done() {
        let script = [step(0, 3, Status::StreamEnd), step(0, 0, Status::StreamEnd)];
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(Scripted::new(script));
        let mut out = [0u8; 6];
        assert_eq!(
            dec.flush(as_uninit_mut(&mut out)),
            Ok(DrainProgress::Done { written: 3 })
        );
        assert_eq!(
            dec.process_bounded(b"next", as_uninit_mut(&mut out)),
            Ok(BoundaryAwareProgress::Boundary {
                consumed: 0,
                written: 0
            })
        );
    }
}
