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
//! To get a plain [`Codec`] from either, wrap it in
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
//! The adapter's own checks give `ContractViolation`: a status that the
//! compcol contract excludes, a call that moves no bytes and does not
//! end the work, a call after `finish`.
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

use ::compcol::{Decoder, Encoder, Flush, Status};

use crate::step::FinishPhase;
use crate::uninit::zero_init_mut;
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainCodec, DrainProgress, Error, ErrorKind,
};

// Opaque subcodes, not part of the API.
/// The compcol codec returned a status that its contract excludes.
pub(crate) const SUBCODE_UNEXPECTED_STATUS: usize = 0x102;
/// The compcol codec moved no bytes and did not end the work.
pub(crate) const SUBCODE_NO_PROGRESS: usize = 0x103;

/// Return type of a single compcol `encode`/`decode`/`finish`/`flush` call.
pub(crate) type CompcolResult = Result<(::compcol::Progress, Status), ::compcol::Error>;

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
    Compcol(Flush),
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

/// The kind and the subcode of an [`Error`] for a compcol error.
fn map_compcol_error(error: ::compcol::Error) -> (ErrorKind, usize) {
    use ::compcol::Error as E;
    use ErrorKind::*;
    match error {
        E::Corrupt => (CorruptStream, 1),
        E::UnexpectedEnd => (CorruptStream, 2),
        E::OutputTooSmall => (BehindLimits, 3),
        E::BadHeader => (CorruptStream, 4),
        E::InvalidBlockType => (CorruptStream, 5),
        E::InvalidHuffmanTree => (CorruptStream, 6),
        E::InvalidDistance => (CorruptStream, 7),
        E::ChecksumMismatch => (CorruptStream, 8),
        E::TrailerMismatch => (CorruptStream, 9),
        E::Unsupported => (Unsupported, 10),
        E::OutputLimitExceeded => (BehindLimits, 11),
        E::NeedDictionary => (Unsupported, 12),
        _ => (ContractViolation, 0),
    }
}

/// Selects the compcol call that `CompcolAdapter::drain_loop` repeats.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DrainMode {
    /// `compcol::Encoder::flush`.
    Flush,
    /// `compcol::Encoder::finish` or `compcol::Decoder::finish`.
    Finish,
}

impl DrainMode {
    /// The status that tells that the call has no more output.
    fn done_status(self) -> Status {
        match self {
            DrainMode::Flush => Status::InputEmpty,
            DrainMode::Finish => Status::StreamEnd,
        }
    }
}

/// The inner codec and the state that both adapters share.
#[derive(Debug, Clone)]
struct CompcolAdapter<I> {
    inner: I,
    /// How far the adapter is in `finish`.
    finish_phase: FinishPhase,
    /// The compcol error behind the last `Err` of the adapter. `None`
    /// if the adapter made that error itself.
    compcol_error: Option<::compcol::Error>,
}

impl<I> CompcolAdapter<I> {
    fn new(inner: I) -> Self {
        Self {
            inner,
            finish_phase: FinishPhase::Open,
            compcol_error: None,
        }
    }

    /// Prepare the adapter for a new stream. The caller resets `inner`.
    fn reset_state(&mut self) {
        self.finish_phase = FinishPhase::Open;
        self.compcol_error = None;
    }

    /// Fail if `finish` was called. Call it before `process_bounded`
    /// and `flush`.
    fn check_open(&mut self) -> Result<(), Error> {
        let open = self.finish_phase.check_open();
        if open.is_err() {
            self.compcol_error = None;
        }
        open
    }

    /// Make an error of the kind `ContractViolation`. The adapter made
    /// it itself, so there is no compcol error behind it.
    fn violation(&mut self, codec_subcode: usize, consumed: usize, written: usize) -> Error {
        self.compcol_error = None;
        Error {
            kind: ErrorKind::ContractViolation,
            codec_subcode,
            consumed,
            written,
        }
    }

    /// Keep the compcol error for `compcol_error()`, and convert it.
    fn compcol_failure(
        &mut self,
        error: ::compcol::Error,
        consumed: usize,
        written: usize,
    ) -> Error {
        self.compcol_error = Some(error);
        let (kind, codec_subcode) = map_compcol_error(error);
        Error {
            kind,
            codec_subcode,
            consumed,
            written,
        }
    }

    /// Fail if the compcol call claims more bytes than it received.
    /// `consumed` and `written` are the counts before the call.
    fn check_counts(
        &mut self,
        progress: ::compcol::Progress,
        input_len: usize,
        output_len: usize,
        consumed: usize,
        written: usize,
    ) -> Result<(), Error> {
        if progress.consumed > input_len || progress.written > output_len {
            return Err(self.violation(Error::SUBCODE_BYTE_COUNT_CLAIM, consumed, written));
        }
        Ok(())
    }

    /// The error for a compcol call that moved no bytes and did not end
    /// the work. compcol 0.7.8 codecs never cause it. See "Atomic
    /// units" above.
    fn no_progress(&mut self, status: Status, consumed: usize, written: usize) -> Error {
        let codec_subcode = if status == Status::OutputFull {
            SUBCODE_NO_PROGRESS
        } else {
            SUBCODE_UNEXPECTED_STATUS
        };
        self.violation(codec_subcode, consumed, written)
    }

    /// Repeat `step` (`encode` or `decode`) until `input` is consumed,
    /// `output` is full, or the stream ends. The stream end gives
    /// `Boundary`.
    fn transfer_loop(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
        mut step: impl FnMut(&mut I, &[u8], &mut [u8]) -> CompcolResult,
    ) -> Result<BoundaryAwareProgress, Error> {
        if output.is_empty() {
            return Ok(BoundaryAwareProgress::OutputFilled { consumed: 0 });
        }
        let output = zero_init_mut(output);
        let (mut consumed, mut written) = (0, 0);
        loop {
            let rest_in = &input[consumed..];
            let rest_out = &mut output[written..];
            let (in_len, out_len) = (rest_in.len(), rest_out.len());
            let (progress, status) = step(&mut self.inner, rest_in, rest_out)
                .map_err(|error| self.compcol_failure(error, consumed, written))?;
            self.check_counts(progress, in_len, out_len, consumed, written)?;
            consumed += progress.consumed;
            written += progress.written;

            if status == Status::StreamEnd {
                return Ok(BoundaryAwareProgress::Boundary { consumed, written });
            }
            // Full output first: a decoder can consume all input and still
            // hold decoded bytes.
            if written == output.len() {
                return Ok(BoundaryAwareProgress::OutputFilled { consumed });
            }
            if consumed == input.len() && status == Status::InputEmpty {
                return Ok(BoundaryAwareProgress::InputConsumed { written });
            }
            if progress.consumed == 0 && progress.written == 0 {
                return Err(self.no_progress(status, consumed, written));
            }
        }
    }

    /// Repeat `step` (`flush` or `finish`, as `mode` says) until it has
    /// no more output, or `output` is full.
    fn drain_loop(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        mode: DrainMode,
        mut step: impl FnMut(&mut I, &mut [u8]) -> CompcolResult,
    ) -> Result<DrainProgress, Error> {
        // An empty output is legal for our codecs, and `Chain` passes one
        // when `output` is full and it asks `second` if it is done. Some
        // compcol codecs fail on an empty output (`lzx` gives
        // `UnexpectedEnd` while it holds decoded bytes), so do not call
        // them. `OutputFilled` makes the caller call again with room.
        if output.is_empty() {
            return Ok(DrainProgress::OutputFilled);
        }
        let output = zero_init_mut(output);
        let mut written = 0;
        loop {
            let rest_out = &mut output[written..];
            let out_len = rest_out.len();
            let (progress, status) = step(&mut self.inner, rest_out)
                .map_err(|error| self.compcol_failure(error, 0, written))?;
            self.check_counts(progress, 0, out_len, 0, written)?;
            written += progress.written;

            if status == mode.done_status() {
                return Ok(DrainProgress::Done { written });
            }
            if written == output.len() {
                return Ok(DrainProgress::OutputFilled);
            }
            // `flush` must not end the stream, and `finish` must not stop
            // before the stream ends.
            if status != Status::OutputFull {
                return Err(self.violation(SUBCODE_UNEXPECTED_STATUS, 0, written));
            }
            if progress.written == 0 {
                return Err(self.no_progress(status, 0, written));
            }
        }
    }

    /// The `finish` of both adapters. `step` is the `finish` of the
    /// inner codec.
    ///
    /// If `finish` reported `Done`, a new call writes no bytes and
    /// reports `Done` again. It does not call `step`.
    fn finish(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        step: impl FnMut(&mut I, &mut [u8]) -> CompcolResult,
    ) -> Result<DrainProgress, Error> {
        if self.finish_phase.is_finished() {
            return Ok(DrainProgress::Done { written: 0 });
        }
        self.finish_phase.start_finish();
        let progress = self.drain_loop(output, DrainMode::Finish, step)?;
        if let DrainProgress::Done { .. } = progress {
            self.finish_phase.finish_done();
        }
        Ok(progress)
    }
}

// ----
// Encoder adapter
// ----

/// A `compcol::Encoder` as a [`BoundaryAwareCodec`].
///
/// An encoder never reaches a stream boundary on its own, so
/// `process_bounded` behaves identically to a plain `process` call and
/// never returns [`Boundary`](BoundaryAwareProgress::Boundary).
#[derive(Debug, Clone)]
pub struct CompcolEncoderAsBoundaryAwareCodec<E> {
    adapter: CompcolAdapter<E>,
    flush_mode: FlushMode,
}

impl<E> CompcolEncoderAsBoundaryAwareCodec<E> {
    /// Wrap `inner`. `flush_mode` decides what [`DrainCodec::flush`] does.
    pub fn new(inner: E, flush_mode: FlushMode) -> Self {
        Self {
            adapter: CompcolAdapter::new(inner),
            flush_mode,
        }
    }

    /// The compcol error behind the last `Err` of this adapter.
    pub fn compcol_error(&self) -> Option<::compcol::Error> {
        self.adapter.compcol_error
    }

    /// The inner compcol codec.
    pub fn inner(&self) -> &E {
        &self.adapter.inner
    }

    /// The inner compcol codec, for configuration only. See
    /// "Access to the inner codec" in the module docs.
    pub fn inner_mut(&mut self) -> &mut E {
        &mut self.adapter.inner
    }

    /// Unwrap the inner compcol codec.
    pub fn into_inner(self) -> E {
        self.adapter.inner
    }

    /// The current flush mode.
    pub fn flush_mode(&self) -> FlushMode {
        self.flush_mode
    }

    /// Change the flush mode between calls.
    pub fn set_flush_mode(&mut self, flush_mode: FlushMode) {
        self.flush_mode = flush_mode;
    }
}

impl<E: Encoder> CompcolEncoderAsBoundaryAwareCodec<E> {
    /// Reset the inner encoder and the adapter for a new stream.
    pub fn reset(&mut self) {
        self.adapter.inner.reset();
        self.adapter.reset_state();
    }
}

impl<E: Encoder> DrainCodec for CompcolEncoderAsBoundaryAwareCodec<E> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.adapter.check_open()?;
        match self.flush_mode {
            FlushMode::Skip => Ok(DrainProgress::Done { written: 0 }),
            FlushMode::Compcol(mode) => {
                self.adapter
                    .drain_loop(output, DrainMode::Flush, |inner, o| inner.flush(o, mode))
            }
        }
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.adapter.finish(output, E::finish)
    }
}

impl<E: Encoder> BoundaryAwareCodec for CompcolEncoderAsBoundaryAwareCodec<E> {
    fn process_bounded(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        self.adapter.check_open()?;
        match self.adapter.transfer_loop(input, output, E::encode)? {
            // `encode` must not signal `StreamEnd`.
            BoundaryAwareProgress::Boundary { consumed, written } => {
                let subcode = SUBCODE_UNEXPECTED_STATUS;
                Err(self.adapter.violation(subcode, consumed, written))
            }
            progress => Ok(progress),
        }
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
#[derive(Debug, Clone)]
pub struct CompcolDecoderAsBoundaryAwareCodec<D> {
    adapter: CompcolAdapter<D>,
}

impl<D> CompcolDecoderAsBoundaryAwareCodec<D> {
    /// Wrap `inner`.
    pub fn new(inner: D) -> Self {
        Self {
            adapter: CompcolAdapter::new(inner),
        }
    }

    /// The compcol error behind the last `Err` of this adapter.
    pub fn compcol_error(&self) -> Option<::compcol::Error> {
        self.adapter.compcol_error
    }

    /// The inner compcol codec.
    pub fn inner(&self) -> &D {
        &self.adapter.inner
    }

    /// The inner compcol codec, for configuration only. See
    /// "Access to the inner codec" in the module docs.
    pub fn inner_mut(&mut self) -> &mut D {
        &mut self.adapter.inner
    }

    /// Unwrap the inner compcol codec.
    pub fn into_inner(self) -> D {
        self.adapter.inner
    }
}

impl<D: Decoder> CompcolDecoderAsBoundaryAwareCodec<D> {
    /// Reset the inner decoder and the adapter for a new stream.
    pub fn reset(&mut self) {
        self.adapter.inner.reset();
        self.adapter.reset_state();
    }
}

impl<D: Decoder> DrainCodec for CompcolDecoderAsBoundaryAwareCodec<D> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.adapter.check_open()?;
        // compcol decoders have no flush method. `decode` with empty
        // input writes the decoded bytes that the decoder holds.
        Ok(match self.adapter.transfer_loop(&[], output, D::decode)? {
            BoundaryAwareProgress::OutputFilled { .. } => DrainProgress::OutputFilled,
            BoundaryAwareProgress::InputConsumed { written }
            | BoundaryAwareProgress::Boundary { written, .. } => DrainProgress::Done { written },
        })
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.adapter.finish(output, D::finish)
    }
}

impl<D: Decoder> BoundaryAwareCodec for CompcolDecoderAsBoundaryAwareCodec<D> {
    fn process_bounded(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        self.adapter.check_open()?;
        self.adapter.transfer_loop(input, output, D::decode)
    }
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use alloc::collections::VecDeque;
    use alloc::vec;
    use alloc::vec::Vec;

    use ::compcol::limit::LimitedDecoder;
    use ::compcol::{gzip, zlib, Status};

    use super::*;
    use crate::boundary_as_codec::BoundaryAwareCodecAsCodec;
    use crate::uninit::as_uninit_mut;

    fn sample() -> Vec<u8> {
        let mut data = Vec::new();
        for i in 0..2000u32 {
            data.extend_from_slice(b"compcol adapter ");
            data.extend_from_slice(&i.to_le_bytes());
        }
        data
    }

    #[derive(Debug, PartialEq)]
    struct Drive {
        output: Vec<u8>,
        consumed: usize,
        boundary: bool,
    }

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
        Ok(Drive { output, consumed: pos, boundary })
    }

    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        let mut enc = gzip::Encoder::new();
        let mut buf = vec![0u8; 4096];
        let mut compressed = Vec::new();
        let mut pos = 0;
        loop {
            let (progress, status) = enc.encode(&data[pos..], &mut buf).unwrap();
            compressed.extend_from_slice(&buf[..progress.written]);
            pos += progress.consumed;
            if status == Status::InputEmpty {
                break;
            }
        }
        loop {
            let (progress, status) = enc.finish(&mut buf).unwrap();
            compressed.extend_from_slice(&buf[..progress.written]);
            if status == Status::StreamEnd {
                break;
            }
        }
        compressed
    }

    fn own_error(kind: ErrorKind, codec_subcode: usize, consumed: usize, written: usize) -> Error {
        Error { kind, codec_subcode, consumed, written }
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
            let compressed = drive(&mut enc, &data, chunk).unwrap().output;
            assert!(compressed.len() < data.len() / 4, "chunk {chunk}");

            let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(gzip::Decoder::new());
            let plain = drive(&mut dec, &compressed, chunk).unwrap();
            assert_eq!(plain.output, data, "chunk {chunk}");
        }
    }

    #[test]
    fn round_trip_as_plain_codec_with_stream_to_stream() {
        use crate::sources_and_sinks::slice::SliceSource;
        use crate::sources_and_sinks::vec::VecSink;
        use crate::stream_to_stream;

        let data = sample();
        let compressed = gzip_compress(&data);

        let dec = BoundaryAwareCodecAsCodec::new(CompcolDecoderAsBoundaryAwareCodec::new(
            gzip::Decoder::new(),
        ));
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
    // 3. Boundary
    // ----

    #[test]
    fn boundary_aware_decoder_reports_boundary_on_data_after_end_in_the_same_chunk() {
        let mut input = gzip_compress(b"hello");
        let stream_len = input.len();
        input.push(b'x');

        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(
            gzip::Decoder::new().with_multi_member(false),
        );
        let mut out = [0u8; 64];
        let progress = dec
            .process_bounded(&input, as_uninit_mut(&mut out))
            .unwrap();
        assert_eq!(
            progress,
            BoundaryAwareProgress::Boundary { consumed: stream_len, written: 5 }
        );
    }

    #[test]
    fn boundary_aware_decoder_reports_boundary_in_a_later_chunk() {
        let input = gzip_compress(b"hello");

        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(
            gzip::Decoder::new().with_multi_member(false),
        );
        let mut out = [0u8; 64];
        let progress = dec
            .process_bounded(&input, as_uninit_mut(&mut out))
            .unwrap();
        assert_eq!(
            progress,
            BoundaryAwareProgress::Boundary { consumed: input.len(), written: 5 }
        );
        assert_eq!(&out[..5], b"hello");
    }

    // ----
    // 4. Error mapping
    // ----

    #[test]
    fn corrupt_input_keeps_the_compcol_error() {
        let mut input = gzip_compress(b"hello");
        *input.last_mut().unwrap() ^= 0xFF; // the CRC32 trailer

        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(gzip::Decoder::new());
        let error = drive(&mut dec, &input, 64).unwrap_err();
        assert_eq!(error.kind, ErrorKind::CorruptStream);
        assert_eq!(dec.compcol_error(), Some(::compcol::Error::TrailerMismatch));
    }

    #[test]
    fn output_limit_is_behind_limits() {
        let input = gzip_compress(&sample());
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(LimitedDecoder::new(
            gzip::Decoder::new(),
            100,
        ));
        let error = drive(&mut dec, &input, 4096).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BehindLimits);
        assert_eq!(error.codec_subcode, 11);
        assert_eq!(dec.compcol_error(), Some(::compcol::Error::OutputLimitExceeded));
    }

    // ----
    // 5. NeedDictionary
    // ----

    #[test]
    fn need_dictionary_recovers_through_inner_mut() {
        let dictionary = b"a preset dictionary for zlib";
        let compressed = {
            let mut enc = zlib::Encoder::new().with_dictionary(dictionary);
            let mut buf = vec![0u8; 4096];
            let mut v = Vec::new();
            let input = b"a preset dictionary helps";
            let (p, _) = enc.encode(input, &mut buf).unwrap();
            v.extend_from_slice(&buf[..p.written]);
            loop {
                let (p, s) = enc.finish(&mut buf).unwrap();
                v.extend_from_slice(&buf[..p.written]);
                if s == Status::StreamEnd {
                    break;
                }
            }
            v
        };

        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(zlib::Decoder::new());
        let mut out = [0u8; 64];
        let error = dec
            .process_bounded(&compressed, as_uninit_mut(&mut out))
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Unsupported);
        assert_eq!(error.codec_subcode, 12);
        assert_eq!(dec.compcol_error(), Some(::compcol::Error::NeedDictionary));
        assert!(dec.inner().dictionary_id().is_some());

        dec.inner_mut().set_dictionary(dictionary).unwrap();
        let rest = &compressed[error.consumed..];
        let progress = dec.process_bounded(rest, as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            progress,
            BoundaryAwareProgress::Boundary { consumed: rest.len(), written: 25 }
        );
        assert_eq!(&out[..25], b"a preset dictionary helps");
    }

    // ----
    // 6. After finish
    // ----

    #[test]
    fn calls_after_finish_fail_until_reset() {
        let after_finish =
            own_error(ErrorKind::ContractViolation, Error::SUBCODE_AFTER_FINISH, 0, 0);
        let compressed = gzip_compress(b"once");
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(gzip::Decoder::new());
        drive(&mut dec, &compressed, 64).unwrap();

        let mut out = [0u8; 64];
        assert_eq!(dec.process_bounded(b"x", as_uninit_mut(&mut out)), Err(after_finish));
        assert_eq!(dec.flush(as_uninit_mut(&mut out)), Err(after_finish));
        assert_eq!(dec.finish(as_uninit_mut(&mut out)), Ok(DrainProgress::Done { written: 0 }));
        assert_eq!(dec.compcol_error(), None);

        dec.reset();
        let mut dec2 = CompcolDecoderAsBoundaryAwareCodec::new(gzip::Decoder::new());
        let r1 = drive(&mut dec, &compressed, 64).unwrap().output;
        let r2 = drive(&mut dec2, &compressed, 64).unwrap().output;
        assert_eq!(r1, r2);
    }

    #[test]
    fn calls_after_an_unfinished_finish_fail() {
        let after_finish =
            own_error(ErrorKind::ContractViolation, Error::SUBCODE_AFTER_FINISH, 0, 0);
        let mut enc =
            CompcolEncoderAsBoundaryAwareCodec::new(gzip::Encoder::new(), FlushMode::Skip);
        let mut out = [0u8; 1];
        assert_eq!(enc.finish(as_uninit_mut(&mut out)), Ok(DrainProgress::OutputFilled));

        assert_eq!(enc.process_bounded(b"x", as_uninit_mut(&mut out)), Err(after_finish));
        assert_eq!(enc.flush(as_uninit_mut(&mut out)), Err(after_finish));
    }

    // ----
    // 7. Flush modes (encoder)
    // ----

    #[test]
    fn flush_skip_writes_nothing() {
        let mut enc =
            CompcolEncoderAsBoundaryAwareCodec::new(zlib::Encoder::new(), FlushMode::Skip);
        let mut out = [0u8; 256];
        enc.process_bounded(b"hello", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            enc.flush(as_uninit_mut(&mut out)),
            Ok(DrainProgress::Done { written: 0 })
        );
    }

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
        let progress = dec.process_bounded(&compressed, as_uninit_mut(&mut out)).unwrap();
        let written = match progress {
            BoundaryAwareProgress::Boundary { written, .. } => written,
            BoundaryAwareProgress::InputConsumed { written } => written,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(&out[..written], input as &[u8]);
    }

    // ----
    // 8. Statuses that real codecs do not produce
    // ----

    #[derive(Default)]
    struct Scripted {
        results: VecDeque<CompcolResult>,
    }

    impl Scripted {
        fn new(results: impl IntoIterator<Item = CompcolResult>) -> Self {
            Self { results: results.into_iter().collect() }
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
        // `Scripted::default()` panics on any call to the inner codec.
        let mut enc =
            CompcolEncoderAsBoundaryAwareCodec::new(Scripted::default(), FlushMode::Skip);
        assert_eq!(
            enc.process_bounded(b"abc", &mut []),
            Ok(BoundaryAwareProgress::OutputFilled { consumed: 0 })
        );
        assert_eq!(enc.finish(&mut []), Ok(DrainProgress::OutputFilled));

        let mut enc = CompcolEncoderAsBoundaryAwareCodec::new(
            Scripted::default(),
            FlushMode::Compcol(Flush::Sync),
        );
        assert_eq!(enc.flush(&mut []), Ok(DrainProgress::OutputFilled));

        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(Scripted::default());
        assert_eq!(dec.flush(&mut []), Ok(DrainProgress::OutputFilled));
        assert_eq!(dec.finish(&mut []), Ok(DrainProgress::OutputFilled));
    }

    #[test]
    fn partial_output_full_calls_again() {
        let script = [step(1, 2, Status::OutputFull), step(2, 3, Status::InputEmpty)];
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(Scripted::new(script));
        let mut out = [0u8; 8];
        assert_eq!(
            dec.process_bounded(b"abc", as_uninit_mut(&mut out)),
            Ok(BoundaryAwareProgress::InputConsumed { written: 5 })
        );
    }

    #[test]
    fn no_progress_with_output_full_is_contract_violation() {
        let script = [step(3, 4, Status::OutputFull), step(0, 0, Status::OutputFull)];
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(Scripted::new(script));
        let mut out = [0u8; 6];
        assert_eq!(
            dec.process_bounded(b"abcdef", as_uninit_mut(&mut out)),
            Err(own_error(ErrorKind::ContractViolation, SUBCODE_NO_PROGRESS, 3, 4))
        );
        assert_eq!(dec.compcol_error(), None);
    }

    #[test]
    fn no_progress_with_input_empty_is_unexpected_status() {
        let script = [step(0, 0, Status::InputEmpty)];
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(Scripted::new(script));
        let mut out = [0u8; 6];
        assert_eq!(
            dec.process_bounded(b"abc", as_uninit_mut(&mut out)),
            Err(own_error(ErrorKind::ContractViolation, SUBCODE_UNEXPECTED_STATUS, 0, 0))
        );
    }

    #[test]
    fn stream_end_from_encode_is_unexpected_status() {
        let script = [step(1, 1, Status::StreamEnd)];
        let mut enc = CompcolEncoderAsBoundaryAwareCodec::new(Scripted::new(script), FlushMode::Skip);
        let mut out = [0u8; 6];
        assert_eq!(
            enc.process_bounded(b"abc", as_uninit_mut(&mut out)),
            Err(own_error(ErrorKind::ContractViolation, SUBCODE_UNEXPECTED_STATUS, 1, 1))
        );
    }

    #[test]
    fn input_empty_from_finish_is_unexpected_status() {
        let script = [step(0, 2, Status::InputEmpty)];
        let mut enc = CompcolEncoderAsBoundaryAwareCodec::new(Scripted::new(script), FlushMode::Skip);
        let mut out = [0u8; 6];
        assert_eq!(
            enc.finish(as_uninit_mut(&mut out)),
            Err(own_error(ErrorKind::ContractViolation, SUBCODE_UNEXPECTED_STATUS, 0, 2))
        );
    }

    #[test]
    fn overclaimed_counts_are_a_contract_violation() {
        let script = [step(1, 1, Status::OutputFull), step(0, 99, Status::OutputFull)];
        let mut dec = CompcolDecoderAsBoundaryAwareCodec::new(Scripted::new(script));
        let mut out = [0u8; 6];
        assert_eq!(
            dec.process_bounded(b"abc", as_uninit_mut(&mut out)),
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
        assert_eq!(dec.flush(as_uninit_mut(&mut out)), Ok(DrainProgress::Done { written: 3 }));
        assert_eq!(
            dec.process_bounded(b"next", as_uninit_mut(&mut out)),
            Ok(BoundaryAwareProgress::Boundary { consumed: 0, written: 0 })
        );
    }
}
