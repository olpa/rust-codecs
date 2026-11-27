//! `encoding_rs` decoders and encoders as [`Codec`]s.
//!
//! - `encoding_rs` crate: <https://crates.io/crates/encoding_rs>
//! - documentation: <https://docs.rs/encoding_rs>
//! - source: <https://github.com/hsivonen/encoding_rs>
//!
//! A decoder changes bytes in a legacy encoding to UTF-8. An encoder
//! changes UTF-8 to bytes in a legacy encoding.
//!
//! Behind the `encoding_rs` feature flag.

use ::encoding_rs::{CoderResult, Decoder, DecoderResult, Encoder, EncoderResult, Encoding};

use crate::step::FinishPhase;
use crate::{Codec, DrainCodec, DrainProgress, Error, ErrorKind, Progress};

// ----
// Error modes and subcodes
// ----

/// What the adapter does with input that the inner codec cannot convert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorMode {
    /// Replace the input and continue.
    ///
    /// A decoder writes U+FFFD REPLACEMENT CHARACTER for each malformed
    /// byte sequence. An encoder writes an HTML numeric character
    /// reference, such as `&#8364;`, for each unmappable character.
    /// This is the behavior of the WHATWG Encoding Standard.
    Replace,
    /// Stop with an error.
    ///
    /// A decoder fails with [`ErrorKind::CorruptStream`]. An encoder
    /// fails with [`ErrorKind::Unsupported`].
    Fatal,
}

/// The decoder found a malformed byte sequence. Only in
/// [`ErrorMode::Fatal`].
pub const SUBCODE_MALFORMED: usize = 1;
/// The encoder found a character that the encoding cannot represent.
/// Only in [`ErrorMode::Fatal`].
pub const SUBCODE_UNMAPPABLE: usize = 2;
/// The input of the encoder is not valid UTF-8.
pub const SUBCODE_INVALID_UTF8: usize = 3;
/// The input of the encoder ended in the middle of a UTF-8 character.
pub const SUBCODE_INCOMPLETE_UTF8: usize = 4;

/// The inner codec made no progress, also with the staging buffer.
const SUBCODE_NO_PROGRESS: usize = 0x101;
/// The inner codec reported the end of input, but it did not read all
/// of the input.
const SUBCODE_INPUT_LEFT: usize = 0x102;
/// The inner codec stopped at a position inside a UTF-8 character.
const SUBCODE_NOT_CHAR_BOUNDARY: usize = 0x103;

fn error(kind: ErrorKind, codec_subcode: usize, consumed: usize, written: usize) -> Error {
    Error {
        kind,
        codec_subcode,
        consumed,
        written,
    }
}

fn contract_violation(codec_subcode: usize, consumed: usize, written: usize) -> Error {
    error(
        ErrorKind::ContractViolation,
        codec_subcode,
        consumed,
        written,
    )
}

// ----
// Staging buffer
// ----

/// The size of the staging buffer.
///
/// `encoding_rs` writes only whole characters. It returns `OutputFull`
/// when the next character does not fit, even if some bytes of output
/// are free. With too small an output, it makes no progress. The
/// adapter then converts into the staging buffer and copies from it.
///
/// One step must fit into the buffer. The worst case is an encoder in
/// [`ErrorMode::Replace`]: it needs room for a numeric character
/// reference (10 bytes) and a character (4 bytes for GB18030), and
/// ISO-2022-JP adds an escape sequence (3 bytes).
const STAGING_LEN: usize = 32;

/// The output that the inner codec wrote, but that did not fit into
/// the output of the caller.
#[derive(Debug, Clone)]
struct Staged {
    buf: [u8; STAGING_LEN],
    pos: usize,
    len: usize,
}

impl Staged {
    const fn new() -> Self {
        Self {
            buf: [0; STAGING_LEN],
            pos: 0,
            len: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.pos == self.len
    }

    /// Copy the held bytes to the start of `out`. Return the number of
    /// copied bytes. After the call, `self` is empty or `out` is full.
    fn drain(&mut self, out: &mut [u8]) -> usize {
        let take = (self.len - self.pos).min(out.len());
        out[..take].copy_from_slice(&self.buf[self.pos..self.pos + take]);
        self.pos += take;
        take
    }
}

// ----
// Transfer loop
// ----

/// Why one call of the inner codec stopped.
enum Status {
    /// The inner codec read all of its input.
    InputEmpty,
    /// The next character does not fit into the output.
    OutputFull,
    /// The inner codec found input that it cannot convert.
    Failed {
        kind: ErrorKind,
        codec_subcode: usize,
    },
}

/// The result of one call of the inner codec.
struct Step {
    status: Status,
    read: usize,
    written: usize,
}

/// How a transfer ended.
enum Transfer {
    /// The inner codec read all of the input. `written` bytes went to
    /// the output. The staging buffer is empty.
    InputEmpty { written: usize },
    /// The output is full. The inner codec read `consumed` bytes.
    OutputFilled { consumed: usize },
}

/// Convert `input_len` bytes of input into `output`.
///
/// `step(offset, dst)` calls the inner codec for the input from
/// `offset`. The closure has the input and the value of `last`.
///
/// The function first copies the staged bytes to `output`. Then it
/// calls `step` until the input is read or `output` is full. When the
/// inner codec stops before `output` is full, the function calls `step`
/// with the staging buffer.
///
/// If `stop_at_input_end` is true, the function returns as soon as all
/// input is read, without a call to `step`. Use `false` for `last`
/// calls: they must reach `InputEmpty` also with empty input.
///
/// The counts of an error are relative to this call.
fn transfer(
    staged: &mut Staged,
    input_len: usize,
    output: &mut [u8],
    stop_at_input_end: bool,
    mut step: impl FnMut(usize, &mut [u8]) -> Step,
) -> Result<Transfer, Error> {
    let mut written = staged.drain(output);
    let mut consumed = 0;
    loop {
        if written == output.len() {
            return Ok(Transfer::OutputFilled { consumed });
        }
        if stop_at_input_end && consumed == input_len {
            return Ok(Transfer::InputEmpty { written });
        }
        // The staging buffer is empty here: `drain` left `output` full,
        // or it emptied the buffer.
        let direct = step(consumed, &mut output[written..]);
        let in_left = input_len - consumed;
        if direct.read > in_left || direct.written > output.len() - written {
            return Err(contract_violation(
                Error::SUBCODE_BYTE_COUNT_CLAIM,
                consumed,
                written,
            ));
        }
        consumed += direct.read;
        written += direct.written;
        match direct.status {
            Status::InputEmpty => return input_empty(consumed, input_len, written),
            Status::Failed {
                kind,
                codec_subcode,
            } => return Err(error(kind, codec_subcode, consumed, written)),
            Status::OutputFull => {
                if written == output.len() {
                    continue;
                }
            }
        }

        // The next character does not fit into the rest of `output`.
        // Convert it into the staging buffer, and copy what fits.
        let staging = step(consumed, &mut staged.buf);
        if staging.read > input_len - consumed || staging.written > STAGING_LEN {
            return Err(contract_violation(
                Error::SUBCODE_BYTE_COUNT_CLAIM,
                consumed,
                written,
            ));
        }
        consumed += staging.read;
        staged.pos = 0;
        staged.len = staging.written;
        written += staged.drain(&mut output[written..]);
        match staging.status {
            Status::InputEmpty => {
                if !staged.is_empty() {
                    return Ok(Transfer::OutputFilled { consumed });
                }
                return input_empty(consumed, input_len, written);
            }
            Status::Failed {
                kind,
                codec_subcode,
            } => return Err(error(kind, codec_subcode, consumed, written)),
            Status::OutputFull => {
                if staging.read == 0 && staging.written == 0 {
                    return Err(contract_violation(SUBCODE_NO_PROGRESS, consumed, written));
                }
            }
        }
    }
}

/// The result for `InputEmpty` from the inner codec. Fail if the inner
/// codec did not read all of the input.
fn input_empty(consumed: usize, input_len: usize, written: usize) -> Result<Transfer, Error> {
    if consumed != input_len {
        return Err(contract_violation(SUBCODE_INPUT_LEFT, consumed, written));
    }
    Ok(Transfer::InputEmpty { written })
}

/// Shared `flush`: write the staged bytes.
fn flush_staged(
    staged: &mut Staged,
    phase: FinishPhase,
    output: &mut [u8],
) -> Result<DrainProgress, Error> {
    phase.check_open()?;
    let written = staged.drain(output);
    if staged.is_empty() {
        Ok(DrainProgress::Done { written })
    } else {
        Ok(DrainProgress::OutputFilled)
    }
}

/// The `finish` result after the inner codec has ended. Write the
/// staged bytes. Record `Done` in `phase`.
fn finish_staged(
    staged: &mut Staged,
    phase: &mut FinishPhase,
    output: &mut [u8],
    written: usize,
) -> DrainProgress {
    let written = written + staged.drain(&mut output[written..]);
    if staged.is_empty() {
        phase.finish_done();
        DrainProgress::Done { written }
    } else {
        DrainProgress::OutputFilled
    }
}

// ----
// Decoder
// ----

/// An `encoding_rs::Decoder` as a [`Codec`].
///
/// The input is bytes in the encoding of the decoder. The output is
/// UTF-8.
///
/// ```
/// # use rust_codecs_core::encoding_rs::{ToUtf8AsCodec, ErrorMode};
/// let codec = ToUtf8AsCodec::new(
///     encoding_rs::SHIFT_JIS.new_decoder_with_bom_removal(),
///     ErrorMode::Replace,
/// );
/// ```
///
/// The decoder sets the BOM handling. Choose it when you make the
/// decoder: `new_decoder`, `new_decoder_with_bom_removal`, or
/// `new_decoder_without_bom_handling`.
///
/// # After `finish`
///
/// `encoding_rs` panics if a decoder is used after the end of its
/// stream. The adapter prevents that. After the first `finish` call,
/// `process` and `flush` fail with [`ErrorKind::ContractViolation`].
/// After `finish` reports `Done`, a new `finish` call reports `Done`
/// again and writes nothing.
///
/// `encoding_rs` has no reset. For a new stream, make a new decoder
/// and a new adapter.
///
/// # Errors
///
/// In [`ErrorMode::Fatal`], a malformed byte sequence gives an error of
/// the kind [`ErrorKind::CorruptStream`] with the subcode
/// [`SUBCODE_MALFORMED`]. The `consumed` field counts the malformed
/// bytes. The malformed bytes can start in an earlier input. To
/// continue after the error, call `process` with the input after
/// `consumed`.
#[derive(Debug)]
pub struct ToUtf8AsCodec {
    inner: Decoder,
    mode: ErrorMode,
    staged: Staged,
    phase: FinishPhase,
    /// The inner decoder reported the end of the stream. Do not call it
    /// again.
    inner_finished: bool,
    had_replacements: bool,
}

impl ToUtf8AsCodec {
    /// A decoder adapter for `inner`.
    pub fn new(inner: Decoder, mode: ErrorMode) -> Self {
        Self {
            inner,
            mode,
            staged: Staged::new(),
            phase: FinishPhase::Open,
            inner_finished: false,
            had_replacements: false,
        }
    }

    /// The inner decoder.
    pub fn get_ref(&self) -> &Decoder {
        &self.inner
    }

    /// The inner decoder, by value. The staged output is lost.
    pub fn into_inner(self) -> Decoder {
        self.inner
    }

    /// The encoding of the decoder. BOM sniffing can change it after
    /// the first bytes of input.
    pub fn encoding(&self) -> &'static Encoding {
        self.inner.encoding()
    }

    /// The error mode.
    pub fn mode(&self) -> ErrorMode {
        self.mode
    }

    /// Whether the decoder wrote U+FFFD for malformed input. Only in
    /// [`ErrorMode::Replace`].
    pub fn had_replacements(&self) -> bool {
        self.had_replacements
    }

    /// Decode `input` into `output`. `last` marks the end of the stream.
    fn decode(&mut self, input: &[u8], output: &mut [u8], last: bool) -> Result<Transfer, Error> {
        let Self {
            inner,
            mode,
            staged,
            had_replacements,
            ..
        } = self;
        transfer(staged, input.len(), output, !last, |offset, dst| {
            let src = &input[offset..];
            match mode {
                ErrorMode::Replace => {
                    let (result, read, written, replaced) = inner.decode_to_utf8(src, dst, last);
                    *had_replacements |= replaced;
                    let status = match result {
                        CoderResult::InputEmpty => Status::InputEmpty,
                        CoderResult::OutputFull => Status::OutputFull,
                    };
                    Step {
                        status,
                        read,
                        written,
                    }
                }
                ErrorMode::Fatal => {
                    let (result, read, written) =
                        inner.decode_to_utf8_without_replacement(src, dst, last);
                    let status = match result {
                        DecoderResult::InputEmpty => Status::InputEmpty,
                        DecoderResult::OutputFull => Status::OutputFull,
                        DecoderResult::Malformed(_, _) => Status::Failed {
                            kind: ErrorKind::CorruptStream,
                            codec_subcode: SUBCODE_MALFORMED,
                        },
                    };
                    Step {
                        status,
                        read,
                        written,
                    }
                }
            }
        })
    }
}

impl DrainCodec for ToUtf8AsCodec {
    /// Write the staged output. A partial character stays in the inner
    /// decoder: only more input or `finish` completes it.
    fn flush(&mut self, output: &mut [u8]) -> Result<DrainProgress, Error> {
        flush_staged(&mut self.staged, self.phase, output)
    }

    /// Write the staged output, and end the stream of the inner decoder.
    /// In [`ErrorMode::Replace`], an incomplete byte sequence at the end
    /// becomes U+FFFD. In [`ErrorMode::Fatal`], it is an error.
    fn finish(&mut self, output: &mut [u8]) -> Result<DrainProgress, Error> {
        if self.phase.is_finished() {
            return Ok(DrainProgress::Done { written: 0 });
        }
        self.phase.start_finish();
        if self.inner_finished {
            return Ok(finish_staged(&mut self.staged, &mut self.phase, output, 0));
        }
        match self.decode(&[], output, true)? {
            Transfer::InputEmpty { written } => {
                self.inner_finished = true;
                Ok(finish_staged(
                    &mut self.staged,
                    &mut self.phase,
                    output,
                    written,
                ))
            }
            Transfer::OutputFilled { .. } => Ok(DrainProgress::OutputFilled),
        }
    }
}

impl Codec for ToUtf8AsCodec {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        self.phase.check_open()?;
        match self.decode(input, output, false)? {
            Transfer::InputEmpty { written } => Ok(Progress::InputConsumed { written }),
            Transfer::OutputFilled { consumed } => Ok(Progress::OutputFilled { consumed }),
        }
    }
}

// ----
// Encoder
// ----

/// Bytes of one UTF-8 character that came at the end of an input.
///
/// The bytes are an incomplete character, or a complete character that
/// did not fit into the output.
#[derive(Debug, Clone)]
struct PendingChar {
    buf: [u8; 4],
    len: usize,
}

impl PendingChar {
    /// The held bytes as a complete character, if they are one.
    fn as_str(&self) -> Option<&str> {
        core::str::from_utf8(&self.buf[..self.len]).ok()
    }
}

/// Whether `bytes` is the start of a valid UTF-8 character, but not a
/// complete character.
fn is_incomplete_char(bytes: &[u8]) -> bool {
    match core::str::from_utf8(bytes) {
        Ok(_) => false,
        Err(e) => e.error_len().is_none(),
    }
}

/// An `encoding_rs::Encoder` as a [`Codec`].
///
/// The input is UTF-8. The output is bytes in the encoding of the
/// encoder. The adapter checks the input. A UTF-8 character can be
/// split between two inputs.
///
/// ```
/// # use rust_codecs_core::encoding_rs::{FromUtf8AsCodec, ErrorMode};
/// let codec = FromUtf8AsCodec::new(
///     encoding_rs::SHIFT_JIS.new_encoder(),
///     ErrorMode::Fatal,
/// );
/// ```
///
/// `encoding_rs` has no encoders for UTF-16LE, UTF-16BE, and
/// "replacement". `new_encoder()` for these encodings gives a UTF-8
/// encoder. See `Encoding::output_encoding`.
///
/// # After `finish`
///
/// After the first `finish` call, `process` and `flush` fail with
/// [`ErrorKind::ContractViolation`]. After `finish` reports `Done`, a
/// new `finish` call reports `Done` again and writes nothing.
///
/// `encoding_rs` has no reset. For a new stream, make a new encoder
/// and a new adapter.
///
/// # Errors
///
/// - The input is not valid UTF-8: [`ErrorKind::CorruptStream`] with
///   the subcode [`SUBCODE_INVALID_UTF8`]. The `consumed` field points
///   to the invalid bytes. This error does not depend on the mode.
/// - The input ends in the middle of a character: `finish` fails with
///   [`ErrorKind::CorruptStream`] and the subcode
///   [`SUBCODE_INCOMPLETE_UTF8`].
/// - In [`ErrorMode::Fatal`], an unmappable character:
///   [`ErrorKind::Unsupported`] with the subcode [`SUBCODE_UNMAPPABLE`].
///   The `consumed` field counts the character. Call
///   [`unmappable`](Self::unmappable) to get it. To continue after the
///   error, call `process` with the input after `consumed`.
#[derive(Debug)]
pub struct FromUtf8AsCodec {
    inner: Encoder,
    mode: ErrorMode,
    staged: Staged,
    pending: PendingChar,
    phase: FinishPhase,
    /// The inner encoder reported the end of the stream. Do not call it
    /// again.
    inner_finished: bool,
    had_unmappables: bool,
    unmappable: Option<char>,
}

impl FromUtf8AsCodec {
    /// An encoder adapter for `inner`.
    pub fn new(inner: Encoder, mode: ErrorMode) -> Self {
        Self {
            inner,
            mode,
            staged: Staged::new(),
            pending: PendingChar {
                buf: [0; 4],
                len: 0,
            },
            phase: FinishPhase::Open,
            inner_finished: false,
            had_unmappables: false,
            unmappable: None,
        }
    }

    /// The inner encoder.
    pub fn get_ref(&self) -> &Encoder {
        &self.inner
    }

    /// The inner encoder, by value. The staged output and a held
    /// partial character are lost.
    pub fn into_inner(self) -> Encoder {
        self.inner
    }

    /// The encoding of the encoder.
    pub fn encoding(&self) -> &'static Encoding {
        self.inner.encoding()
    }

    /// The error mode.
    pub fn mode(&self) -> ErrorMode {
        self.mode
    }

    /// Whether the encoder wrote a numeric character reference for an
    /// unmappable character. Only in [`ErrorMode::Replace`].
    pub fn had_unmappables(&self) -> bool {
        self.had_unmappables
    }

    /// The unmappable character of the last failed call, if the call
    /// failed for this reason. Only in [`ErrorMode::Fatal`].
    pub fn unmappable(&self) -> Option<char> {
        self.unmappable
    }

    /// Encode `input` into `output`. `last` marks the end of the stream.
    fn encode(&mut self, input: &str, output: &mut [u8], last: bool) -> Result<Transfer, Error> {
        let Self {
            inner,
            mode,
            staged,
            had_unmappables,
            unmappable,
            ..
        } = self;
        transfer(staged, input.len(), output, !last, |offset, dst| {
            let Some(src) = input.get(offset..) else {
                return Step {
                    status: Status::Failed {
                        kind: ErrorKind::ContractViolation,
                        codec_subcode: SUBCODE_NOT_CHAR_BOUNDARY,
                    },
                    read: 0,
                    written: 0,
                };
            };
            match mode {
                ErrorMode::Replace => {
                    let (result, read, written, replaced) = inner.encode_from_utf8(src, dst, last);
                    *had_unmappables |= replaced;
                    let status = match result {
                        CoderResult::InputEmpty => Status::InputEmpty,
                        CoderResult::OutputFull => Status::OutputFull,
                    };
                    Step {
                        status,
                        read,
                        written,
                    }
                }
                ErrorMode::Fatal => {
                    let (result, read, written) =
                        inner.encode_from_utf8_without_replacement(src, dst, last);
                    let status = match result {
                        EncoderResult::InputEmpty => Status::InputEmpty,
                        EncoderResult::OutputFull => Status::OutputFull,
                        EncoderResult::Unmappable(c) => {
                            *unmappable = Some(c);
                            Status::Failed {
                                kind: ErrorKind::Unsupported,
                                codec_subcode: SUBCODE_UNMAPPABLE,
                            }
                        }
                    };
                    Step {
                        status,
                        read,
                        written,
                    }
                }
            }
        })
    }

    /// Complete the held character with bytes from `input`, and encode
    /// it into `output`.
    ///
    /// Return the number of bytes taken from `input`, the number of
    /// bytes written, and whether the character is still held. The
    /// character stays held if `input` ends before it is complete, or
    /// if `output` is full.
    fn encode_pending(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(usize, usize, bool), Error> {
        let mut taken = 0;
        while is_incomplete_char(&self.pending.buf[..self.pending.len]) {
            let Some(&byte) = input.get(taken) else {
                return Ok((taken, 0, true));
            };
            self.pending.buf[self.pending.len] = byte;
            self.pending.len += 1;
            taken += 1;
        }
        // A copy, because `encode` borrows all of `self`.
        let buf = self.pending.buf;
        let Ok(text) = core::str::from_utf8(&buf[..self.pending.len]) else {
            self.pending.len = 0;
            // The byte that broke the character is not consumed.
            return Err(error(
                ErrorKind::CorruptStream,
                SUBCODE_INVALID_UTF8,
                taken.saturating_sub(1),
                0,
            ));
        };
        let text_len = text.len();
        match self.encode(text, output, false) {
            Ok(Transfer::InputEmpty { written }) => {
                self.pending.len = 0;
                Ok((taken, written, false))
            }
            Ok(Transfer::OutputFilled { consumed }) => {
                if consumed == text_len {
                    self.pending.len = 0;
                    Ok((taken, output.len(), false))
                } else {
                    Ok((taken, output.len(), true))
                }
            }
            Err(e) => {
                if e.consumed == text_len {
                    self.pending.len = 0;
                }
                Err(Error {
                    consumed: if e.consumed == text_len { taken } else { 0 },
                    ..e
                })
            }
        }
    }
}

impl DrainCodec for FromUtf8AsCodec {
    /// Write the staged output. A held partial character stays: only
    /// more input completes it.
    fn flush(&mut self, output: &mut [u8]) -> Result<DrainProgress, Error> {
        flush_staged(&mut self.staged, self.phase, output)
    }

    /// Write the staged output and a held character, and end the stream
    /// of the inner encoder. The ISO-2022-JP encoder writes the escape
    /// sequence back to ASCII here.
    fn finish(&mut self, output: &mut [u8]) -> Result<DrainProgress, Error> {
        self.unmappable = None;
        if self.phase.is_finished() {
            return Ok(DrainProgress::Done { written: 0 });
        }
        self.phase.start_finish();
        if self.inner_finished {
            return Ok(finish_staged(&mut self.staged, &mut self.phase, output, 0));
        }
        let mut written = 0;
        if self.pending.len > 0 {
            if self.pending.as_str().is_none() {
                return Err(error(
                    ErrorKind::CorruptStream,
                    SUBCODE_INCOMPLETE_UTF8,
                    0,
                    0,
                ));
            }
            let (_, w, still_held) = self.encode_pending(&[], output)?;
            if still_held {
                return Ok(DrainProgress::OutputFilled);
            }
            written = w;
        }
        match self.encode("", &mut output[written..], true)? {
            Transfer::InputEmpty { written: w } => {
                self.inner_finished = true;
                Ok(finish_staged(
                    &mut self.staged,
                    &mut self.phase,
                    output,
                    written + w,
                ))
            }
            Transfer::OutputFilled { .. } => Ok(DrainProgress::OutputFilled),
        }
    }
}

impl Codec for FromUtf8AsCodec {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        self.unmappable = None;
        self.phase.check_open()?;

        //
        // Complete and encode a held character
        //

        let mut taken = 0;
        let mut written = 0;
        if self.pending.len > 0 {
            // The staged bytes come before the held character.
            written = self.staged.drain(output);
            if !self.staged.is_empty() {
                return Ok(Progress::OutputFilled { consumed: 0 });
            }
            let (t, w, still_held) =
                self.encode_pending(input, &mut output[written..])
                    .map_err(|e| Error {
                        written: written + e.written,
                        ..e
                    })?;
            taken = t;
            written += w;
            if still_held {
                if taken == input.len() && written < output.len() {
                    return Ok(Progress::InputConsumed { written });
                }
                return Ok(Progress::OutputFilled { consumed: taken });
            }
        }

        //
        // Encode the valid UTF-8 prefix
        //

        let rest = &input[taken..];
        let Some(chunk) = rest.utf8_chunks().next() else {
            return Ok(Progress::InputConsumed { written });
        };
        let valid = chunk.valid();
        let invalid = chunk.invalid();
        let fail = |e: Error| Error {
            consumed: taken + e.consumed,
            written: written + e.written,
            ..e
        };
        match self
            .encode(valid, &mut output[written..], false)
            .map_err(fail)?
        {
            Transfer::InputEmpty { written: w } => written += w,
            Transfer::OutputFilled { consumed } => {
                return Ok(Progress::OutputFilled {
                    consumed: taken + consumed,
                });
            }
        }

        //
        // Hold an incomplete character at the end, or fail
        //

        if invalid.is_empty() {
            return Ok(Progress::InputConsumed { written });
        }
        let at_end = valid.len() + invalid.len() == rest.len();
        if at_end && is_incomplete_char(invalid) {
            self.pending.buf[..invalid.len()].copy_from_slice(invalid);
            self.pending.len = invalid.len();
            return Ok(Progress::InputConsumed { written });
        }
        Err(error(
            ErrorKind::CorruptStream,
            SUBCODE_INVALID_UTF8,
            taken + valid.len(),
            written,
        ))
    }
}

// ----
// Tests
// ----

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use alloc::string::String;
    use alloc::vec::Vec;
    use core::num::NonZeroUsize;

    use ::encoding_rs::{
        BIG5, EUC_JP, EUC_KR, GB18030, ISO_2022_JP, SHIFT_JIS, UTF_16LE, UTF_8, WINDOWS_1252,
    };

    use super::*;
    use crate::codecs::test_support::{OneByteAtATimeSink, SplitSource};
    use crate::sources_and_sinks::vec::VecSink;
    use crate::stream_to_stream;

    const TEXT: &str = "Hello, 日本語のテキスト, 中文, 한국어, ascii again. ";

    /// Run `codec` over `input` with `stream_to_stream`. The source
    /// gives at most `chunk_size` bytes at a time. If `chunk_size` is 1,
    /// the sink also offers 1 byte at a time.
    fn run(codec: &mut dyn Codec, input: &[u8], chunk_size: usize) -> Vec<u8> {
        let mut source = SplitSource {
            bytes: input,
            pos: 0,
            chunk_size,
        };
        if chunk_size == 1 {
            let mut sink = OneByteAtATimeSink::new();
            stream_to_stream(&mut source, codec, &mut sink).unwrap();
            return sink.bytes;
        }
        let mut sink = VecSink::with_max_window(Vec::new(), NonZeroUsize::new(chunk_size).unwrap());
        stream_to_stream(&mut source, codec, &mut sink).unwrap();
        sink.into_inner()
    }

    fn text(repeat: usize) -> String {
        TEXT.repeat(repeat)
    }

    // ----
    // Sanity checks
    // ----

    /// "grüß" in Latin-1: `ü` is 0xFC, `ß` is 0xDF.
    const GRUSS_LATIN1: &[u8] = b"gr\xFC\xDF";

    /// The encoding for the label "latin1". The WHATWG Encoding Standard
    /// maps it to windows-1252, which is the same as Latin-1 for "grüß".
    fn latin1() -> &'static Encoding {
        Encoding::for_label(b"latin1").unwrap()
    }

    #[test]
    fn sanity_decode_latin1_to_utf8() {
        let mut dec = ToUtf8AsCodec::new(latin1().new_decoder(), ErrorMode::Fatal);
        let mut out = [0u8; 16];
        let progress = dec.process(GRUSS_LATIN1, &mut out).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 6 });
        assert_eq!(&out[..6], "grüß".as_bytes());
        assert_eq!(dec.finish(&mut out), Ok(DrainProgress::Done { written: 0 }));
    }

    #[test]
    fn sanity_encode_utf8_to_latin1() {
        let mut enc = FromUtf8AsCodec::new(latin1().new_encoder(), ErrorMode::Fatal);
        let mut out = [0u8; 16];
        let progress = enc.process("grüß".as_bytes(), &mut out).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 4 });
        assert_eq!(&out[..4], GRUSS_LATIN1);
        assert_eq!(enc.finish(&mut out), Ok(DrainProgress::Done { written: 0 }));
    }

    #[test]
    fn sanity_encoder_rejects_input_that_is_not_utf8() {
        // Latin-1 bytes are not valid UTF-8: 0xFC is not followed by
        // continuation bytes.
        let mut enc = FromUtf8AsCodec::new(latin1().new_encoder(), ErrorMode::Fatal);
        let mut out = [0u8; 16];
        let e = enc.process(GRUSS_LATIN1, &mut out).unwrap_err();
        assert_eq!(
            e,
            error(ErrorKind::CorruptStream, SUBCODE_INVALID_UTF8, 2, 2)
        );
        assert_eq!(&out[..2], b"gr");
    }

    // ----
    // Detailed tests
    // ----

    #[test]
    fn decode_matches_one_shot_decode() {
        let text = text(50);
        for encoding in [SHIFT_JIS, EUC_JP, ISO_2022_JP, GB18030, BIG5, EUC_KR, UTF_8] {
            let (bytes, _, _) = encoding.encode(&text);
            let (expected, _) = encoding.decode_without_bom_handling(&bytes);
            for chunk in [1, 2, 3, 7, 4096] {
                let mut codec = ToUtf8AsCodec::new(
                    encoding.new_decoder_without_bom_handling(),
                    ErrorMode::Replace,
                );
                let output = run(&mut codec, &bytes, chunk);
                assert_eq!(
                    output,
                    expected.as_bytes(),
                    "{} chunk {chunk}",
                    encoding.name()
                );
                assert!(!codec.had_replacements());
            }
        }
    }

    #[test]
    fn decode_utf16_with_bom_sniffing() {
        let mut bytes = alloc::vec![0xFF, 0xFE];
        for unit in TEXT.encode_utf16() {
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        for chunk in [1, 3, 4096] {
            let mut codec = ToUtf8AsCodec::new(WINDOWS_1252.new_decoder(), ErrorMode::Fatal);
            let output = run(&mut codec, &bytes, chunk);
            assert_eq!(output, TEXT.as_bytes(), "chunk {chunk}");
            assert_eq!(codec.encoding(), UTF_16LE);
        }
    }

    #[test]
    fn encode_matches_one_shot_encode() {
        let text = text(50);
        for encoding in [
            SHIFT_JIS,
            EUC_JP,
            ISO_2022_JP,
            GB18030,
            BIG5,
            EUC_KR,
            WINDOWS_1252,
        ] {
            let (expected, _, _) = encoding.encode(&text);
            for chunk in [1, 2, 3, 7, 4096] {
                let mut codec = FromUtf8AsCodec::new(encoding.new_encoder(), ErrorMode::Replace);
                let output = run(&mut codec, text.as_bytes(), chunk);
                assert_eq!(output, &expected[..], "{} chunk {chunk}", encoding.name());
            }
        }
    }

    #[test]
    fn replace_mode_reports_replacements() {
        let mut dec =
            ToUtf8AsCodec::new(UTF_8.new_decoder_without_bom_handling(), ErrorMode::Replace);
        assert_eq!(run(&mut dec, b"a\xFFb", 1), "a\u{FFFD}b".as_bytes());
        assert!(dec.had_replacements());

        let mut enc = FromUtf8AsCodec::new(WINDOWS_1252.new_encoder(), ErrorMode::Replace);
        assert_eq!(run(&mut enc, "a日b".as_bytes(), 1), b"a&#26085;b");
        assert!(enc.had_unmappables());
    }

    #[test]
    fn fatal_decoder_reports_malformed_input() {
        let mut dec = ToUtf8AsCodec::new(
            SHIFT_JIS.new_decoder_without_bom_handling(),
            ErrorMode::Fatal,
        );
        let mut out = [0u8; 64];
        // 0x82 0xA0 is "あ". 0x82 0x20 is malformed.
        let e = dec.process(b"ab\x82\xA0\x82\x20cd", &mut out).unwrap_err();
        assert_eq!(e.kind, ErrorKind::CorruptStream);
        assert_eq!(e.codec_subcode, SUBCODE_MALFORMED);
        assert_eq!(&out[..e.written], "abあ".as_bytes());

        // Continue after the error.
        let rest = &b"ab\x82\xA0\x82\x20cd"[e.consumed..];
        let progress = dec.process(rest, &mut out).unwrap();
        let Progress::InputConsumed { written } = progress else {
            panic!("unexpected: {progress:?}");
        };
        assert!(out[..written].ends_with(b"cd"));
    }

    #[test]
    fn fatal_decoder_rejects_a_truncated_sequence_at_finish() {
        let mut dec = ToUtf8AsCodec::new(
            SHIFT_JIS.new_decoder_without_bom_handling(),
            ErrorMode::Fatal,
        );
        let mut out = [0u8; 16];
        assert_eq!(
            dec.process(b"a\x82", &mut out),
            Ok(Progress::InputConsumed { written: 1 })
        );
        let e = dec.finish(&mut out).unwrap_err();
        assert_eq!(e.kind, ErrorKind::CorruptStream);
        assert_eq!(e.codec_subcode, SUBCODE_MALFORMED);
    }

    #[test]
    fn fatal_encoder_reports_the_unmappable_character() {
        let mut enc = FromUtf8AsCodec::new(WINDOWS_1252.new_encoder(), ErrorMode::Fatal);
        let mut out = [0u8; 64];
        let input = "ab日cd".as_bytes();
        let e = enc.process(input, &mut out).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Unsupported);
        assert_eq!(e.codec_subcode, SUBCODE_UNMAPPABLE);
        assert_eq!(e.consumed, 5);
        assert_eq!(&out[..e.written], b"ab");
        assert_eq!(enc.unmappable(), Some('日'));

        let progress = enc.process(&input[e.consumed..], &mut out).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 2 });
        assert_eq!(&out[..2], b"cd");
        assert_eq!(enc.unmappable(), None);
    }

    #[test]
    fn fatal_encoder_reports_an_unmappable_split_character() {
        let mut enc = FromUtf8AsCodec::new(WINDOWS_1252.new_encoder(), ErrorMode::Fatal);
        let mut out = [0u8; 64];
        let input = "a日".as_bytes();
        assert_eq!(
            enc.process(&input[..2], &mut out),
            Ok(Progress::InputConsumed { written: 1 })
        );
        let e = enc.process(&input[2..], &mut out).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Unsupported);
        assert_eq!(e.consumed, 2);
        assert_eq!(enc.unmappable(), Some('日'));
    }

    #[test]
    fn encoder_rejects_invalid_utf8() {
        let mut enc = FromUtf8AsCodec::new(SHIFT_JIS.new_encoder(), ErrorMode::Replace);
        let mut out = [0u8; 64];
        let e = enc.process(b"ab\xFFcd", &mut out).unwrap_err();
        assert_eq!(
            e,
            error(ErrorKind::CorruptStream, SUBCODE_INVALID_UTF8, 2, 2)
        );
        assert_eq!(&out[..2], b"ab");
    }

    #[test]
    fn encoder_rejects_invalid_utf8_after_a_split_character() {
        let mut enc = FromUtf8AsCodec::new(SHIFT_JIS.new_encoder(), ErrorMode::Replace);
        let mut out = [0u8; 64];
        assert_eq!(
            enc.process(b"a\xE6", &mut out),
            Ok(Progress::InputConsumed { written: 1 })
        );
        let e = enc.process(b"\x97x", &mut out).unwrap_err();
        assert_eq!(e.kind, ErrorKind::CorruptStream);
        assert_eq!(e.codec_subcode, SUBCODE_INVALID_UTF8);
        assert_eq!(e.consumed, 1);
    }

    #[test]
    fn encoder_rejects_an_incomplete_character_at_finish() {
        let mut enc = FromUtf8AsCodec::new(SHIFT_JIS.new_encoder(), ErrorMode::Replace);
        let mut out = [0u8; 64];
        assert_eq!(
            enc.process(b"a\xE6\x97", &mut out),
            Ok(Progress::InputConsumed { written: 1 })
        );
        assert_eq!(
            enc.finish(&mut out),
            Err(error(
                ErrorKind::CorruptStream,
                SUBCODE_INCOMPLETE_UTF8,
                0,
                0
            ))
        );
    }

    #[test]
    fn iso_2022_jp_finish_writes_the_escape_to_ascii_byte_by_byte() {
        let mut enc = FromUtf8AsCodec::new(ISO_2022_JP.new_encoder(), ErrorMode::Fatal);
        let mut out = [0u8; 64];
        let mut pos = 0;
        let input = "日".as_bytes();
        let mut consumed = 0;
        while consumed < input.len() {
            match enc
                .process(&input[consumed..], &mut out[pos..pos + 1])
                .unwrap()
            {
                Progress::InputConsumed { written } => {
                    pos += written;
                    consumed = input.len();
                }
                Progress::OutputFilled { consumed: c } => {
                    pos += 1;
                    consumed += c;
                }
            }
        }
        loop {
            match enc.finish(&mut out[pos..pos + 1]).unwrap() {
                DrainProgress::OutputFilled => pos += 1,
                DrainProgress::Done { written } => {
                    pos += written;
                    break;
                }
            }
        }
        let (expected, _, _) = ISO_2022_JP.encode("日");
        assert_eq!(&out[..pos], &expected[..]);
        assert!(expected.ends_with(b"\x1B(B"));
    }

    #[test]
    fn calls_after_finish_do_not_reach_the_inner_codec() {
        let after_finish = contract_violation(Error::SUBCODE_AFTER_FINISH, 0, 0);
        let mut out = [0u8; 16];

        let mut dec = ToUtf8AsCodec::new(UTF_8.new_decoder(), ErrorMode::Replace);
        assert_eq!(dec.finish(&mut out), Ok(DrainProgress::Done { written: 0 }));
        assert_eq!(dec.process(b"x", &mut out), Err(after_finish));
        assert_eq!(dec.flush(&mut out), Err(after_finish));
        assert_eq!(dec.finish(&mut out), Ok(DrainProgress::Done { written: 0 }));

        let mut enc = FromUtf8AsCodec::new(SHIFT_JIS.new_encoder(), ErrorMode::Replace);
        assert_eq!(enc.finish(&mut out), Ok(DrainProgress::Done { written: 0 }));
        assert_eq!(enc.process(b"x", &mut out), Err(after_finish));
        assert_eq!(enc.flush(&mut out), Err(after_finish));
        assert_eq!(enc.finish(&mut out), Ok(DrainProgress::Done { written: 0 }));
    }

    #[test]
    fn empty_output_makes_no_inner_call() {
        let mut dec = ToUtf8AsCodec::new(UTF_8.new_decoder(), ErrorMode::Replace);
        assert_eq!(
            dec.process(b"abc", &mut []),
            Ok(Progress::OutputFilled { consumed: 0 })
        );
        let mut enc = FromUtf8AsCodec::new(SHIFT_JIS.new_encoder(), ErrorMode::Replace);
        assert_eq!(
            enc.process(b"abc", &mut []),
            Ok(Progress::OutputFilled { consumed: 0 })
        );
    }
}
