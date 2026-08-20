//! Example [`Codec`]s: base64 encode/decode, built on the `base64`
//! crate (<https://docs.rs/base64/>).
//!
//! This codec belongs in its own crate eventually. See the crate
//! docs' note on why it lives here for now.
//!
//! This is the decoder half; see [`super::base64_enc`] for the
//! encoder. Buffers at most one incomplete group on each side of the
//! transform:
//!
//! - a [`PendingInput`] (input side): up to 3 leftover base64
//!   characters, topped up from the next call's input.
//! - a [`Carry`] (output side): the tail of an emitted group that
//!   didn't fit the caller's output buffer, delivered first on the
//!   next call. This is what upholds the fully-consume-or-fully-fill
//!   contract even though base64 can only ever emit whole groups —
//!   any non-empty output buffer works, including a 1-byte one.

use base64::engine::general_purpose::{GeneralPurpose, STANDARD};
use base64::engine::Engine;

use super::base64_shared::{PendingInput, ENCODED_GROUP, GROUP};
use crate::{Carry, Codec, Drain, DrainCodec, Error, ErrorKind, Progress};

/// Base64 decoder, parameterized over the [`Engine`] (alphabet and
/// padding behavior) it decodes with.
///
/// Generic over `E` rather than boxed as `dyn Engine` for the same
/// reason as [`Base64Enc`](crate::codecs::base64_enc::Base64Enc):
/// `encode_slice`/`decode_slice` are generic methods, which can't be
/// called through a trait object.
#[derive(Debug, Clone)]
pub struct Base64Dec<E: Engine = GeneralPurpose> {
    engine: E,
    pending_input: PendingInput<ENCODED_GROUP>,
    pending_output: Carry<GROUP>,
}

impl<E: Engine> Base64Dec<E> {
    /// Build a [`Base64Dec`] that decodes with a caller-supplied `Engine`
    /// (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`).
    pub fn with_engine(engine: E) -> Self {
        Self {
            engine,
            pending_input: PendingInput::new(),
            pending_output: Carry::new(),
        }
    }

    fn stage_group(&mut self, group: &[u8], consumed: usize, written: usize) -> Result<(), Error> {
        let engine = &self.engine;
        let buffer = self
            .pending_output
            .buffer()
            .map_err(|_| Error::new(ErrorKind::BufferOverrun, consumed, written))?;
        let n = engine
            .decode_slice(group, buffer)
            .map_err(|_| Error::new(ErrorKind::Corrupt, consumed, written))?;
        self.pending_output
            .set_len(n)
            .map_err(|_| Error::new(ErrorKind::BufferOverrun, consumed, written))
    }
}

impl<E: Engine> DrainCodec for Base64Dec<E> {
    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        let mut out_pos = self.pending_output.drain(output);
        if !self.pending_output.is_empty() {
            return Ok(Drain::OutputFilled);
        }
        if !self.pending_input.is_empty() {
            // A short trailing group is only valid at true
            // end-of-stream, and only for engines that don't require
            // padding (e.g. URL_SAFE_NO_PAD); the engine itself
            // enforces that — a padded engine like STANDARD rejects an
            // unpadded partial group here.
            let (group, len) = self.pending_input.take_partial();
            self.stage_group(&group[..len], 0, out_pos)
                .map_err(|error| {
                    if error.kind == ErrorKind::Corrupt {
                        Error {
                            kind: ErrorKind::UnexpectedEnd,
                            ..error
                        }
                    } else {
                        error
                    }
                })?;
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
            if !self.pending_output.is_empty() {
                return Ok(Drain::OutputFilled);
            }
        }
        Ok(Drain::Done { written: out_pos })
    }
}

impl<E: Engine> Codec for Base64Dec<E> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        let mut in_pos = 0;

        //
        // ## Drain pending output
        //

        // Deliver the tail of a group from a previous call first.
        let mut out_pos = self.pending_output.drain(output);
        if !self.pending_output.is_empty() {
            return Ok(Progress::OutputFilled { consumed: 0 });
        }

        //
        // ## Collect and encode pending input
        //

        // Top up a pending partial group with fresh input.
        if !self.pending_input.is_empty() {
            in_pos += self.pending_input.fill(input);
            if !self.pending_input.is_full() {
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            if self.pending_input.as_slice().contains(&b'=') {
                // Padding is only valid in the true last group of the
                // whole stream, and `process` can't tell it's at the
                // end — only `finish` can. Defer decoding this group
                // until then. If more input shows up first (right here,
                // or on a future call while the pending group is still
                // full), that proves this wasn't the last group after
                // all.
                if in_pos < input.len() {
                    return Err(Error::new(ErrorKind::Corrupt, in_pos, out_pos));
                }
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            let group = self.pending_input.take();
            self.stage_group(&group, in_pos, out_pos)?;
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
            if !self.pending_output.is_empty() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
        }

        //
        // ## Encode step: fill output as much as possible
        //

        // Bulk-decode as many whole groups as fit both remaining
        // input and output, straight from the caller's slices.
        //
        // This sizing has to be done by hand, same as the encoder:
        // `decode_slice` is all-or-nothing on whatever slice it's
        // given — it either decodes all of it or returns `Err` with
        // nothing written if `output` is too small. Handing it a slice
        // whose length isn't a multiple of `ENCODED_GROUP` would also
        // make it treat that slice as the final chunk, applying
        // end-of-stream padding validation to a group that isn't
        // actually the last one.
        //
        // A group containing padding is excluded from the bulk batch
        // even when it lands last in `input`: `decode_slice` validates
        // padding placement only within the slice it's given, so
        // padding at the end of *this* slice would be accepted even if
        // more (non-padding) input follows in a later `process` call.
        // Capping the batch before that group forces it through the
        // single-group deferred path below instead.
        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let mut groups = (remaining_in / ENCODED_GROUP).min(remaining_out / GROUP);
        if let Some(pad_pos) = input[in_pos..in_pos + groups * ENCODED_GROUP]
            .iter()
            .position(|&b| b == b'=')
        {
            groups = pad_pos / ENCODED_GROUP;
        }
        if groups > 0 {
            let in_bytes = groups * ENCODED_GROUP;
            let out_bytes = groups * GROUP;
            out_pos += self
                .engine
                .decode_slice(
                    &input[in_pos..in_pos + in_bytes],
                    &mut output[out_pos..out_pos + out_bytes],
                )
                .map_err(|_| Error::new(ErrorKind::Corrupt, in_pos, out_pos))?;
            in_pos += in_bytes;
        }

        // After bulk, whole input groups may remain because the next
        // one contains padding (defer it: buffer and stop, `finish`
        // will confirm it's truly last) or because the output's
        // remainder is under one decoded group (decode through
        // pending_output to fill it completely).
        while input.len() - in_pos >= ENCODED_GROUP {
            let next_group: [u8; ENCODED_GROUP] =
                input[in_pos..in_pos + ENCODED_GROUP].try_into().unwrap();
            if next_group.contains(&b'=') {
                self.pending_input.set(&next_group);
                in_pos += ENCODED_GROUP;
                if in_pos < input.len() {
                    return Err(Error::new(ErrorKind::Corrupt, in_pos, out_pos));
                }
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            if out_pos == output.len() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
            self.stage_group(&next_group, in_pos, out_pos)?;
            in_pos += ENCODED_GROUP;
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
            if !self.pending_output.is_empty() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
        }

        //
        // ## Buffer leftover input
        //

        // Buffer any leftover < ENCODED_GROUP characters for the next
        // call.
        if in_pos < input.len() {
            self.pending_input.set(&input[in_pos..]);
        }
        Ok(Progress::InputConsumed { written: out_pos })
    }
}

/// Build a [`Base64Dec`] codec using the standard base64 alphabet with
/// padding. For a different alphabet or padding behavior, use
/// [`Base64Dec::with_engine`].
pub fn base64_dec() -> Base64Dec {
    Base64Dec::with_engine(STANDARD)
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use super::{base64_dec, Base64Dec};
    use crate::codecs::base64_enc::{base64_enc, Base64Enc};
    use crate::sources_and_sinks::vec::encode_string;
    use crate::{Codec, Drain, DrainCodec, Progress};

    const INPUT: &str = "Hello, World! 123";
    const ENCODED: &str = "SGVsbG8sIFdvcmxkISAxMjM=";

    #[test]
    fn round_trip() {
        let encoded = encode_string(base64_enc(), INPUT).unwrap();
        assert_eq!(encoded, ENCODED);
        let decoded = encode_string(base64_dec(), &encoded).unwrap();
        assert_eq!(decoded, INPUT);
    }

    #[test]
    fn round_trip_with_custom_engine() {
        // URL_SAFE_NO_PAD drops the trailing '=' that STANDARD adds,
        // proving with_engine actually swaps the engine rather than
        // silently falling back to STANDARD.
        let encoded = encode_string(Base64Enc::with_engine(URL_SAFE_NO_PAD), INPUT).unwrap();
        assert_eq!(encoded, ENCODED.strip_suffix('=').unwrap());
        let decoded = encode_string(Base64Dec::with_engine(URL_SAFE_NO_PAD), &encoded).unwrap();
        assert_eq!(decoded, INPUT);
    }

    #[test]
    fn decode_truncated_padded_stream_errors() {
        // finish() decodes a short trailing group instead of always
        // erroring (so no-pad engines' final 2-3 char group works),
        // but STANDARD requires padding and must still reject a
        // stream cut off mid-symbol.
        let truncated = &ENCODED[..ENCODED.len() - 2];
        assert!(encode_string(base64_dec(), truncated).is_err());
    }

    #[test]
    fn decode_rejects_padding_before_end_in_one_call() {
        // "QQ==" ("A") followed by more encoded data is corrupt: padding
        // is only valid in the true last group of the stream.
        assert!(encode_string(base64_dec(), "QQ==QQ==").is_err());
    }

    #[test]
    fn decode_rejects_padding_before_end_split_across_calls() {
        // Same corrupt input as above, but fed as two process() calls
        // that each happen to align exactly on the padded group's
        // boundary. Decoding a padded group must be deferred until
        // finish() confirms it's truly last, or this slips through as
        // "AA" instead of being rejected.
        let mut dec = base64_dec();
        let mut out = [0u8; 16];
        dec.process(b"QQ==", &mut out).unwrap();
        assert!(dec.process(b"QQ==", &mut out).is_err());
    }

    #[test]
    fn decode_accepts_padded_final_group_as_sole_input() {
        // A legitimate padded final group handed to `process` on its
        // own (not preceded by any other group in the same call) must
        // still decode successfully once finish() confirms it's last.
        let mut dec = base64_dec();
        let mut out = [0u8; 16];
        let outcome = dec.process(b"QQ==", &mut out).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = dec.finish(&mut out).unwrap();
        assert_eq!(drain, Drain::Done { written: 1 });
        assert_eq!(&out[..1], b"A");
    }
}
