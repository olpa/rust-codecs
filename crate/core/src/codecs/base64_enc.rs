//! Base64 [`Codec`] encoder, built on the `base64` crate
//! (<https://docs.rs/base64/>).
//!
//! Buffers at most one incomplete group on each side of the transform:
//!
//! - a [`PendingInput`] (input side): up to 2 leftover raw bytes,
//!   topped up from the next call's input.
//! - a [`Carry`] (output side): the tail of an emitted group that
//!   didn't fit the caller's output buffer, delivered first on the
//!   next call. This is what upholds the fully-consume-or-fully-fill
//!   contract even though base64 can only ever emit whole groups —
//!   any non-empty output buffer works, including a 1-byte one.

use base64::engine::general_purpose::{GeneralPurpose, STANDARD};
use base64::engine::Engine;

use super::base64_shared::{PendingInput, ENCODED_GROUP, GROUP};
use crate::{Carry, Codec, Drain, DrainCodec, Error, ErrorKind, Progress};

/// Base64 encoder, parameterized over the [`Engine`] (alphabet and
/// padding behavior) it encodes with.
///
/// Generic over `E` rather than boxed as `dyn Engine`: `encode_slice`/
/// `decode_slice` (what this codec actually calls) are generic methods
/// on `Engine`, and generic methods can't go in a trait object's
/// vtable — there's no fixed function pointer for an unbounded set of
/// monomorphizations. `Engine` also has associated types (`Config`,
/// `DecodeEstimate`) that differ per implementation, so a single `dyn
/// Engine` couldn't represent more than one concrete engine type
/// anyway. Monomorphized generics are the only option here.
#[derive(Debug, Clone)]
pub struct Base64Enc<E: Engine = GeneralPurpose> {
    engine: E,
    pending: PendingInput<GROUP>,
    carry: Carry<ENCODED_GROUP>,
}

impl<E: Engine> Base64Enc<E> {
    /// Build a [`Base64Enc`] that encodes with a caller-supplied `Engine`
    /// (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`).
    pub fn with_engine(engine: E) -> Self {
        Self {
            engine,
            pending: PendingInput::new(),
            carry: Carry::new(),
        }
    }

    /// Encode one group directly into the carry. Partial input groups
    /// are reserved for `finish`; partial output is handled by draining
    /// the staged group afterward.
    fn stage_group(&mut self, group: &[u8], consumed: usize, written: usize) -> Result<(), Error> {
        let engine = &self.engine;
        let buffer = self
            .carry
            .buffer()
            .map_err(|_| Error::new(ErrorKind::BufferOverrun, consumed, written))?;
        let n = engine
            .encode_slice(group, buffer)
            .map_err(|_| Error::new(ErrorKind::Corrupt, consumed, written))?;
        self.carry
            .set_len(n)
            .map_err(|_| Error::new(ErrorKind::BufferOverrun, consumed, written))
    }
}

impl<E: Engine> DrainCodec for Base64Enc<E> {
    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        let mut out_pos = self.carry.drain(output);
        if !self.carry.is_empty() {
            return Ok(Drain::OutputFilled);
        }
        if !self.pending.is_empty() {
            // The engine pads a final short group itself — that's why
            // partial groups are deferred to finish and never encoded
            // in process.
            let (group, len) = self.pending.take_partial();
            self.stage_group(&group[..len], 0, out_pos)?;
            out_pos += self.carry.drain(&mut output[out_pos..]);
            if !self.carry.is_empty() {
                return Ok(Drain::OutputFilled);
            }
        }
        Ok(Drain::Done { written: out_pos })
    }
}

impl<E: Engine> Codec for Base64Enc<E> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        let mut in_pos = 0;

        //
        // drain carry
        //

        // Deliver the tail of a group from a previous call first.
        let mut out_pos = self.carry.drain(output);
        if !self.carry.is_empty() {
            return Ok(Progress::OutputFilled { consumed: 0 });
        }

        //
        // collect and encode pending input
        //

        // Top up a pending partial group with fresh input; emit it
        // through the carry once complete.
        if !self.pending.is_empty() {
            in_pos += self.pending.fill(input);
            if !self.pending.is_full() {
                // The top-up took everything `input` had.
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            let group = self.pending.take();
            self.stage_group(&group, in_pos, out_pos)?;
            out_pos += self.carry.drain(&mut output[out_pos..]);
            if !self.carry.is_empty() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
        }

        //
        // encode step: fill output as much as possible
        //

        // Bulk-encode as many whole groups as fit both remaining
        // input and output, straight from the caller's slices.
        //
        // This sizing has to be done by hand: `encode_slice` is
        // all-or-nothing on whatever slice it's given — it computes
        // the padded encoded length of the *entire* input and either
        // encodes all of it or returns `Err` with nothing written if
        // `output` is too small, it never partially fills the buffer.
        // Handing it a slice whose length isn't a multiple of `GROUP`
        // would also make it treat that slice as the final chunk and
        // add padding, which must only ever appear once, in `finish`.
        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let groups = (remaining_in / GROUP).min(remaining_out / ENCODED_GROUP);
        if groups > 0 {
            let in_bytes = groups * GROUP;
            let out_bytes = groups * ENCODED_GROUP;
            out_pos += self
                .engine
                .encode_slice(
                    &input[in_pos..in_pos + in_bytes],
                    &mut output[out_pos..out_pos + out_bytes],
                )
                .map_err(|_| Error::new(ErrorKind::Corrupt, in_pos, out_pos))?;
            in_pos += in_bytes;
        }

        // After bulk, at most one of these holds: a whole input group
        // remains (the output's remainder is under one encoded group —
        // emit through the carry to fill it completely), or the input
        // remainder is under one group (buffer it and report the input
        // consumed).
        if input.len() - in_pos >= GROUP && out_pos < output.len() {
            self.stage_group(&input[in_pos..in_pos + GROUP], in_pos, out_pos)?;
            in_pos += GROUP;
            out_pos += self.carry.drain(&mut output[out_pos..]);
        }
        if input.len() - in_pos >= GROUP {
            debug_assert_eq!(out_pos, output.len());
            return Ok(Progress::OutputFilled { consumed: in_pos });
        }

        //
        // buffer leftover input
        //

        // Buffer any leftover < GROUP bytes for the next call.
        if in_pos < input.len() {
            self.pending.set(&input[in_pos..]);
        }
        Ok(Progress::InputConsumed { written: out_pos })
    }
}

/// Build a [`Base64Enc`] codec using the standard base64 alphabet with
/// padding. For a different alphabet or padding behavior, use
/// [`Base64Enc::with_engine`].
pub fn base64_enc() -> Base64Enc {
    Base64Enc::with_engine(STANDARD)
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use super::base64_enc;
    use crate::{Codec, Drain, DrainCodec, Progress};
    use alloc::vec::Vec;

    const INPUT: &str = "Hello, World! 123";
    const ENCODED: &str = "SGVsbG8sIFdvcmxkISAxMjM=";

    #[test]
    fn encode_into_one_byte_outputs() {
        // Drive process/finish by hand with a 1-byte output each call:
        // the carry must dribble every 4-byte group out one byte at a
        // time, upholding fully-consume-or-fully-fill throughout.
        let input = INPUT.as_bytes();
        let mut enc = base64_enc();
        let mut collected = Vec::new();
        let mut in_pos = 0;
        while in_pos < input.len() {
            let mut out = [0u8; 1];
            match enc.process(&input[in_pos..], &mut out).unwrap() {
                Progress::InputConsumed { written } => {
                    collected.extend_from_slice(&out[..written]);
                    in_pos = input.len();
                }
                Progress::OutputFilled { consumed } => {
                    collected.extend_from_slice(&out);
                    in_pos += consumed;
                }
            }
        }
        loop {
            let mut out = [0u8; 1];
            match enc.finish(&mut out).unwrap() {
                Drain::OutputFilled => collected.extend_from_slice(&out),
                Drain::Done { written } => {
                    collected.extend_from_slice(&out[..written]);
                    break;
                }
            }
        }
        assert_eq!(collected, ENCODED.as_bytes());
    }
}
