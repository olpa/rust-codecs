//! Base64 encoding codec, built on the `base64` crate (<https://docs.rs/base64/>).
//!
//! This codec belongs in its own crate eventually. See the crate
//! docs' note on why it lives here for now.
//!
//! This file's code is mostly AI-generated.

use core::mem::MaybeUninit;

use base64::engine::general_purpose::{GeneralPurpose, STANDARD};
use base64::engine::Engine;

use super::base64_shared::{self, PendingInput, PendingOutput, ENCODED_GROUP, GROUP};
use crate::{Codec, Drain, DrainCodec, Error, ErrorKind, Progress};

/// Base64 encoder, parameterized over the [`Engine`] (alphabet and
/// padding behavior) it encodes with.
#[derive(Debug, Clone)]
pub struct Base64Enc<E: Engine = GeneralPurpose> {
    engine: E,
    pending_input: PendingInput<GROUP>,
    pending_output: PendingOutput<ENCODED_GROUP>,
}

impl<E: Engine> Base64Enc<E> {
    /// Build a [`Base64Enc`] that encodes with a caller-supplied `Engine`
    /// (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`).
    pub fn with_engine(engine: E) -> Self {
        Self {
            engine,
            pending_input: PendingInput::new(),
            pending_output: PendingOutput::new(),
        }
    }

    fn stage_group(&mut self, group: &[u8], consumed: usize, written: usize) -> Result<(), Error> {
        let engine = &self.engine;
        base64_shared::stage_group(&mut self.pending_output, consumed, written, |buffer| {
            engine
                .encode_slice(group, buffer)
                .map_err(|_| ErrorKind::Corrupt)
        })
    }
}

impl<E: Engine> DrainCodec for Base64Enc<E> {
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        let mut out_pos = self.pending_output.drain(output);
        if !self.pending_output.is_empty() {
            debug_assert_eq!(out_pos, output.len());
            return Ok(Drain::OutputFilled);
        }
        if !self.pending_input.is_empty() {
            // The engine pads a final short group itself — that's why
            // partial groups are deferred to finish and never encoded
            // in process.
            let (group, len) = self.pending_input.take_partial();
            self.stage_group(&group[..len], 0, out_pos)?;
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
            if !self.pending_output.is_empty() {
                debug_assert_eq!(out_pos, output.len());
                return Ok(Drain::OutputFilled);
            }
        }
        Ok(Drain::Done { written: out_pos })
    }
}

impl<E: Engine> Codec for Base64Enc<E> {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let mut in_pos = 0;

        //
        // ## Drain pending output
        //

        let mut out_pos = self.pending_output.drain(output);
        if !self.pending_output.is_empty() {
            debug_assert_eq!(out_pos, output.len());
            return Ok(Progress::OutputFilled { consumed: 0 });
        }

        //
        // ## Collect and encode pending input
        //

        if !self.pending_input.is_empty() {
            in_pos += self.pending_input.fill(input);
            if !self.pending_input.is_full() {
                debug_assert_eq!(in_pos, input.len());
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            let group = self.pending_input.take();
            self.stage_group(&group, in_pos, out_pos)?;
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
            if !self.pending_output.is_empty() {
                debug_assert_eq!(out_pos, output.len());
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
        }

        //
        // ## Encode step: fill output as much as possible
        //

        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let groups = (remaining_in / GROUP).min(remaining_out / ENCODED_GROUP);
        if groups > 0 {
            let in_bytes = groups * GROUP;
            let out_bytes = groups * ENCODED_GROUP;
            // `encode_slice` requires an already-initialized `&mut [u8]`
            // and fully overwrites it before returning; block-init once
            // to bridge to that foreign API rather than reading through
            // `output`'s `MaybeUninit<u8>` elements one at a time.
            let dst = base64_shared::zero_init_mut(&mut output[out_pos..out_pos + out_bytes]);
            out_pos += self
                .engine
                .encode_slice(&input[in_pos..in_pos + in_bytes], dst)
                .map_err(|_| Error::new(ErrorKind::Corrupt, in_pos, out_pos))?;
            in_pos += in_bytes;
        }

        // After bulk, at most one of these holds: a whole input group
        // remains (the output's remainder is under one encoded group —
        // emit through pending_output to fill it completely), or the
        // input remainder is under one group (buffer it and report the
        // input consumed).
        if input.len() - in_pos >= GROUP && out_pos < output.len() {
            self.stage_group(&input[in_pos..in_pos + GROUP], in_pos, out_pos)?;
            in_pos += GROUP;
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
        }
        if input.len() - in_pos >= GROUP {
            debug_assert_eq!(out_pos, output.len());
            return Ok(Progress::OutputFilled { consumed: in_pos });
        }

        //
        // ## Buffer leftover input
        //

        // Buffer any leftover < GROUP bytes for the next call.
        if in_pos < input.len() {
            self.pending_input.set(&input[in_pos..]);
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
    use crate::uninit::as_uninit_mut;

    use super::base64_enc;
    use crate::{Codec, Drain, DrainCodec, Progress};
    use alloc::vec::Vec;

    const INPUT: &str = "Hello, World! 123";
    const ENCODED: &str = "SGVsbG8sIFdvcmxkISAxMjM=";

    #[test]
    fn encode_into_one_byte_outputs() {
        // Drive process/finish by hand with a 1-byte output each call:
        // pending_output must dribble every 4-byte group out one byte
        // at a time, upholding fully-consume-or-fully-fill throughout.
        let input = INPUT.as_bytes();
        let mut enc = base64_enc();
        let mut collected = Vec::new();
        let mut in_pos = 0;
        while in_pos < input.len() {
            let mut out = [0u8; 1];
            match enc
                .process(&input[in_pos..], as_uninit_mut(&mut out))
                .unwrap()
            {
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
            match enc.finish(as_uninit_mut(&mut out)).unwrap() {
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
