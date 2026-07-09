//! One-shot `Vec<u8>` helpers over a caller-supplied codec instance.
//!
//! These mirror `compcol::vec::compress_to_vec`/`decompress_to_vec`, but
//! take an already-constructed [`Encoder`]/[`Decoder`] value instead of
//! being generic over `Algorithm` — callers build the codec with their
//! crate's own `<name>_encoder()`/`<name>_decoder()` function and pass it
//! in directly.

use rust_codecs_base::{Decoder, Encoder, Error, Status};

const SCRATCH: usize = 64 * 1024;

/// Run `codec` over all of `input` and return the encoded bytes.
pub fn encode_to_vec<E: Encoder>(mut codec: E, input: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(input.len());
    let mut scratch = vec![0u8; SCRATCH];

    let mut consumed = 0usize;
    while consumed < input.len() {
        let (p, status) = codec.encode(&input[consumed..], &mut scratch)?;
        out.extend_from_slice(&scratch[..p.written]);
        consumed += p.consumed;
        match status {
            Status::InputEmpty => break,
            Status::OutputFull => continue,
            Status::StreamEnd => break,
        }
    }
    loop {
        let (p, status) = codec.finish(&mut scratch)?;
        out.extend_from_slice(&scratch[..p.written]);
        if matches!(status, Status::StreamEnd) {
            break;
        }
        if p.written == 0 {
            return Err(Error::Corrupt);
        }
    }
    Ok(out)
}

/// Run `codec` over all of `input` and return the decoded bytes.
///
/// # Warning: unbounded output
///
/// This grows the output `Vec` with **no upper bound**. Do not call it
/// on untrusted input without an external size cap.
pub fn decode_to_vec<D: Decoder>(mut codec: D, input: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(input.len().saturating_mul(2));
    let mut scratch = vec![0u8; SCRATCH];

    let mut consumed = 0usize;
    while consumed < input.len() {
        let (p, status) = codec.decode(&input[consumed..], &mut scratch)?;
        out.extend_from_slice(&scratch[..p.written]);
        consumed += p.consumed;
        match status {
            Status::InputEmpty => break,
            Status::OutputFull => continue,
            Status::StreamEnd => break,
        }
    }
    loop {
        let (p, status) = codec.finish(&mut scratch)?;
        out.extend_from_slice(&scratch[..p.written]);
        if matches!(status, Status::StreamEnd) {
            break;
        }
        if p.written == 0 {
            return Err(Error::Corrupt);
        }
    }
    Ok(out)
}
