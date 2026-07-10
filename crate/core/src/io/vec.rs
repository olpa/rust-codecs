//! One-shot `Vec<u8>` helper over a caller-supplied [`Codec`](crate::Codec)
//! instance.

use crate::{Codec, Error, Status};

const SCRATCH: usize = 64 * 1024;

/// Run `codec` over all of `input` and return the transformed bytes.
///
/// # Warning: unbounded output
///
/// This grows the output `Vec` with **no upper bound**. Do not call it on
/// untrusted input without an external size cap.
pub fn to_vec<C: Codec>(mut codec: C, input: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(input.len());
    let mut scratch = vec![0u8; SCRATCH];

    let mut consumed = 0usize;
    while consumed < input.len() {
        let (p, status) = codec.process(&input[consumed..], &mut scratch)?;
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
