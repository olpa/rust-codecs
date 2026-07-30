//! One-shot `Vec<u8>` helper over a caller-supplied [`Codec`](crate::Codec)
//! instance.

use alloc::{vec, vec::Vec};

use crate::driver::{DrainEnd, Driver};
use crate::transfer::TransferEnd;
use crate::{Codec, Error};

const SCRATCH: usize = 64 * 1024;

/// Run `codec` over all of `input` and return the transformed bytes.
///
/// # Warning: unbounded output
///
/// This grows the output `Vec` with **no upper bound**. Do not call it on
/// untrusted input without an external size cap.
pub fn to_vec<C: Codec>(codec: C, input: &[u8]) -> Result<Vec<u8>, Error> {
    let mut driver = Driver::new(codec);
    let mut out = Vec::with_capacity(input.len());
    let mut scratch = vec![0u8; SCRATCH];

    // Phase 1: pump the input through `process` — same two-phase shape
    // as `stream_to_stream`, with a single chunk and a reused scratch
    // "slot".
    let mut in_pos = 0;
    while in_pos < input.len() {
        let moved = driver.process(&input[in_pos..], &mut scratch)?;
        in_pos += moved.consumed;
        out.extend_from_slice(&scratch[..moved.written]);
        match moved.end {
            TransferEnd::InputExhausted => {}
            TransferEnd::OutputExhausted => {}
            TransferEnd::StreamEnd => {
                // Trailing input past the in-band end is simply not
                // this stream's to read.
                return Ok(out);
            }
        }
    }

    // Phase 2: drain `finish`.
    loop {
        let moved = driver.finish(&mut scratch)?;
        out.extend_from_slice(&scratch[..moved.written]);
        match moved.end {
            DrainEnd::OutputExhausted => {}
            DrainEnd::Done => return Ok(out),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::to_vec;
    use crate::{Codec, Drain, Error, ErrorKind, Outcome};

    /// A codec that lies: claims more bytes written than the buffer
    /// it was given could hold. Models the kind of poorly written
    /// codec a library must contain rather than trust.
    struct Overclaimer;

    impl Codec for Overclaimer {
        fn process(&mut self, _input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            Ok(Outcome::InputConsumed { written: output.len() + 1 })
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn lying_codec_is_an_error_not_a_panic() {
        // Unchecked, the overclaimed count would make the scratch
        // slice out of range. The trust-boundary validation must turn
        // it into a ContractViolation error instead.
        let result = to_vec(Overclaimer, b"hi");
        assert_eq!(
            result.unwrap_err(),
            Error { kind: ErrorKind::ContractViolation, consumed: 0, written: 0 }
        );
    }
}
