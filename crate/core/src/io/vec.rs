//! One-shot `Vec<u8>` helper over a caller-supplied [`Codec`](crate::Codec)
//! instance.

use crate::{Codec, Engine, Error, Step};

const SCRATCH: usize = 64 * 1024;

/// Run `codec` over all of `input` and return the transformed bytes.
///
/// # Warning: unbounded output
///
/// This grows the output `Vec` with **no upper bound**. Do not call it on
/// untrusted input without an external size cap.
pub fn to_vec<C: Codec>(codec: C, input: &[u8]) -> Result<Vec<u8>, Error> {
    let mut engine = Engine::new(codec);
    let mut out = Vec::with_capacity(input.len());
    let mut scratch = vec![0u8; SCRATCH];

    let mut in_pos = 0;
    loop {
        // The whole input is already in hand, so it's always "at EOF"
        // once `in_pos` catches up to the end — no separate finishing
        // phase needed here, unlike a driver that pulls input in over
        // time.
        let (consumed, step) = engine.step(&input[in_pos..], true, &mut scratch)?;
        in_pos += consumed;
        match step {
            Step::Wrote(n) => out.extend_from_slice(&scratch[..n]),
            Step::NeedInput => {}
            Step::Done => break,
        }
    }
    Ok(out)
}
