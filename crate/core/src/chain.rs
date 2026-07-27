//! [`Chain`]: compose two [`Codec`]s into one.
//!
//! Bytes flow `first` → staging buffer → `second`. Composition happens
//! at the `Codec` level (`Chain` is itself a `Codec`), so every driver in
//! `io` (or a client's own) gets chaining for free without knowing
//! anything about it.

use crate::{Codec, Error, Progress, Status};

/// Composes `A` (encodes/decodes into `staging`) and `B` (reads out of
/// `staging`) into a single [`Codec`].
///
/// `staging` is caller-provided, `S: AsMut<[u8]>` — same convention as
/// the `io` adapters — so it can be a borrowed `&mut [u8]`, an inline
/// `[u8; N]`, or a `Vec<u8>` depending on the environment.
pub struct Chain<A, B, S> {
    first: A,
    second: B,
    staging: S,
    /// Bytes in `staging` written by `first`, not yet all drained by `second`.
    filled: usize,
    /// Of `filled`, how many `second` has already consumed.
    drained: usize,
    /// `first` reported `StreamEnd` (or was finished) — stop feeding it.
    first_ended: bool,
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Chain<A, B, S> {
    /// Build a `Chain`. Rejects an empty `staging` buffer: it could never
    /// hold a byte for `second` to drain, so the chain could never make
    /// progress.
    pub fn new(first: A, second: B, mut staging: S) -> Result<Self, Error> {
        if staging.as_mut().is_empty() {
            return Err(Error::OutputTooSmall);
        }
        Ok(Self { first, second, staging, filled: 0, drained: 0, first_ended: false })
    }
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Codec for Chain<A, B, S> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let _ = (input, output);
        // Stub: not yet implemented. Deliberately wrong (rather than
        // `todo!()`) so driving this through `to_vec` fails fast and
        // deterministically (`InputEmpty` on the first call, no
        // progress) instead of spinning, and the tests below fail as
        // real assertion mismatches.
        Ok((Progress::default(), Status::InputEmpty))
    }

    fn finish(&mut self, output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let _ = output;
        Ok((Progress::default(), Status::InputEmpty))
    }
}

#[cfg(test)]
mod tests {
    use super::Chain;
    use crate::base64::{base64_dec, base64_enc};
    use crate::identity::identity;
    use crate::io::to_vec;
    use crate::rot13::rot13;
    use crate::{Codec, Error, Progress, Status};

    const INPUT: &[u8] = b"Hello, World! 123";

    /// A test-only codec that copies bytes 1:1, like `Identity`, but
    /// self-terminates: once `limit` bytes have been written, `process`
    /// reports `StreamEnd` even with more input still in the caller's
    /// slice. Models a self-describing format that ends before the
    /// input stream does.
    struct EarlyEnd {
        limit: usize,
        done: usize,
    }

    impl Codec for EarlyEnd {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            let status = if self.done >= self.limit {
                Status::StreamEnd
            } else if n == input.len() {
                Status::InputEmpty
            } else {
                Status::OutputFull
            };
            Ok((Progress { consumed: n, written: n }, status))
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
            Ok((Progress::default(), Status::StreamEnd))
        }
    }

    #[test]
    fn rot13_then_rot13_is_identity() {
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 64]).unwrap();
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn base64_enc_then_base64_dec_round_trip() {
        let chain = Chain::new(base64_enc(), base64_dec(), vec![0u8; 64]).unwrap();
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn tiny_staging_buffer_forces_partial_progress() {
        // A 1-byte staging buffer forces `first` and `second` to hand
        // off one byte at a time internally.
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 1]).unwrap();
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn tiny_output_buffer_forces_partial_progress() {
        // A single `process` call with a 1-byte *caller* output buffer:
        // rot13-then-rot13 is the identity, so one byte of `INPUT`
        // should come straight back out, with plenty of input left
        // over to report `OutputFull` rather than `InputEmpty`.
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 8]).unwrap();
        let mut out = [0u8; 1];
        let (progress, status) = chain.process(INPUT, &mut out).unwrap();
        assert_eq!(progress.written, 1);
        assert_eq!(out[0], INPUT[0]);
        assert_eq!(status, Status::OutputFull);
    }

    #[test]
    fn return_clean_no_hoarding_across_calls() {
        // With generous input and output room in a single call, every
        // byte `second` can produce must come out of *this* call — nothing
        // held back to surface only on a later call or on `finish`.
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 64]).unwrap();
        let mut out = [0u8; 64];
        let (progress, status) = chain.process(INPUT, &mut out).unwrap();
        assert_eq!(progress.consumed, INPUT.len());
        assert_eq!(progress.written, INPUT.len());
        assert_eq!(&out[..progress.written], INPUT);
        assert_eq!(status, Status::InputEmpty);
    }

    #[test]
    fn finish_drains_first_through_second() {
        // base64_enc's finish() emits the padding `=`; chained into
        // rot13, that padding must come out rot13'd too (not appended
        // raw after Chain's own finish), so a plain to_vec round trip
        // against the independently-computed expected bytes covers it.
        let expected = to_vec(rot13(), &to_vec(base64_enc(), INPUT).unwrap()).unwrap();
        let chain = Chain::new(base64_enc(), rot13(), vec![0u8; 64]).unwrap();
        assert_eq!(to_vec(chain, INPUT).unwrap(), expected);
    }

    #[test]
    fn first_ends_early_mid_stream() {
        // `first` self-terminates after 3 bytes; `Chain` must latch
        // that, stop feeding `first` the rest of the input, and still
        // finish cleanly through `second` (here, identity).
        let chain = Chain::new(EarlyEnd { limit: 3, done: 0 }, identity(), vec![0u8; 64]).unwrap();
        assert_eq!(to_vec(chain, b"Hello World").unwrap(), b"Hel");
    }

    #[test]
    fn dyn_composition_compiles_and_runs() {
        let first: Box<dyn Codec> = Box::new(rot13());
        let second: Box<dyn Codec> = Box::new(rot13());
        let chain: Chain<Box<dyn Codec>, Box<dyn Codec>, Vec<u8>> =
            Chain::new(first, second, vec![0u8; 64]).unwrap();
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn nested_chain_three_codecs() {
        // rot13 ∘ rot13 ∘ identity == identity, stacked three deep.
        let inner = Chain::new(rot13(), identity(), vec![0u8; 32]).unwrap();
        let outer = Chain::new(rot13(), inner, vec![0u8; 32]).unwrap();
        assert_eq!(to_vec(outer, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn empty_staging_buffer_rejected() {
        let result = Chain::new(rot13(), rot13(), Vec::<u8>::new());
        assert!(matches!(result, Err(Error::OutputTooSmall)));
    }
}
