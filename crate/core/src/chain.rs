//! [`Chain`]: compose two [`Codec`]s into one.
//!
//! Bytes flow `first` → staging buffer → `second`. Composition happens
//! at the `Codec` level (`Chain` is itself a `Codec`), so every driver in
//! `io` (or a client's own) gets chaining for free without knowing
//! anything about it.

use crate::{Codec, Drain, Error, Outcome};

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
    /// Build a `Chain`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `staging` buffer: it could never hold a byte
    /// for `second` to drain, so the chain could never make progress —
    /// a caller bug, not a runtime condition.
    pub fn new(first: A, second: B, mut staging: S) -> Self {
        assert!(!staging.as_mut().is_empty(), "Chain staging buffer must be non-empty");
        Self { first, second, staging, filled: 0, drained: 0, first_ended: false }
    }

    /// Reset staging indices once `second` has taken everything.
    fn reset_staging_if_drained(&mut self) {
        if self.drained >= self.filled {
            self.drained = 0;
            self.filled = 0;
        }
    }
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Codec for Chain<A, B, S> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
        let mut in_pos = 0;
        let mut out_pos = 0;

        loop {
            // Drain whatever's already staged into the caller's output
            // before asking `first` for more — biggest possible chunks,
            // invisible from outside a single call.
            if self.drained < self.filled {
                let staging = self.staging.as_mut();
                let outcome = self
                    .second
                    .process(&staging[self.drained..self.filled], &mut output[out_pos..])
                    .map_err(|e| Error { consumed: in_pos, written: out_pos, ..e })?;
                match outcome {
                    Outcome::InputConsumed { written } => {
                        self.drained = self.filled;
                        out_pos += written;
                        self.reset_staging_if_drained();
                    }
                    Outcome::OutputFilled { consumed } => {
                        self.drained += consumed;
                        self.reset_staging_if_drained();
                        return Ok(Outcome::OutputFilled { consumed: in_pos });
                    }
                    Outcome::StreamEnd { consumed, written } => {
                        self.drained += consumed;
                        out_pos += written;
                        self.reset_staging_if_drained();
                        return Ok(Outcome::StreamEnd { consumed: in_pos, written: out_pos });
                    }
                }
            }

            // Staging is clean (guaranteed above whenever we get here).
            // Return-clean: don't withhold anything `second` could
            // take — only stop when there's genuinely nothing left to
            // feed `first`.
            if self.first_ended {
                // `first` will never consume another byte (self-terminating
                // format, already past its end) — the rest of `input` is
                // simply not this stream's to read. `InputConsumed` means
                // all of it per the `Codec` contract, so that has to
                // include whatever's left here too, or a caller driving
                // off consumed-counts alone would spin forever
                // re-offering the same unconsumed tail.
                return Ok(Outcome::InputConsumed { written: out_pos });
            }
            if in_pos == input.len() {
                return Ok(Outcome::InputConsumed { written: out_pos });
            }
            if out_pos == output.len() {
                // Output is exactly full with staging clean and input
                // remaining: report the bottleneck rather than churn
                // `first`'s bytes into staging that `second` couldn't
                // move anywhere.
                return Ok(Outcome::OutputFilled { consumed: in_pos });
            }

            let staging = self.staging.as_mut();
            let outcome = self
                .first
                .process(&input[in_pos..], &mut staging[self.filled..])
                .map_err(|e| Error { consumed: in_pos, written: out_pos, ..e })?;
            match outcome {
                Outcome::InputConsumed { written } => {
                    in_pos = input.len();
                    self.filled += written;
                }
                Outcome::OutputFilled { consumed } => {
                    in_pos += consumed;
                    self.filled = staging.len();
                }
                Outcome::StreamEnd { consumed, written } => {
                    in_pos += consumed;
                    self.filled += written;
                    self.first_ended = true;
                }
            }
            // Loop around: drain what was just staged.
        }
    }

    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        let mut out_pos = 0;

        loop {
            // Any leftover staged bytes (from a prior `process`/`finish`
            // call that left the caller's output full) go first.
            if self.drained < self.filled {
                let staging = self.staging.as_mut();
                let outcome = self
                    .second
                    .process(&staging[self.drained..self.filled], &mut output[out_pos..])
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?;
                match outcome {
                    Outcome::InputConsumed { written } => {
                        self.drained = self.filled;
                        out_pos += written;
                        self.reset_staging_if_drained();
                    }
                    Outcome::OutputFilled { consumed } => {
                        self.drained += consumed;
                        self.reset_staging_if_drained();
                        return Ok(Drain::OutputFilled);
                    }
                    Outcome::StreamEnd { consumed, written } => {
                        self.drained += consumed;
                        out_pos += written;
                        self.reset_staging_if_drained();
                        return Ok(Drain::Done { written: out_pos });
                    }
                }
                continue;
            }

            if !self.first_ended {
                let staging = self.staging.as_mut();
                let filled = self.filled;
                match self
                    .first
                    .finish(&mut staging[filled..])
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => {
                        self.filled = staging.len();
                    }
                    Drain::Done { written } => {
                        self.filled += written;
                        self.first_ended = true;
                    }
                }
                continue;
            }

            // `first` is fully drained through `second`; finish `second`.
            return match self
                .second
                .finish(&mut output[out_pos..])
                .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
            {
                Drain::OutputFilled => Ok(Drain::OutputFilled),
                Drain::Done { written } => Ok(Drain::Done { written: out_pos + written }),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Chain;
    use crate::base64::{base64_dec, base64_enc};
    use crate::identity::identity;
    use crate::io::to_vec;
    use crate::rot13::rot13;
    use crate::{Codec, Drain, Error, Outcome};

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
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(Outcome::StreamEnd { consumed: n, written: n })
            } else if n == input.len() {
                Ok(Outcome::InputConsumed { written: n })
            } else {
                Ok(Outcome::OutputFilled { consumed: n })
            }
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn rot13_then_rot13_is_identity() {
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 64]);
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn base64_enc_then_base64_dec_round_trip() {
        let chain = Chain::new(base64_enc(), base64_dec(), vec![0u8; 64]);
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn tiny_staging_buffer_forces_partial_progress() {
        // A 1-byte staging buffer forces `first` and `second` to hand
        // off one byte at a time internally.
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 1]);
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn base64_round_trip_through_one_byte_staging() {
        // The carry contract means even base64's 4-byte groups squeeze
        // through a 1-byte staging buffer — impossible before, when a
        // buffer below the atomic unit was a hard error.
        let chain = Chain::new(base64_enc(), base64_dec(), vec![0u8; 1]);
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn tiny_output_buffer_forces_partial_progress() {
        // A single `process` call with a 1-byte *caller* output buffer:
        // rot13-then-rot13 is the identity, so one byte of `INPUT`
        // should come straight back out, with plenty of input left
        // over to report `OutputFilled` rather than `InputConsumed`.
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 8]);
        let mut out = [0u8; 1];
        let outcome = chain.process(INPUT, &mut out).unwrap();
        assert!(matches!(outcome, Outcome::OutputFilled { .. }));
        assert_eq!(out[0], INPUT[0]);
    }

    #[test]
    fn return_clean_no_hoarding_across_calls() {
        // With generous input and output room in a single call, every
        // byte `second` can produce must come out of *this* call — nothing
        // held back to surface only on a later call or on `finish`.
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 64]);
        let mut out = [0u8; 64];
        let outcome = chain.process(INPUT, &mut out).unwrap();
        assert_eq!(outcome, Outcome::InputConsumed { written: INPUT.len() });
        assert_eq!(&out[..INPUT.len()], INPUT);
    }

    #[test]
    fn finish_drains_first_through_second() {
        // base64_enc's finish() emits the padding `=`; chained into
        // rot13, that padding must come out rot13'd too (not appended
        // raw after Chain's own finish), so a plain to_vec round trip
        // against the independently-computed expected bytes covers it.
        let expected = to_vec(rot13(), &to_vec(base64_enc(), INPUT).unwrap()).unwrap();
        let chain = Chain::new(base64_enc(), rot13(), vec![0u8; 64]);
        assert_eq!(to_vec(chain, INPUT).unwrap(), expected);
    }

    #[test]
    fn first_ends_early_mid_stream() {
        // `first` self-terminates after 3 bytes; `Chain` must latch
        // that, stop feeding `first` the rest of the input, and still
        // finish cleanly through `second` (here, identity).
        let chain = Chain::new(EarlyEnd { limit: 3, done: 0 }, identity(), vec![0u8; 64]);
        assert_eq!(to_vec(chain, b"Hello World").unwrap(), b"Hel");
    }

    #[test]
    fn dyn_composition_compiles_and_runs() {
        let first: Box<dyn Codec> = Box::new(rot13());
        let second: Box<dyn Codec> = Box::new(rot13());
        let chain: Chain<Box<dyn Codec>, Box<dyn Codec>, Vec<u8>> =
            Chain::new(first, second, vec![0u8; 64]);
        assert_eq!(to_vec(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn nested_chain_three_codecs() {
        // rot13 ∘ rot13 ∘ identity == identity, stacked three deep.
        let inner = Chain::new(rot13(), identity(), vec![0u8; 32]);
        let outer = Chain::new(rot13(), inner, vec![0u8; 32]);
        assert_eq!(to_vec(outer, INPUT).unwrap(), INPUT);
    }

    #[test]
    #[should_panic(expected = "staging buffer must be non-empty")]
    fn empty_staging_buffer_panics() {
        let _ = Chain::new(rot13(), rot13(), Vec::<u8>::new());
    }
}
