//! [`Engine`]: the drive loop shared by [`CodecReader`](crate::io::CodecReader),
//! [`CodecWriter`](crate::io::CodecWriter), and [`to_vec`](crate::io::to_vec) —
//! process-vs-finish selection and end-of-stream latching, instead of
//! each of the three re-implementing it slightly differently.

use crate::{Codec, Drain, Error, Outcome};

/// What to do after one crank of [`Engine::step`].
#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Nothing was written; call again with more input (or, once at
    /// EOF, the same empty input again to move toward `finish`).
    NeedInput,
    /// `output` was empty, so nothing could be written regardless of
    /// how much input there was; call again with a fresh, non-empty
    /// buffer.
    NeedOutput,
    /// `n` bytes landed in `output`. There may or may not be more to
    /// come — call again to find out.
    Wrote(usize),
    /// The codec has emitted everything it will ever emit. Every later
    /// call returns this immediately without touching the codec again.
    Done,
}

/// Drives a single [`Codec`] through `process`/`finish`, hiding the
/// bookkeeping every driver needs: which method to call, and latching
/// the end of the stream once seen (a call can both deliver final
/// bytes *and* end — `Done` only surfaces once nothing more is
/// written).
pub struct Engine<C> {
    codec: C,
    finishing: bool,
    done: bool,
}

impl<C: Codec> Engine<C> {
    pub fn new(codec: C) -> Self {
        Self { codec, finishing: false, done: false }
    }

    /// Unwrap this engine, discarding its `process`/`finish` state, and
    /// return the codec.
    pub fn into_inner(self) -> C {
        self.codec
    }

    /// Whether the codec has already emitted everything it ever will.
    /// Lets a driver that latches its own progress across a call (e.g.
    /// pulling the next chunk from an iterator only once it needs to)
    /// check *before* doing that work, rather than finding out only
    /// after an unnecessary pull — `step` can report `Wrote(n)` and
    /// become done in the very same call, since a call can both
    /// deliver final bytes and end the stream.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// One turn of the crank. `input` is the bytes available right now
    /// (may be empty); `at_eof` says no more will ever come once
    /// `input` runs out. Returns how much of `input` was consumed and
    /// what happened.
    ///
    /// Once finishing has started (a prior call saw `at_eof` with
    /// empty input), `input` is ignored — only `output` matters —
    /// matching `Codec::finish`'s contract of being callable
    /// repeatedly until [`Drain::Done`].
    pub fn step(
        &mut self,
        input: &[u8],
        at_eof: bool,
        output: &mut [u8],
    ) -> Result<(usize, Step), Error> {
        if self.done {
            return Ok((0, Step::Done));
        }

        if self.finishing || (input.is_empty() && at_eof) {
            self.finishing = true;
            return match self.codec.finish(output).and_then(|d| d.validated(output.len()))? {
                Drain::OutputFilled => {
                    if output.is_empty() {
                        // Trivially "filled" a zero-length buffer — the
                        // codec owes bytes it had nowhere to put.
                        Ok((0, Step::NeedOutput))
                    } else {
                        Ok((0, Step::Wrote(output.len())))
                    }
                }
                Drain::Done { written } => {
                    self.done = true;
                    if written > 0 {
                        Ok((0, Step::Wrote(written)))
                    } else {
                        Ok((0, Step::Done))
                    }
                }
            };
        }

        if input.is_empty() {
            // Not at EOF: nothing to do until the caller supplies more.
            return Ok((0, Step::NeedInput));
        }
        if output.is_empty() {
            return Ok((0, Step::NeedOutput));
        }

        match self.codec.process(input, output).and_then(|o| o.validated(input.len(), output.len()))? {
            Outcome::InputConsumed { written } => {
                let step = if written > 0 { Step::Wrote(written) } else { Step::NeedInput };
                Ok((input.len(), step))
            }
            Outcome::OutputFilled { consumed } => Ok((consumed, Step::Wrote(output.len()))),
            Outcome::StreamEnd { consumed, written } => {
                self.done = true;
                let step = if written > 0 { Step::Wrote(written) } else { Step::Done };
                Ok((consumed, step))
            }
        }
    }

    /// Pass `Codec::flush` straight through. Sync-flush (drain to an
    /// in-band boundary without ending the stream) is codec-specific
    /// and orthogonal to the process/finish state this engine tracks —
    /// it never ends the stream, so there's no latching to do.
    pub fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        self.codec.flush(output).and_then(|d| d.validated(output.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::{Engine, Step};
    use crate::base64::base64_enc;
    use crate::io::to_vec;
    use crate::rot13::rot13;
    use crate::{Codec, Drain, Error, Outcome};

    /// A test-only codec that copies bytes 1:1 and signals `StreamEnd`
    /// as soon as `process` sees all of its input, counting how many
    /// times each method is called — so a test can prove `Engine`
    /// doesn't call the codec again once it's `Done`.
    #[derive(Default)]
    struct CountingCodec {
        process_calls: usize,
        finish_calls: usize,
    }

    impl Codec for CountingCodec {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            self.process_calls += 1;
            let n = input.len().min(output.len());
            output[..n].copy_from_slice(&input[..n]);
            if n == input.len() {
                Ok(Outcome::StreamEnd { consumed: n, written: n })
            } else {
                Ok(Outcome::OutputFilled { consumed: n })
            }
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            self.finish_calls += 1;
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn process_reports_wrote_when_output_has_room() {
        let mut engine = Engine::new(rot13());
        let mut out = [0u8; 8];
        let (consumed, step) = engine.step(b"Hello", false, &mut out).unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(step, Step::Wrote(5));
        assert_eq!(&out[..5], to_vec(rot13(), b"Hello").unwrap().as_slice());
    }

    #[test]
    fn process_reports_need_input_when_buffering_without_output() {
        // base64_enc needs 3 raw bytes per group; one byte just gets
        // buffered, with nothing to write yet.
        let mut engine = Engine::new(base64_enc());
        let mut out = [0u8; 8];
        let (consumed, step) = engine.step(b"A", false, &mut out).unwrap();
        assert_eq!(consumed, 1);
        assert_eq!(step, Step::NeedInput);
    }

    #[test]
    fn small_output_forces_multiple_wrote_steps() {
        let mut engine = Engine::new(rot13());
        let input = b"Hello";
        let mut in_pos = 0;
        let mut collected = Vec::new();
        loop {
            let mut out = [0u8; 2];
            let (consumed, step) = engine.step(&input[in_pos..], false, &mut out).unwrap();
            in_pos += consumed;
            match step {
                Step::Wrote(n) => collected.extend_from_slice(&out[..n]),
                other => panic!("unexpected {other:?}"),
            }
            if in_pos == input.len() {
                break;
            }
        }
        assert_eq!(collected, to_vec(rot13(), input).unwrap());
    }

    #[test]
    fn finish_reaches_done_immediately_for_stateless_codec() {
        let mut engine = Engine::new(rot13());
        let mut out = [0u8; 8];
        let (consumed, step) = engine.step(&[], true, &mut out).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(step, Step::Done);
    }

    #[test]
    fn finish_delivers_final_bytes_then_done_on_next_call() {
        let mut engine = Engine::new(base64_enc());
        let mut out = [0u8; 8];
        // Buffer one leftover byte (not a full group of 3).
        let (_, step) = engine.step(b"A", false, &mut out).unwrap();
        assert_eq!(step, Step::NeedInput);

        // Finishing must flush the padded group in this call...
        let (consumed, step) = engine.step(&[], true, &mut out).unwrap();
        assert_eq!(consumed, 0);
        assert!(matches!(step, Step::Wrote(n) if n > 0));

        // ...and only report Done on the next one.
        let (consumed, step) = engine.step(&[], true, &mut out).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(step, Step::Done);
    }

    #[test]
    fn finish_spans_multiple_small_buffers() {
        // A 2-byte buffer is smaller than base64's 4-byte padded
        // trailer group; the codec's carry spreads the group across
        // two finish steps (this exact case was an OutputTooSmall
        // error before the fully-consume-or-fully-fill contract).
        let mut engine = Engine::new(base64_enc());
        let mut big = [0u8; 8];
        let (_, step) = engine.step(b"A", false, &mut big).unwrap();
        assert_eq!(step, Step::NeedInput);

        let mut collected = Vec::new();
        loop {
            let mut tiny = [0u8; 2];
            match engine.step(&[], true, &mut tiny).unwrap() {
                (_, Step::Wrote(n)) => collected.extend_from_slice(&tiny[..n]),
                (_, Step::Done) => break,
                (_, other) => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(collected, b"QQ==");
    }

    #[test]
    fn process_reports_need_output_without_touching_codec() {
        let mut engine = Engine::new(CountingCodec::default());
        let (consumed, step) = engine.step(b"hi", false, &mut []).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(step, Step::NeedOutput);
        assert_eq!(engine.codec.process_calls, 0);
    }

    #[test]
    fn finish_with_empty_buffer_still_reaches_done_for_stateless_codec() {
        // `finish` with an empty buffer is still called: a codec that
        // owes nothing reports `Done` right away, rather than Engine
        // assuming it needs output and asking for a buffer it has no
        // use for.
        let mut engine = Engine::new(CountingCodec::default());
        let (consumed, step) = engine.step(&[], true, &mut []).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(step, Step::Done);
        assert_eq!(engine.codec.finish_calls, 1);
    }

    #[test]
    fn finish_reports_need_output_when_stuck_with_an_empty_buffer() {
        // base64_enc actually needs room to flush a pending leftover's
        // padded group. An empty buffer can't provide it — that's
        // `NeedOutput` — and a fresh non-empty one makes progress.
        let mut engine = Engine::new(base64_enc());
        let mut big = [0u8; 8];
        let (_, step) = engine.step(b"A", false, &mut big).unwrap();
        assert_eq!(step, Step::NeedInput);

        let (consumed, step) = engine.step(&[], true, &mut []).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(step, Step::NeedOutput);

        let (_, step) = engine.step(&[], true, &mut big).unwrap();
        assert!(matches!(step, Step::Wrote(n) if n > 0));
    }

    #[test]
    fn need_output_then_real_buffer_makes_progress() {
        let mut engine = Engine::new(rot13());
        let (consumed, step) = engine.step(b"Hello", false, &mut []).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(step, Step::NeedOutput);

        let mut out = [0u8; 8];
        let (consumed, step) = engine.step(b"Hello", false, &mut out).unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(step, Step::Wrote(5));
    }

    #[test]
    fn is_done_can_be_true_even_when_last_step_reported_wrote() {
        // CountingCodec's process reports StreamEnd as soon as it's
        // consumed all its input, in the very same call that writes
        // the bytes — so `is_done` must already be true right after,
        // without a further `step` call needed to discover it.
        let mut engine = Engine::new(CountingCodec::default());
        let mut out = [0u8; 8];
        let (consumed, step) = engine.step(b"hi", true, &mut out).unwrap();
        assert_eq!(consumed, 2);
        assert_eq!(step, Step::Wrote(2));
        assert!(engine.is_done());
    }

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
        // Unchecked, the overclaimed count would make to_vec slice out
        // of range (and make CodecReader break std::io::Read's
        // contract). The trust-boundary validation must turn it into
        // a ContractViolation error instead.
        let result = to_vec(Overclaimer, b"hi");
        assert_eq!(
            result.unwrap_err(),
            Error { kind: crate::ErrorKind::ContractViolation, consumed: 0, written: 0 }
        );
    }

    #[test]
    fn done_is_idempotent_and_does_not_touch_the_codec_again() {
        let mut engine = Engine::new(CountingCodec::default());
        let mut out = [0u8; 8];

        let (consumed, step) = engine.step(b"hi", true, &mut out).unwrap();
        assert_eq!(consumed, 2);
        assert_eq!(step, Step::Wrote(2));
        assert_eq!(engine.codec.process_calls, 1);
        assert_eq!(engine.codec.finish_calls, 0);

        let (consumed, step) = engine.step(b"more", true, &mut out).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(step, Step::Done);
        assert_eq!(engine.codec.process_calls, 1);
        assert_eq!(engine.codec.finish_calls, 0);
    }
}
