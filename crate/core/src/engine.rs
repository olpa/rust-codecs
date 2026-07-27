//! [`Engine`]: the drive loop shared by [`CodecReader`](crate::io::CodecReader),
//! [`CodecWriter`](crate::io::CodecWriter), and [`to_vec`](crate::io::to_vec) —
//! process-vs-finish selection, `StreamEnd` latching, and one no-progress
//! error, instead of each of the three re-implementing it slightly
//! differently.

use crate::{Codec, Error, Progress, Status};

/// What to do after one crank of [`Engine::step`].
#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Nothing was written; call again with more input (or, once at
    /// EOF, the same empty input again to move toward `finish`).
    NeedInput,
    /// `n` bytes landed in `output`. There may or may not be more to
    /// come — call again to find out.
    Wrote(usize),
    /// The codec has emitted everything it will ever emit. Every later
    /// call returns this immediately without touching the codec again.
    Done,
}

/// Drives a single [`Codec`] through `process`/`finish`, hiding the
/// bookkeeping every driver needs: which method to call, latching
/// `StreamEnd` once seen (a call can both deliver final bytes *and*
/// reach `StreamEnd` — `Done` only surfaces once nothing more is
/// written), and erroring rather than spinning when a fixed-size buffer
/// can never fit the codec's next atomic unit.
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

    /// One turn of the crank. `input` is the bytes available right now
    /// (may be empty); `at_eof` says no more will ever come once
    /// `input` runs out. Returns how much of `input` was consumed and
    /// what happened.
    ///
    /// Once finishing has started (either because a prior call saw
    /// `at_eof` with empty input, or the codec is mid-`finish` from a
    /// call that ran out of output room), `input` is ignored — only
    /// `output` matters — matching `Codec::finish`'s contract of being
    /// callable repeatedly with a fresh output buffer.
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
            let (p, status) = self.codec.finish(output)?;
            self.latch(status);
            return Ok((0, self.finish_step(p)?));
        }

        if input.is_empty() {
            // Not at EOF: nothing to do until the caller supplies more.
            return Ok((0, Step::NeedInput));
        }

        let (p, status) = self.codec.process(input, output)?;
        self.latch(status);
        if p.written == 0 {
            return Ok((p.consumed, if self.done { Step::Done } else { Step::NeedInput }));
        }
        Ok((p.consumed, Step::Wrote(p.written)))
    }

    /// Pass `Codec::flush` straight through. Sync-flush (drain to an
    /// in-band boundary without ending the stream) is codec-specific
    /// and orthogonal to the process/finish state this engine tracks —
    /// it never reports `StreamEnd`, so there's no latching to do.
    pub fn flush(&mut self, output: &mut [u8]) -> Result<(Progress, Status), Error> {
        self.codec.flush(output)
    }

    fn latch(&mut self, status: Status) {
        if matches!(status, Status::StreamEnd) {
            self.done = true;
        }
    }

    /// Turn a `finish` call's progress into a `Step`, once `self.done`
    /// has already been updated by `latch`.
    fn finish_step(&self, p: Progress) -> Result<Step, Error> {
        if p.written > 0 {
            return Ok(Step::Wrote(p.written));
        }
        if self.done {
            return Ok(Step::Done);
        }
        // No progress, and `finish` is contractually re-callable with a
        // *fresh* output buffer to make more — but every caller in this
        // crate hands `Engine` the same fixed-size buffer each time, so
        // "try again" can never help. That's specifically what a
        // buffer smaller than the codec's minimum atomic output size
        // looks like from here.
        Err(Error::OutputTooSmall)
    }
}

#[cfg(test)]
mod tests {
    use super::{Engine, Step};
    use crate::base64::base64_enc;
    use crate::io::to_vec;
    use crate::rot13::rot13;
    use crate::{Codec, Error, Progress, Status};

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
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
            self.process_calls += 1;
            let n = input.len().min(output.len());
            output[..n].copy_from_slice(&input[..n]);
            let status = if n == input.len() { Status::StreamEnd } else { Status::OutputFull };
            Ok((Progress { consumed: n, written: n }, status))
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
            self.finish_calls += 1;
            Ok((Progress::default(), Status::StreamEnd))
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
    fn no_progress_during_finish_errors_output_too_small() {
        let mut engine = Engine::new(base64_enc());
        let mut big = [0u8; 8];
        let (_, step) = engine.step(b"A", false, &mut big).unwrap();
        assert_eq!(step, Step::NeedInput);

        // A 2-byte buffer can never fit base64's minimum 4-byte
        // encoded group.
        let mut tiny = [0u8; 2];
        let result = engine.step(&[], true, &mut tiny);
        assert!(matches!(result, Err(Error::OutputTooSmall)));
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
