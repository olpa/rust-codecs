//! Test-only codecs shared by more than one module of the crate.

use core::mem::MaybeUninit;

use crate::uninit::copy_to_uninit;
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, Error, ErrorKind,
    Progress,
};

/// A codec that holds output:
/// - At the start, holds `held` of 'X'.
/// - For each input byte, adds `per_input` of 'X' to hold.
/// - Each call releases the held 'X's, as many as fit.
/// - Takes new input only after it releases all held 'X's.
///
/// `finish` first releases the held 'X's, then emits the trailer.
#[derive(Default)]
pub(crate) struct HoldsOutput {
    pub(crate) held: usize,
    pub(crate) per_input: usize,
    pub(crate) trailer: &'static [u8],
    pub(crate) trailer_written: usize,
}

impl HoldsOutput {
    fn release_held(&mut self, output: &mut [MaybeUninit<u8>]) -> usize {
        let n = self.held.min(output.len());
        for slot in &mut output[..n] {
            slot.write(b'X');
        }
        self.held -= n;
        n
    }
}

impl DrainCodec for HoldsOutput {
    /// Releases the held 'X's. Does not emit the trailer.
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let n = self.release_held(output);
        if self.held > 0 {
            return Ok(DrainProgress::OutputFilled);
        }
        Ok(DrainProgress::Done { written: n })
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let n = self.release_held(output);
        if self.held > 0 {
            return Ok(DrainProgress::OutputFilled);
        }
        let rest = &mut output[n..];
        let left = &self.trailer[self.trailer_written..];
        let m = left.len().min(rest.len());
        copy_to_uninit(&mut rest[..m], &left[..m]);
        self.trailer_written += m;
        if self.trailer_written == self.trailer.len() {
            Ok(DrainProgress::Done { written: n + m })
        } else {
            Ok(DrainProgress::OutputFilled)
        }
    }
}

impl Codec for HoldsOutput {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let n = self.release_held(output);
        if self.held > 0 {
            return Ok(Progress::OutputFilled { consumed: 0 });
        }
        self.held = self.per_input * input.len();
        Ok(Progress::InputConsumed { written: n })
    }
}

/// Adds an in-band end to a plain `Codec`:
/// - Passes input bytes to `inner` until it finds a `|`.
/// - Consumes the `|` and reports `Boundary`.
/// - `finish` calls `inner.finish`. Then `inner` writes its remaining
///   output.
///
/// Counts its `finish` calls.
#[derive(Default)]
pub(crate) struct EndsAtBar<C> {
    pub(crate) inner: C,
    pub(crate) finish_calls: usize,
}

impl<C: Codec> DrainCodec for EndsAtBar<C> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.inner.flush(output)
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.finish_calls += 1;
        self.inner.finish(output)
    }
}

impl<C: Codec> BoundaryAwareCodec for EndsAtBar<C> {
    fn process(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        let Some(marker) = input.iter().position(|&b| b == b'|') else {
            return self.inner.process(input, output).map(Into::into);
        };
        Ok(match self.inner.process(&input[..marker], output)? {
            Progress::InputConsumed { written } => BoundaryAwareProgress::Boundary {
                consumed: marker + 1,
                written,
            },
            Progress::OutputFilled { consumed } => BoundaryAwareProgress::OutputFilled { consumed },
        })
    }
}

/// Answers every `process` call with `process`, and every `finish`
/// call with `drain`. Does not check the buffers, so a test can make
/// it lie about its byte counts.
pub(crate) struct Scripted {
    pub(crate) process: BoundaryAwareProgress,
    pub(crate) drain: DrainProgress,
}

impl DrainCodec for Scripted {
    fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }

    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(self.drain)
    }
}

impl BoundaryAwareCodec for Scripted {
    fn process(
        &mut self,
        _input: &[u8],
        _output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        Ok(self.process)
    }
}

/// Runs `inner`, then turns its result into an error. The error
/// carries the counts of the result, so the bytes that `inner` wrote
/// still count. Fails on every `process`, `flush` and `finish` call.
///
/// Counts its calls.
#[derive(Default)]
pub(crate) struct FailsAfterInner<C> {
    pub(crate) inner: C,
    pub(crate) calls: usize,
}

impl<C> FailsAfterInner<C> {
    fn fail_drain(
        &mut self,
        output_len: usize,
        result: Result<DrainProgress, Error>,
    ) -> Result<DrainProgress, Error> {
        self.calls += 1;
        let written = match result? {
            DrainProgress::Done { written } => written,
            DrainProgress::OutputFilled => output_len,
        };
        Err(Error::new(ErrorKind::CorruptStream, 0, written))
    }
}

impl<C: Codec> DrainCodec for FailsAfterInner<C> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let result = self.inner.flush(output);
        self.fail_drain(output.len(), result)
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let result = self.inner.finish(output);
        self.fail_drain(output.len(), result)
    }
}

impl<C: Codec> Codec for FailsAfterInner<C> {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        self.calls += 1;
        let (consumed, written) = match self.inner.process(input, output)? {
            Progress::InputConsumed { written } => (input.len(), written),
            Progress::OutputFilled { consumed } => (consumed, output.len()),
        };
        Err(Error::new(ErrorKind::CorruptStream, consumed, written))
    }
}
