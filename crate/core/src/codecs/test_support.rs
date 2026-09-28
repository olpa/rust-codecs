//! Test-only codecs shared by more than one module of the crate.

use core::mem::MaybeUninit;

use crate::{Codec, DrainCodec, DrainProgress, Error, Progress};

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
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let n = self.release_held(output);
        if self.held > 0 {
            return Ok(DrainProgress::OutputFilled);
        }
        let rest = &mut output[n..];
        let left = &self.trailer[self.trailer_written..];
        let m = left.len().min(rest.len());
        rest[..m].write_copy_of_slice(&left[..m]);
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
