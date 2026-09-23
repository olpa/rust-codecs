//! Test-only codecs shared by more than one `shared_io` submodule.

use core::mem::MaybeUninit;

use crate::{Codec, DrainCodec, DrainProgress, Error, Progress};

/// A codec with nothing to process, only a multi-byte trailer to emit
/// from `finish` — stands in for a format whose finalization (a
/// checksum, a footer) is bigger than one read/write buffer.
pub(super) struct EmitsTrailerOnFinish {
    trailer: &'static [u8],
    position: usize,
}

impl EmitsTrailerOnFinish {
    pub(super) fn new(trailer: &'static [u8]) -> Self {
        Self {
            trailer,
            position: 0,
        }
    }
}

impl DrainCodec for EmitsTrailerOnFinish {
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let n = (self.trailer.len() - self.position).min(output.len());
        output[..n].write_copy_of_slice(&self.trailer[self.position..self.position + n]);
        self.position += n;
        if self.position == self.trailer.len() {
            Ok(DrainProgress::Done { written: n })
        } else {
            Ok(DrainProgress::OutputFilled)
        }
    }
}

impl Codec for EmitsTrailerOnFinish {
    fn process(
        &mut self,
        _input: &[u8],
        _output: &mut [MaybeUninit<u8>],
    ) -> Result<Progress, Error> {
        Ok(Progress::InputConsumed { written: 0 })
    }
}
