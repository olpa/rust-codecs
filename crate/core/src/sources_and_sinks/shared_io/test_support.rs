//! Test-only doubles shared by more than one `shared_io` submodule.

use core::convert::Infallible;
use core::mem::MaybeUninit;

use crate::{Codec, DrainCodec, DrainProgress, Error, Progress};

use super::sink::RetryingWrite;

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

/// A minimal [`RetryingWrite`] over a borrowed byte slice.
/// Panics (via the slice index) if written past capacity;
/// tests using this should size the slice generously.
pub(super) struct SliceWriter<'a> {
    remaining: &'a mut [u8],
}

impl<'a> SliceWriter<'a> {
    pub(super) fn new(remaining: &'a mut [u8]) -> Self {
        Self { remaining }
    }

    pub(super) fn remaining_len(&self) -> usize {
        self.remaining.len()
    }
}

impl<'a> RetryingWrite for SliceWriter<'a> {
    type Error = Infallible;

    fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        let n = buf.len();
        self.remaining[..n].copy_from_slice(buf);
        let remaining = core::mem::take(&mut self.remaining);
        self.remaining = &mut remaining[n..];
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
