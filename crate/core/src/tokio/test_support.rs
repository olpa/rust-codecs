//! Shared helpers for the tests: a sample tokio codec, its input, a
//! source and a sink that do nothing.

use core::convert::Infallible;
use core::mem::MaybeUninit;

use tokio_util::codec::AnyDelimiterCodec;

use crate::{Sink, Source};

/// A codec from tokio-util that is both a `Decoder` and an `Encoder`.
/// The decoder cuts the input at `|`, and the encoder ends each item
/// with `|`.
pub fn bar_delimited_codec() -> AnyDelimiterCodec {
    AnyDelimiterCodec::new(b"|".to_vec(), b"|".to_vec())
}

/// Three strings, each followed by `|`.
pub fn three_strings_input() -> Vec<u8> {
    b"hello|tokio|world|".to_vec()
}

/// A source that is at its end at once, like `/dev/null`.
pub struct NullSource;

impl Source for NullSource {
    type Error = Infallible;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        Ok(None)
    }

    fn consume(&mut self, _amount: usize) {}
}

/// A sink that ignores everything written to it, like `/dev/null`.
pub struct NullSink([MaybeUninit<u8>; 64]);

impl Default for NullSink {
    fn default() -> Self {
        Self([MaybeUninit::uninit(); 64])
    }
}

impl Sink for NullSink {
    type Error = Infallible;

    fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error> {
        Ok(Some(&mut self.0))
    }

    fn commit(&mut self, _amount: usize) -> Result<(), Self::Error> {
        Ok(())
    }
}
