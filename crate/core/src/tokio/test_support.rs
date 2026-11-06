//! Shared helpers for the tests: a sample tokio `Decoder`, inputs and a chunked source.

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use core::convert::Infallible;
use core::mem::MaybeUninit;

use crate::sources_and_sinks::slice::SliceSource;
use crate::{Sink, Source};

// Copied from the `tokio_util::codec` module documentation:
// a four-byte little-endian length followed by that many bytes of
// UTF-8 string data.
pub struct LengthPrefixedStringTokioDecoder {}

const MAX: usize = 8 * 1024 * 1024;

impl Decoder for LengthPrefixedStringTokioDecoder {
    type Item = String;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            // Not enough data to read length marker.
            return Ok(None);
        }

        // Read length marker.
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_le_bytes(length_bytes) as usize;

        // Check that the length is not too large to avoid a denial of
        // service attack where the server runs out of memory.
        if length > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", length),
            ));
        }

        if src.len() < 4 + length {
            // The full string has not yet arrived.
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Use advance to modify src such that it no longer contains
        // this frame.
        let data = src[4..4 + length].to_vec();
        src.advance(4 + length);

        // Convert the data to a string, or fail if it is not valid utf-8.
        match String::from_utf8(data) {
            Ok(string) => Ok(Some(string)),
            Err(utf8_error) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                utf8_error.utf8_error(),
            )),
        }
    }
}

/// The encoder for `LengthPrefixedStringTokioDecoder`: a four-byte
/// little-endian length followed by the bytes of the string.
pub struct LengthPrefixedStringTokioEncoder {}

impl<'a> Encoder<&'a str> for LengthPrefixedStringTokioEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: &'a str, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(4 + item.len());
        dst.put_u32_le(item.len() as u32);
        dst.put_slice(item.as_bytes());
        Ok(())
    }
}

pub fn frame(s: &str) -> Vec<u8> {
    let mut v = (s.len() as u32).to_le_bytes().to_vec();
    v.extend_from_slice(s.as_bytes());
    v
}

pub fn three_strings_input() -> Vec<u8> {
    let mut input = frame("hello");
    input.extend(frame("tokio"));
    input.extend(frame("world"));
    input
}

/// A source with small chunks, so objects span chunk boundaries.
pub struct Chunked<'a>(pub SliceSource<'a>, pub usize);

impl Source for Chunked<'_> {
    type Error = core::convert::Infallible;
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        let n = self.1;
        Ok(self.0.chunk()?.map(|c| &c[..c.len().min(n)]))
    }
    fn consume(&mut self, amount: usize) {
        self.0.consume(amount)
    }
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

#[test]
fn sample_decoder_decodes_three_strings() {
    let mut decoder = LengthPrefixedStringTokioDecoder {};
    let mut buf = BytesMut::from(&three_strings_input()[..]);

    assert_eq!(decoder.decode(&mut buf).unwrap().as_deref(), Some("hello"));
    assert_eq!(decoder.decode(&mut buf).unwrap().as_deref(), Some("tokio"));
    assert_eq!(decoder.decode(&mut buf).unwrap().as_deref(), Some("world"));
    assert!(buf.is_empty());
}
