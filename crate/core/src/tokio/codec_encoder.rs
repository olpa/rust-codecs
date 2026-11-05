//! `CodecTokioEncoder`: a plain codec as a tokio `Encoder`.

use std::io;

use bytes::BytesMut;
use tokio_util::codec::Encoder;

use super::codec_support::{finish_all, process_all, to_io_error};
use crate::Codec;

/// The item that ends the stream of a [`CodecTokioEncoder`].
///
/// Tokio does not tell an `Encoder` that the stream is over, so the
/// client sends this item as the last one. The encoder then calls
/// `finish` of the codec and appends its output, for example a
/// trailer or padding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Finish;

/// Runs a [`Codec`] as a tokio `Encoder`. An item is a chunk of the
/// input of one stream: any type that is `AsRef<[u8]>`. The encoder
/// appends the output of the codec to the buffer of tokio.
///
/// The encoder accepts only a plain codec. A boundary is a marker in
/// the input of a self-delimiting format. The input of an encoder is a
/// payload, and the edges of the items already mark the streams.
///
/// The error type is `std::io::Error`. The crate [`Error`](crate::Error)
/// is its source.
pub struct CodecTokioEncoder<C: Codec> {
    codec: C,
}

impl<C: Codec> CodecTokioEncoder<C> {
    pub fn new(codec: C) -> Self {
        Self { codec }
    }

    pub fn codec(&self) -> &C {
        &self.codec
    }

    pub fn codec_mut(&mut self) -> &mut C {
        &mut self.codec
    }

    pub fn into_inner(self) -> C {
        self.codec
    }
}

impl<C: Codec, T: AsRef<[u8]>> Encoder<T> for CodecTokioEncoder<C> {
    type Error = io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), io::Error> {
        process_all(dst, item.as_ref(), |input, output| {
            self.codec.process(input, output).map(Into::into)
        })
        .map_err(to_io_error)
    }
}

impl<C: Codec> Encoder<Finish> for CodecTokioEncoder<C> {
    type Error = io::Error;

    fn encode(&mut self, _item: Finish, dst: &mut BytesMut) -> Result<(), io::Error> {
        finish_all(&mut self.codec, dst).map_err(to_io_error)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::BytesMut;
    use tokio_util::codec::Encoder;

    use super::{CodecTokioEncoder, Finish};
    use crate::codecs::rot13::Rot13;
    use crate::codecs::test_support::{FailsAfterInner, FailsWithOverclaim, HoldsOutput};
    use crate::{Error, ErrorKind};

    /// `finish` appends the trailer of the codec.
    #[test]
    fn finish_appends_the_trailer() {
        let codec = HoldsOutput {
            trailer: b"END",
            ..Default::default()
        };
        let mut encoder = CodecTokioEncoder::new(codec);
        let mut dst = BytesMut::new();

        encoder.encode("ab", &mut dst).unwrap();
        assert!(dst.is_empty());
        encoder.encode(Finish, &mut dst).unwrap();

        assert_eq!(&dst[..], b"END");
    }

    /// The output is larger than the free space of one codec call.
    #[test]
    fn finish_writes_output_larger_than_one_call() {
        let codec = HoldsOutput {
            per_input: 10_000,
            ..Default::default()
        };
        let mut encoder = CodecTokioEncoder::new(codec);
        let mut dst = BytesMut::new();

        encoder.encode("a", &mut dst).unwrap();
        encoder.encode(Finish, &mut dst).unwrap();

        assert_eq!(dst.len(), 10_000);
        assert!(dst.iter().all(|&b| b == b'X'));
    }

    #[test]
    fn codec_error_is_the_source_of_the_io_error() {
        let codec = FailsAfterInner::<Rot13>::default();
        let mut encoder = CodecTokioEncoder::new(codec);

        let error = encoder.encode("a", &mut BytesMut::new()).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let inner = error.get_ref().unwrap().downcast_ref::<Error>().unwrap();
        assert_eq!(inner.kind, ErrorKind::CorruptStream);
    }

    #[test]
    fn lying_codec_gives_a_byte_count_error() {
        let mut encoder = CodecTokioEncoder::new(FailsWithOverclaim);

        let error = encoder.encode("a", &mut BytesMut::new()).unwrap_err();

        let inner = error.get_ref().unwrap().downcast_ref::<Error>().unwrap();
        assert_eq!(inner.kind, ErrorKind::ByteCountClaim);
    }
}
