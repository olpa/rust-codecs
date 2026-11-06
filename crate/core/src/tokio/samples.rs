//! Samples of the adapters between tokio codecs and the codecs of this
//! crate, used as examples.
//!
//! - A tokio `Decoder` runs as a codec of this crate.
//! - Codecs of this crate run through the tokio framing types
//!   `FramedRead` and `FramedWrite`. The codecs:
//!   - `Rot13`: a plain `Codec`. It has no boundary.
//!   - `EndsAtBar<Rot13>`: a boundary-aware codec. A stream ends at `|`.

use bytes::Bytes;
use futures_util::SinkExt;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

use super::test_support::{bar_delimited_codec, three_strings_input, NullSink, NullSource};
use super::{
    BoundaryAwareCodecAsTokioDecoder, CodecAsTokioDecoder, CodecAsTokioEncoder, Finish,
    TokioDecoderAsBoundaryAwareCodec, TokioEncoderAsBoundaryAwareCodec,
};
use crate::codecs::rot13::Rot13;
use crate::codecs::test_support::EndsAtBar;
use crate::sources_and_sinks::slice::SliceSource;
use crate::sources_and_sinks::vec::VecSink;
use crate::{stream_to_stream, BoundaryAwareCodec};

// ----
// A tokio Decoder to rust-codecs BoundaryAwareCodec
// ----

/// Wrap a tokio `Decoder` with `TokioDecoderAsBoundaryAwareCodec`, then read its
/// objects through the codec interface: three strings, then nothing.
///
/// Each object is one logical stream. The adapter writes no bytes, so
/// the sink can ignore its input. The object comes out of `take_item`.
#[test]
fn read_three_strings_and_then_nothing() {
    let input = three_strings_input();
    let mut source = SliceSource::new(&input);
    let mut sink = NullSink::default();

    // A tokio decoder, wrapped by our adapter.
    let mut adapter = TokioDecoderAsBoundaryAwareCodec::new(bar_delimited_codec());

    // Run one logical stream and return the decoded object. `&mut dyn`
    // lets the closure keep the adapter for `take_item`.
    let mut next_object = || {
        let codec: &mut dyn BoundaryAwareCodec = &mut adapter;
        stream_to_stream(&mut source, codec, &mut sink).unwrap();
        adapter.take_item()
    };

    assert_eq!(next_object(), Some(Bytes::from("hello")));
    assert_eq!(next_object(), Some(Bytes::from("tokio")));
    assert_eq!(next_object(), Some(Bytes::from("world")));
    assert_eq!(next_object(), None);
}

// ----
// A tokio Encoder to rust-codecs BoundaryAwareCodec
// ----

/// The codec ignores its input, so the source is empty. The pump goes
/// to `finish`, and the codec writes the encoded items there, as one
/// stream.
#[test]
fn encode_three_items() {
    let mut source = NullSource;
    let mut sink = VecSink::default();
    let codec = TokioEncoderAsBoundaryAwareCodec::with_items(
        bar_delimited_codec(),
        ["hello", "tokio", "world"],
    );

    stream_to_stream(&mut source, codec, &mut sink).unwrap();

    assert_eq!(sink.into_inner(), three_strings_input());
}

// ----
// rust-codecs Codec and BoundaryAwareCodec to tokio Decoder, with FramedRead
// ----

/// A plain codec gives an item for each chunk of output, like
/// `BytesCodec`. The client does not wait for EOF.
#[tokio::test]
async fn rot13_framed_read() {
    let (mut remote, local) = tokio::io::duplex(64);
    let mut reader = FramedRead::new(local, CodecAsTokioDecoder::new(Rot13));

    remote.write_all(b"uryyb ").await.unwrap();
    assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"hello ");
    remote.write_all(b"jbeyq").await.unwrap();
    assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"world");

    // Closing the connection is EOF. A stateless codec has no more output.
    drop(remote);
    assert!(reader.next().await.is_none());
}

/// A boundary-aware codec gives one item for each logical stream.
/// `FramedRead` also decodes the last stream at EOF if it has no `|`.
#[tokio::test]
async fn ends_at_bar_framed_read() {
    let input: &[u8] = b"uryyb|jbeyq|tbbq";
    let codec = BoundaryAwareCodecAsTokioDecoder::new(EndsAtBar::<Rot13>::default());
    let mut reader = FramedRead::new(input, codec);

    assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"hello");
    assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"world");
    assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"good");
    assert!(reader.next().await.is_none());
}

// ----
// rust-codecs Codec to tokio Encoder, with FramedWrite
// ----

/// Items are chunks of one stream. The marker `Finish` ends the stream.
#[tokio::test]
async fn rot13_framed_write() {
    let mut writer = FramedWrite::new(Vec::new(), CodecAsTokioEncoder::new(Rot13));

    writer.send("hello ").await.unwrap();
    writer.send("world").await.unwrap();
    writer.send(Finish).await.unwrap();

    assert_eq!(writer.into_inner(), b"uryyb jbeyq");
}
