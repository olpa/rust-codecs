//! DESIGN SKETCH. This file does not compile yet.
//!
//! `mod.rs` does not declare it. It shows the planned API that runs a
//! rust-codecs codec through the tokio framing types `FramedRead`,
//! `FramedWrite` and `Framed`. This is the reverse of
//! `TokioDecoderCodec`, which runs a tokio `Decoder` as a codec.
//!
//! New types:
//! - `CodecTokioDecoder<C: Codec>` implements `Decoder`. It behaves like
//!   tokio-util `BytesCodec`: `Item = BytesMut`, the output that the
//!   codec made from the bytes that are in the buffer now. At EOF,
//!   `decode_eof` calls `finish` and returns its output as a last item.
//! - `BoundaryAwareCodecTokioDecoder<C: BoundaryAwareCodec>` implements
//!   `Decoder`. It behaves like tokio-util `LinesCodec`: `Item =
//!   BytesMut`, all output of one logical stream. The item comes at the
//!   boundary. At EOF, it ends the last stream and returns its output.
//! - `CodecTokioEncoder<C: Codec>` implements `Encoder<T>` for each
//!   `T: AsRef<[u8]>`. An item is a chunk of the input of one stream.
//!   It also implements `Encoder<Finish>`. `Finish` is a marker that
//!   ends the stream.
//! - `CodecTokioPair<D, E>` implements `Decoder + Encoder`, as `Framed`
//!   needs one type for both directions.
//!
//! Why two decoders: the rule for an item is different. For a plain
//! codec, an item is a chunk, and the client gets it as soon as there
//! is output. For a boundary-aware codec, an item is a stream, and the
//! client gets it at the boundary.
//!
//! The encoder accepts only a plain `Codec`. A boundary is a marker in
//! the input bytes of a self-delimiting format. That fits a decoder,
//! which cuts a byte stream into items. The input of an encoder is a
//! payload, and the edges of the items already mark the streams.
//!
//! The codecs in the samples:
//! - `Rot13`: a plain `Codec`. It has no boundary.
//! - `EndsAtBar<Rot13>`: a boundary-aware codec. A stream ends at `|`.
//!
//! New dev-dependencies for the tests: `tokio` (features `rt`, `macros`,
//! `io-util`), `tokio-stream` (for `StreamExt::next`) and `futures-util`
//! (for `SinkExt::send`).
//!
//! Decided: the end of the stream in the encoder.
//! `FramedWrite` never tells the encoder that the stream is over, and
//! `Encoder` has no hook for it. So the client sends the marker
//! `Finish` as the last item. A direct call to the encoder does not
//! work: `encoder_mut()` and `write_buffer_mut()` both borrow the
//! writer mutably.
//!
//! Decided: the error type.
//! Tokio requires `Error: From<std::io::Error>`. The adapters follow
//! `BytesCodec` and `LengthDelimitedCodec` of tokio-util: `type Error =
//! std::io::Error`. The crate `Error` becomes the source of the
//! `io::Error`, so a client can get it back with `downcast_ref`. The
//! kind maps as `CorruptStream` to `InvalidData` and `UnexpectedEnd` to
//! `UnexpectedEof`. (`LinesCodec` uses its own error enum with an `Io`
//! variant instead. That adds a public type, and this crate does not
//! need it.)

use futures_util::SinkExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, FramedRead, FramedWrite};

use super::{
    BoundaryAwareCodecTokioDecoder, CodecTokioDecoder, CodecTokioEncoder, CodecTokioPair, Finish,
};
use crate::codecs::rot13::Rot13;
use crate::codecs::test_support::EndsAtBar;

// ----
// FramedRead: bytes from an AsyncRead, objects from the codec
// ----

/// A plain codec gives an item for each chunk of output, like
/// `BytesCodec`. The client does not wait for EOF.
#[tokio::test]
async fn rot13_framed_read() {
    let (mut remote, local) = tokio::io::duplex(64);
    let mut reader = FramedRead::new(local, CodecTokioDecoder::new(Rot13));

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
    let codec = BoundaryAwareCodecTokioDecoder::new(EndsAtBar::<Rot13>::default());
    let mut reader = FramedRead::new(input, codec);

    assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"hello");
    assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"world");
    assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"good");
    assert!(reader.next().await.is_none());
}

// ----
// FramedWrite: objects to the codec, bytes to an AsyncWrite
// ----

/// Items are chunks of one stream. The marker `Finish` ends the stream.
#[tokio::test]
async fn rot13_framed_write() {
    let mut writer = FramedWrite::new(Vec::new(), CodecTokioEncoder::new(Rot13));

    writer.send("hello ").await.unwrap();
    writer.send("world").await.unwrap();
    writer.send(Finish).await.unwrap();

    assert_eq!(writer.into_inner(), b"uryyb jbeyq");
}

// ----
// Framed: both directions on one connection
// ----

/// `Framed` needs one type that is a `Decoder` and an `Encoder`. The
/// pair holds two codecs, one for each direction. The directions are
/// independent: the decoder cuts streams at `|`, and the encoder is a
/// plain codec. The other end of the connection is a plain byte stream.
#[tokio::test]
async fn framed_both_directions() {
    let (local, mut remote) = tokio::io::duplex(64);
    let codec = CodecTokioPair::new(
        BoundaryAwareCodecTokioDecoder::new(EndsAtBar::<Rot13>::default()),
        CodecTokioEncoder::new(Rot13),
    );
    let mut framed = Framed::new(local, codec);

    // Receive: the remote end sends encoded bytes.
    remote.write_all(b"uryyb|jbeyq|").await.unwrap();
    assert_eq!(&framed.next().await.unwrap().unwrap()[..], b"hello");
    assert_eq!(&framed.next().await.unwrap().unwrap()[..], b"world");

    // Send: the remote end gets encoded bytes.
    framed.send("foo").await.unwrap();
    framed.send(Finish).await.unwrap();
    let mut received = [0u8; 3];
    remote.read_exact(&mut received).await.unwrap();
    assert_eq!(&received, b"sbb");
}
