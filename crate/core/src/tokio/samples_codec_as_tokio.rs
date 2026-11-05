//! Samples that run codecs of this crate through the tokio framing
//! types `FramedRead` and `FramedWrite`.
//!
//! The codecs:
//! - `Rot13`: a plain `Codec`. It has no boundary.
//! - `EndsAtBar<Rot13>`: a boundary-aware codec. A stream ends at `|`.

use futures_util::SinkExt;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{BoundaryAwareCodecTokioDecoder, CodecTokioDecoder, CodecTokioEncoder, Finish};
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
