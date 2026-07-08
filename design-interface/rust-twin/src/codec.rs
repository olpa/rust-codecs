/// An incremental bytes-in/bytes-out transform.
///
/// This is the twin of Python's `codecs.IncrementalEncoder` and
/// `codecs.IncrementalDecoder`. One trait covers both directions:
/// [`StreamReader`](crate::StreamReader) drives its codec as a decoder,
/// [`StreamWriter`](crate::StreamWriter) drives its codec as an encoder.
/// For a directional codec, construct it in the direction you need and pass
/// that instance to the wrapper.
pub trait Codec {
    /// Transform the next chunk of the stream.
    ///
    /// `last` is `true` exactly once, when the input is exhausted (reader) or
    /// the stream is finished (writer), so a codec that buffers state across
    /// chunks can flush its tail. The returned bytes may be empty even for a
    /// non-empty `input` (e.g. a codec waiting for a complete unit), and may
    /// be non-empty for an empty `input` when `last` is `true`.
    fn transform(&mut self, input: &[u8], last: bool) -> Vec<u8>;
}
