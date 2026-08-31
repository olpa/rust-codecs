use embedded_io::{BufRead, ErrorKind, Read, Write};

use crate::sources_and_sinks::shared_io::{
    retry_write_all, EintrFillBuf, EintrRead, LendingSource, RetryingWrite, ScratchSink,
    ScratchSource,
};
use crate::{Sink, Source};

fn is_interrupted<E: embedded_io::Error>(e: &E) -> bool {
    e.kind() == ErrorKind::Interrupted
}

/// Wraps an `embedded_io::Read`, recognizing `Interrupted`.
struct EmbeddedReader<R>(R);

impl<R: Read> EintrRead for EmbeddedReader<R> {
    type Error = R::Error;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read(buf)
    }

    fn is_interrupted(err: &Self::Error) -> bool {
        is_interrupted(err)
    }
}

/// A `Source` over `embedded_io::Read`, reading into an owned scratch
/// buffer.
pub struct EmbeddedSource<R, S>(ScratchSource<EmbeddedReader<R>, S>);

impl<R: Read, S: AsMut<[u8]>> EmbeddedSource<R, S> {
    /// Build an `EmbeddedSource`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`.
    pub fn new(inner: R, buffer: S) -> Self {
        Self(ScratchSource::new(EmbeddedReader(inner), buffer))
    }

    pub fn get_ref(&self) -> &R {
        &self.0.get_ref().0
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.0.get_mut().0
    }

    /// Reclaim the reader, discarding the scratch buffer and any
    /// unconsumed bytes still in it.
    pub fn into_inner(self) -> R {
        self.0.into_inner().0
    }

    /// The unconsumed bytes already pulled from the reader into the
    /// scratch buffer, but not yet yielded to the caller.
    pub fn pending(&mut self) -> &[u8] {
        self.0.pending()
    }

    /// Reclaim both the reader and the scratch buffer; any unconsumed
    /// bytes still in it are lost.
    pub fn into_parts(self) -> (R, S) {
        let (reader, buffer) = self.0.into_parts();
        (reader.0, buffer)
    }
}

impl<R: Read, S: AsMut<[u8]>> Source for EmbeddedSource<R, S> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        self.0.chunk()
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// Wraps an `embedded_io::BufRead`, recognizing `Interrupted`.
struct EmbeddedBufReader<R>(R);

impl<R: BufRead> EintrFillBuf for EmbeddedBufReader<R> {
    type Error = R::Error;

    fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        self.0.fill_buf()
    }

    fn is_interrupted(err: &Self::Error) -> bool {
        is_interrupted(err)
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// A `Source` over `embedded_io::BufRead`, using the `BufRead`'s
/// buffer directly to expose input as `u8` slices.
pub struct BufReadSource<R>(LendingSource<EmbeddedBufReader<R>>);

impl<R: BufRead> BufReadSource<R> {
    pub fn new(inner: R) -> Self {
        Self(LendingSource::new(EmbeddedBufReader(inner)))
    }

    pub fn get_ref(&self) -> &R {
        &self.0.get_ref().0
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.0.get_mut().0
    }

    /// Return the wrapped reader. Unlike `EmbeddedSource::into_inner`,
    /// nothing is lost — there's no scratch buffer to discard.
    pub fn into_inner(self) -> R {
        self.0.into_inner().0
    }
}

impl<R: BufRead> Source for BufReadSource<R> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        self.0.chunk()
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// An `embedded_io::Write::write` error, or a synthesized
/// `ZeroWrite` for a zero-length write on non-empty input.
///
/// `embedded_io::Write::write`'s doc comment says implementations
/// "must not" do this, but that's wishful thinking on the trait's
/// part, not something the type system enforces: nothing stops a
/// backend from returning `Ok(0)`.
#[derive(Debug)]
pub enum WriteError<E> {
    Io(E),
    ZeroWrite,
}

impl<E: core::fmt::Debug> core::fmt::Display for WriteError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<E: embedded_io::Error> core::error::Error for WriteError<E> {}

impl<E: embedded_io::Error> embedded_io::Error for WriteError<E> {
    fn kind(&self) -> ErrorKind {
        match self {
            WriteError::Io(e) => e.kind(),
            WriteError::ZeroWrite => ErrorKind::WriteZero,
        }
    }
}

/// Wraps an `embedded_io::Write`, retrying via `retry_write_all` —
/// unlike `std::io::Write::write_all`, `embedded_io`'s doesn't retry
/// on its own.
struct EmbeddedWriter<W>(W);

impl<W: Write> RetryingWrite for EmbeddedWriter<W> {
    type Error = WriteError<W::Error>;

    fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        retry_write_all(
            &mut self.0,
            |w, buf| w.write(buf).map_err(WriteError::Io),
            buf,
            is_interrupted,
            || WriteError::ZeroWrite,
        )
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush().map_err(WriteError::Io)
    }
}

/// A `Sink` over `embedded_io::Write`, staging writes in an owned
/// scratch buffer.
pub struct EmbeddedSink<W, S>(ScratchSink<EmbeddedWriter<W>, S>);

impl<W: Write, S: AsMut<[u8]>> EmbeddedSink<W, S> {
    /// Build an `EmbeddedSink`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`.
    pub fn new(inner: W, buffer: S) -> Self {
        Self(ScratchSink::new(EmbeddedWriter(inner), buffer))
    }

    pub fn get_ref(&self) -> &W {
        &self.0.get_ref().0
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.0.get_mut().0
    }

    /// Reclaim the writer, discarding the scratch buffer and any
    /// uncommitted staged bytes.
    pub fn into_inner(self) -> W {
        self.0.into_inner().0
    }

    /// Reclaim both the writer and the scratch buffer; any uncommitted
    /// staged bytes are lost.
    pub fn into_parts(self) -> (W, S) {
        let (writer, buffer) = self.0.into_parts();
        (writer.0, buffer)
    }
}

impl<W: Write, S: AsMut<[u8]>> Sink for EmbeddedSink<W, S> {
    type Error = WriteError<W::Error>;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        self.0.spare()
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        self.0.commit(amount)
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.0.finish()
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush()
    }
}

#[cfg(test)]
mod tests {
    use embedded_io::ErrorKind;

    use super::{is_interrupted, EmbeddedSink, EmbeddedSource};
    use crate::identity::identity;
    use crate::stream_to_stream;
    use crate::Sink;

    #[test]
    fn is_interrupted_recognizes_the_embedded_io_kind() {
        assert!(is_interrupted(&ErrorKind::Interrupted));
        assert!(!is_interrupted(&ErrorKind::Other));
    }

    #[test]
    fn embedded_source_feeds_embedded_sink_end_to_end() {
        let mut input = EmbeddedSource::new(&b"embedded to embedded"[..], [0u8; 3]);
        let mut bytes = [0u8; 32];
        let written = {
            let mut output = EmbeddedSink::new(&mut bytes[..], [0u8; 3]);
            stream_to_stream(&mut input, identity(), &mut output).unwrap();
            32 - output.into_inner().len()
        };
        assert_eq!(&bytes[..written], b"embedded to embedded");
    }
}
