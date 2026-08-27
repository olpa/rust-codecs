use std::io::{BufRead, Read, Write};

use crate::sources_and_sinks::shared_io::{
    EintrFillBuf, EintrRead, LendingSource, RetryingWrite, ScratchSink, ScratchSource,
};
use crate::{Sink, Source};

fn is_interrupted(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::Interrupted
}

/// Wraps a `std::io::Read`, recognizing `Interrupted`.
struct StdReader<R>(R);

impl<R: Read> EintrRead for StdReader<R> {
    type Error = std::io::Error;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read(buf)
    }

    fn is_interrupted(err: &Self::Error) -> bool {
        is_interrupted(err)
    }
}

/// A `Source` over `std::io::Read`, reading into an owned scratch
/// buffer.
pub struct StdSource<R, S>(ScratchSource<StdReader<R>, S>);

impl<R: Read, S: AsMut<[u8]>> StdSource<R, S> {
    /// Build a `StdSource`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`.
    pub fn new(inner: R, buffer: S) -> Self {
        Self(ScratchSource::new(StdReader(inner), buffer))
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

    /// Reclaim both the reader and the scratch buffer; any unconsumed
    /// bytes still in it are lost.
    pub fn into_parts(self) -> (R, S) {
        let (reader, buffer) = self.0.into_parts();
        (reader.0, buffer)
    }
}

impl<R: Read, S: AsMut<[u8]>> Source for StdSource<R, S> {
    type Error = std::io::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        self.0.chunk()
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// Wraps a `std::io::BufRead`, recognizing `Interrupted`.
struct StdBufReader<R>(R);

impl<R: BufRead> EintrFillBuf for StdBufReader<R> {
    type Error = std::io::Error;

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

/// A `Source` over `std::io::BufRead`, using the `BufRead`'s buffer
/// directly to expose input as `u8` slices.
pub struct BufReadSource<R>(LendingSource<StdBufReader<R>>);

impl<R: BufRead> BufReadSource<R> {
    pub fn new(inner: R) -> Self {
        Self(LendingSource::new(StdBufReader(inner)))
    }

    pub fn get_ref(&self) -> &R {
        &self.0.get_ref().0
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.0.get_mut().0
    }

    /// Return the wrapped reader. Unlike `StdSource::into_inner`,
    /// nothing is lost — there's no scratch buffer to discard.
    pub fn into_inner(self) -> R {
        self.0.into_inner().0
    }
}

impl<R: BufRead> Source for BufReadSource<R> {
    type Error = std::io::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        self.0.chunk()
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// Wraps a `std::io::Write`, delegating to its own `write_all`.
struct StdWriter<W>(W);

impl<W: Write> RetryingWrite for StdWriter<W> {
    type Error = std::io::Error;

    fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.0.write_all(buf)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush()
    }
}

/// A `Sink` over `std::io::Write`, staging writes in an owned scratch
/// buffer.
pub struct StdSink<W, S>(ScratchSink<StdWriter<W>, S>);

impl<W: Write, S: AsMut<[u8]>> StdSink<W, S> {
    /// Build a `StdSink`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`.
    pub fn new(inner: W, buffer: S) -> Self {
        Self(ScratchSink::new(StdWriter(inner), buffer))
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

impl<W: Write, S: AsMut<[u8]>> Sink for StdSink<W, S> {
    type Error = std::io::Error;

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
    use std::io::Cursor;

    use super::{is_interrupted, StdSink, StdSource};
    use crate::identity::identity;
    use crate::stream_to_stream;

    #[test]
    fn is_interrupted_recognizes_the_std_io_kind() {
        assert!(is_interrupted(&std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "eintr"
        )));
        assert!(!is_interrupted(&std::io::Error::other("boom")));
    }

    #[test]
    fn std_source_feeds_std_sink_end_to_end() {
        let mut input = StdSource::new(Cursor::new(b"std to std".as_slice()), [0u8; 3]);
        let mut output = StdSink::new(Vec::new(), [0u8; 3]);
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"std to std");
    }
}
