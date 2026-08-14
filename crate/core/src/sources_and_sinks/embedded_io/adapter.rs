use embedded_io::{Read, Write};

use crate::{Source, Sink};

pub struct EmbeddedSource<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
    eof: bool,
}

impl<R: Read, S: AsMut<[u8]>> EmbeddedSource<R, S> {
    pub fn new(inner: R, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "EmbeddedSource buffer must be non-empty"
        );
        Self {
            inner,
            buffer,
            pos: 0,
            len: 0,
            eof: false,
        }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Reclaim both the reader and the scratch buffer, e.g. to reuse
    /// the buffer's allocation for another `EmbeddedSource`.
    /// `into_inner` drops the buffer; this is the exhaustive teardown.
    pub fn into_parts(self) -> (R, S) {
        (self.inner, self.buffer)
    }
}

impl<R: Read, S: AsMut<[u8]>> Source for EmbeddedSource<R, S> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        // Only refills once the current window is fully consumed; an
        // unconsumed remainder overlaps the previous call's chunk.
        if self.pos == self.len && !self.eof {
            self.len = self.inner.read(self.buffer.as_mut())?;
            self.pos = 0;
            self.eof = self.len == 0;
        }
        Ok((self.pos < self.len).then_some(&self.buffer.as_mut()[self.pos..self.len]))
    }

    fn consume(&mut self, amount: usize) {
        assert!(amount <= self.len - self.pos);
        self.pos += amount;
    }
}

pub struct EmbeddedSink<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: Write, S: AsMut<[u8]>> EmbeddedSink<W, S> {
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "EmbeddedSink buffer must be non-empty"
        );
        Self {
            inner,
            buffer,
            offered: 0,
        }
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Reclaim both the writer and the scratch buffer, e.g. to reuse
    /// the buffer's allocation for another `EmbeddedSink`.
    /// `into_inner` drops the buffer; this is the exhaustive teardown.
    pub fn into_parts(self) -> (W, S) {
        (self.inner, self.buffer)
    }
}

impl<W: Write, S: AsMut<[u8]>> Sink for EmbeddedSink<W, S> {
    type Error = W::Error;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        self.offered = self.buffer.as_mut().len();
        Ok(Some(self.buffer.as_mut()))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        assert!(amount <= self.offered);
        self.inner.write_all(&self.buffer.as_mut()[..amount])?;
        self.offered = 0;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}

#[cfg(all(test, feature = "identity", feature = "alloc"))]
mod tests {
    use super::{EmbeddedSource, EmbeddedSink};
    use crate::identity::identity;
    use crate::stream_to_stream;
    use crate::sources_and_sinks::vec::{VecSource, VecSink};

    #[test]
    fn embedded_input_can_feed_vec_output() {
        let mut input = EmbeddedSource::new(&b"embedded to vec"[..], [0u8; 3]);
        let mut output = VecSink::default();
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"embedded to vec");
    }

    #[test]
    fn vec_input_can_feed_embedded_output() {
        let mut input = VecSource::new(b"vec to embedded".to_vec());
        let mut bytes = [0u8; 32];
        let remaining = {
            let mut output = EmbeddedSink::new(&mut bytes[..], [0u8; 3]);
            stream_to_stream(&mut input, identity(), &mut output).unwrap();
            output.into_inner().len()
        };
        assert_eq!(&bytes[..bytes.len() - remaining], b"vec to embedded");
    }
}
