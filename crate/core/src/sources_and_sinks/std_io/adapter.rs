use std::io::{Read, Write};

use crate::{Source, Sink};

pub struct StdSource<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
    eof: bool,
}

impl<R: Read, S: AsMut<[u8]>> StdSource<R, S> {
    pub fn new(inner: R, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "StdSource buffer must be non-empty"
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
}

impl<R: Read, S: AsMut<[u8]>> Source for StdSource<R, S> {
    type Error = std::io::Error;

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

pub struct StdSink<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: Write, S: AsMut<[u8]>> StdSink<W, S> {
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "StdSink buffer must be non-empty"
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
}

impl<W: Write, S: AsMut<[u8]>> Sink for StdSink<W, S> {
    type Error = std::io::Error;

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

#[cfg(all(test, feature = "identity"))]
mod tests {
    use std::io::Cursor;

    use super::{StdSource, StdSink};
    use crate::identity::identity;
    use crate::stream_to_stream;
    use crate::sources_and_sinks::vec::{VecSource, VecSink};

    #[test]
    fn std_input_can_feed_vec_output() {
        let mut input = StdSource::new(Cursor::new(b"std to vec"), [0u8; 3]);
        let mut output = VecSink::default();
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"std to vec");
    }

    #[test]
    fn vec_input_can_feed_std_output() {
        let mut input = VecSource::new(b"vec to std".to_vec());
        let mut output = StdSink::new(Vec::new(), [0u8; 3]);
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"vec to std");
    }
}
