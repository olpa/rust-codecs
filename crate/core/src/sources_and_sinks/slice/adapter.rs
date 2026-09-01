use core::convert::Infallible;
use core::mem::MaybeUninit;

use crate::uninit::as_uninit_mut;
use crate::{Sink, Source};

/// A borrowed `&[u8]` used directly as an input stream.
pub struct SliceSource<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SliceSource<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    pub fn consumed(&self) -> usize {
        self.pos
    }
}

impl Source for SliceSource<'_> {
    type Error = Infallible;
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        // Not `self.bytes.get(self.pos..)`: that returns `Some(&[])` when
        // `pos == len`, but exhaustion must report `None`.
        Ok((self.pos < self.bytes.len()).then_some(&self.bytes[self.pos..]))
    }
    fn consume(&mut self, amount: usize) {
        self.pos += amount.min(self.bytes.len() - self.pos);
    }
}

/// A borrowed `&mut [u8]` used directly as an output stream.
pub struct SliceSink<'a> {
    bytes: &'a mut [u8],
    pos: usize,
}

impl<'a> SliceSink<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    pub fn written(&self) -> usize {
        self.pos
    }
}

impl Sink for SliceSink<'_> {
    type Error = Infallible;
    fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error> {
        // Not `self.bytes.get_mut(self.pos..)`: that returns `Some(&mut [])`
        // when `pos == len`, but a full sink must report `None`.
        Ok((self.pos < self.bytes.len()).then_some(as_uninit_mut(&mut self.bytes[self.pos..])))
    }
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        self.pos += amount.min(self.bytes.len() - self.pos);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_chunk_is_none_once_fully_consumed() {
        let mut src = SliceSource::new(&[1, 2, 3]);
        assert_eq!(src.chunk().unwrap(), Some(&[1, 2, 3][..]));
        src.consume(3);
        // pos == len here: must be None, not Some(&[]).
        assert_eq!(src.chunk().unwrap(), None);
    }

    #[test]
    fn sink_spare_is_none_once_fully_filled() {
        let mut buf = [0u8; 3];
        let mut sink = SliceSink::new(&mut buf);
        assert_eq!(sink.spare().unwrap().unwrap().len(), 3);
        sink.commit(3).unwrap();
        // pos == len here: must be None, not Some(&mut []).
        assert!(sink.spare().unwrap().is_none());
    }
}
