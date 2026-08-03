use core::convert::Infallible;

use crate::{Source, Sink};

pub(crate) struct SliceSource<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SliceSource<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self { Self { bytes, pos: 0 } }
    pub(crate) fn consumed(&self) -> usize { self.pos }
}

impl Source for SliceSource<'_> {
    type Error = Infallible;
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        Ok((self.pos < self.bytes.len()).then_some(&self.bytes[self.pos..]))
    }
    fn consume(&mut self, amount: usize) { self.pos += amount; }
}

pub(crate) struct SliceSink<'a> {
    bytes: &'a mut [u8],
    pos: usize,
}

impl<'a> SliceSink<'a> {
    pub(crate) fn new(bytes: &'a mut [u8]) -> Self { Self { bytes, pos: 0 } }
    pub(crate) fn written(&self) -> usize { self.pos }
}

impl Sink for SliceSink<'_> {
    type Error = Infallible;
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        Ok((self.pos < self.bytes.len()).then_some(&mut self.bytes[self.pos..]))
    }
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        self.pos += amount;
        Ok(())
    }
}
