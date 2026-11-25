use alloc::vec::Vec;
use core::convert::Infallible;

use crate::{Sink, Source};

// `VecSource` and `VecSink` have no `get_mut` method. `get_ref` and
// `into_inner` exist, so `get_mut` may look missing by accident. It is
// not. Each type keeps a position (`pos`) that must match
// the vec's real size. A public `get_mut` would let outside code
// resize the vec directly, and that could make the position wrong
// with no check to catch it.

/// `Vec<u8>` input stream.
#[derive(Debug)]
pub struct VecSource {
    inner: Vec<u8>,
    pos: usize,
}

impl VecSource {
    pub fn new(inner: Vec<u8>) -> Self {
        Self { inner, pos: 0 }
    }

    pub fn get_ref(&self) -> &Vec<u8> {
        &self.inner
    }

    // Forbidden to add: see the file-level comment above.
    // pub fn get_mut(&mut self) -> &mut Vec<u8> {
    //     &mut self.inner
    // }

    pub fn into_inner(self) -> Vec<u8> {
        self.inner
    }
}

impl Source for VecSource {
    type Error = Infallible;
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        // Not `self.inner.get(self.pos..)`: that returns `Some(&[])` when
        // `pos == len`, but exhaustion must report `None`.
        Ok((self.pos < self.inner.len()).then_some(&self.inner[self.pos..]))
    }
    fn consume(&mut self, amount: usize) {
        debug_assert!(
            amount <= self.inner.len() - self.pos,
            "consume({amount}) exceeds the remaining bytes"
        );
        self.pos += amount.min(self.inner.len() - self.pos);
    }
}

/// `Vec<u8>` auto-growing output stream.
#[derive(Debug)]
pub struct VecSink {
    // - `inner[..pos]` is the output.
    // - `inner[pos..len]` is initialized but not committed: space that
    //   was lent before and not used.
    // - `inner[len..capacity]` is raw memory that is never lent.
    inner: Vec<u8>,
    pos: usize,
    grow_by: usize,
    // Set by the last `spare`, cleared by `commit`.
    offered: usize,
}

impl VecSink {
    /// The minimum extra capacity requested each time the vec runs out of
    /// spare space. `Vec::reserve`'s underlying allocator still amortizes
    /// growth on top of this (roughly doubling capacity once it's past a
    /// small size), so this value is a floor, not the actual step size.
    pub const DEFAULT_GROWTH: usize = 1024;

    /// Build a `VecSink` that grows by [`Self::DEFAULT_GROWTH`].
    pub fn new(inner: Vec<u8>) -> Self {
        Self::with_growth(inner, Self::DEFAULT_GROWTH)
    }

    /// Build a `VecSink` that requests at least `grow_by` bytes of extra
    /// capacity each time the vec is full. A `grow_by` of 0 means
    /// [`Self::DEFAULT_GROWTH`].
    ///
    /// The bytes already in `inner` stay. The sink appends after them.
    pub fn with_growth(inner: Vec<u8>, grow_by: usize) -> Self {
        Self {
            pos: inner.len(),
            inner,
            grow_by: if grow_by > 0 {
                grow_by
            } else {
                Self::DEFAULT_GROWTH
            },
            offered: 0,
        }
    }

    /// Return the committed bytes.
    pub fn get_ref(&self) -> &[u8] {
        &self.inner[..self.pos]
    }

    // Forbidden to add: see the file-level comment above.
    // pub fn get_mut(&mut self) -> &mut Vec<u8> {
    //     &mut self.inner
    // }

    /// Return the vec with the committed bytes only.
    pub fn into_inner(mut self) -> Vec<u8> {
        self.inner.truncate(self.pos);
        self.inner
    }
}

impl Default for VecSink {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl Sink for VecSink {
    type Error = Infallible;
    // The sink writes zeros into new capacity of the vec before it lends
    // the capacity to a codec. Each byte is zeroed only once. Space that
    // the codec does not commit stays initialized, and the next `spare`
    // lends it again without zeroing.
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        if self.pos == self.inner.len() {
            self.inner.reserve(self.grow_by);
            self.inner.resize(self.inner.capacity(), 0);
        }
        let spare = &mut self.inner[self.pos..];
        self.offered = spare.len();
        Ok(Some(spare))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        debug_assert!(
            amount <= self.offered,
            "commit({amount}) exceeds the {} bytes offered by spare()",
            self.offered
        );
        self.pos += amount.min(self.offered);
        self.offered = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_chunk_is_none_once_fully_consumed() {
        let mut src = VecSource::new(alloc::vec![1, 2, 3]);
        assert_eq!(src.chunk().unwrap(), Some(&[1, 2, 3][..]));
        src.consume(3);
        // pos == len here: must be None, not Some(&[]).
        assert_eq!(src.chunk().unwrap(), None);
    }

    #[test]
    fn sink_spare_is_never_none_even_after_filling() {
        let mut sink = VecSink::with_growth(Vec::new(), 4);
        let offered = sink.spare().unwrap().unwrap().len();
        sink.spare().unwrap().unwrap().fill(b'a');
        sink.commit(offered).unwrap();
        // Unlike `SliceSink`, `VecSink` grows on demand: filling every
        // byte offered so far must not produce `None`.
        assert!(sink.spare().unwrap().is_some());
    }

    #[test]
    fn sink_keeps_existing_bytes_and_returns_only_committed_bytes() {
        let mut sink = VecSink::with_growth(alloc::vec![b'x'], 4);
        let spare = sink.spare().unwrap().unwrap();
        assert!(spare.len() > 2);
        spare[..2].copy_from_slice(b"ab");
        sink.commit(2).unwrap();
        assert_eq!(sink.get_ref(), b"xab");
        assert_eq!(sink.into_inner(), b"xab");
    }

    #[test]
    fn sink_offers_uncommitted_space_again() {
        let mut sink = VecSink::with_growth(Vec::new(), 4);
        let spare = sink.spare().unwrap().unwrap();
        let (first_ptr, first_len) = (spare.as_ptr(), spare.len());
        spare[..2].copy_from_slice(b"ab");
        sink.commit(1).unwrap();
        let spare = sink.spare().unwrap().unwrap();
        assert_eq!(spare.as_ptr(), first_ptr.wrapping_add(1));
        assert_eq!(spare.len(), first_len - 1);
        assert_eq!(spare[0], b'b');
    }
}
