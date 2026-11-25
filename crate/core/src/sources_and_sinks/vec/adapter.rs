use alloc::vec::Vec;
use core::convert::Infallible;
use core::num::NonZeroUsize;

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
///
/// You can limit the size of the window that `spare` returns. Otherwise,
/// the window is all spare capacity, and the growth of `Vec` sets its size.
pub struct VecSink {
    // - `inner[..pos]` is the output.
    // - `inner[pos..len]` is initialized but not committed: space that
    //   was lent before and not used.
    // - `inner[len..capacity]` is raw memory that is never lent.
    inner: Vec<u8>,
    pos: usize,
    max_window_len: Option<NonZeroUsize>,
    // Set by the last `spare`, cleared by `commit`.
    offered: usize,
}

impl VecSink {
    /// Build a `VecSink` with no maximum window.
    pub fn new(inner: Vec<u8>) -> Self {
        Self::build(inner, None)
    }

    /// Build a `VecSink` with a maximum window of `max_window_len` bytes.
    pub fn with_max_window(inner: Vec<u8>, max_window_len: NonZeroUsize) -> Self {
        Self::build(inner, Some(max_window_len))
    }

    fn build(inner: Vec<u8>, max_window_len: Option<NonZeroUsize>) -> Self {
        Self {
            pos: inner.len(),
            inner,
            max_window_len,
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

impl core::fmt::Debug for VecSink {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("VecSink")
            .field("len", &self.pos)
            .field("capacity", &self.inner.capacity())
            .field("max_window_len", &self.max_window_len)
            .field("offered", &self.offered)
            .finish_non_exhaustive()
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
            let new_len = match self.max_window_len {
                Some(n) => self.inner.len() + n.get(),
                None => {
                    // Not 1 byte: `Vec` grows by its amortized step (roughly doubles).
                    self.inner.reserve(1);
                    self.inner.capacity()
                }
            };
            self.inner.resize(new_len, 0);
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
    fn sink_grows_when_all_offered_space_is_committed() {
        let mut sink = VecSink::with_max_window(Vec::new(), NonZeroUsize::new(4).unwrap());
        let spare = sink.spare().unwrap().unwrap();
        spare.fill(b'a');
        let offered = spare.len();
        sink.commit(offered).unwrap();
        // Unlike `SliceSink`, `VecSink` grows on demand: filling every
        // byte offered so far must not produce `None` or an empty slice.
        let spare = sink.spare().unwrap().unwrap();
        assert!(!spare.is_empty());
    }

    #[test]
    fn sink_keeps_existing_bytes_and_returns_only_committed_bytes() {
        let mut sink = VecSink::with_max_window(alloc::vec![b'x'], NonZeroUsize::new(4).unwrap());
        let spare = sink.spare().unwrap().unwrap();
        assert!(spare.len() > 2);
        spare[..2].copy_from_slice(b"ab");
        sink.commit(2).unwrap();
        assert_eq!(sink.get_ref(), b"xab");
        assert_eq!(sink.into_inner(), b"xab");
    }

    #[test]
    fn sink_offers_uncommitted_space_again() {
        let mut sink = VecSink::with_max_window(Vec::new(), NonZeroUsize::new(4).unwrap());
        let spare = sink.spare().unwrap().unwrap();
        let (first_ptr, first_len) = (spare.as_ptr(), spare.len());
        spare[..2].copy_from_slice(b"ab");
        sink.commit(1).unwrap();
        let spare = sink.spare().unwrap().unwrap();
        assert_eq!(spare.as_ptr(), first_ptr.wrapping_add(1));
        assert_eq!(spare.len(), first_len - 1);
        assert_eq!(spare[0], b'b');
    }

    #[test]
    fn sink_window_is_at_most_max_window_len() {
        let mut sink =
            VecSink::with_max_window(Vec::with_capacity(64), NonZeroUsize::new(4).unwrap());
        for _ in 0..3 {
            let spare = sink.spare().unwrap().unwrap();
            assert_eq!(spare.len(), 4);
            sink.commit(4).unwrap();
        }
    }

    #[test]
    fn sink_without_max_window_lends_all_spare_capacity() {
        let mut sink = VecSink::new(Vec::with_capacity(64));
        let spare = sink.spare().unwrap().unwrap();
        assert!(spare.len() >= 64);
    }
}
