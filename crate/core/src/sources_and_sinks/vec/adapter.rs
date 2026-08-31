use core::convert::Infallible;

use crate::{Sink, Source};

/// An owned `Vec<u8>` used directly as an input stream.
pub struct VecSource {
    inner: alloc::vec::Vec<u8>,
    pos: usize,
}

impl VecSource {
    pub fn new(inner: alloc::vec::Vec<u8>) -> Self {
        Self { inner, pos: 0 }
    }

    pub fn get_ref(&self) -> &alloc::vec::Vec<u8> {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut alloc::vec::Vec<u8> {
        &mut self.inner
    }

    pub fn into_inner(self) -> alloc::vec::Vec<u8> {
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
        assert!(amount <= self.inner.len() - self.pos);
        self.pos += amount;
    }
}

/// A `Vec<u8>` output stream. Codec output is written straight into
/// the vector's spare allocation without zero-initializing it first.
pub struct VecSink {
    inner: alloc::vec::Vec<u8>,
    grow_by: usize,
    offered: usize,
}

impl VecSink {
    // The minimum extra capacity requested each time the vec runs out of
    // spare space. `Vec::reserve`'s underlying allocator still amortizes
    // growth on top of this (roughly doubling capacity once it's past a
    // small size), so this value is a floor, not the actual step size.
    pub const DEFAULT_GROWTH: usize = 1024;

    pub fn new(inner: alloc::vec::Vec<u8>) -> Self {
        Self::with_growth(inner, Self::DEFAULT_GROWTH)
    }

    pub fn with_growth(inner: alloc::vec::Vec<u8>, grow_by: usize) -> Self {
        debug_assert!(grow_by > 0, "VecSink growth must be non-zero");
        Self {
            inner,
            grow_by: if grow_by > 0 { grow_by } else { Self::DEFAULT_GROWTH },
            offered: 0,
        }
    }

    pub fn get_ref(&self) -> &alloc::vec::Vec<u8> {
        &self.inner
    }

    // No `get_mut`: `commit`'s `unsafe { set_len }` trusts `self.offered`
    // (captured at the last `spare` call) to still describe the vec's
    // actual spare capacity. A caller reaching in via `&mut Vec<u8>`
    // between `spare` and `commit` (e.g. `shrink_to_fit`) could
    // invalidate that without tripping any check, turning `commit`
    // into real unsoundness rather than a panic.

    pub fn into_inner(self) -> alloc::vec::Vec<u8> {
        self.inner
    }
}

impl Default for VecSink {
    fn default() -> Self {
        Self::new(alloc::vec::Vec::new())
    }
}

impl Sink for VecSink {
    type Error = Infallible;
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        if self.inner.spare_capacity_mut().is_empty() {
            self.inner.reserve(self.grow_by);
        }
        let spare = self.inner.spare_capacity_mut();
        self.offered = spare.len();
        // DESIGN: codecs are trusted, cooperative extensions of this
        // high-performance library. In particular, a codec must initialize
        // every byte it reports as written. Deliberately do not zero this
        // spare capacity: avoiding that initialization is the reason this
        // adapter writes into `Vec` allocation directly. Treat a codec that
        // claims unwritten bytes as outside the supported safety contract,
        // not as an adversarial implementation this adapter must defend
        // against.
        // SAFETY: this deliberately lends uninitialized `u8` storage to
        // `Codec::process`/`finish`. The codec contract permits claiming
        // only the prefix it initialized; `commit` is the sole operation
        // that adds that prefix to the Vec's length. Codecs must treat the
        // output slice as write-only before initialization.
        Ok(Some(unsafe {
            core::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), spare.len())
        }))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        // `offered` resets to 0 below, so a stray commit without a prior
        // `spare` call hits this assert instead of silently growing the
        // vec past what was actually initialized.
        assert!(amount <= self.offered);
        // SAFETY: the codec reported that exactly this prefix was written.
        unsafe { self.inner.set_len(self.inner.len() + amount) };
        self.offered = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{VecSink, VecSource};
    use crate::identity::identity;
    use crate::stream_to_stream;
    use crate::{Sink, Source};

    #[test]
    fn vec_to_vec_uses_the_shared_pump() {
        let data = alloc::vec![1, 2, 3, 4, 5];
        let mut input = VecSource::new(data.clone());
        let mut output = VecSink::with_growth(alloc::vec::Vec::new(), 2);

        let totals = stream_to_stream(&mut input, identity(), &mut output).unwrap();

        assert_eq!(totals.consumed, data.len());
        assert_eq!(totals.written, data.len());
        assert_eq!(output.into_inner(), data);
    }

    #[test]
    fn source_chunk_is_none_once_fully_consumed() {
        let mut src = VecSource::new(alloc::vec![1, 2, 3]);
        assert_eq!(src.chunk().unwrap(), Some(&[1, 2, 3][..]));
        src.consume(3);
        // pos == len here: must be None, not Some(&[]).
        assert_eq!(src.chunk().unwrap(), None);
    }

    #[test]
    #[should_panic]
    fn commit_more_than_offered_panics() {
        let mut sink = VecSink::new(alloc::vec::Vec::new());
        let offered = sink.spare().unwrap().unwrap().len();
        sink.commit(offered + 1).unwrap();
    }
}
