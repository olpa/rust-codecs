//! Adapter from [`BoundaryAwareCodec`] to [`Codec`].

use core::mem::MaybeUninit;

use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, Error, ErrorKind,
    Progress,
};

/// A [`BoundaryAwareCodec`] as a [`Codec`].
///
/// Use this adapter when you need a [`Codec`] and you know that the
/// input has no boundary. Otherwise, `process` returns an error of the
/// kind [`ErrorKind::ContractViolation`] with the subcode
/// [`SUBCODE_UNEXPECTED_BOUNDARY`].
///
/// After the boundary, each later call of `process` returns the same
/// error, with `consumed` and `written` zero. It does not call the
/// inner codec.
#[derive(Debug, Clone)]
pub struct BoundaryAwareCodecAsCodec<C> {
    inner: C,
    /// The inner codec reported a boundary. After a boundary, the
    /// adapter does not call `process_bounded` again.
    boundary_seen: bool,
}

impl<C> BoundaryAwareCodecAsCodec<C> {
    /// An adapter for `inner`.
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            boundary_seen: false,
        }
    }

    /// The inner codec.
    pub fn get_ref(&self) -> &C {
        &self.inner
    }

    /// The inner codec, mutable.
    pub fn get_mut(&mut self) -> &mut C {
        &mut self.inner
    }

    /// The inner codec, by value.
    pub fn into_inner(self) -> C {
        self.inner
    }
}

impl<C: DrainCodec> DrainCodec for BoundaryAwareCodecAsCodec<C> {
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.inner.flush(output)
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.inner.finish(output)
    }
}

impl<C: BoundaryAwareCodec> Codec for BoundaryAwareCodecAsCodec<C> {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        if self.boundary_seen {
            return Err(unexpected_boundary(0, 0));
        }
        match self.inner.process_bounded(input, output)? {
            BoundaryAwareProgress::InputConsumed { written } => {
                Ok(Progress::InputConsumed { written })
            }
            BoundaryAwareProgress::OutputFilled { consumed } => {
                Ok(Progress::OutputFilled { consumed })
            }
            BoundaryAwareProgress::Boundary { consumed, written } => {
                self.boundary_seen = true;
                Err(unexpected_boundary(consumed, written))
            }
        }
    }
}

/// The error for a boundary from the inner codec.
fn unexpected_boundary(consumed: usize, written: usize) -> Error {
    Error {
        kind: ErrorKind::ContractViolation,
        codec_subcode: SUBCODE_UNEXPECTED_BOUNDARY,
        consumed,
        written,
    }
}

/// The subcode for a boundary from the inner codec of
/// [`BoundaryAwareCodecAsCodec`].
pub const SUBCODE_UNEXPECTED_BOUNDARY: usize = 0x200;

#[cfg(test)]
mod tests {
    use crate::uninit::as_uninit_mut;

    use super::*;

    #[derive(Default)]
    struct AlwaysBoundary {
        /// The number of `process_bounded` calls.
        calls: usize,
    }

    impl DrainCodec for AlwaysBoundary {
        fn flush(&mut self, _: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
        fn finish(&mut self, _: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl BoundaryAwareCodec for AlwaysBoundary {
        fn process_bounded(
            &mut self,
            _input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<BoundaryAwareProgress, Error> {
            self.calls += 1;
            Ok(BoundaryAwareProgress::Boundary {
                consumed: 1,
                written: 2,
            })
        }
    }

    #[test]
    fn boundary_becomes_contract_violation() {
        let mut codec = BoundaryAwareCodecAsCodec::new(AlwaysBoundary::default());
        let mut out = [0u8; 8];
        let err = codec.process(b"x", as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(err.kind, ErrorKind::ContractViolation);
        assert_eq!(err.codec_subcode, SUBCODE_UNEXPECTED_BOUNDARY);
        assert_eq!(err.consumed, 1);
        assert_eq!(err.written, 2);
    }

    #[test]
    fn process_after_boundary_does_not_call_the_inner_codec() {
        let mut codec = BoundaryAwareCodecAsCodec::new(AlwaysBoundary::default());
        let mut out = [0u8; 8];
        codec.process(b"x", as_uninit_mut(&mut out)).unwrap_err();
        let err = codec.process(b"y", as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(err, unexpected_boundary(0, 0));
        assert_eq!(codec.get_ref().calls, 1);
    }

    #[test]
    fn input_consumed_passes_through() {
        use crate::codecs::identity::identity;
        let mut codec = BoundaryAwareCodecAsCodec::new(identity());
        let mut out = [0u8; 8];
        let progress = codec.process(b"hello", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 5 });
        assert_eq!(&out[..5], b"hello");
    }
}
