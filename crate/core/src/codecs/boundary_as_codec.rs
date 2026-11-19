//! Adapter from [`BoundaryAwareCodec`] to [`Codec`].

use core::mem::MaybeUninit;

use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, Error, ErrorKind,
    Progress,
};

/// A [`BoundaryAwareCodec`] as a [`Codec`].
///
/// Every call to `process` delegates to `process_bounded`. If the inner
/// codec reports [`Boundary`](BoundaryAwareProgress::Boundary), this adapter
/// returns [`ErrorKind::ContractViolation`], because a plain [`Codec`]
/// promises that nothing follows the stream.
#[derive(Debug, Clone)]
pub struct BoundaryAwareCodecAsCodec<C> {
    inner: C,
}

impl<C> BoundaryAwareCodecAsCodec<C> {
    /// Wrap `inner`.
    pub fn new(inner: C) -> Self {
        Self { inner }
    }

    /// The inner codec.
    pub fn get_ref(&self) -> &C {
        &self.inner
    }

    /// The inner codec, for configuration only.
    pub fn get_mut(&mut self) -> &mut C {
        &mut self.inner
    }

    /// Unwrap the inner codec.
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
        match self.inner.process_bounded(input, output)? {
            BoundaryAwareProgress::InputConsumed { written } => {
                Ok(Progress::InputConsumed { written })
            }
            BoundaryAwareProgress::OutputFilled { consumed } => {
                Ok(Progress::OutputFilled { consumed })
            }
            BoundaryAwareProgress::Boundary { consumed, written } => Err(Error {
                kind: ErrorKind::ContractViolation,
                codec_subcode: SUBCODE_UNEXPECTED_BOUNDARY,
                consumed,
                written,
            }),
        }
    }
}

/// [`BoundaryAwareCodecAsCodec`] found a boundary where none was expected.
pub const SUBCODE_UNEXPECTED_BOUNDARY: usize = 0x200;

#[cfg(test)]
mod tests {
    use crate::uninit::as_uninit_mut;

    use super::*;

    struct AlwaysBoundary;

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
            Ok(BoundaryAwareProgress::Boundary {
                consumed: 1,
                written: 2,
            })
        }
    }

    #[test]
    fn boundary_becomes_contract_violation() {
        let mut codec = BoundaryAwareCodecAsCodec::new(AlwaysBoundary);
        let mut out = [0u8; 8];
        let err = codec.process(b"x", as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(err.kind, ErrorKind::ContractViolation);
        assert_eq!(err.codec_subcode, SUBCODE_UNEXPECTED_BOUNDARY);
        assert_eq!(err.consumed, 1);
        assert_eq!(err.written, 2);
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
