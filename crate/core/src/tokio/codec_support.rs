//! Shared code of the adapters that run a rust-codecs codec as a tokio
//! `Decoder` or `Encoder`.

use core::mem::MaybeUninit;
use std::io;

use bytes::BytesMut;

use crate::{BoundaryAwareProgress, DrainCodec, DrainProgress, Error, ErrorKind};

/// The adapters ask `BytesMut` for at least this much free space for
/// each codec call.
const MIN_SPARE: usize = 4096;

/// Tokio wants `std::io::Error`. The crate `Error` becomes its source,
/// so a client can get it back with `downcast_ref`.
pub(super) fn to_io_error(error: Error) -> io::Error {
    let kind = match error.kind {
        ErrorKind::CorruptStream => io::ErrorKind::InvalidData,
        ErrorKind::UnexpectedEnd => io::ErrorKind::UnexpectedEof,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}

/// Give `fill` the free space at the end of `out`. `fill` returns the
/// number of bytes that it initialized and a result. Commit these bytes.
fn with_spare<R>(out: &mut BytesMut, fill: impl FnOnce(&mut [MaybeUninit<u8>]) -> (usize, R)) -> R {
    out.reserve(MIN_SPARE);
    let spare = out.spare_capacity_mut();
    let spare_len = spare.len();
    let (written, result) = fill(spare);
    let written = written.min(spare_len);
    // SAFETY: the codec contract says that the codec initialized
    // `written` bytes of the space. The count is not larger than the
    // space. This is the same trust as in `VecSink::commit`.
    unsafe { out.set_len(out.len() + written) };
    result
}

/// One call of a codec that reads `input` and writes to the end of
/// `out`. The byte counts of the result are valid.
fn step<F>(out: &mut BytesMut, input: &[u8], call: &mut F) -> Result<BoundaryAwareProgress, Error>
where
    F: FnMut(&[u8], &mut [MaybeUninit<u8>]) -> Result<BoundaryAwareProgress, Error>,
{
    with_spare(out, |spare| {
        let spare_len = spare.len();
        let checked = match call(input, spare) {
            Ok(progress) => progress.validated(input.len(), spare_len),
            Err(error) => Err(error
                .validated(input.len(), spare_len)
                .unwrap_or_else(|invalid| invalid)),
        };
        let written = match checked {
            Ok(
                BoundaryAwareProgress::InputConsumed { written }
                | BoundaryAwareProgress::Boundary { written, .. },
            ) => written,
            Ok(BoundaryAwareProgress::OutputFilled { .. }) => spare_len,
            Err(error) => error.written,
        };
        (written, checked)
    })
}

/// Process all of `input`. Append the output to `out`.
///
/// A plain codec never reports a boundary. A boundary is treated like
/// the end of the input.
pub(super) fn process_all<F>(out: &mut BytesMut, mut input: &[u8], mut call: F) -> Result<(), Error>
where
    F: FnMut(&[u8], &mut [MaybeUninit<u8>]) -> Result<BoundaryAwareProgress, Error>,
{
    loop {
        match step(out, input, &mut call)? {
            BoundaryAwareProgress::OutputFilled { consumed } => input = &input[consumed..],
            BoundaryAwareProgress::InputConsumed { .. }
            | BoundaryAwareProgress::Boundary { .. } => return Ok(()),
        }
    }
}

/// Call `finish` until it is done. Append the output to `out`.
pub(super) fn finish_all<C: DrainCodec + ?Sized>(
    codec: &mut C,
    out: &mut BytesMut,
) -> Result<(), Error> {
    loop {
        let progress = with_spare(out, |spare| {
            let spare_len = spare.len();
            let checked = match codec.finish(spare) {
                Ok(progress) => progress.validated(spare_len),
                Err(error) => Err(error
                    .validated(0, spare_len)
                    .unwrap_or_else(|invalid| invalid)),
            };
            let written = match checked {
                Ok(DrainProgress::Done { written }) => written,
                Ok(DrainProgress::OutputFilled) => spare_len,
                Err(error) => error.written,
            };
            (written, checked)
        })?;
        if let DrainProgress::Done { .. } = progress {
            return Ok(());
        }
    }
}

/// Process one call of a boundary-aware codec. See [`step`].
pub(super) fn bounded_step<C: crate::BoundaryAwareCodec>(
    codec: &mut C,
    out: &mut BytesMut,
    input: &[u8],
) -> Result<BoundaryAwareProgress, Error> {
    step(out, input, &mut |i, o| codec.process_bounded(i, o))
}
