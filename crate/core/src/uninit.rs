//! Helpers for the `&mut [MaybeUninit<u8>]` output buffers of codecs.
//!
//! These helpers let the crate support Rust versions older than 1.93.
//! They replace standard methods that are stable only since Rust 1.93.

use core::mem::MaybeUninit;

pub(crate) fn as_uninit_mut(bytes: &mut [u8]) -> &mut [MaybeUninit<u8>] {
    unsafe { &mut *(bytes as *mut [u8] as *mut [MaybeUninit<u8>]) }
}

/// Copy `src` into `dst`. Panics if the lengths differ.
///
/// Replaces this method, which is stable only since Rust 1.93:
/// `impl<T> [MaybeUninit<T>] { pub fn write_copy_of_slice(&mut self, src: &[T]) -> &mut [T] where T: Copy }`
///
/// ```
/// use core::mem::MaybeUninit;
/// use rust_codecs_core::uninit::copy_to_uninit;
///
/// let mut output = [MaybeUninit::<u8>::uninit(); 3];
/// copy_to_uninit(&mut output, b"abc");
/// ```
pub fn copy_to_uninit(dst: &mut [MaybeUninit<u8>], src: &[u8]) {
    assert_eq!(
        dst.len(),
        src.len(),
        "source and destination lengths differ"
    );
    for (d, s) in dst.iter_mut().zip(src) {
        d.write(*s);
    }
}

/// Fill `dst` with zeros and return it as `&mut [u8]`.
///
/// Use it to pass a codec's output buffer to a function that accepts
/// only `&mut [u8]`, for example `base64::Engine::encode_slice`. The
/// cost is one `memset`.
///
/// ```
/// use core::mem::MaybeUninit;
/// use rust_codecs_core::uninit::zero_init_mut;
///
/// let mut output = [MaybeUninit::<u8>::uninit(); 3];
/// let bytes: &mut [u8] = zero_init_mut(&mut output);
/// bytes.copy_from_slice(b"abc");
/// ```
pub fn zero_init_mut(dst: &mut [MaybeUninit<u8>]) -> &mut [u8] {
    // SAFETY: `write_bytes` sets every byte of `dst` to 0. This
    // satisfies the precondition of `assume_init_mut`.
    unsafe {
        core::ptr::write_bytes(dst.as_mut_ptr().cast::<u8>(), 0, dst.len());
        assume_init_mut(dst)
    }
}

/// View `s` as initialized bytes.
///
/// Replaces this method, which is stable only since Rust 1.93:
/// `impl<T> [MaybeUninit<T>] { pub unsafe fn assume_init_mut(&mut self) -> &mut [T] }`
///
/// # Safety
///
/// Every byte of `s` must be initialized.
unsafe fn assume_init_mut(s: &mut [MaybeUninit<u8>]) -> &mut [u8] {
    // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`. The caller
    // guarantees that every byte is initialized.
    unsafe { &mut *(s as *mut [MaybeUninit<u8>] as *mut [u8]) }
}
