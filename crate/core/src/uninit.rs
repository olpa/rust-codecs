//! Bridges between `&mut [u8]` and `&mut [MaybeUninit<u8>]`.

use core::mem::MaybeUninit;

pub(crate) fn as_uninit_mut(bytes: &mut [u8]) -> &mut [MaybeUninit<u8>] {
    unsafe { &mut *(bytes as *mut [u8] as *mut [MaybeUninit<u8>]) }
}
