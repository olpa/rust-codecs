//! Ways to run a [`Codec`](crate::Codec) over bytes you already have.
//!
//! - [`stream`]: wrap a `std::io::Read`/`Write` so the transform runs on
//!   the fly as the wrapped stream is used.
//! - [`vec`]: run the transform once over an in-memory buffer and get a
//!   `Vec<u8>` back.
//! - [`FinishWrite`]: finish a runtime-built, boxed chain of
//!   [`CodecWriter`]s (e.g. one assembled from a list of codec names)
//!   without knowing its depth at compile time.

mod copy;
mod stream;
mod vec;

pub use copy::{copy, CopyError};
pub use stream::{CodecReader, CodecWriter, FinishWrite};
pub use vec::to_vec;
