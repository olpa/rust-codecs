//! Ways to run a [`Codec`](crate::Codec) over bytes you already have.
//!
//! - [`stream`]: wrap a `std::io::Read`/`Write` so the transform runs on
//!   the fly as the wrapped stream is used.
//! - `embedded`: native `embedded_io::Read`/`Write` wrappers, enabled by
//!   the `embedded-io` feature.
//! - [`vec`]: run the transform once over an in-memory buffer and get a
//!   `Vec<u8>` back.
//! - [`stream_to_stream`]: drive a transform between two iterator-based
//!   streams of byte buffers.
//!
//! A runtime-built chain of codecs (e.g. one assembled from a list of
//! codec names) is a [`Chain`](crate::Chain), not a stack of nested
//! adapters — fold the codecs into one `Chain` first, then wrap that
//! single codec in one [`CodecReader`]/[`CodecWriter`].

#[cfg(feature = "embedded-io")]
mod embedded;
#[cfg(feature = "std")]
mod stream;
mod stream_to_stream;
#[cfg(feature = "alloc")]
mod vec;

#[cfg(feature = "embedded-io")]
pub use embedded::{EmbeddedCodecReader, EmbeddedCodecWriter, EmbeddedError};
#[cfg(feature = "std")]
pub use stream::{CodecReader, CodecWriter};
pub use stream_to_stream::{stream_to_stream, CopyError, Totals};
#[cfg(feature = "alloc")]
pub use vec::to_vec;
