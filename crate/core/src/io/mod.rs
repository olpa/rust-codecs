//! Ways to run a [`Codec`](crate::Codec) over bytes you already have.
//!
//! - [`stream`]: wrap a `std::io::Read`/`Write` so the transform runs on
//!   the fly as the wrapped stream is used.
//! - `embedded`: native `embedded_io::Read`/`Write` wrappers, enabled by
//!   the `embedded-io` feature.
//! - [`VecInput`]/[`VecOutput`]: use owned vectors as stream endpoints.
//! - [`stream_to_stream`]: transfer between supported stream adapters.
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
mod adapters;
#[cfg(feature = "std")]
mod std_adapters;
#[cfg(feature = "embedded-io")]
mod embedded_adapters;

#[cfg(feature = "embedded-io")]
pub use embedded::{EmbeddedCodecReader, EmbeddedCodecWriter, EmbeddedError};
#[cfg(feature = "std")]
pub use stream::{CodecReader, CodecWriter};
pub use stream_to_stream::{stream_to_stream, CopyError, Input, Output, Totals};
#[cfg(feature = "alloc")]
pub use adapters::{VecInput, VecOutput};
#[cfg(feature = "std")]
pub use std_adapters::{StdInput, StdOutput};
#[cfg(feature = "embedded-io")]
pub use embedded_adapters::{EmbeddedInput, EmbeddedOutput};
