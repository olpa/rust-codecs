//! Ways to run a [`Codec`](crate::Codec) over bytes you already have.
//!
//! - `std`: wrap a `std::io::Read`/`Write` so the transform runs on the
//!   fly as the wrapped stream is used.
//! - `embedded`: native `embedded_io::Read`/`Write` wrappers, enabled by
//!   the `embedded-io` feature.
//! - `vec`: use owned vectors ([`VecSource`]/[`VecSink`]) as stream
//!   endpoints — fully in-memory, so there's no reader/writer wrapper.
//! - [`stream_to_stream`]: transfer between supported stream adapters.
//!
//! A runtime-built chain of codecs (e.g. one assembled from a list of
//! codec names) is a [`Chain`](crate::Chain), not a stack of nested
//! adapters — fold the codecs into one `Chain` first, then wrap that
//! single codec in one [`CodecReader`]/[`CodecWriter`].

#[cfg(feature = "embedded-io")]
mod embedded;
#[cfg(feature = "std")]
mod std;
mod stream_to_stream;
#[cfg(any(feature = "std", feature = "embedded-io"))]
mod slice_adapters;
#[cfg(feature = "alloc")]
mod vec;

#[cfg(feature = "embedded-io")]
pub use self::embedded::{EmbeddedCodecReader, EmbeddedCodecWriter, EmbeddedError, EmbeddedSource, EmbeddedSink};
#[cfg(feature = "std")]
pub use self::std::{CodecReader, CodecWriter, StdSource, StdSink};
pub use stream_to_stream::{stream_to_stream, CopyError, Source, Sink, Totals};
#[cfg(feature = "alloc")]
pub use self::vec::{VecSource, VecSink};
