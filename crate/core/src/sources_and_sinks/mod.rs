//! Concrete [`Source`](crate::Source)/[`Sink`](crate::Sink) backends, one
//! per underlying byte transport.
//!
//! - [`std_io`][]: `std::io::Read`/`Write`.
//! - [`embedded_io`][]: `embedded_io::Read`/`Write`, enabled by the
//!   `embedded-io` feature.
//! - [`mod@vec`][]: an owned `Vec<u8>` — fully in-memory, so there's no
//!   reader/writer wrapper on top, just the adapter.
//! - [`mod@slice`][]: borrowed `&[u8]`/`&mut [u8]` slices — also used
//!   internally to give `std_io`/`embedded_io`'s `CodecReader`/
//!   `CodecWriter` a `Source`/`Sink` over the caller's own buffer for
//!   the duration of one call.

#[cfg(feature = "embedded-io")]
pub mod embedded_io;
#[cfg(feature = "std")]
pub mod std_io;
#[cfg(any(feature = "std", feature = "embedded-io"))]
pub mod slice;
#[cfg(feature = "alloc")]
pub mod vec;
