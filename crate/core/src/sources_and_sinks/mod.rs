//! Concrete [`Source`](crate::Source)/[`Sink`](crate::Sink) backends
//! and their associated `CodecReader`/`CodecWriter`.

#[cfg(feature = "embedded-io")]
pub mod embedded_io;
pub mod slice;
#[cfg(feature = "std")]
pub mod std_io;
#[cfg(feature = "alloc")]
pub mod vec;

pub mod shared_io;
