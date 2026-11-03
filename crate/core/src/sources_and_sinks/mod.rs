//! Concrete [`Source`](crate::Source)/[`Sink`](crate::Sink) backends
//! and their associated `CodecReader`/`CodecWriter`.

#[cfg(feature = "embedded-io")]
#[cfg_attr(docsrs, doc(cfg(feature = "embedded-io")))]
pub mod embedded_io;
pub mod slice;
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub mod std_io;
#[cfg(feature = "alloc")]
#[cfg_attr(docsrs, doc(cfg(feature = "alloc")))]
pub mod vec;

pub mod shared_io;
