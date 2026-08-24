//! The [`Pump`](crate::Pump)-driving core behind `std_io`/`embedded_io`'s
//! `CodecReader`/`CodecWriter`: one function per `Read`/`Write`
//! operation (`pump_read`/`pump_write`/`pump_finish`/`pump_flush`),
//! each combining a [`Pump`](crate::Pump) with a
//! [`Source`](crate::Source)/[`Sink`](crate::Sink) for exactly one
//! bounded call.
//!
//! Public so a third-party `Source`/`Sink` backend can build its own
//! incremental `Read`/`Write`-style wrapper the same way this crate's
//! own backends do — hold a `Pump<C>` alongside your adapter, and call
//! these instead of reimplementing the chunk/commit drive loop. See
//! `std_io::wrapper`'s `CodecReader`/`CodecWriter` for the pattern:
//! each `Read`/`Write` method is a single call into one of these,
//! followed by mapping [`DriveError`](crate::DriveError) into your own
//! error type.

mod read;
pub use read::pump_read;

mod write;
pub use write::{pump_finish, pump_flush, pump_write};

/// Test doubles for exercising a `Source`/`Sink` backend built on top
/// of this module — a codec that ends its stream in-band, one that
/// buffers everything until `flush`/`finish`, and readers/writers that
/// record how often they were called. `std_io`'s and `embedded_io`'s
/// own test suites use these; a third-party backend can too, via the
/// `test-support` feature, rather than reimplementing the same doubles
/// for its own tests.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
