//! The [`Pump`](crate::Pump)-driving core behind `std_io`/`embedded_io`'s
//! `CodecReader`/`CodecWriter`: one function per `Read`/`Write`
//! operation (`end_capable_pump_read`/`pump_write`/`pump_finish`/`pump_flush`),
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
pub use read::end_capable_pump_read;

mod write;
pub use write::{pump_finish, pump_flush, pump_write};

/// Generic `Source`/`Sink` engines a backend's adapter module builds
/// on: the scratch-buffer/`BufRead`-forwarding/`spare`-`commit`
/// bookkeeping that's identical across `std::io`/`embedded_io`-style
/// backends, parameterized over the one thing that differs — how a
/// backend makes and retries a single `read`/`fill_buf`/`write` call.
/// See `std_io::adapter`/`embedded_io::adapter` for how a backend
/// plugs into these.
mod sink;
pub use sink::{RetryingWrite, ScratchSink};

mod source;
pub use source::{LendingSource, RetryingFillBuf, RetryingRead, ScratchSource};

/// The retry-on-interrupted loop a `RetryingRead`/`RetryingWrite` impl
/// needs for a single `read`/`write` call — shared because that loop
/// is identical across backends once each supplies its own
/// "is this error interrupted" predicate.
mod retry;
pub use retry::{retry_fill_buf, retry_on_interrupted, retry_write_all};

/// Test doubles for exercising a `CodecReader`/`CodecWriter`-style
/// wrapper built on top of a `Source`/`Sink` backend: a codec that ends
/// its stream in-band, and one that buffers everything until
/// `flush`/`finish`. `std_io`'s and `embedded_io`'s own test suites use
/// these; a third-party backend can too, via the `test-support`
/// feature, rather than reimplementing the same doubles for its own
/// tests.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
