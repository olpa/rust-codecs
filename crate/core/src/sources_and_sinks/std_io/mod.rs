//! Adapters for `std::io` backend.
//!
//! Behind the `std` feature flag.
//!
//! The wrappers return every failure as an `io::Error`. Its payload is a
//! [`StreamError`](crate::StreamError). The payload holds the byte
//! counts of the failed call.
//!
//! The wrapped reader or writer can report an error. The wrapper must
//! keep the byte counts, so it cannot return the original error
//! unchanged. It puts the original error in the payload. For this
//! reason, `raw_os_error` returns `None` on the wrapper error. To get
//! the original error, downcast as shown below:
//!
//! ```
//! use rust_codecs_core::{StreamError, StreamErrorKind};
//!
//! fn os_code(error: &std::io::Error) -> Option<i32> {
//!     let payload = error
//!         .get_ref()?
//!         .downcast_ref::<StreamError<std::io::Error>>()?;
//!     match &payload.kind {
//!         StreamErrorKind::Io(original) => original.raw_os_error(),
//!         _ => None,
//!     }
//! }
//! ```

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSink, StdSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};

pub use super::shared_io::CodecWriterFinishError;
