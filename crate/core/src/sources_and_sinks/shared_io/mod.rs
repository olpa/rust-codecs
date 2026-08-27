//! Shared logic for I/O backends.
//! Public so a third-party `Source`/`Sink` backend can reuse it.

mod read;
pub use read::end_capable_pump_read;

mod write;
pub use write::{pump_finish, pump_flush, pump_write};

mod sink;
pub use sink::{RetryingWrite, ScratchSink};

mod source;
pub use source::{EintrFillBuf, EintrRead, LendingSource, ScratchSource};

mod retry;
pub use retry::{retry_fill_buf, retry_on_interrupted, retry_write_all};
