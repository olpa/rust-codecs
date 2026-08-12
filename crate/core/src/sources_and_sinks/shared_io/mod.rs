//! Transport-independent glue shared by the `std_io` and `embedded_io`
//! reader/writer wrappers — factored out because it's identical across
//! backends, not part of the public API.

mod read;
pub(crate) use read::pump_read;

mod write;
pub(crate) use write::pump_write;
