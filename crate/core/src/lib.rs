//! Core crate for RustCodecs: re-exports the shared vocabulary and stream
//! adapters a codec crate and its clients build on, so neither ever has to
//! `use compcol` directly — `compcol` is an implementation detail behind
//! this crate's boundary.
//!
//! - [`Codec`]: implement this to add a codec.
//! - [`Error`], [`Progress`], [`Status`]: the shared vocabulary [`Codec`]'s
//!   methods speak in.
//! - [`io`]: stream adapters (`std::io::Read`/`Write`) and one-shot
//!   `Vec<u8>` helpers built on top of [`Codec`].

pub use compcol::{Error, Progress, Status};

mod codec;
pub use codec::Codec;

pub mod io;

#[cfg(feature = "identity")]
pub mod identity;

#[cfg(feature = "rot13")]
pub mod rot13;

#[cfg(feature = "base64")]
pub mod base64;
