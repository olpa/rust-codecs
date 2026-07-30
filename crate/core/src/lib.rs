//! Core crate for RustCodecs: the [`Codec`] trait, its vocabulary, and
//! the stream adapters a codec crate and its clients build on.
//!
//! - [`Codec`]: implement this to add a codec.
//! - [`Outcome`], [`Drain`], [`Error`], [`ErrorKind`]: the vocabulary
//!   [`Codec`]'s methods speak in. The contract in one sentence: every
//!   call fully consumes its input, fully fills its output, or ends
//!   the stream.
//! - [`Carry`]: helper for codecs with a minimum atomic output unit,
//!   letting an emitted unit span output buffers.
//! - [`io`]: stream adapters (`std::io::Read`/`Write`) and one-shot
//!   `Vec<u8>` helpers built on top of [`Codec`].

mod codec;
pub use codec::{Codec, Drain, Error, ErrorKind, Outcome};

mod carry;
pub use carry::Carry;

mod transfer;
mod driver;

mod chain;
pub use chain::Chain;

pub mod io;

#[cfg(feature = "identity")]
pub mod identity;

#[cfg(feature = "rot13")]
pub mod rot13;

#[cfg(feature = "base64")]
pub mod base64;

#[cfg(feature = "json")]
pub mod json;
