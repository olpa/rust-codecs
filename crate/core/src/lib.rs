#![cfg_attr(not(feature = "std"), no_std)]
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
//! - [`io`]: adapters for driving codecs between iterator, `Vec`,
//!   `std::io`, and `embedded_io` streams.

#[cfg(feature = "alloc")]
extern crate alloc;

mod codec;
pub use codec::{Codec, Drain, Error, ErrorKind, Outcome};

mod carry;
pub use carry::Carry;

mod transfer;
mod driver;

mod chain;
pub use chain::Chain;

pub mod io;

mod codecs;
#[allow(unused_imports)]
pub use codecs::*;
