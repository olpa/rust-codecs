//! Core crate for RustCodecs: re-exports the pieces a codec crate
//! and its clients need, so neither ever has to `use compcol` directly.
//!
//! - [`Encoder`] / [`Decoder`]: implement these to add a codec.
//! - [`Error`], [`Progress`], [`Status`]: the shared vocabulary their
//!   methods speak in.
//! - [`io`]: stream adapters (`std::io::Read`/`Write`) and one-shot
//!   `Vec<u8>` helpers built on top of `Encoder`/`Decoder`.
//!
//! What is deliberately **not** re-exported: `compcol::Algorithm`. Codec
//! crates should instead expose a pair of plain constructor functions,
//! e.g. `rot13_encoder()` / `rot13_decoder()`, that build the codec type
//! directly — see `design-interface/rust-twin-v2` for why.

pub use compcol::{Decoder, Encoder, Error, Progress, Status};

pub mod io;
