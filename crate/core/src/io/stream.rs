//! `std::io::Read`/`Write` adapters, straight from `compcol::io`.
//!
//! Kept as a thin re-export: these types carry no `Algorithm` dependency
//! and their compcol names are already the clearest choice (noun = which
//! transform, suffix = which stream direction).

pub use compcol::io::{DecoderReader, DecoderWriter, EncoderReader, EncoderWriter};
