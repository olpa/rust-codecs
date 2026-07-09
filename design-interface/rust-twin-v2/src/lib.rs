//! Second Rust twin of the Python `codecs` stream-wrapping reference
//! (`../python-ref`), this time built on the [`compcol`] crate's
//! [`Encoder`](compcol::Encoder) / [`Decoder`](compcol::Decoder) traits
//! instead of the homegrown `Codec` trait of `../rust-twin-v1`.
//!
//! | Python                          | This crate                                |
//! |---------------------------------|-------------------------------------------|
//! | the registered `"my-rot13"` codec | the explicit [`Rot13`] value            |
//! | `codecs.getreader(enc)(stream)` | `compcol::io::DecoderReader::new(stream, rot13_decoder())` |
//! | `codecs.getwriter(enc)(stream)` | `compcol::io::EncoderWriter::new(stream, rot13_encoder())` |
//!
//! The stream wrappers are **not** reimplemented here: implementing
//! compcol's traits buys us its `io` adapters (and `vec` one-shot helpers)
//! for free. This crate only contributes the codec itself.
//!
//! See `README.md` for the discussion of whether the `Encoder`/`Decoder`
//! trait split is actually necessary — `Rot13` implements *both* traits
//! with the same transform, which is half of the answer.

mod rot13;

pub use rot13::{rot13_decoder, rot13_encoder, Rot13};
