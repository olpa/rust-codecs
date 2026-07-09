//! Ways to run a [`rust_codecs_base::Encoder`]/[`rust_codecs_base::Decoder`]
//! over bytes you already have.
//!
//! - [`stream`]: wrap a `std::io::Read`/`Write` so the transform runs
//!   on the fly as the wrapped stream is used.
//! - [`vec`]: run the transform once over an in-memory buffer and get a
//!   `Vec<u8>` back.

pub mod stream;
pub mod vec;

pub use stream::{DecoderReader, DecoderWriter, EncoderReader, EncoderWriter};
pub use vec::{decode_to_vec, encode_to_vec};
