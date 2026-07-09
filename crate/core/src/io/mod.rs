//! Ways to run an [`Encoder`](crate::Encoder)/[`Decoder`](crate::Decoder)
//! over bytes you already have.
//!
//! - [`stream`]: wrap a `std::io::Read`/`Write` so the transform runs
//!   on the fly as the wrapped stream is used.
//! - [`vec`]: run the transform once over an in-memory buffer and get a
//!   `Vec<u8>` back.

mod stream;
mod vec;

pub use stream::{DecoderReader, DecoderWriter, EncoderReader, EncoderWriter};
pub use vec::{decode_to_vec, encode_to_vec};
