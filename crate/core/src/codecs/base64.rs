//! Example [`Codec`](crate::Codec)s: base64 encode/decode, built on the
//! `base64` crate (<https://docs.rs/base64/>).
//!
//! This codec belongs in its own crate eventually. See the crate
//! docs' note on why it lives here for now.
//!
//! The encoder and decoder live in [`base64_enc`] and [`base64_dec`]
//! respectively; [`base64_shared`] holds the constants and buffering
//! helpers both sides need. [`base64_enc()`]/[`base64_dec()`] build the
//! standard base64 alphabet with padding. To use a different alphabet
//! or padding behavior (e.g. URL-safe, or no padding), construct
//! [`Base64Enc::with_engine`]/[`Base64Dec::with_engine`] with any other
//! `base64` crate [`Engine`](base64::engine::Engine).

#[path = "base64_shared.rs"]
mod base64_shared;

#[path = "base64_dec.rs"]
mod base64_dec;
#[path = "base64_enc.rs"]
mod base64_enc;

pub use base64_dec::{base64_dec, Base64Dec};
pub use base64_enc::{base64_enc, Base64Enc};

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use super::{base64_dec, base64_enc, Base64Dec, Base64Enc};
    use crate::sources_and_sinks::vec::encode_string;

    const INPUT: &str = "Hello, World! 123";
    const ENCODED: &str = "SGVsbG8sIFdvcmxkISAxMjM=";

    #[test]
    fn round_trip() {
        let encoded = encode_string(base64_enc(), INPUT).unwrap();
        assert_eq!(encoded, ENCODED);
        let decoded = encode_string(base64_dec(), &encoded).unwrap();
        assert_eq!(decoded, INPUT);
    }

    #[test]
    fn round_trip_with_custom_engine() {
        // URL_SAFE_NO_PAD drops the trailing '=' that STANDARD adds,
        // proving with_engine actually swaps the engine rather than
        // silently falling back to STANDARD.
        let encoded = encode_string(Base64Enc::with_engine(URL_SAFE_NO_PAD), INPUT).unwrap();
        assert_eq!(encoded, ENCODED.strip_suffix('=').unwrap());
        let decoded = encode_string(Base64Dec::with_engine(URL_SAFE_NO_PAD), &encoded).unwrap();
        assert_eq!(decoded, INPUT);
    }
}
