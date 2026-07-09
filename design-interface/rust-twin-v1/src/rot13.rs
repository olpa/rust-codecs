use crate::Codec;

/// ROT13 over ASCII letters, all other bytes unchanged.
///
/// Twin of `rot13_codec.py`, except there is no registry: instead of
/// registering under `"my-rot13"` and looking it up by name, callers create
/// a `Rot13` value and pass it to a stream wrapper explicitly. ROT13 is
/// self-inverse, so the same codec serves as encoder and decoder, and it is
/// stateless, so `transform` never buffers.
pub struct Rot13;

fn rot13_byte(b: u8) -> u8 {
    match b {
        b'A'..=b'M' | b'a'..=b'm' => b + 13,
        b'N'..=b'Z' | b'n'..=b'z' => b - 13,
        _ => b,
    }
}

impl Codec for Rot13 {
    fn transform(&mut self, input: &[u8], _last: bool) -> Vec<u8> {
        input.iter().copied().map(rot13_byte).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_letters_and_keeps_the_rest() {
        let mut codec = Rot13;
        assert_eq!(codec.transform(b"Hello, world!\n", false), b"Uryyb, jbeyq!\n");
    }

    #[test]
    fn is_self_inverse() {
        let mut codec = Rot13;
        let once = codec.transform(b"Hello, world!\n", false);
        assert_eq!(codec.transform(&once, true), b"Hello, world!\n");
    }
}
