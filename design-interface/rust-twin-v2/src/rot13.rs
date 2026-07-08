use compcol::{Algorithm, Decoder, Encoder, Error, Progress, Status};

/// ROT13 over ASCII letters, all other bytes unchanged.
///
/// One zero-sized type plays every role compcol distinguishes:
///
/// - it implements [`Encoder`] and [`Decoder`] with the *same* transform
///   (ROT13 is self-inverse and stateless — no history, no tail, no
///   in-band end marker);
/// - it implements [`Algorithm`], with itself as both associated codec
///   type, so the compcol-idiomatic constructors `Rot13::encoder()` /
///   `Rot13::decoder()` work and the `vec` one-shot helpers accept it.
///
/// Because both trait impls exist on the same type, a direct call like
/// `codec.finish(&mut buf)` is ambiguous and needs
/// `Encoder::finish(&mut codec, ..)` — see the README for why that is
/// evidence in the "do we need two traits?" question.
#[derive(Debug, Clone, Copy, Default)]
pub struct Rot13;

fn rot13_byte(b: u8) -> u8 {
    match b {
        b'A'..=b'M' | b'a'..=b'm' => b + 13,
        b'N'..=b'Z' | b'n'..=b'z' => b - 13,
        _ => b,
    }
}

/// The shared byte-for-byte transform behind both trait impls.
///
/// ROT13 maps one input byte to one output byte with no carried state, so
/// a call processes `min(input, output)` bytes and the status is fully
/// determined by which buffer ran out first.
fn transcribe(input: &[u8], output: &mut [u8]) -> (Progress, Status) {
    let n = input.len().min(output.len());
    for (out, &inp) in output[..n].iter_mut().zip(&input[..n]) {
        *out = rot13_byte(inp);
    }
    let status = if n == input.len() {
        Status::InputEmpty
    } else {
        Status::OutputFull
    };
    (
        Progress {
            consumed: n,
            written: n,
        },
        status,
    )
}

impl Encoder for Rot13 {
    fn encode(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok(transcribe(input, output))
    }

    /// No tail: a ROT13 stream ends wherever the input ends.
    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }

    fn reset(&mut self) {}

    // `flush` keeps the default no-op: ROT13 never buffers output, so
    // every byte is already at a sync boundary.
}

impl Decoder for Rot13 {
    fn decode(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok(transcribe(input, output))
    }

    /// No trailer to verify: any point in the stream is a valid end, so
    /// `decode` never reports [`Status::StreamEnd`] and `finish` always
    /// does, immediately.
    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }

    fn reset(&mut self) {}

    /// One input byte is one output byte, so skipping `n` decoded bytes
    /// is just consuming `n` input bytes — no scratch buffer, no
    /// transform. This is the "accelerated skip" case the trait method
    /// exists for.
    fn discard_output(&mut self, input: &[u8], n: usize) -> Result<(Progress, Status), Error> {
        let k = input.len().min(n);
        let status = if k == n {
            // Asked-for amount discarded; the caller can move on. This is
            // the status compcol's own default-impl bridge reports here.
            Status::OutputFull
        } else {
            Status::InputEmpty
        };
        Ok((
            Progress {
                consumed: k,
                written: k,
            },
            status,
        ))
    }
}

impl Algorithm for Rot13 {
    const NAME: &'static str = "rot13";

    type Encoder = Rot13;
    type Decoder = Rot13;
    type EncoderConfig = ();
    type DecoderConfig = ();

    fn encoder_with(_config: ()) -> Rot13 {
        Rot13
    }

    fn decoder_with(_config: ()) -> Rot13 {
        Rot13
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compcol::io::{DecoderReader, EncoderWriter};
    use compcol::vec::{compress_to_vec, decompress_to_vec};
    use std::io::{Cursor, Read, Write};

    #[test]
    fn one_shot_roundtrip_via_algorithm_helpers() {
        let encoded = compress_to_vec::<Rot13>(b"Hello, world!\n").unwrap();
        assert_eq!(encoded, b"Uryyb, jbeyq!\n");
        let decoded = decompress_to_vec::<Rot13>(&encoded).unwrap();
        assert_eq!(decoded, b"Hello, world!\n");
    }

    #[test]
    fn encode_reports_output_full_and_resumes() {
        let mut enc = Rot13::encoder();
        let input = b"Hello, world!\n";
        let mut out = [0u8; 5];

        let (p, status) = enc.encode(input, &mut out).unwrap();
        assert_eq!((p.consumed, p.written), (5, 5));
        assert_eq!(status, Status::OutputFull);
        assert_eq!(&out, b"Uryyb");

        let mut rest = [0u8; 32];
        let (p, status) = enc.encode(&input[5..], &mut rest).unwrap();
        assert_eq!((p.consumed, p.written), (9, 9));
        assert_eq!(status, Status::InputEmpty);
        assert_eq!(&rest[..9], b", jbeyq!\n");

        let (p, status) = Encoder::finish(&mut enc, &mut rest).unwrap();
        assert_eq!((p.consumed, p.written), (0, 0));
        assert_eq!(status, Status::StreamEnd);
    }

    #[test]
    fn discard_output_skips_without_decoding() {
        let mut dec = Rot13::decoder();
        let encoded = b"Uryyb, jbeyq!\n";

        // Skip the "Uryyb, " prefix (7 decoded bytes)...
        let (p, status) = dec.discard_output(encoded, 7).unwrap();
        assert_eq!((p.consumed, p.written), (7, 7));
        assert_eq!(status, Status::OutputFull);

        // ...then decode the rest normally.
        let mut out = [0u8; 32];
        let (p, _) = dec.decode(&encoded[p.consumed..], &mut out).unwrap();
        assert_eq!(&out[..p.written], b"world!\n");
    }

    #[test]
    fn reader_decodes_on_the_fly() {
        let raw = Cursor::new(b"Uryyb, jbeyq!\n".to_vec());
        let mut reader = DecoderReader::new(raw, Rot13::decoder());
        let mut out = String::new();
        reader.read_to_string(&mut out).unwrap();
        assert_eq!(out, "Hello, world!\n");
    }

    #[test]
    fn writer_encodes_on_the_fly() {
        let mut writer = EncoderWriter::new(Vec::new(), Rot13::encoder());
        writer.write_all(b"Hello, ").unwrap();
        writer.write_all(b"world!\n").unwrap();
        let raw = writer.finish().unwrap();
        assert_eq!(raw, b"Uryyb, jbeyq!\n");
    }

    #[test]
    fn readers_stack_like_python_chain() {
        let raw = Cursor::new(b"Hello, world!\n".to_vec());
        let reader1 = DecoderReader::new(raw, Rot13::decoder());
        let reader2 = DecoderReader::new(reader1, Rot13::decoder());
        let reader3 = DecoderReader::new(reader2, Rot13::decoder());
        let mut reader4 = DecoderReader::new(reader3, Rot13::decoder());
        let mut out = Vec::new();
        reader4.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"Hello, world!\n");
    }
}
