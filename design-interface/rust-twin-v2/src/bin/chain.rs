//! Twin of `chain.py`: a `DecoderReader` is itself a readable stream, so
//! wrapped readers can be stacked. `Rot13` implements `Encoder` and
//! `Decoder` with the identical transform, so `rot13_encoder()` and
//! `rot13_decoder()` are interchangeable here — the layers below mix both
//! constructors. Four ROT13 layers cancel out (ROT13 is self-inverse), so
//! the output matches the plain-text input regardless of which constructor
//! built each layer.

use std::fs::File;

use compcol::io::DecoderReader;
use rust_twin_v2::{rot13_decoder, rot13_encoder};

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/input-hello.txt"))?;
    let reader1 = DecoderReader::new(raw, rot13_encoder());
    let reader2 = DecoderReader::new(reader1, rot13_decoder());
    let reader3 = DecoderReader::new(reader2, rot13_encoder());
    let mut reader4 = DecoderReader::new(reader3, rot13_decoder());
    std::io::copy(&mut reader4, &mut std::io::stdout().lock())?;
    Ok(())
}
