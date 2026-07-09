//! Twin of `chain.py`: both `DecoderReader` and `EncoderReader` are
//! themselves readable streams, so wrapped readers can be stacked. Four
//! ROT13 layers cancel out (ROT13 is self-inverse), so the output matches
//! the plain-text input regardless of whether a layer decodes or encodes.

use std::fs::File;

use compcol::io::{DecoderReader, EncoderReader};
use rust_twin_v2::{rot13_decoder, rot13_encoder};

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/input-hello.txt"))?;
    let reader1 = EncoderReader::new(raw, rot13_encoder());
    let reader2 = DecoderReader::new(reader1, rot13_decoder());
    let reader3 = EncoderReader::new(reader2, rot13_encoder());
    let mut reader4 = DecoderReader::new(reader3, rot13_decoder());
    std::io::copy(&mut reader4, &mut std::io::stdout().lock())?;
    Ok(())
}
