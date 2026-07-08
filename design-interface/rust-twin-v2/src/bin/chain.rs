//! Twin of `chain.py`: a `DecoderReader` is itself a readable stream, so
//! wrapped readers can be stacked. Four ROT13 readers cancel out (ROT13 is
//! self-inverse), so the output matches the plain-text input.

use std::fs::File;

use compcol::io::DecoderReader;
use compcol::Algorithm;
use rust_twin_v2::Rot13;

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/input-hello.txt"))?;
    let reader1 = DecoderReader::new(raw, Rot13::decoder());
    let reader2 = DecoderReader::new(reader1, Rot13::decoder());
    let reader3 = DecoderReader::new(reader2, Rot13::decoder());
    let mut reader4 = DecoderReader::new(reader3, Rot13::decoder());
    std::io::copy(&mut reader4, &mut std::io::stdout().lock())?;
    Ok(())
}
