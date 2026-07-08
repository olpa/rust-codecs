//! Twin of `wrap-input.py`: wrap a byte input stream with a codec to get a
//! decoded input stream. The wrapper is compcol's `DecoderReader`; only the
//! codec is ours.

use std::fs::File;
use std::io::Read;

use compcol::io::DecoderReader;
use compcol::Algorithm;
use rust_twin_v2::Rot13;

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/encoded-hello.txt"))?;
    let mut reader = DecoderReader::new(raw, Rot13::decoder());
    let mut decoded = String::new();
    reader.read_to_string(&mut decoded)?;
    print!("{decoded}");
    Ok(())
}
