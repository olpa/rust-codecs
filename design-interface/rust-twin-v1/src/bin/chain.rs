//! Twin of `chain.py`: a `StreamReader` is itself a readable stream, so
//! wrapped readers can be stacked. Four ROT13 readers cancel out (ROT13 is
//! self-inverse), so the output matches the plain-text input.

use std::fs::File;

use rust_twin::{Rot13, StreamReader};

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/input-hello.txt"))?;
    let reader1 = StreamReader::new(raw, Rot13);
    let reader2 = StreamReader::new(reader1, Rot13);
    let reader3 = StreamReader::new(reader2, Rot13);
    let mut reader4 = StreamReader::new(reader3, Rot13);
    std::io::copy(&mut reader4, &mut std::io::stdout().lock())?;
    Ok(())
}
