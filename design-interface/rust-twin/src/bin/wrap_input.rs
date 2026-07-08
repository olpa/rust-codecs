//! Twin of `wrap-input.py`: wrap a byte input stream with a codec to get a
//! decoded input stream.

use std::fs::File;
use std::io::Read;

use rust_twin::{Rot13, StreamReader};

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/encoded-hello.txt"))?;
    let mut reader = StreamReader::new(raw, Rot13);
    let mut decoded = String::new();
    reader.read_to_string(&mut decoded)?;
    print!("{decoded}");
    Ok(())
}
