//! Twin of `connect.py`: connect an input stream to an output stream through
//! a codec — wrap the input with a decoding reader and copy it into the
//! output (`std::io::copy` is the twin of `shutil.copyfileobj`).

use std::fs::File;

use rust_twin::{Rot13, StreamReader};

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/encoded-hello.txt"))?;
    let mut reader = StreamReader::new(raw, Rot13);
    std::io::copy(&mut reader, &mut std::io::stdout().lock())?;
    Ok(())
}
