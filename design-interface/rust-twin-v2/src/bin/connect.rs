//! Twin of `connect.py`: connect an input stream to an output stream through
//! a codec — wrap the input with a decoding reader and `std::io::copy` it
//! into the output (the twin of `shutil.copyfileobj`).

use std::fs::File;

use compcol::io::DecoderReader;
use rust_twin_v2::rot13_decoder;

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/encoded-hello.txt"))?;
    let mut reader = DecoderReader::new(raw, rot13_decoder());
    std::io::copy(&mut reader, &mut std::io::stdout().lock())?;
    Ok(())
}
