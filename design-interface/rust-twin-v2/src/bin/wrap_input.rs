//! Twin of `wrap-input.py`: wrap a byte input stream with a codec to get a
//! decoded input stream. The wrapper is compcol's `DecoderReader`; only the
//! codec is ours.

use std::fs::File;
use std::io;

use compcol::io::DecoderReader;
use rust_twin_v2::rot13_decoder;

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/encoded-hello.txt"))?;
    let mut reader = DecoderReader::new(raw, rot13_decoder());
    io::copy(&mut reader, &mut io::stdout())?;
    Ok(())
}
