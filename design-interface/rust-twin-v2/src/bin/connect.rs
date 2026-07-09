//! Twin of `connect.py`: connect an input stream to an output stream through
//! a codec — wrap the input with a decoding reader, the output with an
//! encoding writer, and `std::io::copy` between them (the twin of
//! `shutil.copyfileobj`).

use std::fs::File;

use compcol::io::{DecoderReader, EncoderWriter};
use rust_twin_v2::{rot13_decoder, rot13_encoder};

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/input-hello.txt"))?;
    let mut reader = DecoderReader::new(raw, rot13_decoder());
    let mut writer = EncoderWriter::new(std::io::stdout().lock(), rot13_encoder());
    std::io::copy(&mut reader, &mut writer)?;
    let _stdout = writer.finish()?;
    Ok(())
}
