//! Twin of `wrap-output.py`: wrap a byte output stream (stdout) with a codec
//! to get an encoding output stream, via compcol's `EncoderWriter`.

use std::io::Write;

use compcol::io::EncoderWriter;
use compcol::Algorithm;
use rust_twin_v2::Rot13;

fn main() -> std::io::Result<()> {
    let plain = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/input-hello.txt"))?;
    let mut writer = EncoderWriter::new(std::io::stdout().lock(), Rot13::encoder());
    writer.write_all(&plain)?;
    let _stdout = writer.finish()?;
    Ok(())
}
