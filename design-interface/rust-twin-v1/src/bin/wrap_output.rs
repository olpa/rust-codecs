//! Twin of `wrap-output.py`: wrap a byte output stream (stdout) with a codec
//! to get an encoding output stream.

use std::io::Write;

use rust_twin::{Rot13, StreamWriter};

fn main() -> std::io::Result<()> {
    let plain = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/input-hello.txt"))?;
    let mut writer = StreamWriter::new(std::io::stdout().lock(), Rot13);
    writer.write_all(&plain)?;
    let _stdout = writer.finish()?;
    Ok(())
}
