use rust_codecs_core::{
    rot13::rot13,
    sources_and_sinks::std_io::CodecWriter,
};
use std::io;

fn main()
    -> Result<(), Box<dyn std::error::Error>> {
    let mut out = CodecWriter::new(
        io::stdout(), rot13(), [0u8; 4096])?;
    io::copy(&mut io::stdin(), &mut out)?;
    out.finish()?;
    Ok(())
}
