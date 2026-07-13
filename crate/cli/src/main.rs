//! Test harness for chaining codecs: builds a `CodecReader` chain from
//! `--readers` and a `CodecWriter` chain from `--writers`, then copies
//! stdin through the readers, through the writers, to stdout.
//!
//! ```text
//! echo hello | cargo run -p cli -- --readers identity identity rot13 --writers rot13 rot13 identity
//! ```
//!
//! Reader codecs apply in the order listed (first listed runs on the raw
//! bytes first). Writer codecs also apply in the order listed (first
//! listed runs first, closest to the incoming bytes, before reaching
//! stdout).

use std::io::{self, Read};

use rust_codecs_core::io::{CodecReader, CodecWriter, FinishWrite};
use rust_codecs_core::Codec;

fn make_codec(name: &str) -> Result<Box<dyn Codec>, String> {
    match name {
        "identity" => Ok(Box::new(rust_codecs_core::identity::identity())),
        "rot13" => Ok(Box::new(rust_codecs_core::rot13::rot13())),
        "b64-enc" => Ok(Box::new(rust_codecs_core::base64::b64_enc())),
        "b64-dec" => Ok(Box::new(rust_codecs_core::base64::b64_dec())),
        other => Err(format!(
            "unknown codec {other:?} (expected \"identity\", \"rot13\", \"b64-enc\", or \"b64-dec\")"
        )),
    }
}

enum Mode {
    None,
    Readers,
    Writers,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<(Vec<String>, Vec<String>), String> {
    let mut mode = Mode::None;
    let mut readers = Vec::new();
    let mut writers = Vec::new();
    for arg in args {
        match arg.as_str() {
            "--readers" => mode = Mode::Readers,
            "--writers" => mode = Mode::Writers,
            _ => match mode {
                Mode::None => {
                    return Err(format!(
                        "unexpected argument {arg:?} before --readers/--writers"
                    ))
                }
                Mode::Readers => readers.push(arg),
                Mode::Writers => writers.push(arg),
            },
        }
    }
    Ok((readers, writers))
}

fn run() -> Result<(), String> {
    let (reader_names, writer_names) = parse_args(std::env::args().skip(1))?;

    let mut reader: Box<dyn Read> = Box::new(io::stdin());
    for name in &reader_names {
        let codec = make_codec(name)?;
        reader = Box::new(CodecReader::new(reader, codec));
    }

    let mut writer: Box<dyn FinishWrite> = Box::new(io::stdout());
    for name in writer_names.iter().rev() {
        let codec = make_codec(name)?;
        writer = Box::new(CodecWriter::new(writer, codec));
    }

    io::copy(&mut reader, &mut writer).map_err(|e| e.to_string())?;
    writer.finish_boxed().map_err(|e| e.to_string())?;
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
