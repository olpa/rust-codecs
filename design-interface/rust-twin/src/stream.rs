use std::io::{self, Read, Write};

use crate::Codec;

const CHUNK_SIZE: usize = 8192;

/// Wraps a byte input stream and decodes it on the fly through a codec.
///
/// Twin of `codecs.getreader(enc)(stream)` / `codecs.StreamReader`: it is
/// itself a readable stream ([`std::io::Read`]), so wrapped readers can be
/// stacked, and it pulls from the underlying stream incrementally — one
/// chunk per refill, never the whole stream at once.
pub struct StreamReader<R, C> {
    inner: R,
    codec: C,
    /// Decoded bytes not yet handed to the caller: `decoded[pos..]`.
    decoded: Vec<u8>,
    pos: usize,
    eof: bool,
}

impl<R: Read, C: Codec> StreamReader<R, C> {
    pub fn new(inner: R, codec: C) -> Self {
        StreamReader {
            inner,
            codec,
            decoded: Vec::new(),
            pos: 0,
            eof: false,
        }
    }
}

impl<R: Read, C: Codec> Read for StreamReader<R, C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // The codec may return no output for a given chunk (buffering), so
        // keep refilling until we have decoded bytes or hit finalized EOF.
        while self.pos == self.decoded.len() {
            if self.eof {
                return Ok(0);
            }
            let mut chunk = [0u8; CHUNK_SIZE];
            let n = self.inner.read(&mut chunk)?;
            if n == 0 {
                self.eof = true;
            }
            self.decoded = self.codec.transform(&chunk[..n], self.eof);
            self.pos = 0;
        }
        let available = &self.decoded[self.pos..];
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.pos += n;
        Ok(n)
    }
}

/// Wraps a byte output stream and encodes everything written to it.
///
/// Twin of `codecs.getwriter(enc)(stream)` / `codecs.StreamWriter`: it is
/// itself a writable stream ([`std::io::Write`]) and pushes each write
/// through the codec straight into the underlying stream.
///
/// Call [`finish`](StreamWriter::finish) when done so a buffering codec can
/// flush its tail (Python has no equivalent step because its `StreamWriter`
/// never signals end-of-stream to the codec).
pub struct StreamWriter<W, C> {
    inner: W,
    codec: C,
}

impl<W: Write, C: Codec> StreamWriter<W, C> {
    pub fn new(inner: W, codec: C) -> Self {
        StreamWriter { inner, codec }
    }

    /// Signal end-of-stream to the codec, write its tail, and return the
    /// underlying stream.
    pub fn finish(mut self) -> io::Result<W> {
        let tail = self.codec.transform(&[], true);
        self.inner.write_all(&tail)?;
        self.inner.flush()?;
        Ok(self.inner)
    }
}

impl<W: Write, C: Codec> Write for StreamWriter<W, C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let encoded = self.codec.transform(buf, false);
        self.inner.write_all(&encoded)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Rot13;
    use std::io::Cursor;

    #[test]
    fn reader_decodes_on_the_fly() {
        let raw = Cursor::new(b"Uryyb, jbeyq!\n".to_vec());
        let mut reader = StreamReader::new(raw, Rot13);
        let mut out = String::new();
        reader.read_to_string(&mut out).unwrap();
        assert_eq!(out, "Hello, world!\n");
    }

    #[test]
    fn reader_serves_small_buffers_across_refills() {
        let raw = Cursor::new(b"Uryyb, jbeyq!\n".to_vec());
        let mut reader = StreamReader::new(raw, Rot13);
        let mut out = Vec::new();
        let mut buf = [0u8; 3];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, b"Hello, world!\n");
    }

    #[test]
    fn writer_encodes_on_the_fly() {
        let mut writer = StreamWriter::new(Vec::new(), Rot13);
        writer.write_all(b"Hello, ").unwrap();
        writer.write_all(b"world!\n").unwrap();
        let raw = writer.finish().unwrap();
        assert_eq!(raw, b"Uryyb, jbeyq!\n");
    }

    #[test]
    fn readers_stack_like_python_chain() {
        let raw = Cursor::new(b"Hello, world!\n".to_vec());
        let reader1 = StreamReader::new(raw, Rot13);
        let reader2 = StreamReader::new(reader1, Rot13);
        let reader3 = StreamReader::new(reader2, Rot13);
        let mut reader4 = StreamReader::new(reader3, Rot13);
        let mut out = Vec::new();
        reader4.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"Hello, world!\n");
    }
}
