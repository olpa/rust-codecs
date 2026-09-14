//! This file proves that `rust_codecs_core`'s public traits form a
//! usable surface from outside the crate. A third party can implement
//! `Source`, `Sink`, `Codec`, and `BoundaryAwareCodec`. It can also
//! drive them through `Pump` and `shared_io`, using only the public
//! API.
//!
//! This is not a correctness test. It builds real objects and makes
//! one call on each. Each call sits behind `black_box`, so the
//! compiler cannot optimize the construction away. The test checks
//! nothing about the resulting bytes.

use core::convert::Infallible;
use core::hint::black_box;
use core::mem::MaybeUninit;

use rust_codecs_core::sources_and_sinks::shared_io::{
    boundary_aware_pump_read, pump_finish, pump_write,
};
use rust_codecs_core::{
    BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, DriveError, Error,
    Progress, Pump, Sink, Source,
};

const DUMMY: &[u8] = b"dummy";

/// A dummy `Source`. It does nothing but return a fixed slice.
struct DummySource;

impl Source for DummySource {
    type Error = Infallible;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Infallible> {
        Ok(Some(b"dummy"))
    }

    fn consume(&mut self, _amount: usize) {}
}

/// A dummy `Sink`. It does nothing.
struct DummySink;

impl Sink for DummySink {
    type Error = Infallible;

    fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Infallible> {
        Ok(None)
    }

    fn commit(&mut self, _amount: usize) -> Result<(), Infallible> {
        Ok(())
    }
}

/// A dummy `Codec`. It transforms nothing, but it correctly signals
/// the end of its stream through `finish`. It is a plain `Codec`, so
/// the crate's blanket impl also makes it a `BoundaryAwareCodec`. This
/// one type can drive both `DummyReaderWrapper` and
/// `DummyWriterWrapper`.
struct DummyCodec;

impl DrainCodec for DummyCodec {
    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }
}

impl Codec for DummyCodec {
    fn process(
        &mut self,
        _input: &[u8],
        _output: &mut [MaybeUninit<u8>],
    ) -> Result<Progress, Error> {
        Ok(Progress::InputConsumed { written: 0 })
    }
}

/// A dummy `BoundaryAwareCodec`. It signals the logical end of its
/// stream in-band, on its first call, instead of only through
/// `finish`. It cannot also implement `Codec`, since that would
/// conflict with the crate's blanket impl. So only
/// `DummyReaderWrapper` can drive it.
struct DummyBoundaryCodec;

impl DrainCodec for DummyBoundaryCodec {
    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }
}

impl BoundaryAwareCodec for DummyBoundaryCodec {
    fn process(
        &mut self,
        input: &[u8],
        _output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        Ok(BoundaryAwareProgress::Boundary {
            consumed: input.len(),
            written: 0,
        })
    }
}

/// A `Read`-shaped wrapper. It proxies to `Pump` through
/// `shared_io::boundary_aware_pump_read`, the same way
/// `std_io`'s and `embedded_io`'s `CodecReader` do.
struct DummyReaderWrapper<I: Source, C: BoundaryAwareCodec> {
    input: I,
    pump: Pump<C>,
}

impl<I: Source, C: BoundaryAwareCodec> DummyReaderWrapper<I, C> {
    fn new(input: I, codec: C) -> Self {
        Self {
            input,
            pump: Pump::new(codec),
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, DriveError<I::Error, Infallible>> {
        boundary_aware_pump_read(&mut self.pump, &mut self.input, buf)
    }
}

/// A `Write`-shaped wrapper. It proxies to `Pump` through
/// `shared_io`'s `pump_write` and `pump_finish`, the same way
/// `std_io`'s and `embedded_io`'s `CodecWriter` do.
struct DummyWriterWrapper<O: Sink, C: Codec> {
    output: O,
    pump: Pump<C>,
}

impl<O: Sink, C: Codec> DummyWriterWrapper<O, C> {
    fn new(output: O, codec: C) -> Self {
        Self {
            output,
            pump: Pump::new(codec),
        }
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize, DriveError<Infallible, O::Error>> {
        pump_write(&mut self.pump, &mut self.output, buf)
    }

    fn finish(mut self) -> Result<O, DriveError<Infallible, O::Error>> {
        pump_finish(&mut self.pump, &mut self.output)?;
        Ok(self.output)
    }
}

/// A `Source` that feeds `DummyReaderWrapper`.
struct DummyReader {
    pos: usize,
}

impl DummyReader {
    fn new() -> Self {
        Self { pos: 0 }
    }
}

impl Source for DummyReader {
    type Error = Infallible;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Infallible> {
        Ok(if self.pos < DUMMY.len() {
            Some(&DUMMY[self.pos..])
        } else {
            None
        })
    }

    fn consume(&mut self, amount: usize) {
        self.pos += amount;
    }
}

/// A `Sink` that receives `DummyWriterWrapper`'s output.
struct DummyWriter {
    buf: [u8; DUMMY.len()],
    len: usize,
    offered: usize,
}

impl DummyWriter {
    fn new() -> Self {
        Self {
            buf: [0; DUMMY.len()],
            len: 0,
            offered: 0,
        }
    }
}

impl Sink for DummyWriter {
    type Error = Infallible;

    fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Infallible> {
        if self.len == self.buf.len() {
            return Ok(None);
        }
        let spare = &mut self.buf[self.len..];
        self.offered = spare.len();
        // Safe. `MaybeUninit<u8>` has the same layout as `u8`. An
        // already-initialized `u8` is always a valid `MaybeUninit<u8>`.
        Ok(Some(unsafe {
            &mut *(spare as *mut [u8] as *mut [MaybeUninit<u8>])
        }))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Infallible> {
        self.len += amount;
        self.offered = 0;
        Ok(())
    }
}

#[test]
fn public_interfaces_are_instantiatable() {
    let mut source = DummySource;
    black_box(source.chunk().unwrap());
    source.consume(0);

    let mut sink = DummySink;
    black_box(sink.spare().unwrap());
    sink.commit(0).unwrap();

    let mut plain_reader = DummyReaderWrapper::new(DummyReader::new(), DummyCodec);
    let mut out = [0u8; DUMMY.len()];
    black_box(plain_reader.read(&mut out).unwrap());

    let mut boundary_reader = DummyReaderWrapper::new(DummyReader::new(), DummyBoundaryCodec);
    black_box(boundary_reader.read(&mut out).unwrap());

    let mut writer = DummyWriterWrapper::new(DummyWriter::new(), DummyCodec);
    black_box(writer.write(DUMMY).unwrap());
    black_box(writer.finish().unwrap());
}
