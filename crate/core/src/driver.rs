//! Bufferless codec lifecycle shared by directional frontends.
//!
//! Endpoint adapters retain the buffers dictated by their direction:
//! readers own input storage, writers own output storage, and chunked
//! frontends lend both current windows. This driver owns only codec
//! lifecycle, so using it never introduces a byte copy.

use crate::transfer::{transfer, Step};
use crate::{Codec, Drain, DriveError, Error, Sink, Source};

/// Exact progress and boundary of one validated `finish` or `flush`
/// call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainStep {
    pub(crate) written: usize,
    pub(crate) end: DrainEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainEnd {
    SinkExhausted,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpTransfer {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: PumpEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpEnd {
    SourceExhausted,
    SinkExhausted,
    StreamEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpDrain {
    pub(crate) written: usize,
    pub(crate) end: DrainEnd,
}

/// A bufferless lifecycle wrapper around a codec.
pub(crate) struct Driver<C> {
    codec: C,
    done: bool,
}

impl<C: Codec> Driver<C> {
    pub(crate) fn new(codec: C) -> Self {
        Self { codec, done: false }
    }

    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    pub(crate) fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Step, Error> {
        if self.done {
            return Ok(Step {
                consumed: 0,
                written: 0,
                end: crate::transfer::StepEnd::StreamEnd,
            });
        }
        let moved = transfer(&mut self.codec, input, output)?;
        if moved.end == crate::transfer::StepEnd::StreamEnd {
            self.done = true;
        }
        Ok(moved)
    }

    pub(crate) fn transfer_from<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        let mut consumed = 0;
        let mut written = 0;
        let mut output_first = false;
        loop {
            let (moved, offered) = if output_first {
                let Some(spare) = output.spare().map_err(DriveError::Sink)? else {
                    return Ok(PumpTransfer { consumed, written, end: PumpEnd::SinkExhausted });
                };
                if spare.is_empty() {
                    return Err(DriveError::EmptySlot);
                }
                let offered = spare.len();
                let Some(chunk) = input.chunk().map_err(DriveError::Source)? else {
                    output.commit(0).map_err(DriveError::Sink)?;
                    return Ok(PumpTransfer { consumed, written, end: PumpEnd::SourceExhausted });
                };
                let moved = match self.process(chunk, spare) {
                    Ok(moved) => moved,
                    Err(error) => {
                        output.commit(0).map_err(DriveError::Sink)?;
                        return Err(DriveError::Codec(error));
                    }
                };
                (moved, offered)
            } else {
                let Some(chunk) = input.chunk().map_err(DriveError::Source)? else {
                    return Ok(PumpTransfer { consumed, written, end: PumpEnd::SourceExhausted });
                };
                let Some(spare) = output.spare().map_err(DriveError::Sink)? else {
                    return Ok(PumpTransfer { consumed, written, end: PumpEnd::SinkExhausted });
                };
                if spare.is_empty() {
                    return Err(DriveError::EmptySlot);
                }
                let offered = spare.len();
                let moved = match self.process(chunk, spare) {
                    Ok(moved) => moved,
                    Err(error) => {
                        output.commit(0).map_err(DriveError::Sink)?;
                        return Err(DriveError::Codec(error));
                    }
                };
                (moved, offered)
            };
            input.consume(moved.consumed);
            output.commit(moved.written).map_err(DriveError::Sink)?;
            consumed += moved.consumed;
            written += moved.written;
            if moved.end == crate::transfer::StepEnd::StreamEnd {
                return Ok(PumpTransfer { consumed, written, end: PumpEnd::StreamEnd });
            }
            output_first = moved.written == offered;
        }
    }

    pub(crate) fn finish_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, true)
    }

    #[cfg_attr(not(any(feature = "std", feature = "embedded-io")), allow(dead_code))]
    pub(crate) fn flush_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, false)
    }

    fn drain_to<O: Sink>(
        &mut self,
        output: &mut O,
        finishing: bool,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        let mut written = 0;
        loop {
            let moved = match output.spare().map_err(DriveError::Sink)? {
                Some(spare) => {
                    if spare.is_empty() {
                        return Err(DriveError::EmptySlot);
                    }
                    let result = if finishing { self.finish(spare) } else { self.flush(spare) };
                    match result {
                        Ok(moved) => Ok(moved),
                        Err(error) => {
                            output.commit(0).map_err(DriveError::Sink)?;
                            return Err(DriveError::Codec(error));
                        }
                    }
                }
                None => {
                    let moved = if finishing { self.finish(&mut []) } else { self.flush(&mut []) }
                        .map_err(DriveError::Codec)?;
                    return Ok(PumpDrain { written, end: if moved.end == DrainEnd::Done {
                        DrainEnd::Done
                    } else {
                        DrainEnd::SinkExhausted
                    }});
                }
            }
            .map_err(DriveError::Codec)?;
            output.commit(moved.written).map_err(DriveError::Sink)?;
            written += moved.written;
            if moved.end == DrainEnd::Done {
                return Ok(PumpDrain { written, end: DrainEnd::Done });
            }
        }
    }

    pub(crate) fn finish(&mut self, output: &mut [u8]) -> Result<DrainStep, Error> {
        if self.done {
            return Ok(DrainStep {
                written: 0,
                end: DrainEnd::Done,
            });
        }
        let output_len = output.len();
        let moved = normalize_drain(self.codec.finish(output), output_len)?;
        if moved.end == DrainEnd::Done {
            self.done = true;
        }
        Ok(moved)
    }

    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) fn flush(&mut self, output: &mut [u8]) -> Result<DrainStep, Error> {
        if self.done {
            return Ok(DrainStep {
                written: 0,
                end: DrainEnd::Done,
            });
        }
        let output_len = output.len();
        normalize_drain(self.codec.flush(output), output_len)
    }
}

fn normalize_drain(
    result: Result<Drain, Error>,
    output_len: usize,
) -> Result<DrainStep, Error> {
    Ok(match result?.validated(output_len)? {
        Drain::OutputFilled => DrainStep {
            written: output_len,
            end: DrainEnd::SinkExhausted,
        },
        Drain::Done { written } => DrainStep {
            written,
            end: DrainEnd::Done,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{DrainEnd, Driver};
    use crate::transfer::StepEnd;
    use crate::{Codec, Drain, Error, ErrorKind, Outcome};

    struct Scripted {
        process: Outcome,
        drain: Drain,
    }

    impl Codec for Scripted {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            Ok(self.process)
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(self.drain)
        }

        fn flush(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(self.drain)
        }
    }

    #[test]
    fn process_uses_the_shared_transfer_boundary() {
        let mut driver = Driver::new(Scripted {
            process: Outcome::OutputFilled { consumed: 2 },
            drain: Drain::Done { written: 0 },
        });
        let moved = driver.process(b"abc", &mut [0; 4]).unwrap();
        assert_eq!(moved.consumed, 2);
        assert_eq!(moved.written, 4);
        assert_eq!(moved.end, StepEnd::OutputExhausted);
    }

    #[test]
    fn in_band_end_latches_completion() {
        let mut driver = Driver::new(Scripted {
            process: Outcome::StreamEnd {
                consumed: 1,
                written: 2,
            },
            drain: Drain::OutputFilled,
        });
        driver.process(b"abc", &mut [0; 4]).unwrap();
        assert!(driver.is_done());
        assert_eq!(driver.finish(&mut []).unwrap().end, DrainEnd::Done);
        let repeated = driver.process(b"trailing", &mut [0; 4]).unwrap();
        assert_eq!(repeated.consumed, 0);
        assert_eq!(repeated.written, 0);
        assert_eq!(repeated.end, StepEnd::StreamEnd);
    }

    #[test]
    fn finish_normalizes_output_progress_and_latches_done() {
        let mut filled = Driver::new(Scripted {
            process: Outcome::InputConsumed { written: 0 },
            drain: Drain::OutputFilled,
        });
        let moved = filled.finish(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 3);
        assert_eq!(moved.end, DrainEnd::SinkExhausted);
        assert!(!filled.is_done());

        let mut done = Driver::new(Scripted {
            process: Outcome::InputConsumed { written: 0 },
            drain: Drain::Done { written: 2 },
        });
        let moved = done.finish(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, DrainEnd::Done);
        assert!(done.is_done());
    }

    #[test]
    fn flush_does_not_end_the_stream() {
        let mut driver = Driver::new(Scripted {
            process: Outcome::InputConsumed { written: 0 },
            drain: Drain::Done { written: 2 },
        });
        let moved = driver.flush(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, DrainEnd::Done);
        assert!(!driver.is_done());
    }

    #[test]
    fn drain_overclaims_are_contract_violations() {
        let mut driver = Driver::new(Scripted {
            process: Outcome::InputConsumed { written: 0 },
            drain: Drain::Done { written: 4 },
        });
        assert_eq!(
            driver.finish(&mut [0; 3]),
            Err(Error::new(ErrorKind::ContractViolation, 0, 0))
        );
    }
}
