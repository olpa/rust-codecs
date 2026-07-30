//! A resumable Sans-I/O driver over caller-provided storage.
//!
//! This module owns codec scheduling but performs no endpoint I/O. An
//! adapter fills [`StreamDriver::input_buffer`], commits either bytes or
//! EOF, drains [`StreamDriver::output`], and calls
//! [`StreamDriver::advance`] until the next external action is needed.

use crate::transfer::{transfer, TransferEnd};
use crate::{Codec, Drain, Error, ErrorKind};

/// The next action at the boundary between a driver and an endpoint
/// adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriverState {
    /// The driver can make progress without endpoint I/O.
    Runnable,
    /// Fill `input_buffer`, then call `commit_input`; committing zero
    /// bytes declares EOF.
    NeedInput,
    /// Deliver bytes from `output`, then call `consume_output`.
    HaveOutput,
    /// A requested codec flush reached its sync point. The adapter may
    /// flush its endpoint before calling `acknowledge_flush`.
    Flushed,
    /// The codec stream ended and all generated output was consumed.
    Finished,
    /// A codec error was already returned and the driver cannot resume.
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Processing,
    Flushing,
    FlushComplete,
    Finishing,
    Done,
    Failed,
}

/// Exact codec-side progress accumulated by the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DriverTotals {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
}

/// A codec stream state machine with stable input and output storage.
pub(crate) struct StreamDriver<C, I, O> {
    codec: C,
    input: I,
    in_pos: usize,
    in_len: usize,
    output: O,
    out_pos: usize,
    out_len: usize,
    phase: Phase,
    failure: Option<Error>,
    totals: DriverTotals,
}

impl<C: Codec, I: AsMut<[u8]>, O: AsMut<[u8]>> StreamDriver<C, I, O> {
    pub(crate) fn new(codec: C, mut input: I, mut output: O) -> Self {
        assert!(
            !input.as_mut().is_empty(),
            "driver input buffer must be non-empty"
        );
        assert!(
            !output.as_mut().is_empty(),
            "driver output buffer must be non-empty"
        );
        Self {
            codec,
            input,
            in_pos: 0,
            in_len: 0,
            output,
            out_pos: 0,
            out_len: 0,
            phase: Phase::Processing,
            failure: None,
            totals: DriverTotals {
                consumed: 0,
                written: 0,
            },
        }
    }

    pub(crate) fn state(&self) -> DriverState {
        if self.out_pos < self.out_len {
            return DriverState::HaveOutput;
        }
        match self.phase {
            Phase::Processing if self.in_pos == self.in_len => DriverState::NeedInput,
            Phase::Processing | Phase::Flushing | Phase::Finishing => DriverState::Runnable,
            Phase::FlushComplete => DriverState::Flushed,
            Phase::Done => DriverState::Finished,
            Phase::Failed if self.failure.is_some() => DriverState::Runnable,
            Phase::Failed => DriverState::Failed,
        }
    }

    pub(crate) fn input_buffer(&mut self) -> &mut [u8] {
        assert_eq!(
            self.state(),
            DriverState::NeedInput,
            "driver does not need input"
        );
        self.input.as_mut()
    }

    /// Commit bytes placed in `input_buffer`; zero bytes declares EOF.
    pub(crate) fn commit_input(&mut self, len: usize) {
        assert_eq!(
            self.state(),
            DriverState::NeedInput,
            "driver does not need input"
        );
        assert!(
            len <= self.input.as_mut().len(),
            "committed input exceeds buffer"
        );
        self.in_pos = 0;
        self.in_len = len;
        if len == 0 {
            self.phase = Phase::Finishing;
        }
    }

    pub(crate) fn output(&mut self) -> &[u8] {
        assert_eq!(
            self.state(),
            DriverState::HaveOutput,
            "driver has no output"
        );
        &self.output.as_mut()[self.out_pos..self.out_len]
    }

    pub(crate) fn consume_output(&mut self, len: usize) {
        assert_eq!(
            self.state(),
            DriverState::HaveOutput,
            "driver has no output"
        );
        assert!(
            len <= self.out_len - self.out_pos,
            "consumed output exceeds pending bytes"
        );
        self.out_pos += len;
        if self.out_pos == self.out_len {
            self.out_pos = 0;
            self.out_len = 0;
        }
    }

    /// Begin a resumable codec flush. It is legal only at an input
    /// boundary, after all previously generated output was consumed.
    pub(crate) fn request_flush(&mut self) {
        assert_eq!(
            self.state(),
            DriverState::NeedInput,
            "driver is not at an input boundary"
        );
        self.phase = Phase::Flushing;
    }

    pub(crate) fn acknowledge_flush(&mut self) {
        assert_eq!(
            self.state(),
            DriverState::Flushed,
            "codec flush is not complete"
        );
        self.phase = Phase::Processing;
    }

    pub(crate) fn totals(&self) -> DriverTotals {
        self.totals
    }

    /// Input already obtained from an endpoint but left past an
    /// in-band stream end or codec error.
    pub(crate) fn unconsumed_input(&mut self) -> &[u8] {
        &self.input.as_mut()[self.in_pos..self.in_len]
    }

    /// Run until endpoint action, flush completion, stream completion,
    /// or a codec error is reached.
    pub(crate) fn advance(&mut self) -> Result<DriverState, Error> {
        loop {
            match self.state() {
                DriverState::Runnable => {}
                external => return Ok(external),
            }

            match self.phase {
                Phase::Processing => self.process_once()?,
                Phase::Flushing => self.drain_once(false)?,
                Phase::Finishing => self.drain_once(true)?,
                Phase::Failed => {
                    let error = self
                        .failure
                        .take()
                        .expect("runnable failed driver has an error");
                    return Err(error);
                }
                Phase::FlushComplete | Phase::Done => unreachable!("external state returned above"),
            }
        }
    }

    fn process_once(&mut self) -> Result<(), Error> {
        let input_remaining = self.in_len - self.in_pos;
        let output_capacity = self.output.as_mut().len();
        let result = {
            let input = &self.input.as_mut()[self.in_pos..self.in_len];
            let output = self.output.as_mut();
            transfer(&mut self.codec, input, output)
        };
        match result {
            Ok(moved) => {
                self.in_pos += moved.consumed;
                self.out_len = moved.written;
                self.totals.consumed += moved.consumed;
                self.totals.written += moved.written;
                if moved.end == TransferEnd::StreamEnd {
                    self.phase = Phase::Done;
                }
                Ok(())
            }
            Err(error) => self.latch_error(error, input_remaining, output_capacity),
        }
    }

    fn drain_once(&mut self, finishing: bool) -> Result<(), Error> {
        let output_capacity = self.output.as_mut().len();
        let result = {
            let output = self.output.as_mut();
            if finishing {
                self.codec.finish(output)
            } else {
                self.codec.flush(output)
            }
        };
        match result.and_then(|drain| drain.validated(output_capacity)) {
            Ok(Drain::OutputFilled) => {
                self.out_len = output_capacity;
                self.totals.written += output_capacity;
                Ok(())
            }
            Ok(Drain::Done { written }) => {
                self.out_len = written;
                self.totals.written += written;
                self.phase = if finishing {
                    Phase::Done
                } else {
                    Phase::FlushComplete
                };
                Ok(())
            }
            Err(error) => self.latch_error(error, 0, output_capacity),
        }
    }

    fn latch_error(
        &mut self,
        mut error: Error,
        input_remaining: usize,
        output_capacity: usize,
    ) -> Result<(), Error> {
        if error.consumed > input_remaining || error.written > output_capacity {
            error = Error::new(ErrorKind::ContractViolation, 0, 0);
        }
        self.in_pos += error.consumed;
        self.out_len = error.written;
        self.totals.consumed += error.consumed;
        self.totals.written += error.written;
        self.phase = Phase::Failed;
        self.failure = Some(error);
        if self.out_len == 0 {
            self.failure = None;
            Err(error)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DriverState, DriverTotals, StreamDriver};
    use crate::{Codec, Drain, Error, ErrorKind, Outcome};

    struct Identity;

    impl Codec for Identity {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            let n = input.len().min(output.len());
            output[..n].copy_from_slice(&input[..n]);
            if n == input.len() {
                Ok(Outcome::InputConsumed { written: n })
            } else {
                Ok(Outcome::OutputFilled { consumed: n })
            }
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    fn submit<C: Codec, I: AsMut<[u8]>, O: AsMut<[u8]>>(
        driver: &mut StreamDriver<C, I, O>,
        bytes: &[u8],
    ) {
        driver.input_buffer()[..bytes.len()].copy_from_slice(bytes);
        driver.commit_input(bytes.len());
    }

    #[test]
    fn input_and_output_suspend_independently() {
        let mut driver = StreamDriver::new(Identity, [0; 4], [0; 3]);
        assert_eq!(driver.advance(), Ok(DriverState::NeedInput));
        submit(&mut driver, b"abcd");

        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"abc");
        driver.consume_output(1);
        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"bc");
        driver.consume_output(2);

        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"d");
        driver.consume_output(1);
        assert_eq!(driver.advance(), Ok(DriverState::NeedInput));
        assert_eq!(
            driver.totals(),
            DriverTotals {
                consumed: 4,
                written: 4
            }
        );
    }

    struct Trailer {
        remaining: usize,
    }

    impl Codec for Trailer {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            Ok(Outcome::InputConsumed { written: 0 })
        }

        fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            let n = self.remaining.min(output.len());
            output[..n].fill(b'!');
            self.remaining -= n;
            if self.remaining == 0 {
                Ok(Drain::Done { written: n })
            } else {
                Ok(Drain::OutputFilled)
            }
        }
    }

    #[test]
    fn eof_finishes_across_output_buffers() {
        let mut driver = StreamDriver::new(Trailer { remaining: 5 }, [0; 2], [0; 2]);
        driver.commit_input(0);
        for expected in [b"!!".as_slice(), b"!!", b"!"] {
            assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
            assert_eq!(driver.output(), expected);
            driver.consume_output(expected.len());
        }
        assert_eq!(driver.advance(), Ok(DriverState::Finished));
    }

    struct Hoarder(Vec<u8>);

    impl Codec for Hoarder {
        fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            self.0.extend_from_slice(input);
            Ok(Outcome::InputConsumed { written: 0 })
        }

        fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            let n = self.0.len().min(output.len());
            output[..n].copy_from_slice(&self.0[..n]);
            self.0.drain(..n);
            if self.0.is_empty() {
                Ok(Drain::Done { written: n })
            } else {
                Ok(Drain::OutputFilled)
            }
        }
    }

    #[test]
    fn a_zero_output_process_turn_requests_more_input() {
        let mut driver = StreamDriver::new(Hoarder(Vec::new()), [0; 3], [0; 2]);
        submit(&mut driver, b"abc");
        assert_eq!(driver.advance(), Ok(DriverState::NeedInput));
        driver.commit_input(0);
        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"ab");
        driver.consume_output(2);
        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"c");
    }

    struct EndsAfter(usize);

    impl Codec for EndsAfter {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            let n = self.0.min(input.len()).min(output.len());
            output[..n].copy_from_slice(&input[..n]);
            Ok(Outcome::StreamEnd {
                consumed: n,
                written: n,
            })
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            unreachable!()
        }
    }

    #[test]
    fn in_band_end_preserves_unconsumed_buffered_input() {
        let mut driver = StreamDriver::new(EndsAfter(2), [0; 5], [0; 5]);
        submit(&mut driver, b"abcde");
        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"ab");
        driver.consume_output(2);
        assert_eq!(driver.advance(), Ok(DriverState::Finished));
        assert_eq!(driver.unconsumed_input(), b"cde");
    }

    struct Flushes {
        remaining: usize,
    }

    impl Codec for Flushes {
        fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            Ok(Outcome::InputConsumed {
                written: input.len(),
            })
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }

        fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            let n = self.remaining.min(output.len());
            output[..n].fill(b'F');
            self.remaining -= n;
            if self.remaining == 0 {
                Ok(Drain::Done { written: n })
            } else {
                Ok(Drain::OutputFilled)
            }
        }
    }

    #[test]
    fn flush_is_resumable_and_acknowledged_explicitly() {
        let mut driver = StreamDriver::new(Flushes { remaining: 3 }, [0; 2], [0; 2]);
        driver.request_flush();
        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"FF");
        driver.consume_output(2);
        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"F");
        driver.consume_output(1);
        assert_eq!(driver.advance(), Ok(DriverState::Flushed));
        driver.acknowledge_flush();
        assert_eq!(driver.advance(), Ok(DriverState::NeedInput));
    }

    struct FailsAfterProgress;

    impl Codec for FailsAfterProgress {
        fn process(&mut self, _input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            output[..2].copy_from_slice(b"ok");
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            unreachable!()
        }
    }

    #[test]
    fn pending_output_precedes_a_latched_error() {
        let mut driver = StreamDriver::new(FailsAfterProgress, [0; 3], [0; 3]);
        submit(&mut driver, b"abc");
        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"ok");
        driver.consume_output(2);
        assert_eq!(driver.advance(), Err(Error::new(ErrorKind::Corrupt, 1, 2)));
        assert_eq!(driver.state(), DriverState::Failed);
        assert_eq!(driver.unconsumed_input(), b"bc");
    }

    struct OverclaimsError;

    impl Codec for OverclaimsError {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            Err(Error::new(
                ErrorKind::Corrupt,
                input.len() + 1,
                output.len() + 1,
            ))
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            unreachable!()
        }
    }

    #[test]
    fn overclaimed_error_progress_is_a_contract_violation() {
        let mut driver = StreamDriver::new(OverclaimsError, [0; 1], [0; 1]);
        submit(&mut driver, b"x");
        assert_eq!(
            driver.advance(),
            Err(Error::new(ErrorKind::ContractViolation, 0, 0))
        );
        assert_eq!(driver.state(), DriverState::Failed);
    }

    #[test]
    fn one_byte_storage_is_sufficient() {
        let mut driver = StreamDriver::new(Identity, [0; 1], [0; 1]);
        submit(&mut driver, b"x");
        assert_eq!(driver.advance(), Ok(DriverState::HaveOutput));
        assert_eq!(driver.output(), b"x");
        driver.consume_output(1);
        driver.commit_input(0);
        assert_eq!(driver.advance(), Ok(DriverState::Finished));
    }
}
