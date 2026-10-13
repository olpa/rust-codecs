use core::fmt;

/// Contains the error and the `CodecWriter` when its `finish` method
/// fails.
///
/// Thus, the caller keeps the wrapped writer. `E` is the error type.
/// `T` is the type of the `CodecWriter`.
pub struct CodecWriterFinishError<E, T> {
    error: E,
    inner: T,
}

impl<E, T> CodecWriterFinishError<E, T> {
    pub fn new(error: E, inner: T) -> Self {
        Self { error, inner }
    }

    pub fn error(&self) -> &E {
        &self.error
    }

    pub fn into_error(self) -> E {
        self.error
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn into_parts(self) -> (E, T) {
        (self.error, self.inner)
    }
}

/// This implementation shows only the error. Thus, the `CodecWriter`
/// type does not have to implement `Debug`.
impl<E: fmt::Debug, T> fmt::Debug for CodecWriterFinishError<E, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodecWriterFinishError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<E: fmt::Display, T> fmt::Display for CodecWriterFinishError<E, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl<E: core::error::Error, T> core::error::Error for CodecWriterFinishError<E, T> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        self.error.source()
    }
}
