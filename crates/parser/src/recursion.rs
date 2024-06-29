use cli_common::ParseErrorKind;

pub struct RecursionGuard {
    remaining: usize,
}

impl RecursionGuard {
    pub fn new(max_depth: usize) -> Self {
        RecursionGuard {
            remaining: max_depth,
        }
    }

    pub fn dec(&mut self) -> Result<(), ParseErrorKind> {
        if self.remaining == 0 {
            return Err(ParseErrorKind::MaximumRecursionDepthReached);
        }

        self.remaining = self.remaining - 1;

        Ok(())
    }
}

impl Drop for RecursionGuard {
    fn drop(&mut self) {
        self.remaining = self.remaining + 1;
    }
}
