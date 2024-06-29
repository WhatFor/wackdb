use core::fmt;

#[derive(Clone, PartialEq, Debug)]
pub struct ParseError {
    pub kind: ParseErrorKind,
    pub position: usize,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ParseErrorKind {
    ExpectedEOF,
    ExpectedValue,
    ExpectedStatemnt,
    ExpectedIdentifier,
    ExpressionNotClosed,
    ExpectedKeyword(String),
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseErrorKind::ExpectedEOF => {
                write!(f, "End of file not found. Query may be corrupted.")?
            }
            ParseErrorKind::ExpectedKeyword(keyword) => {
                write!(f, "Expected keyword. Expected {keyword}.")?
            }
            ParseErrorKind::ExpectedValue => write!(f, "Value expected.")?,
            ParseErrorKind::ExpectedStatemnt => write!(f, "Statement expected.")?,
            ParseErrorKind::ExpectedIdentifier => write!(f, "Identifier expected.")?,
            ParseErrorKind::ExpressionNotClosed => write!(f, "Expression not closed.")?,
        }

        Ok(())
    }
}
