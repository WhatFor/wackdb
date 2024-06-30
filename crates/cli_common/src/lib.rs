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
    ExpectedDataType,
    ExpectedParentheses(String),
    ExpressionNotClosed,
    ExpectedKeyword(String),
    MaximumRecursionDepthReached,
    UnsupportedSyntax,
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
            ParseErrorKind::MaximumRecursionDepthReached => {
                write!(f, "Maximum recursion depth reached.")?
            }
            ParseErrorKind::ExpectedParentheses(message) => {
                write!(f, "Expected parentheses. Expected {message}.")?
            }
            ParseErrorKind::ExpectedValue => write!(f, "Value expected.")?,
            ParseErrorKind::ExpectedDataType => write!(f, "Datatype expected.")?,
            ParseErrorKind::ExpectedStatemnt => write!(f, "Statement expected.")?,
            ParseErrorKind::ExpectedIdentifier => write!(f, "Identifier expected.")?,
            ParseErrorKind::ExpressionNotClosed => write!(f, "Expression not closed.")?,
            ParseErrorKind::UnsupportedSyntax => write!(f, "Unsupported syntax.")?,
        }

        Ok(())
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ExecuteError {
    pub kind: ExecuteErrorKind,
    pub position: usize,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ExecuteErrorKind {
    Err,
}

impl fmt::Display for ExecuteErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecuteErrorKind::Err => write!(f, "Error")?,
        }

        Ok(())
    }
}
