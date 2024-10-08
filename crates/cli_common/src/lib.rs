use thiserror::Error;

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

#[derive(Clone, PartialEq, Debug, Error)]
#[error("Parse error: {kind:?}")]
pub struct ExecuteError {
    pub kind: ExecuteErrorKind,
    pub position: usize,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ExecuteErrorKind {
    Err,
}
