use std::fmt::Display;

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
    ExpectedQualifier(String),
    ExpectedDataType,
    ExpectedParentheses(String),
    ExpressionNotClosed,
    ExpectedKeyword(String),
    MaximumRecursionDepthReached,
    UnsupportedSyntax,
}

#[derive(Debug)]
pub struct ExecuteResult {
    pub results: Vec<StatementResult>,
    pub errors: Vec<anyhow::Error>,
}

#[derive(Default, Debug, PartialEq, Clone)]
pub struct StatementResult {
    pub result_set: ResultSet,
}

#[derive(Default, Debug, PartialEq, Clone)]
pub struct ResultSet {
    pub columns: Vec<ColumnResult>,
    pub rows: Vec<Vec<ExprResult>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnResult {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExprResult {
    Short(u16),
    Int(i32),
    Long(i64),
    Byte(u8),
    Bool(bool),
    String(String),
    Null,
}

impl Display for ExprResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExprResult::Long(x) => write!(f, "{}", x),
            ExprResult::Short(x) => write!(f, "{}", x),
            ExprResult::Int(x) => write!(f, "{}", x),
            ExprResult::Byte(x) => write!(f, "{}", x),
            ExprResult::Bool(x) => write!(f, "{}", x),
            ExprResult::String(x) => write!(f, "{}", x),
            ExprResult::Null => write!(f, "NULL"),
        }
    }
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
