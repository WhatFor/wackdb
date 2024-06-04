use std::ops::Range;

use super::lexer::Token;

pub mod parser;

pub struct Node {
    pub pos: Range<usize>,
    pub tok: Token,
}

#[derive(PartialEq, Debug)]
pub enum Program {
    Stmts(Vec<Query>),
}

#[derive(PartialEq, Debug)]
pub enum Query {
    Select,
    Update,
    Insert,
    Delete,
}
