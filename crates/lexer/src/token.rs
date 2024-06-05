#[derive(Debug, PartialEq)]
pub enum Keyword {
    Select,
    Insert,
    Update,
    Delete,
    Where,
}

#[derive(Debug, PartialEq)]
pub enum Identifier {
    Table(Slice),
}

#[derive(Debug, PartialEq)]
pub enum Value {
    SingleQuoted(Slice),
}

#[derive(Debug, PartialEq)]
pub enum Arithmetic {
    Multiply,
    Divide,
    Modulo,
    Plus,
    Minus,
}

#[derive(Debug, PartialEq)]
pub struct Slice {
    pub start: usize,
    pub end: usize,
}

impl Slice {
    pub fn new(start: usize, end: usize) -> Slice {
        Slice { start, end }
    }
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Space,
    NewLine,
    Dot,
    Comma,
    ParenOpen,
    ParenClose,
    SquareOpen,
    SquareClose,
    SquiglyOpen,
    SquiglyClose,
    Colon,
    Semicolon,
    Keyword(Keyword),
    Arithmetic(Arithmetic),
    Numeric(Slice),
    Identifier(Identifier),
    Value(Value),
    EOF,
    Unknown,
}