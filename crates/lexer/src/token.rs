#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Keyword {
    Select,
    Insert,
    Update,
    Delete,
    Where,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Ident {
    pub value: Slice,
}

impl Ident {
    pub fn new(value: Slice) -> Self {
        Ident { value }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value {
    SingleQuoted(Slice),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Arithmetic {
    Multiply,
    Divide,
    Modulo,
    Plus,
    Minus,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Slice {
    pub start: usize,
    pub end: usize,
}

impl Slice {
    pub fn new(start: usize, end: usize) -> Slice {
        Slice { start, end }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
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
    Identifier(Ident),
    Value(Value),
    EOF,
    Null,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LocatableToken {
    pub token: Token,
    pub position: usize,
}

impl LocatableToken {
    pub fn at_position(token: Token, position: usize) -> Self {
        LocatableToken { token, position }
    }
}
