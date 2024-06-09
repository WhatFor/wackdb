#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Keyword {
    Select,
    From,
    Insert,
    Update,
    Delete,
    Where,
    And,
    Or,
    Set,
    Into,
    Values,
    Inner,
    Left,
    Right,
    Join,
    On,
    Limit,
    Offset,
    Between,
    Array,
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
pub enum Logical {
    In,
    Not,
    Like,
    Then,
    Else,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Comparison {
    Equal,              // =
    Equal2,             // ==
    GreaterThanOrEqual, // >=
    LessThanOrEqual,    // <=
    NotEqual,           // <>
    GreaterThan,        // >
    LessThan,           // <
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Bitwise {
    LeftShift,  // <<
    RightShift, // >>
    And,        // &
    Or,         // |
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
    Logical(Logical),
    Comparison(Comparison),
    Bitwise(Bitwise),
    Numeric(Slice),
    Identifier(Ident),
    Comment(Slice),
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
