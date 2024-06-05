#[derive(Clone, PartialEq, Debug)]
pub struct Position {
    pub line: usize,
    pub column: usize,
}

impl Position {
    pub fn new(line: usize, column: usize) -> Self {
        Position {
            line,
            column
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ParseError {
    pub message: String,
    pub pos: Position,
}