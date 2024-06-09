#[derive(Clone, PartialEq, Debug)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}
