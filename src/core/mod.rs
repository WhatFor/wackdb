use parser::Program;

use self::lexer::Token;

mod lexer;
mod parser;

#[derive(Debug, PartialEq)]
pub struct Wack {
    buf: String,
    pub tokens: Vec<Token>,
}

impl Wack {
    pub fn lex(buf: String) -> Wack {
        crate::core::lexer::lexer::Lexer::new(buf).lex()
    }

    pub fn parse(self) -> Result<Program, ()> {
        // TODO: Is this how I want to interface with the parser?
        //       Might make more sense to call it direct
        crate::core::parser::parser::Parser::new(self.tokens).parse()
    }
}
