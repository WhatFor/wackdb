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

    pub fn parse(self) {
        let _nodes = crate::core::parser::Parser::new(self.tokens).parse();
    }
}
