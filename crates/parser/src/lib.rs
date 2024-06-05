use std::ops::Range;

use lexer::token::{Keyword, Token};

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

pub struct Parser {
    tokens: Vec<Token>,
    pub curr_pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Parser {
        Parser {
            tokens,
            curr_pos: 0,
        }
    }

    pub fn parse(&mut self) -> Result<Program, ()> {
        let program = self.parse_program();

        // todo: this is dumb; Choose a type, Result or Option.
        match program {
            Some(prog) => Ok(prog),
            None => Err(()),
        }
    }

    fn parse_program(&mut self) -> Option<Program> {
        let mut statements = vec![];

        loop {
            if self.is_end() {
                break;
            }

            self.next_significant_token();

            let query = self.parse_query();

            // todo: this is pretty rubbish. need a concept of an error type to return
            //       with positional info. need to also support multiple errors and recovery,
            //       but that's advanced.
            if query.is_none() {
                println!("expected a query, didn't find one. Maybe at the end?");
                break;
            }

            statements.push(query.unwrap());
        }

        // TODO: We're not exactly 'expecting' any statements with this, so return an empty program.
        //       Need to somewhere introduce the idea of 'expect' fn.
        if statements.is_empty() {
            return None;
        }

        Some(Program::Stmts(statements))
    }

    fn parse_query(&mut self) -> Option<Query> {
        let next = self.eat();
        match next {
            Token::Keyword(Keyword::Select) => Some(Query::Select),
            Token::Keyword(Keyword::Insert) => Some(Query::Insert),
            _ => {
                println!("Unhandled token. Probably me being lazy");
                None
            }
        }
    }

    // Get the next token without consuming it
    fn peek(&self) -> Option<&Token> {
        match self.curr_pos < self.tokens.len() {
            true => Some(&self.tokens[self.curr_pos]),
            false => None,
        }
    }

    // Throw an error if the next token is not expected
    fn _expect(&self, token: Token) {
        if self.tokens[self.curr_pos] != token {
            panic!("Unexpected token")
        }
    }

    // Consume and return the next token
    fn eat(&mut self) -> &Token {
        if self.curr_pos >= self.tokens.len() {
            // todo
            panic!("AHHH")
        }

        self.curr_pos += 1;
        &self.tokens[self.curr_pos - 1]
    }

    // Move to the next significant token
    fn next_significant_token(&mut self) {
        while self.is_significant_token() == false {
            self.eat();
        }
    }

    // Check if the current token is non-whitespace
    fn is_significant_token(&self) -> bool {
        let next = self.peek();

        match next {
            Some(tok) => match tok {
                Token::Space => false,
                _ => true,
            },
            None => false,
        }
    }

    // True if all tokens parsed
    fn is_end(&self) -> bool {
        self.curr_pos >= self.tokens.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::{*};

    #[test]
    fn test_simple_select_statement() {
        let tokens = vec![Token::Keyword(Keyword::Select)];
        let lexer = Parser::new(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Select]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_missing_statement() {
        let tokens = vec![Token::Semicolon];
        let lexer = Parser::new(tokens).parse();

        let expected = Err(());

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_insert_statement() {
        let tokens = vec![Token::Keyword(Keyword::Insert)];
        let lexer = Parser::new(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Insert]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_multiple_select_statements() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Keyword(Keyword::Select),
            Token::Keyword(Keyword::Select),
        ];
        let lexer = Parser::new(tokens).parse();

        let expected = Ok(Program::Stmts(vec![
            Query::Select,
            Query::Select,
            Query::Select,
        ]));

        assert_eq!(lexer, expected);
    }
}