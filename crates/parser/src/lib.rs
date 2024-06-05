use core::panic;
use std::ops::Range;

use lexer::token::{Identifier, Keyword, Token};

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

        if self.tokens.is_empty() {
            return Ok(Program::Stmts(vec![]));
        }

        let program = self.parse_program();

        // todo: this is dumb; Choose a type, Result or Option.
        match program {
            Some(prog) => Ok(prog),
            None => Err(()),
        }
    }

    fn parse_program(&mut self) -> Option<Program> {
        let mut statements = vec![];
        let mut parsed_full = false;

        loop {
            if self.is_end() {
                break;
            }

            match self.peek() {
                Some(Token::EOF) => {
                    parsed_full = true;
                    self.eat();
                    break;
                },
                _ => { }
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

        if !parsed_full {
            panic!("Did not find EOF.");
        }

        Some(Program::Stmts(statements))
    }

    //query: (simpleStatement SEMICOLON_SYMBOL?)?
    fn parse_query(&mut self) -> Option<Query> {
        let next = self.peek();
        match next {
            Some(Token::Keyword(Keyword::Select)) => self.parse_select_statement(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert_statement(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update_statement(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete_statement(),
            _ => {
                println!("Unhandled token. Probably me being lazy");
                None
            }
        }
    }

    fn parse_select_statement(&mut self) -> Option<Query> {
        if self.lookahead(Token::Keyword(Keyword::Select)) {
            self.parse_select_expression_body();
            // optionally parse orderClause?
            // optionally parse limitClause?

            Some(Query::Select)

        } else {
            panic!("todo: error. expected select statement, didn't find token.");
        }
    }

    fn parse_select_expression_body(&mut self) -> () {
        self.match_(Token::Keyword(Keyword::Select));
        self.parse_select_item_list();
        // optionally parse fromClause?
        // optionally parse whereClause?
        // optionally parse groupByClause?

        ()
    }

    fn parse_select_item_list(&mut self) -> () {

        self.next_significant_token();
        self.parse_select_item();
        self.next_significant_token();

        while self.lookahead(Token::Comma) {
            self.match_(Token::Comma);
            self.next_significant_token();
            self.parse_select_item();
        }

        ()
    }

    fn parse_select_item(&mut self) -> () {
        let identifier = match self.peek() {
            // todo: this should probably not be as specific as a 'table' identifier,
            //       and should probably just be a string or something.
            Some(Token::Identifier(Identifier::Table(table))) => Some(table),
            _ => None,
        };

        match identifier {
            Some(_) => {
                self.eat();
            },
            None => {
                panic!("todo error: expected an identifier!");
            }
        }

        ()
    }

    fn parse_insert_statement(&mut self) -> Option<Query> {
        if self.match_(Token::Keyword(Keyword::Insert)) {
            Some(Query::Insert)

        } else {
            panic!("todo: error. expected insert statement, didn't find token.");
        }
    }

    fn parse_update_statement(&mut self) -> Option<Query> {
        if self.match_(Token::Keyword(Keyword::Update)) {
            Some(Query::Update)

        } else {
            panic!("todo: error. expected update statement, didn't find token.");
        }
    }

    fn parse_delete_statement(&mut self) -> Option<Query> {
        if self.match_(Token::Keyword(Keyword::Delete)) {
            Some(Query::Delete)

        } else {
            panic!("todo: error. expected delete statement, didn't find token.");
        }
    }


    // Check if the next token is of a certain type
    fn lookahead(&self, token: Token) -> bool {
        match self.curr_pos < self.tokens.len() {
            true => self.tokens[self.curr_pos] == token,
            false => false,
        }
    }

    // Get the next token without consuming it
    fn peek(&self) -> Option<&Token> {
        match self.curr_pos < self.tokens.len() {
            true => Some(&self.tokens[self.curr_pos]),
            false => None,
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

    fn match_(&mut self, token: Token) -> bool {
        let matched = self.lookahead(token);

        if matched {
            self.eat();
        }

        matched
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
mod parser_tests {
    use lexer::token::Slice;

    use crate::{*};

    #[test]
    fn test_simple_select_statement() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(Identifier::Table(Slice::new(0,1))),
            Token::EOF,
        ];

        let lexer = Parser::new(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Select]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_select_statement_with_multiple_select_items() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(Identifier::Table(Slice::new(0,1))),
            Token::Comma,
            Token::Identifier(Identifier::Table(Slice::new(0,1))),
            Token::EOF,
        ];

        let lexer = Parser::new(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Select]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_empty_tokens() {
        let tokens = vec![];
        let lexer = Parser::new(tokens).parse();
        let expected = Ok(Program::Stmts(vec![]));

        assert_eq!(lexer, expected);
    }

    #[test]
    #[should_panic] // todo: real errors instead of panic
    fn test_incomplete_input_missing_select_items_list() {
        let tokens = vec![Token::Keyword(Keyword::Select)];
        let _ = Parser::new(tokens).parse();
    }

    #[test]
    #[should_panic] // todo: real errors instead of panic
    fn test_incomplete_input_missing_select_item_after_comma() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(Identifier::Table(Slice::new(0,1))),
            Token::Comma,
        ];
        let _ = Parser::new(tokens).parse();
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
        let tokens = vec![Token::Keyword(Keyword::Insert), Token::EOF];
        let lexer = Parser::new(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Insert]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_update_statement() {
        let tokens = vec![Token::Keyword(Keyword::Update), Token::EOF];
        let lexer = Parser::new(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Update]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_delete_statement() {
        let tokens = vec![Token::Keyword(Keyword::Delete), Token::EOF];
        let lexer = Parser::new(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Delete]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_multiple_select_statements() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(Identifier::Table(Slice::new(0,1))),
            Token::Keyword(Keyword::Select),
            Token::Identifier(Identifier::Table(Slice::new(0,1))),
            Token::Keyword(Keyword::Select),
            Token::Identifier(Identifier::Table(Slice::new(0,1))),
            Token::EOF,
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