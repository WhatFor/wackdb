use core::panic;
use std::ops::Range;

use cli_common::ParseError;
use lexer::token::{Identifier as LexerIdentifier, Keyword, LocatableToken, Token};

pub struct Node {
    pub pos: Range<usize>,
    pub tok: Token,
}

#[derive(PartialEq, Debug)]
pub enum Program {
    Stmts(Vec<Query>),
    Empty,
}

#[derive(PartialEq, Debug)]
pub enum Query {
    Select(SelectExpressionBody),
    Update,
    Insert,
    Delete,
}

#[derive(PartialEq, Debug)]
pub struct SelectExpressionBody {
    pub select_item_list: SelectItemList,
}

#[derive(PartialEq, Debug)]
pub struct SelectItemList {
    pub item_list: Vec<SelectItem>,
}

#[derive(PartialEq, Debug)]
pub struct SelectItem {
    pub identifier: Identifier,
}

#[derive(PartialEq, Debug)]
pub struct Identifier {
    pub name: String, //  todo: should be &str indexing into input buffer? not sure
}

pub struct Parser {
    tokens: Vec<LocatableToken>,
    errors: Vec<ParseError>,
    pub curr_pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<LocatableToken>) -> Parser {
        Parser {
            tokens,
            errors: vec![],
            curr_pos: 0,
        }
    }

    /// Create a new parser, but without token positions.
    /// Largely used just for testing.
    pub fn new_positionless(tokens: Vec<Token>) -> Parser {
        Parser {
            tokens: tokens
                .iter()
                .map(|t| LocatableToken {
                    token: *t,
                    position: 0,
                })
                .collect(),
            errors: vec![],
            curr_pos: 0,
        }
    }

    pub fn parse(&mut self) -> Result<Program, Vec<ParseError>> {
        if self.tokens.is_empty() {
            return Ok(Program::Stmts(vec![]));
        }

        let parse_result = self.parse_program();

        match self.errors.is_empty() {
            true => Ok(parse_result.unwrap()),
            false => Err(self.errors.clone()),
        }
    }

    /// The main entry point of the parser.
    /// Attempts to find one or more queries.
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
                }
                _ => {}
            }

            self.next_significant_token();

            let query = self.parse_query();

            match query {
                Some(q) => statements.push(q),
                None => break,
            }
        }

        if statements.is_empty() {
            return Some(Program::Empty);
        }

        if !parsed_full {
            self.push_error("End of file not found");
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
                self.push_error(
                    "Unexpected token. Expected one of Select, Insert, Update or Delete.",
                );
                None
            }
        }
    }

    fn parse_select_statement(&mut self) -> Option<Query> {
        if self.lookahead(Token::Keyword(Keyword::Select)) {
            let exp_body = self.parse_select_expression_body()?;
            // optionally parse orderClause?
            // optionally parse limitClause?

            Some(Query::Select(exp_body))
        } else {
            self.push_error("Unexpected token. Expected Select keyword.");
            None
        }
    }

    fn parse_select_expression_body(&mut self) -> Option<SelectExpressionBody> {
        self.match_(Token::Keyword(Keyword::Select));

        let select_item_list = self.parse_select_item_list()?;
        self.parse_from_clause_optional()?;
        self.parse_where_clause_optional()?;
        self.parse_group_by_clause_optional()?;

        Some(SelectExpressionBody { select_item_list })
    }

    fn parse_select_item_list(&mut self) -> Option<SelectItemList> {
        let mut item_list = vec![];

        self.next_significant_token();
        item_list.push(self.parse_select_item()?);
        self.next_significant_token();

        while self.lookahead(Token::Comma) {
            self.match_(Token::Comma);
            self.next_significant_token();
            item_list.push(self.parse_select_item()?);
        }

        Some(SelectItemList { item_list })
    }

    fn parse_select_item(&mut self) -> Option<SelectItem> {
        let identifier = match self.peek() {
            // todo: this should probably not be as specific as a 'table' identifier,
            //       and should probably just be a string or something.
            Some(Token::Identifier(LexerIdentifier::Table(table))) => Some(table),
            _ => None,
        };

        match identifier {
            Some(_) => {
                self.eat();
            }
            None => {
                self.push_error("Unexpected token. Expected identifier.");
            }
        }

        Some(SelectItem {
            identifier: Identifier {
                name: String::from(""), // todo: Slice is just a pointer to a part of the lexed string.
            },
        })
    }

    fn parse_from_clause_optional(&mut self) -> Option<()> {
        Some(())
    }

    fn parse_where_clause_optional(&mut self) -> Option<()> {
        Some(())
    }

    fn parse_group_by_clause_optional(&mut self) -> Option<()> {
        Some(())
    }

    fn parse_insert_statement(&mut self) -> Option<Query> {
        if self.match_(Token::Keyword(Keyword::Insert)) {
            Some(Query::Insert)
        } else {
            self.push_error("Unexpected token. Expected Insert keyword.");
            None
        }
    }

    fn parse_update_statement(&mut self) -> Option<Query> {
        if self.match_(Token::Keyword(Keyword::Update)) {
            Some(Query::Update)
        } else {
            self.push_error("Unexpected token. Expected Update keyword.");
            None
        }
    }

    fn parse_delete_statement(&mut self) -> Option<Query> {
        if self.match_(Token::Keyword(Keyword::Delete)) {
            Some(Query::Delete)
        } else {
            self.push_error("Unexpected token. Expected Delete keyword.");
            None
        }
    }

    /// Check if the next token is of a certain type
    fn lookahead(&self, token: Token) -> bool {
        match self.curr_pos < self.tokens.len() {
            true => self.tokens[self.curr_pos].token == token,
            false => false,
        }
    }

    /// Get the next token without consuming it
    fn peek(&self) -> Option<&Token> {
        match self.curr_pos < self.tokens.len() {
            true => Some(&self.tokens[self.curr_pos].token),
            false => None,
        }
    }

    /// Get the next token without consuming it.
    /// Includes location data.
    fn peek_with_location(&self) -> Option<&LocatableToken> {
        match self.curr_pos < self.tokens.len() {
            true => Some(&self.tokens[self.curr_pos]),
            false => None,
        }
    }

    /// Consume and return the next token
    fn eat(&mut self) -> &LocatableToken {
        if self.curr_pos >= self.tokens.len() {
            panic!("Unexpected end of token stream. This should never happen.")
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

    /// Add a new error to the errors list.
    fn push_error(&mut self, message: &str) {
        let current_token = self.peek_with_location();
        let position = match current_token {
            Some(t) => t.position,
            _ => 0,
        };

        self.errors.push(ParseError {
            message: String::from(message),
            position,
        })
    }
}

#[cfg(test)]
mod parser_tests {
    use crate::*;
    use lexer::token::Slice;

    #[test]
    #[ignore = "waiting on more fleshed out select parsing"]
    fn test_simple_select_statement() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(LexerIdentifier::Table(Slice::new(0, 1))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens).parse();

        //let expected = Ok(Program::Stmts(vec![Query::Select]));

        //assert_eq!(lexer, expected);
    }

    #[test]
    #[ignore = "waiting on more fleshed out select parsing"]
    fn test_select_statement_with_multiple_select_items() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(LexerIdentifier::Table(Slice::new(0, 1))),
            Token::Comma,
            Token::Identifier(LexerIdentifier::Table(Slice::new(0, 1))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens).parse();

        // let expected = Ok(Program::Stmts(vec![Query::Select]));

        // assert_eq!(lexer, expected);
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
        let _ = Parser::new_positionless(tokens).parse();
    }

    #[test]
    #[should_panic] // todo: real errors instead of panic
    fn test_incomplete_input_missing_select_item_after_comma() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(LexerIdentifier::Table(Slice::new(0, 1))),
            Token::Comma,
        ];
        let _ = Parser::new_positionless(tokens).parse();
    }

    #[test]
    fn test_missing_statement() {
        let tokens = vec![Token::Semicolon];
        let lexer = Parser::new_positionless(tokens).parse();

        let errors = match lexer {
            Ok(_) => vec![],
            Err(e) => e,
        };

        //  TODO: Not actually checking which error.
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_simple_insert_statement() {
        let tokens = vec![Token::Keyword(Keyword::Insert), Token::EOF];
        let lexer = Parser::new_positionless(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Insert]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_update_statement() {
        let tokens = vec![Token::Keyword(Keyword::Update), Token::EOF];
        let lexer = Parser::new_positionless(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Update]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_delete_statement() {
        let tokens = vec![Token::Keyword(Keyword::Delete), Token::EOF];
        let lexer = Parser::new_positionless(tokens).parse();

        let expected = Ok(Program::Stmts(vec![Query::Delete]));

        assert_eq!(lexer, expected);
    }

    #[test]
    #[ignore = "waiting on more fleshed out select parsing"]
    fn test_multiple_select_statements() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(LexerIdentifier::Table(Slice::new(0, 1))),
            Token::Keyword(Keyword::Select),
            Token::Identifier(LexerIdentifier::Table(Slice::new(0, 1))),
            Token::Keyword(Keyword::Select),
            Token::Identifier(LexerIdentifier::Table(Slice::new(0, 1))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens).parse();

        // let expected = Ok(Program::Stmts(vec![
        //     Query::Select,
        //     Query::Select,
        //     Query::Select,
        // ]));

        // assert_eq!(lexer, expected);
    }
}