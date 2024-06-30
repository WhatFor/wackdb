use cli_common::{ParseError, ParseErrorKind};
use core::panic;

use ast::*;
use lexer::token::{
    Arithmetic, Bitwise, Comparison, Ident as LexerIdent, Keyword, LocatableToken, Logical, Slice,
    Token, Value as LexerValue,
};
use recursion::*;

pub mod ast;
mod recursion;

pub struct Parser<'a> {
    tokens: Vec<LocatableToken>,
    buf: &'a str,
    recursion_guard: RecursionGuard,
    errors: Vec<ParseError>,
    pub curr_pos: usize,
}

/// By default, don't let expression depth go past 50.
/// If it does, that's one crazy query.
const MAX_DEPTH: usize = 50;

impl<'a> Parser<'a> {
    pub fn new(tokens: Vec<LocatableToken>, buf: &'a str) -> Parser {
        Parser {
            tokens,
            buf,
            recursion_guard: RecursionGuard::new(MAX_DEPTH),
            errors: vec![],
            curr_pos: 0,
        }
    }

    /// Create a new parser, but without token positions.
    /// Largely used just for testing.
    pub fn new_positionless(tokens: Vec<Token>, buf: &'a str) -> Parser<'a> {
        Parser {
            tokens: tokens
                .iter()
                .map(|t| LocatableToken {
                    token: *t,
                    position: 0,
                })
                .collect(),
            buf,
            recursion_guard: RecursionGuard::new(MAX_DEPTH),
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
            self.push_error(ParseErrorKind::ExpectedEOF);
        }

        Some(Program::Stmts(statements))
    }

    fn parse_query(&mut self) -> Option<Statement> {
        let next = self.peek();
        let query = match next {
            Some(Token::Keyword(Keyword::Select)) => self.parse_select_statement(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert_statement(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update_statement(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete_statement(),
            Some(Token::Keyword(Keyword::Create)) => self.parse_create_statement(),
            _ => {
                self.push_error(ParseErrorKind::ExpectedStatemnt);
                None
            }
        };

        self.next_significant_token();
        if self.lookahead(Token::Semicolon) {
            self.match_(Token::Semicolon);
        }

        query
    }

    fn parse_select_statement(&mut self) -> Option<Statement> {
        if self.lookahead(Token::Keyword(Keyword::Select)) {
            let exp_body = self.parse_select_expression_body()?;
            // optionally parse limitClause?

            Some(Statement::Select(exp_body))
        } else {
            self.push_error(ParseErrorKind::ExpectedKeyword(String::from("Select")));
            None
        }
    }

    fn parse_select_expression_body(&mut self) -> Option<SelectExpressionBody> {
        self.match_(Token::Keyword(Keyword::Select));

        let select_item_list = self.parse_select_item_list()?;
        let from_clause = self.parse_from_clause_optional();
        let where_clause = self.parse_where_clause_optional();
        let group_by_clause = self.parse_group_by_clause_optional();
        let order_by_clause = self.parse_order_by_clause_optional();

        Some(SelectExpressionBody {
            select_item_list,
            from_clause,
            where_clause,
            order_by_clause,
            group_by_clause,
        })
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

        Some(SelectItemList::from(item_list))
    }

    /// Parse a select item, such as:
    ///     Name
    ///     1 + 2
    ///     *
    ///     'Hello'
    ///     users.Name
    ///     users.Email AS UserEmail
    fn parse_select_item(&mut self) -> Option<SelectItem> {
        match self.peek() {
            Some(Token::Arithmetic(Arithmetic::Multiply)) => {
                self.eat();
                Some(SelectItem::new(Expr::Wildcard))
            }
            Some(Token::Identifier(_)) => self.parse_object_name(),
            _ => {
                let expr = self.parse_expr();

                match expr {
                    Some(e) => Some(SelectItem::new(e)),
                    None => {
                        self.push_error(ParseErrorKind::ExpectedIdentifier);
                        None
                    }
                }
            }
        }
    }

    /// Parse a potentially qualified and aliased object name,
    /// such as:
    ///     name
    ///     id AS UserId
    ///     users.name
    ///     users.email AS UserEmail
    fn parse_object_name(&mut self) -> Option<SelectItem> {
        let slice = match self.peek() {
            Some(Token::Identifier(LexerIdent { value })) => Some(value),
            _ => None,
        }
        .unwrap();

        let identifier_str = String::from(self.resolve_slice(slice));
        self.eat();

        let qualified_identifier = self.parse_qualified_identifier();
        let alias = self.pase_identifier_alias();

        match qualified_identifier {
            Some(qualified) => {
                let qualified_select_item = match alias {
                    Some(alias) => SelectItem::aliased_qualified_identifier(
                        vec![&identifier_str, &qualified],
                        alias,
                    ),
                    None => SelectItem::qualified_identifier(vec![&identifier_str, &qualified]),
                };

                Some(qualified_select_item)
            }
            None => {
                let select_item = match alias {
                    Some(alias) => SelectItem::aliased_identifier(&identifier_str, alias),
                    None => SelectItem::simple_identifier(&identifier_str),
                };

                Some(select_item)
            }
        }
    }

    /// Parse an optional identifier qualifier, i.e. the identifier
    /// following the dot in:
    ///     users.name
    /// or
    ///     u.email
    fn parse_qualified_identifier(&mut self) -> Option<String> {
        self.next_significant_token();
        match self.peek() {
            Some(Token::Dot) => {
                self.eat();

                match self.peek() {
                    Some(Token::Identifier(LexerIdent { value })) => {
                        let identifier = Some(String::from(self.resolve_slice(value)));
                        self.eat();
                        identifier
                    }
                    _ => {
                        self.push_error(ParseErrorKind::ExpectedIdentifier);
                        None
                    }
                }
            }
            _ => None,
        }
    }

    /// Parse an optional alias, such as:
    ///     name AS UserName
    fn pase_identifier_alias(&mut self) -> Option<Identifier> {
        self.next_significant_token();
        match self.peek() {
            Some(Token::Keyword(Keyword::As)) => {
                self.eat();
                self.next_significant_token();

                match self.peek() {
                    Some(Token::Identifier(ident)) => {
                        let value = Identifier {
                            value: String::from(self.resolve_slice(&ident.value)),
                        };
                        self.eat();
                        Some(value)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn parse_from_clause_optional(&mut self) -> Option<FromClause> {
        self.next_significant_token();

        if self.match_(Token::Keyword(Keyword::From)) {
            self.next_significant_token();
            match self.peek() {
                Some(Token::Identifier(LexerIdent { value })) => {
                    let identifier_str = String::from(self.resolve_slice(value));
                    self.eat();

                    let alias = self.parse_table_alias();

                    Some(FromClause {
                        identifier: Identifier {
                            value: identifier_str,
                        },
                        alias,
                    })
                }
                _ => {
                    self.push_error(ParseErrorKind::ExpectedIdentifier);
                    None
                }
            }
        } else {
            None
        }
    }

    /// Parse an optional identifier, assumed to be following a table name, for example:
    ///     Users u
    /// Where the u will be parsed.
    fn parse_table_alias(&mut self) -> Option<Identifier> {
        self.next_significant_token();

        match self.peek() {
            Some(Token::Identifier(LexerIdent { value })) => {
                let identifier_str = String::from(self.resolve_slice(value));
                self.eat();
                Some(Identifier::from(identifier_str))
            }
            _ => None,
        }
    }

    fn parse_where_clause_optional(&mut self) -> Option<WhereClause> {
        self.next_significant_token();

        if self.match_(Token::Keyword(Keyword::Where)) {
            let expr = self.parse_expr()?;

            Some(WhereClause { expr })
        } else {
            None
        }
    }

    fn parse_order_by_clause_optional(&mut self) -> Option<OrderByClause> {
        self.next_significant_token();

        if self.match_(Token::Keyword(Keyword::Order)) {
            self.next_significant_token();

            if self.match_(Token::Keyword(Keyword::By)) {
                self.next_significant_token();

                match self.peek() {
                    Some(Token::Identifier(LexerIdent { value })) => {
                        let identifier_str = String::from(self.resolve_slice(value));
                        self.eat();

                        let dir = self.parse_order_direction();

                        Some(OrderByClause {
                            identifier: Identifier {
                                value: identifier_str,
                            },
                            dir,
                        })
                    }
                    _ => {
                        self.push_error(ParseErrorKind::ExpectedIdentifier);
                        None
                    }
                }
            } else {
                self.push_error(ParseErrorKind::ExpectedKeyword(String::from("BY")));
                None
            }
        } else {
            None
        }
    }

    fn parse_order_direction(&mut self) -> OrderDirection {
        self.next_significant_token();
        match self.peek() {
            Some(Token::Keyword(Keyword::Asc)) => {
                self.eat();
                OrderDirection::Asc
            }
            Some(Token::Keyword(Keyword::Desc)) => {
                self.eat();
                OrderDirection::Desc
            }
            _ => OrderDirection::Asc,
        }
    }

    fn parse_group_by_clause_optional(&mut self) -> Option<GroupByClause> {
        self.next_significant_token();

        if self.match_(Token::Keyword(Keyword::Group)) {
            self.next_significant_token();

            if self.match_(Token::Keyword(Keyword::By)) {
                self.next_significant_token();

                match self.peek() {
                    Some(Token::Identifier(LexerIdent { value })) => {
                        let identifier_str = String::from(self.resolve_slice(value));
                        self.eat();

                        Some(GroupByClause {
                            identifier: Identifier {
                                value: identifier_str,
                            },
                        })
                    }
                    _ => {
                        self.push_error(ParseErrorKind::ExpectedIdentifier);
                        None
                    }
                }
            } else {
                self.push_error(ParseErrorKind::ExpectedKeyword(String::from("BY")));
                None
            }
        } else {
            None
        }
    }

    /// Parse a new expression
    pub fn parse_expr(&mut self) -> Option<Expr> {
        self.parse_subexpr(0)
    }

    fn parse_subexpr(&mut self, precedence: u8) -> Option<Expr> {
        let depth_guard = self.recursion_guard.dec();

        match depth_guard {
            Err(err) => {
                self.push_error(err);
                return None;
            }
            _ => {}
        }

        let mut expr = self.parse_prefix()?;

        loop {
            let next_precedence = self.next_expr_precedence();

            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }

        Some(expr)
    }

    fn parse_prefix(&mut self) -> Option<Expr> {
        self.next_significant_token();

        let expr = match self.peek() {
            Some(token) => match token {
                Token::Keyword(Keyword::True) | Token::Keyword(Keyword::False) | Token::Null => {
                    let val = self.parse_value();
                    Some(Expr::Value(val?))
                }
                Token::Identifier(i) => {
                    let val = self.buf[i.value.start..i.value.end].to_string();
                    self.eat();

                    Some(Expr::Identifier(Identifier::from(val)))
                }
                Token::Numeric(_) | Token::Value(LexerValue::SingleQuoted(_)) => {
                    let val = self.parse_value();
                    Some(Expr::Value(val?))
                }
                Token::ParenOpen => {
                    self.match_(Token::ParenOpen);
                    let sub_expr = self.parse_subexpr(0);

                    if self.match_(Token::ParenClose) {
                        sub_expr
                    } else {
                        self.push_error(ParseErrorKind::ExpressionNotClosed);
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        };

        expr
    }

    fn parse_infix(&mut self, expr: Expr, precedence: u8) -> Option<Expr> {
        self.next_significant_token();

        let binary_op = match self.peek()? {
            Token::Arithmetic(Arithmetic::Plus) => Some(BinaryOperator::Plus),
            Token::Arithmetic(Arithmetic::Minus) => Some(BinaryOperator::Minus),
            Token::Arithmetic(Arithmetic::Multiply) => Some(BinaryOperator::Multiply),
            Token::Arithmetic(Arithmetic::Divide) => Some(BinaryOperator::Divide),
            Token::Arithmetic(Arithmetic::Modulo) => Some(BinaryOperator::Modulo),
            Token::Comparison(Comparison::GreaterThan) => Some(BinaryOperator::GreaterThan),
            Token::Comparison(Comparison::GreaterThanOrEqual) => {
                Some(BinaryOperator::GreaterThanOrEqual)
            }
            Token::Comparison(Comparison::LessThan) => Some(BinaryOperator::LessThan),
            Token::Comparison(Comparison::LessThanOrEqual) => Some(BinaryOperator::LessThanOrEqual),
            Token::Comparison(Comparison::Equal) => Some(BinaryOperator::Equal),
            Token::Comparison(Comparison::NotEqual) => Some(BinaryOperator::NotEqual),
            Token::Keyword(Keyword::And) => Some(BinaryOperator::And),
            Token::Keyword(Keyword::Or) => Some(BinaryOperator::Or),
            Token::Keyword(Keyword::Xor) => Some(BinaryOperator::Xor),
            Token::Bitwise(Bitwise::Or) => Some(BinaryOperator::BitwiseOr),
            Token::Bitwise(Bitwise::And) => Some(BinaryOperator::BitwiseAnd),
            Token::Bitwise(Bitwise::Xor) => Some(BinaryOperator::BitwiseXor),
            _ => None,
        };

        if let Some(op) = binary_op {
            self.eat();

            let right = self.parse_subexpr(precedence)?;

            return Some(Expr::BinaryOperator {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            });
        }

        // Handle IS [NOT] TRUE/FALSE/NULL
        if self.lookahead(Token::Logical(Logical::Is)) {
            self.eat();
            self.next_significant_token();

            let logical_expr = match self.peek() {
                Some(token) => match token {
                    // IS NULL
                    Token::Null => Some(Expr::IsNull(Box::new(expr))),
                    // IS TRUE
                    Token::Keyword(Keyword::True) => Some(Expr::IsTrue(Box::new(expr))),
                    // IS FALSE
                    Token::Keyword(Keyword::False) => Some(Expr::IsFalse(Box::new(expr))),
                    // IS NOT...
                    Token::Logical(Logical::Not) => {
                        self.eat();
                        self.next_significant_token();

                        match self.peek() {
                            Some(is_not_token) => match is_not_token {
                                // IS NOT NULL
                                Token::Null => Some(Expr::IsNotNull(Box::new(expr))),
                                // IS NOT TRUE
                                Token::Keyword(Keyword::True) => {
                                    Some(Expr::IsNotTrue(Box::new(expr)))
                                }
                                // IS NOT FALSE
                                Token::Keyword(Keyword::False) => {
                                    Some(Expr::IsNotFalse(Box::new(expr)))
                                }
                                _ => {
                                    self.push_error(ParseErrorKind::ExpectedKeyword(String::from(
                                        "NULL, TRUE or FALSE",
                                    )));
                                    None
                                }
                            },
                            None => {
                                self.push_error(ParseErrorKind::ExpectedKeyword(String::from(
                                    "NOT, NULL, TRUE or FALSE",
                                )));
                                None
                            }
                        }
                    }
                    _ => {
                        self.push_error(ParseErrorKind::ExpectedKeyword(String::from(
                            "Complete logical expression",
                        )));
                        None
                    }
                },
                None => {
                    self.push_error(ParseErrorKind::ExpectedKeyword(String::from(
                        "Complete logical expression",
                    )));
                    None
                }
            };

            if logical_expr.is_some() {
                self.eat();
            }

            return logical_expr;
        }

        None
    }

    fn next_expr_precedence(&mut self) -> u8 {
        self.next_significant_token();
        match self.peek() {
            Some(token) => match token {
                Token::Logical(Logical::Is) => 17,
                Token::Comparison(Comparison::Equal)
                | Token::Comparison(Comparison::Equal2)
                | Token::Comparison(Comparison::NotEqual)
                | Token::Comparison(Comparison::GreaterThan)
                | Token::Comparison(Comparison::GreaterThanOrEqual)
                | Token::Comparison(Comparison::LessThan)
                | Token::Comparison(Comparison::LessThanOrEqual) => 20,
                Token::Bitwise(Bitwise::Or) => 21,
                Token::Arithmetic(Arithmetic::Plus) | Token::Arithmetic(Arithmetic::Minus) => 30,
                Token::Arithmetic(Arithmetic::Multiply)
                | Token::Arithmetic(Arithmetic::Divide)
                | Token::Arithmetic(Arithmetic::Modulo) => 40,
                Token::Logical(Logical::Not) => 50,
                Token::ParenOpen => 50,
                _ => 0,
            },
            None => 0,
        }
    }

    fn parse_value(&mut self) -> Option<Value> {
        let value = match self.peek() {
            Some(s) => match s {
                Token::Null => Some(Value::Null),
                Token::Keyword(Keyword::True) => Some(Value::Boolean(true)),
                Token::Keyword(Keyword::False) => Some(Value::Boolean(false)),
                Token::Numeric(s) => Some(Value::Number(self.buf[s.start..s.end].to_string())),
                Token::Value(LexerValue::SingleQuoted(s)) => Some(Value::String(
                    // todo: string interning? we indexing into buf here and maybe not great
                    self.buf[s.start..s.end].to_string(),
                    QuoteType::Single,
                )),
                _ => {
                    self.push_error(ParseErrorKind::ExpectedValue);
                    None
                }
            },
            _ => {
                self.push_error(ParseErrorKind::ExpectedValue);
                None
            }
        };

        if value.is_some() {
            self.eat();
        }

        value
    }

    fn parse_insert_statement(&mut self) -> Option<Statement> {
        if self.match_(Token::Keyword(Keyword::Insert)) {
            Some(Statement::Insert)
        } else {
            self.push_error(ParseErrorKind::ExpectedKeyword(String::from("INSERT")));
            None
        }
    }

    fn parse_update_statement(&mut self) -> Option<Statement> {
        if self.match_(Token::Keyword(Keyword::Update)) {
            Some(Statement::Update)
        } else {
            self.push_error(ParseErrorKind::ExpectedKeyword(String::from("UPDATE")));
            None
        }
    }

    fn parse_delete_statement(&mut self) -> Option<Statement> {
        if self.match_(Token::Keyword(Keyword::Delete)) {
            Some(Statement::Delete)
        } else {
            self.push_error(ParseErrorKind::ExpectedKeyword(String::from("DELETE")));
            None
        }
    }

    fn parse_create_statement(&mut self) -> Option<Statement> {
        if self.match_(Token::Keyword(Keyword::Create)) {
            self.next_significant_token();

            let create_statement = match self.peek() {
                Some(Token::Keyword(Keyword::Table)) => self.parse_create_table_statement(),
                Some(Token::Keyword(Keyword::Database)) => self.parse_create_database_statement(),
                _ => {
                    self.push_error(ParseErrorKind::UnsupportedSyntax);
                    None
                }
            };

            match create_statement {
                Some(statement) => Some(Statement::Create(statement)),
                None => None,
            }
        } else {
            self.push_error(ParseErrorKind::ExpectedKeyword(String::from("CREATE")));
            None
        }
    }

    fn parse_create_table_statement(&mut self) -> Option<CreateExpression> {
        // Eat the 'TABLE' keyword
        self.eat();

        let table_name = self.parse_unqualified_object_name()?;
        let column_list = self.parse_table_create_column_list()?;

        Some(CreateExpression::Table(CreateTableBody {
            table_name,
            column_list,
        }))
    }

    fn parse_create_database_statement(&mut self) -> Option<CreateExpression> {
        // Eat the 'DATABASE' keyword
        self.eat();

        let database_name = self.parse_unqualified_object_name()?;

        Some(CreateExpression::Database(CreateDatabaseBody {
            database_name,
        }))
    }

    fn parse_unqualified_object_name(&mut self) -> Option<Identifier> {
        self.next_significant_token();
        let identifier = match self.peek() {
            Some(Token::Identifier(LexerIdent { value })) => Some(value),
            _ => None,
        };

        match identifier {
            Some(id) => {
                let identifier_str = String::from(self.resolve_slice(id));
                self.eat();

                Some(Identifier {
                    value: identifier_str,
                })
            }
            None => {
                self.push_error(ParseErrorKind::ExpectedIdentifier);
                None
            }
        }
    }

    fn parse_table_create_column_list(&mut self) -> Option<Vec<ColumnDefinition>> {
        self.next_significant_token();

        if self.match_(Token::ParenOpen) == false {
            self.push_error(ParseErrorKind::ExpectedParentheses("(".to_string()));
            return None;
        }

        let mut columns = vec![];

        while self.lookahead(Token::ParenClose) == false {
            self.match_(Token::Comma);
            self.next_significant_token();

            let column_definition = self.parse_column_definition()?;
            columns.push(column_definition);
        }

        if self.match_(Token::ParenClose) == false {
            self.push_error(ParseErrorKind::ExpectedParentheses(")".to_string()));
            return None;
        }

        Some(columns)
    }

    fn parse_column_definition(&mut self) -> Option<ColumnDefinition> {
        self.next_significant_token();

        match self.peek() {
            Some(Token::Identifier(LexerIdent { value })) => {
                let identifier_str = String::from(self.resolve_slice(value));

                self.eat();
                self.next_significant_token();

                let datatype = match self.peek() {
                    Some(Token::Keyword(Keyword::Int)) => Some(DataType::Int),
                    _ => {
                        self.push_error(ParseErrorKind::ExpectedDataType);
                        None
                    }
                }?;

                self.eat();

                Some(ColumnDefinition {
                    column_name: Identifier::from(identifier_str),
                    datatype,
                    nullable: false, // TODO
                })
            }
            x => {
                println!("found: {x:?}");
                self.push_error(ParseErrorKind::ExpectedIdentifier);
                None
            }
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

    /// For a slice, resolve the string value from the input buffer.
    fn resolve_slice(&self, slice: &Slice) -> &str {
        &self.buf[slice.start..slice.end]
    }

    /// Consume and return the next token
    fn eat(&mut self) -> &LocatableToken {
        if self.curr_pos >= self.tokens.len() {
            panic!("Unexpected end of token stream. This should never happen.")
        }

        self.curr_pos += 1;
        &self.tokens[self.curr_pos - 1]
    }

    /// If the next token is as expected, consume it and return true
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
    fn push_error(&mut self, kind: ParseErrorKind) {
        let current_token = self.peek_with_location();
        let position = match current_token {
            Some(t) => t.position,
            _ => 0,
        };

        self.errors.push(ParseError { kind, position })
    }
}

#[cfg(test)]
mod parser_tests {
    use crate::*;
    use lexer::token::{Comparison, Slice, Value as LexerValue};
    use pretty_assertions::assert_eq;

    const EMPTY_QUERY: &'static str = "";

    #[test]
    fn test_simple_select_statement() {
        let query = String::from("select a");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("a")]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_aliased_select_statement() {
        let query = String::from("select a AS b");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Space,
            Token::Keyword(Keyword::As),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(12, 13))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::aliased_identifier(
                    "a",
                    Identifier {
                        value: String::from("b"),
                    },
                )]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_qualified_select_statement() {
        let query = String::from("select a.b");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Dot,
            Token::Identifier(LexerIdent::new(Slice::new(9, 10))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::qualified_identifier(
                    vec!["a", "b"],
                )]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_qualified_aliased_select_statement() {
        let query = String::from("select a.b AS c");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Dot,
            Token::Identifier(LexerIdent::new(Slice::new(9, 10))),
            Token::Space,
            Token::Keyword(Keyword::As),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(14, 15))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![
                    SelectItem::aliased_qualified_identifier(
                        vec!["a", "b"],
                        Identifier {
                            value: "c".to_string(),
                        },
                    ),
                ]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_select_wildcard_statement() {
        let query = String::from("select * from a");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Arithmetic(Arithmetic::Multiply),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(14, 15))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::new(Expr::Wildcard)]),
                from_clause: Some(FromClause {
                    identifier: Identifier {
                        value: String::from("a"),
                    },
                    alias: None,
                }),
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_qualified_table_select_statement() {
        let query = String::from("select u.Name from Users u");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Dot,
            Token::Identifier(LexerIdent::new(Slice::new(9, 13))),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(19, 24))),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(25, 26))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::qualified_identifier(
                    vec!["u", "Name"],
                )]),
                from_clause: Some(FromClause {
                    identifier: Identifier {
                        value: String::from("Users"),
                    },
                    alias: Some(Identifier::from("u".to_string())),
                }),
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_constant_number() {
        let query = String::from("select 1;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Numeric(Slice::new(7, 8)),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::Value(Value::Number(String::from("1"))),
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn text_expression_constant_string() {
        let query = String::from("select 'hello';");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Value(LexerValue::SingleQuoted(Slice::new(8, 13))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::Value(Value::String(String::from("hello"), QuoteType::Single)),
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_constant_number_plus() {
        let query = String::from("select 1 + 2;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Numeric(Slice::new(7, 8)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Plus),
            Token::Space,
            Token::Numeric(Slice::new(11, 12)),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::BinaryOperator {
                        left: Box::new(Expr::Value(Value::Number(String::from("1")))),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(Value::Number(String::from("2")))),
                    },
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_constant_number_minus() {
        let query = String::from("select 1 - 2;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Numeric(Slice::new(7, 8)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Minus),
            Token::Space,
            Token::Numeric(Slice::new(11, 12)),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::BinaryOperator {
                        left: Box::new(Expr::Value(Value::Number(String::from("1")))),
                        op: BinaryOperator::Minus,
                        right: Box::new(Expr::Value(Value::Number(String::from("2")))),
                    },
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_constant_number_divide() {
        let query = String::from("select 1 / 2;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Numeric(Slice::new(7, 8)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Divide),
            Token::Space,
            Token::Numeric(Slice::new(11, 12)),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::BinaryOperator {
                        left: Box::new(Expr::Value(Value::Number(String::from("1")))),
                        op: BinaryOperator::Divide,
                        right: Box::new(Expr::Value(Value::Number(String::from("2")))),
                    },
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_constant_number_multiply() {
        let query = String::from("select 1 * 2;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Numeric(Slice::new(7, 8)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Multiply),
            Token::Space,
            Token::Numeric(Slice::new(11, 12)),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::BinaryOperator {
                        left: Box::new(Expr::Value(Value::Number(String::from("1")))),
                        op: BinaryOperator::Multiply,
                        right: Box::new(Expr::Value(Value::Number(String::from("2")))),
                    },
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_constant_number_plus_multiple() {
        let query = String::from("select 1 + 2 + 3 + 4;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Numeric(Slice::new(7, 8)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Plus),
            Token::Space,
            Token::Numeric(Slice::new(11, 12)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Plus),
            Token::Space,
            Token::Numeric(Slice::new(15, 16)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Plus),
            Token::Space,
            Token::Numeric(Slice::new(19, 20)),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::BinaryOperator {
                        left: Box::new(Expr::BinaryOperator {
                            left: Box::new(Expr::BinaryOperator {
                                left: Box::new(Expr::Value(Value::Number(String::from("1")))),
                                op: BinaryOperator::Plus,
                                right: Box::new(Expr::Value(Value::Number(String::from("2")))),
                            }),
                            op: BinaryOperator::Plus,
                            right: Box::new(Expr::Value(Value::Number(String::from("3")))),
                        }),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(Value::Number(String::from("4")))),
                    },
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_constant_arithmetic_precedence() {
        let query = String::from("select 1 + 2 * 3;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Numeric(Slice::new(7, 8)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Plus),
            Token::Space,
            Token::Numeric(Slice::new(11, 12)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Multiply),
            Token::Space,
            Token::Numeric(Slice::new(15, 16)),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::BinaryOperator {
                        // (1 +
                        left: Box::new(Expr::Value(Value::Number(String::from("1")))),
                        op: BinaryOperator::Plus,
                        // (2 * 3)
                        right: Box::new(Expr::BinaryOperator {
                            left: Box::new(Expr::Value(Value::Number(String::from("2")))),
                            op: BinaryOperator::Multiply,
                            right: Box::new(Expr::Value(Value::Number(String::from("3")))),
                        }),
                        // )
                    },
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_constant_arithmetic_precedence_respects_parens() {
        let query = String::from("select (1 + 2) * 3;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::ParenOpen,
            Token::Numeric(Slice::new(8, 9)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Plus),
            Token::Space,
            Token::Numeric(Slice::new(12, 13)),
            Token::ParenClose,
            Token::Space,
            Token::Arithmetic(Arithmetic::Multiply),
            Token::Space,
            Token::Numeric(Slice::new(17, 18)),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem {
                    expr: Expr::BinaryOperator {
                        // (1 + 2)
                        left: Box::new(Expr::BinaryOperator {
                            left: Box::new(Expr::Value(Value::Number(String::from("1")))),
                            op: BinaryOperator::Plus,
                            right: Box::new(Expr::Value(Value::Number(String::from("2")))),
                        }),
                        op: BinaryOperator::Multiply,
                        // * 3
                        right: Box::new(Expr::Value(Value::Number(String::from("3")))),
                    },
                    alias: None,
                }]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_expression_parens_not_closed() {
        let query = String::from("select (1 + 2;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::ParenOpen,
            Token::Numeric(Slice::new(8, 9)),
            Token::Space,
            Token::Arithmetic(Arithmetic::Plus),
            Token::Space,
            Token::Numeric(Slice::new(12, 13)),
            Token::EOF,
        ];

        let actual = Parser::new_positionless(tokens, &query).parse();

        let errors = match actual {
            Ok(_) => vec![],
            Err(e) => e,
        };

        assert_eq!(
            errors[0],
            ParseError {
                position: 0,
                kind: ParseErrorKind::ExpressionNotClosed,
            }
        );
    }

    #[test]
    fn test_select_statement_with_multiple_select_items() {
        let query = String::from("select a,b");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Comma,
            Token::Identifier(LexerIdent::new(Slice::new(9, 10))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![
                    SelectItem::simple_identifier("a"),
                    SelectItem::simple_identifier("b"),
                ]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_select_statement_with_greater_than_comparison() {
        let query = String::from("select a from b where c > d");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(14, 15))),
            Token::Space,
            Token::Keyword(Keyword::Where),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(22, 23))),
            Token::Space,
            Token::Comparison(Comparison::GreaterThan),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(26, 27))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("a")]),
                from_clause: Some(FromClause {
                    identifier: Identifier {
                        value: String::from("b"),
                    },
                    alias: None,
                }),
                where_clause: Some(WhereClause {
                    expr: Expr::BinaryOperator {
                        left: Box::new(Expr::Identifier(Identifier {
                            value: String::from("c"),
                        })),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Identifier(Identifier {
                            value: String::from("d"),
                        })),
                    },
                }),
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_select_statement_with_is_null_clause() {
        let query = String::from("select a from b where c is null");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(14, 15))),
            Token::Space,
            Token::Keyword(Keyword::Where),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(22, 23))),
            Token::Space,
            Token::Logical(Logical::Is),
            Token::Space,
            Token::Null,
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("a")]),
                from_clause: Some(FromClause {
                    identifier: Identifier {
                        value: String::from("b"),
                    },
                    alias: None,
                }),
                where_clause: Some(WhereClause {
                    expr: Expr::IsNull(Box::new(Expr::Identifier(Identifier {
                        value: String::from("c"),
                    }))),
                }),
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_select_statement_with_is_true_clause() {
        let query = String::from("select a from b where c is true");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(14, 15))),
            Token::Space,
            Token::Keyword(Keyword::Where),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(22, 23))),
            Token::Space,
            Token::Logical(Logical::Is),
            Token::Space,
            Token::Keyword(Keyword::True),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("a")]),
                from_clause: Some(FromClause {
                    identifier: Identifier {
                        value: String::from("b"),
                    },
                    alias: None,
                }),
                where_clause: Some(WhereClause {
                    expr: Expr::IsTrue(Box::new(Expr::Identifier(Identifier {
                        value: String::from("c"),
                    }))),
                }),
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_select_statement_with_is_not_null_clause() {
        let query = String::from("select a from b where c is NOT null");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(14, 15))),
            Token::Space,
            Token::Keyword(Keyword::Where),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(22, 23))),
            Token::Space,
            Token::Logical(Logical::Is),
            Token::Space,
            Token::Logical(Logical::Not),
            Token::Space,
            Token::Null,
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("a")]),
                from_clause: Some(FromClause {
                    identifier: Identifier {
                        value: String::from("b"),
                    },
                    alias: None,
                }),
                where_clause: Some(WhereClause {
                    expr: Expr::IsNotNull(Box::new(Expr::Identifier(Identifier {
                        value: String::from("c"),
                    }))),
                }),
                order_by_clause: None,
                group_by_clause: None,
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_multiple_select_statements() {
        let query = String::from("select a;select b;select c;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Semicolon,
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(16, 17))),
            Token::Semicolon,
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(25, 26))),
            Token::Semicolon,
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![
            Statement::Select(SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("a")]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            }),
            Statement::Select(SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("b")]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            }),
            Statement::Select(SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("c")]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            }),
        ]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_multiple_select_statements_no_semicolon() {
        let query = String::from("select a select b");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Space,
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(16, 17))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![
            Statement::Select(SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("a")]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            }),
            Statement::Select(SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("b")]),
                from_clause: None,
                where_clause: None,
                order_by_clause: None,
                group_by_clause: None,
            }),
        ]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_full_select_statement() {
        let query = String::from(
            "select Name, Age from Users where c = 1 group by Name order by Name desc;",
        );

        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 11))),
            Token::Comma,
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(13, 16))),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(22, 27))),
            Token::Space,
            Token::Keyword(Keyword::Where),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(34, 35))),
            Token::Space,
            Token::Comparison(Comparison::Equal),
            Token::Space,
            Token::Numeric(Slice::new(38, 39)),
            Token::Space,
            Token::Keyword(Keyword::Group),
            Token::Space,
            Token::Keyword(Keyword::By),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(49, 53))),
            Token::Space,
            Token::Keyword(Keyword::Order),
            Token::Space,
            Token::Keyword(Keyword::By),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(63, 67))),
            Token::Space,
            Token::Keyword(Keyword::Desc),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected: Result<Program, Vec<ParseError>> =
            Ok(Program::Stmts(vec![Statement::Select(
                SelectExpressionBody {
                    select_item_list: SelectItemList::from(vec![
                        SelectItem::simple_identifier("Name"),
                        SelectItem::simple_identifier("Age"),
                    ]),
                    from_clause: Some(FromClause {
                        identifier: Identifier {
                            value: String::from("Users"),
                        },
                        alias: None,
                    }),
                    where_clause: Some(WhereClause {
                        expr: Expr::BinaryOperator {
                            left: Box::new(Expr::Identifier(Identifier {
                                value: String::from("c"),
                            })),
                            op: BinaryOperator::Equal,
                            right: Box::new(Expr::Value(Value::Number(String::from("1")))),
                        },
                    }),
                    group_by_clause: Some(GroupByClause {
                        identifier: Identifier {
                            value: String::from("Name"),
                        },
                    }),
                    order_by_clause: Some(OrderByClause {
                        dir: OrderDirection::Desc,
                        identifier: Identifier {
                            value: String::from("Name"),
                        },
                    }),
                },
            )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_select_statement_with_group_by() {
        let query = String::from("select a from b group by c;");
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(7, 8))),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(14, 15))),
            Token::Space,
            Token::Keyword(Keyword::Group),
            Token::Space,
            Token::Keyword(Keyword::By),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(25, 26))),
            Token::EOF,
        ];

        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Select(
            SelectExpressionBody {
                select_item_list: SelectItemList::from(vec![SelectItem::simple_identifier("a")]),
                from_clause: Some(FromClause {
                    identifier: Identifier {
                        value: String::from("b"),
                    },
                    alias: None,
                }),
                where_clause: None,
                order_by_clause: None,
                group_by_clause: Some(GroupByClause {
                    identifier: Identifier {
                        value: String::from("c"),
                    },
                }),
            },
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_empty_tokens() {
        let tokens = vec![];
        let actual = Parser::new_positionless(tokens, &EMPTY_QUERY).parse();
        let expected = Ok(Program::Stmts(vec![]));

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_select_statement_missing_select_items_list() {
        let tokens = vec![Token::Keyword(Keyword::Select), Token::EOF];
        let actual = Parser::new_positionless(tokens, &EMPTY_QUERY).parse();

        let errors = match actual {
            Ok(_) => vec![],
            Err(e) => e,
        };

        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0],
            ParseError {
                position: 0,
                kind: ParseErrorKind::ExpectedIdentifier,
            }
        );
    }

    #[test]
    fn test_select_statement_missing_select_item_after_comma() {
        let tokens = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier(LexerIdent::new(Slice::new(0, 1))),
            Token::Comma,
            Token::EOF,
        ];

        let actual = Parser::new_positionless(tokens, &String::from("select a,")).parse();

        let errors = match actual {
            Ok(_) => vec![],
            Err(e) => e,
        };

        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0],
            ParseError {
                position: 0,
                kind: ParseErrorKind::ExpectedIdentifier,
            }
        );
    }

    #[test]
    fn test_missing_statement() {
        let tokens = vec![Token::Semicolon];
        let lexer = Parser::new_positionless(tokens, &EMPTY_QUERY).parse();

        let errors = match lexer {
            Ok(_) => vec![],
            Err(e) => e,
        };

        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0],
            ParseError {
                position: 0,
                kind: ParseErrorKind::ExpectedStatemnt,
            }
        );
    }

    #[test]
    fn test_simple_insert_statement() {
        let tokens = vec![Token::Keyword(Keyword::Insert), Token::EOF];
        let lexer = Parser::new_positionless(tokens, &EMPTY_QUERY).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Insert]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_update_statement() {
        let tokens = vec![Token::Keyword(Keyword::Update), Token::EOF];
        let lexer = Parser::new_positionless(tokens, &EMPTY_QUERY).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Update]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_delete_statement() {
        let tokens = vec![Token::Keyword(Keyword::Delete), Token::EOF];
        let lexer = Parser::new_positionless(tokens, &EMPTY_QUERY).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Delete]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_create_table_statement() {
        let query = String::from("CREATE TABLE Users (Id INT, Age INT)");
        let tokens = vec![
            Token::Keyword(Keyword::Create),
            Token::Space,
            Token::Keyword(Keyword::Table),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(13, 18))),
            Token::Space,
            Token::ParenOpen,
            Token::Identifier(LexerIdent::new(Slice::new(20, 22))),
            Token::Space,
            Token::Keyword(Keyword::Int),
            Token::Comma,
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(28, 31))),
            Token::Space,
            Token::Keyword(Keyword::Int),
            Token::ParenClose,
            Token::EOF,
        ];
        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Create(
            CreateExpression::Table(CreateTableBody {
                table_name: Identifier::from("Users".to_string()),
                column_list: vec![
                    ColumnDefinition {
                        column_name: Identifier::from("Id".to_string()),
                        datatype: DataType::Int,
                        nullable: false,
                    },
                    ColumnDefinition {
                        column_name: Identifier::from("Age".to_string()),
                        datatype: DataType::Int,
                        nullable: false,
                    },
                ],
            }),
        )]));

        assert_eq!(lexer, expected);
    }

    #[test]
    fn test_simple_create_database_statement() {
        let query = String::from("CREATE Database Db");
        let tokens = vec![
            Token::Keyword(Keyword::Create),
            Token::Space,
            Token::Keyword(Keyword::Database),
            Token::Space,
            Token::Identifier(LexerIdent::new(Slice::new(16, 18))),
            Token::EOF,
        ];
        let lexer = Parser::new_positionless(tokens, &query).parse();

        let expected = Ok(Program::Stmts(vec![Statement::Create(
            CreateExpression::Database(CreateDatabaseBody {
                database_name: Identifier::from("Db".to_string()),
            }),
        )]));

        assert_eq!(lexer, expected);
    }
}
