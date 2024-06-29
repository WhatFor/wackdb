use token::*;
pub mod token;

pub struct Lexer<'a> {
    buf: &'a String,
    chars: Vec<(usize, char)>,
    len: usize,
    pos: usize,
}

pub struct LexResult<'a> {
    pub tokens: Vec<LocatableToken>,
    pub buf: &'a String,
}

impl<'a> Lexer<'a> {
    pub fn new(buf: &'a String) -> Lexer<'a> {
        let len = buf.len();
        let chars = buf.char_indices().collect();
        Lexer {
            buf,
            chars,
            len,
            pos: 0,
        }
    }

    pub fn lex(mut self) -> LexResult<'a> {
        let mut tokens = Vec::new();
        let mut prev_index = self.pos;

        loop {
            if self.pos >= self.len {
                tokens.push(LocatableToken::at_position(Token::EOF, self.len));
                break;
            }

            let (curr_offset, curr_ch) = self.chars[self.pos];

            let token = match curr_ch {
                // Single-quote String
                '\'' => {
                    let end_pos = self.scan_to(curr_offset + 1, '\'') + 1;

                    let slice = &self.buf[curr_offset..end_pos];

                    self.pos += slice.len();

                    Token::Value(Value::SingleQuoted(Slice::new(
                        curr_offset + 1,
                        end_pos - 1,
                    )))
                }
                // Space
                ' ' => {
                    self.pos += 1;
                    Token::Space
                }
                // NewLine
                '\n' | '\r' => {
                    self.pos += 1;
                    Token::NewLine
                }
                //Dot - only if the next char isn't numeric
                '.' if self.pos + 1 < self.len && !self.chars[self.pos + 1].1.is_numeric() => {
                    self.pos += 1;
                    Token::Dot
                }
                // Comment, double dashed
                '-' if self.pos + 1 < self.len && self.chars[self.pos + 1].1 == '-' => {
                    let end_pos = self.scan_until(curr_offset, |c| c == '\r' || c == '\n');

                    let slice = &self.buf[curr_offset..end_pos];
                    self.pos += slice.len();

                    Token::Comment(Slice::new(curr_offset, end_pos))
                }
                //Comma
                ',' => {
                    self.pos += 1;
                    Token::Comma
                }
                //ParenOpen
                '(' => {
                    self.pos += 1;
                    Token::ParenOpen
                }
                //ParenClose
                ')' => {
                    self.pos += 1;
                    Token::ParenClose
                }
                //SquareOpen
                '[' => {
                    self.pos += 1;
                    Token::SquareOpen
                }
                //SquareClose
                ']' => {
                    self.pos += 1;
                    Token::SquareClose
                }
                //SquiglyOpen
                '{' => {
                    self.pos += 1;
                    Token::SquiglyOpen
                }
                //SquiglyClose
                '}' => {
                    self.pos += 1;
                    Token::SquiglyClose
                }
                //Colon
                ':' => {
                    self.pos += 1;
                    Token::Colon
                }
                //Semicolon
                ';' => {
                    self.pos += 1;
                    Token::Semicolon
                }
                // Arithmetic
                '*' => {
                    self.pos += 1;
                    Token::Arithmetic(Arithmetic::Multiply)
                }
                '/' => {
                    self.pos += 1;
                    Token::Arithmetic(Arithmetic::Divide)
                }
                '%' => {
                    self.pos += 1;
                    Token::Arithmetic(Arithmetic::Modulo)
                }
                '+' => {
                    self.pos += 1;
                    Token::Arithmetic(Arithmetic::Plus)
                }
                // Comparison and Bitwise
                '|' => {
                    self.pos += 1;
                    Token::Bitwise(Bitwise::Or)
                }
                '&' => {
                    self.pos += 1;
                    Token::Bitwise(Bitwise::And)
                }
                '^' => {
                    self.pos += 1;
                    Token::Bitwise(Bitwise::Xor)
                }
                '=' | '!' | '>' | '<' => {
                    let end_pos = self.scan_until(curr_offset, |c| {
                        c != '=' && c != '!' && c != '>' && c != '<'
                    });

                    let slice = &self.buf[curr_offset..end_pos];
                    self.pos += slice.len();

                    match slice {
                        ">=" => Token::Comparison(Comparison::GreaterThanOrEqual),
                        "<=" => Token::Comparison(Comparison::LessThanOrEqual),
                        "<>" => Token::Comparison(Comparison::NotEqual),
                        ">" => Token::Comparison(Comparison::GreaterThan),
                        "<" => Token::Comparison(Comparison::LessThan),
                        "==" => Token::Comparison(Comparison::Equal2),
                        "=" => Token::Comparison(Comparison::Equal),
                        ">>" => Token::Bitwise(Bitwise::RightShift),
                        "<<" => Token::Bitwise(Bitwise::LeftShift),
                        _ => break,
                    }
                }
                // Only include minus if the next char isn't a number
                '-' if !(self.pos + 1 < self.len && self.chars[self.pos + 1].1.is_numeric()) => {
                    self.pos += 1;
                    Token::Arithmetic(Arithmetic::Minus)
                }
                // Alphabetical (can start with _, # or @)
                c if c.is_alphabetic() || c == '_' || c == '#' || c == '@' => {
                    let end_pos =
                        self.scan_until(curr_offset, |c| c == ' ' || c == ',' || c == ';');

                    let slice = &self.buf[curr_offset..end_pos];
                    self.pos += slice.len();

                    match slice {
                        // Keywords
                        s if s.eq_ignore_ascii_case("select") => Token::Keyword(Keyword::Select),
                        s if s.eq_ignore_ascii_case("insert") => Token::Keyword(Keyword::Insert),
                        s if s.eq_ignore_ascii_case("where") => Token::Keyword(Keyword::Where),

                        s if s.eq_ignore_ascii_case("as") => Token::Keyword(Keyword::As),
                        s if s.eq_ignore_ascii_case("from") => Token::Keyword(Keyword::From),
                        s if s.eq_ignore_ascii_case("and") => Token::Keyword(Keyword::And),
                        s if s.eq_ignore_ascii_case("or") => Token::Keyword(Keyword::Or),
                        s if s.eq_ignore_ascii_case("xor") => Token::Keyword(Keyword::Xor),
                        s if s.eq_ignore_ascii_case("update") => Token::Keyword(Keyword::Update),
                        s if s.eq_ignore_ascii_case("delete") => Token::Keyword(Keyword::Delete),
                        s if s.eq_ignore_ascii_case("set") => Token::Keyword(Keyword::Set),
                        s if s.eq_ignore_ascii_case("into") => Token::Keyword(Keyword::Into),
                        s if s.eq_ignore_ascii_case("values") => Token::Keyword(Keyword::Values),
                        s if s.eq_ignore_ascii_case("inner") => Token::Keyword(Keyword::Inner),
                        s if s.eq_ignore_ascii_case("join") => Token::Keyword(Keyword::Join),
                        s if s.eq_ignore_ascii_case("left") => Token::Keyword(Keyword::Left),
                        s if s.eq_ignore_ascii_case("right") => Token::Keyword(Keyword::Right),
                        s if s.eq_ignore_ascii_case("on") => Token::Keyword(Keyword::On),
                        s if s.eq_ignore_ascii_case("limit") => Token::Keyword(Keyword::Limit),
                        s if s.eq_ignore_ascii_case("offset") => Token::Keyword(Keyword::Offset),
                        s if s.eq_ignore_ascii_case("between") => Token::Keyword(Keyword::Between),
                        s if s.eq_ignore_ascii_case("array") => Token::Keyword(Keyword::Array),
                        s if s.eq_ignore_ascii_case("order") => Token::Keyword(Keyword::Order),
                        s if s.eq_ignore_ascii_case("group") => Token::Keyword(Keyword::Group),
                        s if s.eq_ignore_ascii_case("by") => Token::Keyword(Keyword::By),
                        s if s.eq_ignore_ascii_case("asc") => Token::Keyword(Keyword::Asc),
                        s if s.eq_ignore_ascii_case("desc") => Token::Keyword(Keyword::Desc),
                        s if s.eq_ignore_ascii_case("create") => Token::Keyword(Keyword::Create),
                        s if s.eq_ignore_ascii_case("table") => Token::Keyword(Keyword::Table),
                        s if s.eq_ignore_ascii_case("database") => {
                            Token::Keyword(Keyword::Database)
                        }
                        // Logical
                        s if s.eq_ignore_ascii_case("is") => Token::Logical(Logical::Is),
                        s if s.eq_ignore_ascii_case("in") => Token::Logical(Logical::In),
                        s if s.eq_ignore_ascii_case("not") => Token::Logical(Logical::Not),
                        s if s.eq_ignore_ascii_case("like") => Token::Logical(Logical::Like),
                        s if s.eq_ignore_ascii_case("then") => Token::Logical(Logical::Then),
                        s if s.eq_ignore_ascii_case("else") => Token::Logical(Logical::Else),
                        // Datatypes
                        s if s.eq_ignore_ascii_case("int") => Token::Keyword(Keyword::Int),
                        // Other
                        s if s.eq_ignore_ascii_case("null") => Token::Null,
                        s if s.eq_ignore_ascii_case("true") => Token::Keyword(Keyword::True),
                        s if s.eq_ignore_ascii_case("false") => Token::Keyword(Keyword::False),
                        _ => Token::Identifier(Ident::new(Slice::new(curr_offset, end_pos))),
                    }
                }
                c if c == '-' || c == '.' || c.is_numeric() => {
                    // Very greedily collect the number and include alphabetical to be handled later.
                    let end_pos = self.scan_until(curr_offset, |c| {
                        c.is_numeric() == false
                            && c.is_alphabetic() == false
                            && c != '.'
                            && c != '-'
                    });

                    self.pos += end_pos - curr_offset;

                    let mut seen_dot = false;
                    let mut is_unknown = false;

                    for i in self.buf[curr_offset + 1..end_pos].to_string().chars() {
                        if i == '.' {
                            if seen_dot {
                                is_unknown = true;
                            }
                            seen_dot = true;
                        }

                        if i.is_alphabetic() {
                            is_unknown = true;
                        }
                    }

                    match is_unknown {
                        true => Token::Unknown,
                        false => Token::Numeric(Slice::new(curr_offset, end_pos)),
                    }
                }
                _ => Token::Unknown,
            };

            tokens.push(LocatableToken::at_position(token, curr_offset));

            if prev_index == self.pos {
                panic!("Critical Lexer Error: Lexer iteration did not collect a token and is stuck. This is a bug.");
            }

            prev_index = self.pos;
        }

        LexResult {
            buf: self.buf,
            tokens,
        }
    }

    /// Given a start point and a char to find, scan until the char is found
    /// and return the end_offset.
    /// Only really works when we expect to end the current token by one and one
    /// character only. For more complex scenarios, use scan_until.
    fn scan_to(&self, start_offset: usize, char: char) -> usize {
        let mut cursor = start_offset;

        loop {
            if cursor >= self.len {
                break;
            }

            let (_, ch) = self.chars[cursor];

            if ch == char {
                break;
            }

            cursor += 1;
        }

        cursor
    }

    /// Given the function end_func, scan the input until the func returns true,
    /// returning the index at that point.
    fn scan_until<F>(&self, start_offset: usize, end_func: F) -> usize
    where
        F: Fn(char) -> bool,
    {
        let mut cursor = start_offset;

        loop {
            if cursor >= self.len {
                break;
            }

            let (_, ch) = self.chars[cursor];

            if end_func(ch) {
                break;
            }

            cursor += 1;
        }

        cursor
    }
}

#[cfg(test)]
mod lexer_tests {
    use crate::*;

    fn to_token_vec_without_locations(tokens: Vec<LocatableToken>) -> Vec<Token> {
        tokens.iter().map(|t| t.token).collect()
    }

    #[test]
    fn test_simple_tokens() {
        let str = String::from(",.(){}[];: \n\r");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Comma,
            Token::Dot,
            Token::ParenOpen,
            Token::ParenClose,
            Token::SquiglyOpen,
            Token::SquiglyClose,
            Token::SquareOpen,
            Token::SquareClose,
            Token::Semicolon,
            Token::Colon,
            Token::Space,
            Token::NewLine,
            Token::NewLine,
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_arithmetic_tokens() {
        let str = String::from("*/%-+");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Arithmetic(Arithmetic::Multiply),
            Token::Arithmetic(Arithmetic::Divide),
            Token::Arithmetic(Arithmetic::Modulo),
            Token::Arithmetic(Arithmetic::Minus),
            Token::Arithmetic(Arithmetic::Plus),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_simple_char_positioning() {
        let str = String::from("*/%-+");
        let lexer = Lexer::new(&str).lex();
        let actual = lexer.tokens;

        let expected = vec![
            LocatableToken::at_position(Token::Arithmetic(Arithmetic::Multiply), 0),
            LocatableToken::at_position(Token::Arithmetic(Arithmetic::Divide), 1),
            LocatableToken::at_position(Token::Arithmetic(Arithmetic::Modulo), 2),
            LocatableToken::at_position(Token::Arithmetic(Arithmetic::Minus), 3),
            LocatableToken::at_position(Token::Arithmetic(Arithmetic::Plus), 4),
            LocatableToken::at_position(Token::EOF, 5),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_keywords() {
        let str = String::from("select from inSERt WHERE AS Update and or xor set into values inner left right join on limit offset between array order group by asc desc True FALSE CREATE TABLE Database");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Keyword(Keyword::From),
            Token::Space,
            Token::Keyword(Keyword::Insert),
            Token::Space,
            Token::Keyword(Keyword::Where),
            Token::Space,
            Token::Keyword(Keyword::As),
            Token::Space,
            Token::Keyword(Keyword::Update),
            Token::Space,
            Token::Keyword(Keyword::And),
            Token::Space,
            Token::Keyword(Keyword::Or),
            Token::Space,
            Token::Keyword(Keyword::Xor),
            Token::Space,
            Token::Keyword(Keyword::Set),
            Token::Space,
            Token::Keyword(Keyword::Into),
            Token::Space,
            Token::Keyword(Keyword::Values),
            Token::Space,
            Token::Keyword(Keyword::Inner),
            Token::Space,
            Token::Keyword(Keyword::Left),
            Token::Space,
            Token::Keyword(Keyword::Right),
            Token::Space,
            Token::Keyword(Keyword::Join),
            Token::Space,
            Token::Keyword(Keyword::On),
            Token::Space,
            Token::Keyword(Keyword::Limit),
            Token::Space,
            Token::Keyword(Keyword::Offset),
            Token::Space,
            Token::Keyword(Keyword::Between),
            Token::Space,
            Token::Keyword(Keyword::Array),
            Token::Space,
            Token::Keyword(Keyword::Order),
            Token::Space,
            Token::Keyword(Keyword::Group),
            Token::Space,
            Token::Keyword(Keyword::By),
            Token::Space,
            Token::Keyword(Keyword::Asc),
            Token::Space,
            Token::Keyword(Keyword::Desc),
            Token::Space,
            Token::Keyword(Keyword::True),
            Token::Space,
            Token::Keyword(Keyword::False),
            Token::Space,
            Token::Keyword(Keyword::Create),
            Token::Space,
            Token::Keyword(Keyword::Table),
            Token::Space,
            Token::Keyword(Keyword::Database),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_datatypes() {
        let str = String::from("INT ");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Keyword(Keyword::Int), Token::Space, Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_null() {
        let str = String::from("null Null NULL");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Null,
            Token::Space,
            Token::Null,
            Token::Space,
            Token::Null,
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_comparison() {
        let str = String::from("= == >= <= <> > <");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Comparison(Comparison::Equal),
            Token::Space,
            Token::Comparison(Comparison::Equal2),
            Token::Space,
            Token::Comparison(Comparison::GreaterThanOrEqual),
            Token::Space,
            Token::Comparison(Comparison::LessThanOrEqual),
            Token::Space,
            Token::Comparison(Comparison::NotEqual),
            Token::Space,
            Token::Comparison(Comparison::GreaterThan),
            Token::Space,
            Token::Comparison(Comparison::LessThan),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_bitwise() {
        let str = String::from("<< >> | & ^");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Bitwise(Bitwise::LeftShift),
            Token::Space,
            Token::Bitwise(Bitwise::RightShift),
            Token::Space,
            Token::Bitwise(Bitwise::Or),
            Token::Space,
            Token::Bitwise(Bitwise::And),
            Token::Space,
            Token::Bitwise(Bitwise::Xor),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_logical() {
        let str = String::from("Is In Not THEN like elSE");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Logical(Logical::Is),
            Token::Space,
            Token::Logical(Logical::In),
            Token::Space,
            Token::Logical(Logical::Not),
            Token::Space,
            Token::Logical(Logical::Then),
            Token::Space,
            Token::Logical(Logical::Like),
            Token::Space,
            Token::Logical(Logical::Else),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_doubledash_comment() {
        let str = String::from("LIKE --comment");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Logical(Logical::Like),
            Token::Space,
            Token::Comment(Slice::new(5, 14)),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_doubledash_comment_multiline() {
        let str = String::from(
            "*
/
--comment
-",
        );
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Arithmetic(Arithmetic::Multiply),
            Token::NewLine,
            Token::Arithmetic(Arithmetic::Divide),
            Token::NewLine,
            Token::Comment(Slice::new(4, 13)),
            Token::NewLine,
            Token::Arithmetic(Arithmetic::Minus),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_keywords_positioning() {
        let str = String::from("select inSERt WHERE");
        let lexer = Lexer::new(&str).lex();
        let actual = lexer.tokens;

        let expected = vec![
            LocatableToken::at_position(Token::Keyword(Keyword::Select), 0),
            LocatableToken::at_position(Token::Space, 6),
            LocatableToken::at_position(Token::Keyword(Keyword::Insert), 7),
            LocatableToken::at_position(Token::Space, 13),
            LocatableToken::at_position(Token::Keyword(Keyword::Where), 14),
            LocatableToken::at_position(Token::EOF, 19),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_keyword_list() {
        let str = String::from("select hello, world");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(Ident::new(Slice::new(7, 12))),
            Token::Comma,
            Token::Space,
            Token::Identifier(Ident::new(Slice::new(14, 19))),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_identifier_not_greedily_consuming_semicolon() {
        let str = String::from("select hello;");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(Ident::new(Slice::new(7, 12))),
            Token::Semicolon,
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_keywords_not_greedy() {
        let str = String::from("selecting");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        // Should not match on Token::Keyword for Select!
        let expected = vec![Token::Identifier(Ident::new(Slice::new(0, 9))), Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_multiline() {
        let str = String::from(
            "*
/

-",
        );
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Arithmetic(Arithmetic::Multiply),
            Token::NewLine,
            Token::Arithmetic(Arithmetic::Divide),
            Token::NewLine,
            Token::NewLine,
            Token::Arithmetic(Arithmetic::Minus),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_numeric() {
        let str = String::from("12 4");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Numeric(Slice::new(0, 2)),
            Token::Space,
            Token::Numeric(Slice::new(3, 4)),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_bad_numeric_invalid_identifier_unknown_token() {
        // This should not lex as a number, nor as an identifier.
        // It is not valid as a number due to the 'a' char,
        // and identifiers cannot begin with a number.
        let str = String::from("12a0");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Unknown, Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_identifier_starts_with_underscore() {
        let str = String::from("_hello");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Identifier(Ident::new(Slice::new(0, 6))), Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_identifier_starts_with_hash() {
        let str = String::from("#hello");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Identifier(Ident::new(Slice::new(0, 6))), Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_identifier_starts_with_at() {
        let str = String::from("@hello");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Identifier(Ident::new(Slice::new(0, 6))), Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_identifier_end_with_underscore() {
        let str = String::from("hello_");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Identifier(Ident::new(Slice::new(0, 6))), Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_identifier_contains_underscore() {
        let str = String::from("hello_world");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Identifier(Ident::new(Slice::new(0, 11))), Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_identifier_contains_number() {
        let str = String::from("hello999world");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Identifier(Ident::new(Slice::new(0, 13))), Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_numeric_negative() {
        let str = String::from("-12 4");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Numeric(Slice::new(0, 3)),
            Token::Space,
            Token::Numeric(Slice::new(4, 5)),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_numeric_float() {
        let str = String::from("12.1 1.9");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Numeric(Slice::new(0, 4)),
            Token::Space,
            Token::Numeric(Slice::new(5, 8)),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_numeric_float_short_syntax() {
        let str = String::from(".1");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Numeric(Slice::new(0, 2)), Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_invalid_numeric_float_with_multiple_dots() {
        let str = String::from("12.1.1");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![Token::Unknown, Token::EOF];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_basic_insert() {
        let str = String::from("insert users 'John', 'Doe'");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Keyword(Keyword::Insert),
            Token::Space,
            Token::Identifier(Ident::new(Slice::new(7, 12))),
            Token::Space,
            Token::Value(Value::SingleQuoted(Slice::new(14, 18))),
            Token::Comma,
            Token::Space,
            Token::Value(Value::SingleQuoted(Slice::new(22, 25))),
            Token::EOF,
        ];

        assert_eq!(actual_without_locations, expected);
    }

    #[test]
    fn test_string_indexing() {
        let str = String::from("insert users ");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let identifier_str = match &actual_without_locations[2] {
            Token::Identifier(Ident { value: x }) => Some(&str[x.start..x.end]),
            _ => None,
        };

        assert_ne!(identifier_str, None);
        assert_eq!(identifier_str.unwrap(), "users");
    }
}
