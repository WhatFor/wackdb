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
                //Dot
                '.' => {
                    self.pos += 1;
                    Token::Dot
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
                // Only include minus if the next char isn't a number
                '-' if !(self.pos + 1 < self.len && self.chars[self.pos + 1].1.is_numeric()) => {
                    self.pos += 1;
                    Token::Arithmetic(Arithmetic::Minus)
                }
                // Alphabetical
                c if c.is_alphabetic() => {
                    let end_pos = self.scan_until(curr_offset, |c| c == ' ' || c == ',');

                    let slice = &self.buf[curr_offset..end_pos];
                    self.pos += slice.len();

                    match slice {
                        s if s.eq_ignore_ascii_case("select") => Token::Keyword(Keyword::Select),
                        s if s.eq_ignore_ascii_case("insert") => Token::Keyword(Keyword::Insert),
                        s if s.eq_ignore_ascii_case("where") => Token::Keyword(Keyword::Where),
                        _ => Token::Identifier(Ident::new(Slice::new(curr_offset, end_pos))),
                    }
                }
                c if c == '-' || c.is_numeric() => {
                    let end_pos = self.scan_until(curr_offset, |c| {
                        c.is_numeric() == false && c != '.' && c != '-'
                    });
                    self.pos += end_pos - curr_offset;

                    Token::Numeric(Slice::new(curr_offset, end_pos))
                }
                _ => Token::Unknown,
            };

            tokens.push(LocatableToken::at_position(token, curr_offset)); // todo: position is manipluated a lot. this is probably wrong a bunch.
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
        let str = String::from("select inSERt WHERE");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Keyword(Keyword::Insert),
            Token::Space,
            Token::Keyword(Keyword::Where),
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

    // TODO: This test is weird. Should this parse as a number followed by an identifier?
    //       Probably not. It should probably just fall back into a 'unknown symbol' or something,
    //       or maybe just a 'unknown keyword'?
    #[test]
    fn test_bad_numeric() {
        let str = String::from("12a0");
        let lexer = Lexer::new(&str).lex();
        let actual_without_locations = to_token_vec_without_locations(lexer.tokens);

        let expected = vec![
            Token::Numeric(Slice::new(0, 2)),
            Token::Identifier(Ident::new(Slice::new(2, 4))),
            Token::EOF,
        ];

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
