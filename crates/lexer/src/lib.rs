use token::{*};
pub mod token;

pub struct Lexer {
    buf: String,
    chars: Vec<(usize, char)>,
    len: usize,
    pos: usize,
}

pub struct LexResult {
    pub tokens: Vec<Token>,
    pub buf: String,
}

impl Lexer {
    pub fn new(buf: String) -> Lexer {
        let len = buf.len();
        let chars = buf.char_indices().collect();
        Lexer {
            buf,
            chars,
            len,
            pos: 0,
        }
    }

    pub fn lex(mut self) -> LexResult {
        let mut tokens = Vec::new();

        loop {
            if self.pos >= self.len {
                tokens.push(Token::EOF);
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
                    let end_pos = self.scan_until(
                        curr_offset,
                        |c| c == ' ' || c == ',');

                    let slice = &self.buf[curr_offset..end_pos];
                    self.pos += slice.len();

                    match slice {
                        s if s.eq_ignore_ascii_case("select") => {
                            Token::Keyword(Keyword::Select)
                        }
                        s if s.eq_ignore_ascii_case("insert") => {
                            Token::Keyword(Keyword::Insert)
                        }
                        s if s.eq_ignore_ascii_case("where") => {
                            Token::Keyword(Keyword::Where)
                        }
                        _ => Token::Identifier(Identifier::Table(Slice::new(curr_offset, end_pos))),
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

            //println!("{:?}", token);
            //std::thread::sleep(core::time::Duration::from_millis(1000));

            tokens.push(token);
        }

        LexResult {
            buf: self.buf,
            tokens,
        }
    }

    /// Given a start point and a char to find, scan until the char is found
    /// and return the end_offset.
    fn scan_to(&self, start_offset: usize, char: char) -> usize {
        let mut cursor = start_offset;

        loop {
            if cursor >= self.len {
                break;
            }

            let (_, ch) = self.chars[cursor];

            // todo: char doesn't quite work. lots of things can end a string.
            //       e.g. tabs, newlines, plus, minus, divide, etc.
            //       nor will it handle dots or hex stuff in numbers.
            if ch == char {
                break;
            }

            cursor += 1;
        }

        cursor
    }

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
    use crate::{*};

    #[test]
    fn test_simple_tokens() {
        let str = ",.(){}[];: \n\r";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

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

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_arithmetic_tokens() {
        let str = "*/%-+";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Arithmetic(Arithmetic::Multiply),
            Token::Arithmetic(Arithmetic::Divide),
            Token::Arithmetic(Arithmetic::Modulo),
            Token::Arithmetic(Arithmetic::Minus),
            Token::Arithmetic(Arithmetic::Plus),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_keywords() {
        let str = "select inSERt WHERE";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Keyword(Keyword::Insert),
            Token::Space,
            Token::Keyword(Keyword::Where),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_keyword_list() {
        let str = "select hello, world";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Keyword(Keyword::Select),
            Token::Space,
            Token::Identifier(Identifier::Table(Slice::new(7, 12))),
            Token::Comma,
            Token::Space,
            Token::Identifier(Identifier::Table(Slice::new(14, 19))),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_keywords_not_greedy() {
        let str = "selecting";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        // Should not match on Token::Keyword for Select!
        let expected = vec![
            Token::Identifier(Identifier::Table(Slice::new(0, 9))),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_multiline() {
        let str = "*
/

-";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Arithmetic(Arithmetic::Multiply),
            Token::NewLine,
            Token::Arithmetic(Arithmetic::Divide),
            Token::NewLine,
            Token::NewLine,
            Token::Arithmetic(Arithmetic::Minus),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_numeric() {
        let str = "12 4";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Numeric(Slice::new(0, 2)),
            Token::Space,
            Token::Numeric(Slice::new(3, 4)),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    // TODO: This test is weird. Should this parse as a number followed by an identifier?
    //       Probably not. It should probably just fall back into a 'unknown symbol' or something,
    //       or maybe just a 'unknown keyword'?
    #[test]
    fn test_bad_numeric() {
        let str = "12a0";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Numeric(Slice::new(0, 2)),
            Token::Identifier(Identifier::Table(Slice::new(2, 4))),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_numeric_negative() {
        let str = "-12 4";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Numeric(Slice::new(0, 3)),
            Token::Space,
            Token::Numeric(Slice::new(4, 5)),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_numeric_float() {
        let str = "12.1 1.9";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Numeric(Slice::new(0, 4)),
            Token::Space,
            Token::Numeric(Slice::new(5, 8)),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_basic_insert() {
        let str = "insert users 'John', 'Doe'";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let expected = vec![
            Token::Keyword(Keyword::Insert),
            Token::Space,
            Token::Identifier(Identifier::Table(Slice::new(7, 12))),
            Token::Space,
            Token::Value(Value::SingleQuoted(Slice::new(14, 18))),
            Token::Comma,
            Token::Space,
            Token::Value(Value::SingleQuoted(Slice::new(22, 25))),
            Token::EOF,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_string_indexing() {
        let str = "insert users ";
        let lexer = Lexer::new(str.into()).lex();
        let actual = lexer.tokens;

        let identifier_str = match &actual[2] {
            Token::Identifier(Identifier::Table(x)) => Some(&str[x.start..x.end]),
            _ => None,
        };

        assert_ne!(identifier_str, None);
        assert_eq!(identifier_str.unwrap(), "users");
    }
}