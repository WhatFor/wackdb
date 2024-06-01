use std::vec;

use crate::core::parser::parser::Program;

use super::lexer::Token;

pub mod parser;

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

    pub fn parse(&mut self) -> Vec<Program> {
        let len = self.tokens.len();

        //let mut st = Arena::new();

        loop {
            if self.curr_pos >= len {
                break;
            }

            // let node = match self.tokens[self.curr_pos] {
            //     _ => Program::new(),
            // };

            // todo: build the tree. probs need recursion...?

            self.curr_pos += 1;

            // st.add_node(node);
        }

        // println!("Nodes: {:?}", st.collect());

        // todo: lifetimes?
        //st.collect()
        vec![]
    }
}
