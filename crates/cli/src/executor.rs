use anyhow::Error;
use cli_common::{ParseError, StatementResult};
use engine::engine::Engine;
use lexer::Lexer;
use parser::Parser;

#[derive(Debug)]
pub enum CommandResult {
    _UnrecognisedCommand,
    ParseError(Vec<ParseError>),
    ExecuteError(Error),
    Ok(Vec<StatementResult>),
}

pub fn eval_command(engine: &Engine, input: &str) -> CommandResult {
    let input_str = input.to_string();

    let lexer = Lexer::new(&input_str);
    let lex_result = lexer.lex();

    log::trace!("Lexing complete. Tokens: {:?}", lex_result.tokens);

    let mut parser = Parser::new(lex_result.tokens, &input_str);
    let parse_result = parser.parse();

    match parse_result {
        Ok(ast) => {
            log::trace!("Parsing complete. Tokens: {:?}", ast);
            let execute_result = engine.execute(&ast);

            match execute_result {
                Ok(ok_result) => {
                    for err in ok_result.errors {
                        println!("{err:?}");
                    }

                    CommandResult::Ok(ok_result.results)
                }
                Err(err) => CommandResult::ExecuteError(err),
            }
        }
        Err(e) => CommandResult::ParseError(e),
    }
}
