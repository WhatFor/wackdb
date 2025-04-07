use std::{
    io::{stdin, stdout, Write},
    process::exit,
};

use anyhow::Error;
use cli_common::ParseError;
use engine::engine::{Engine, StatementResult};
use lexer::Lexer;
use parser::Parser;

pub struct Repl {
    engine: Engine,
}

#[derive(Debug)]
pub enum Result {
    Exit,
    Help,
    RunDebug,
    NoInput,
    UnrecognisedInput,
    Ok(CommandResult),
}

#[derive(Debug)]
pub enum CommandResult {
    _UnrecognisedCommand,
    ParseError(Vec<ParseError>),
    ExecuteError(Error),
    Failed(String),
    Ok(Vec<StatementResult>),
}

impl Repl {
    pub fn new() -> Self {
        let engine = Engine::new();
        engine.init();

        Repl { engine }
    }

    pub fn run(&self) {
        loop {
            Repl::print_prompt();

            let mut buf = String::new();
            match stdin().read_line(&mut buf) {
                Ok(_) => {
                    let command_status = self.handle_repl_command(&buf);

                    match command_status {
                        Result::Ok(command_result) => match command_result {
                            CommandResult::_UnrecognisedCommand => {
                                println!("Error! Unrecognised command.");
                            }
                            CommandResult::Failed(err) => {
                                println!("Program Error: {err}");
                            }
                            CommandResult::ParseError(err) => {
                                for e in err {
                                    let message = e.kind;
                                    let pos = e.position;
                                    println!("Syntax Error: {message:?} (Position {pos})");
                                }
                            }
                            CommandResult::ExecuteError(err) => {
                                println!("Execution Error: {err:?}");
                            }
                            CommandResult::Ok(results) => {
                                for result in results {
                                    if result.result_set.columns.is_empty() {
                                        println!("No results");
                                        continue;
                                    }

                                    let repl_output = tabled::Table::new(result.result_set.columns)
                                        .with(tabled::settings::Disable::row(
                                            tabled::settings::object::Rows::first(),
                                        ))
                                        .with(tabled::settings::Rotate::Top)
                                        .with(tabled::settings::Rotate::Right)
                                        .to_string();

                                    println!("{repl_output}");
                                }
                            }
                        },
                        Result::Help => {
                            println!("Sorry, you're on your own.");
                        }
                        Result::RunDebug => {
                            self.eval_command("CREATE TABLE TestTable (Id INT, Age INT);");
                            self.eval_command("INSERT INTO TestTable (Id, Age) VALUES (1, 20);");
                            self.eval_command("SELECT * FROM TestTable;");
                        }
                        Result::UnrecognisedInput => {
                            println!("Error! Command not recognised.");
                        }
                        Result::Exit => {
                            println!("Goodbye.");
                            break;
                        }
                        Result::NoInput => {
                            continue;
                        }
                    };
                }
                Err(err) => eprintln!("{err}"),
            }
        }

        exit(0);
    }

    pub fn eval_command(&self, input: &str) -> CommandResult {
        let input_str = input.to_string();

        let lexer = Lexer::new(&input_str);
        let lex_result = lexer.lex();

        let mut parser = Parser::new(lex_result.tokens, &input_str);
        let parse_result = parser.parse();

        match parse_result {
            Ok(ast) => {
                let execute_result = self.engine.execute(&ast);

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

    pub fn eval_file(&self, file: &str) -> CommandResult {
        match std::fs::read_to_string(file) {
            Ok(file_content) => self.eval_command(&file_content),
            Err(_) => CommandResult::Failed(String::from("Failed to open file.")),
        }
    }

    /// Handle user input via REPL. Input is assumed
    /// to be validated as a command by this point.
    /// This will either eval a command or
    /// short-circuit for a meta command.
    fn handle_repl_command(&self, buf: &str) -> Result {
        let fmt_buf = buf.trim();

        if Repl::is_meta_command(fmt_buf) {
            Repl::handle_meta_command(fmt_buf)
        } else {
            let command_result = self.eval_command(fmt_buf);
            Result::Ok(command_result)
        }
    }

    fn is_meta_command(buf: &str) -> bool {
        buf.starts_with('.')
    }

    fn handle_meta_command(buf: &str) -> Result {
        match buf.to_lowercase().as_ref() {
            ".exit" | ".quit" | ".close" => Result::Exit,
            ".help" | ".h" | "?" | ".?" => Result::Help,
            ".dbg" => Result::RunDebug,
            "" => Result::NoInput,
            _ => Result::UnrecognisedInput,
        }
    }

    fn print_prompt() {
        print!("> ");
        stdout().flush().unwrap();
    }
}
