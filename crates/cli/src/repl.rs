use std::{
    io::{stdin, stdout, Write},
    process::exit,
};

use anyhow::Error;
use cli_common::ParseError;
use engine::Engine;
use lexer::Lexer;
use parser::Parser;

pub struct Repl {
    engine: Engine,
}

#[derive(Debug)]
pub enum ReplResult {
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
    Ok,
}

impl Repl {
    pub fn new() -> Self {
        let engine = Engine::new();
        engine.init();

        Repl { engine }
    }

    pub fn run(&self) {
        loop {
            self.print_prompt();

            let mut buf = String::new();
            match stdin().read_line(&mut buf) {
                Ok(_) => {
                    let command_status = self.handle_repl_command(buf);

                    match command_status {
                        ReplResult::Ok(command_result) => match command_result {
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
                            CommandResult::Ok => {
                                // TODO: https://github.com/zhiburt/tabled
                                println!("OK!");
                            }
                        },
                        ReplResult::Help => {
                            println!("Sorry, you're on your own.");
                        }
                        ReplResult::RunDebug => {
                            self.eval_command("CREATE TABLE TestTable (Id INT, Age INT);");
                            self.eval_command("INSERT INTO TestTable (Id, Age) VALUES (1, 20);");
                            self.eval_command("SELECT * FROM TestTable;");
                        }
                        ReplResult::UnrecognisedInput => {
                            println!("Error! Command not recognised.");
                        }
                        ReplResult::Exit => {
                            println!("Goodbye.");
                            break;
                        }
                        ReplResult::NoInput => {
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
                            println!("{err:?}")
                        }

                        // todo: bit of a mess of error types
                        CommandResult::Ok
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
    fn handle_repl_command(&self, buf: String) -> ReplResult {
        let fmt_buf = buf.trim();
        let is_meta = self.is_meta_command(&fmt_buf);

        match is_meta {
            true => self.handle_meta_command(&fmt_buf),
            false => {
                let command_result = self.eval_command(&fmt_buf);
                ReplResult::Ok(command_result)
            }
        }
    }

    fn is_meta_command(&self, buf: &str) -> bool {
        buf.starts_with(".")
    }

    fn handle_meta_command(&self, buf: &str) -> ReplResult {
        match buf.to_lowercase().as_ref() {
            ".exit" => ReplResult::Exit,
            ".help" => ReplResult::Help,
            ".dbg" => ReplResult::RunDebug,
            "" => ReplResult::NoInput,
            _ => ReplResult::UnrecognisedInput,
        }
    }

    fn print_prompt(&self) {
        print!("> ");
        stdout().flush().unwrap();
    }
}
