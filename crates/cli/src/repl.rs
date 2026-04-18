use std::{
    io::{stdin, stdout, Write},
    process::exit,
};

use crate::executor::CommandResult;
use engine::engine::Engine;

pub struct Repl {
    engine: Engine,
}

#[derive(Debug)]
pub enum Result {
    Exit,
    Help,
    NoInput,
    UnrecognisedInput,
    Ok(CommandResult),
}

impl Repl {
    pub fn new() -> Self {
        let mut engine = Engine::default();
        engine.init();

        Repl { engine }
    }

    pub fn run(&mut self) {
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
                                        println!("No columns selected/found");
                                        continue;
                                    }

                                    if result.result_set.rows.is_empty() {
                                        println!("No results found");
                                        continue;
                                    }

                                    // TODO: tabled assumes a certain format of input. Maybe I don't want to use it.
                                    // let repl_output = tabled::Table::new(result.result_set.rows)
                                    //     .with(tabled::settings::Disable::row(
                                    //         tabled::settings::object::Rows::first(),
                                    //     ))
                                    //     .with(tabled::settings::Rotate::Top)
                                    //     .with(tabled::settings::Rotate::Right)
                                    //     .to_string();
                                    // println!("{repl_output}");
                                    //
                                    print!("|");

                                    for header in result.result_set.columns {
                                        print!(" {} |", header.alias.unwrap_or(header.name));
                                    }

                                    println!();

                                    for row in result.result_set.rows {
                                        println!(
                                            "| {} |",
                                            row.iter()
                                                .map(|r| r.to_string())
                                                .collect::<Vec<String>>()
                                                .join(" | ")
                                        );
                                    }
                                }
                            }
                        },
                        Result::Help => {
                            println!("Sorry, you're on your own.");
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

    /// Handle user input via REPL. Input is assumed
    /// to be validated as a command by this point.
    /// This will either eval a command or
    /// short-circuit for a meta command.
    fn handle_repl_command(&mut self, buf: &str) -> Result {
        let fmt_buf = buf.trim();

        if Repl::is_meta_command(fmt_buf) {
            Repl::handle_meta_command(fmt_buf)
        } else {
            let command_result = crate::executor::eval_command(&mut self.engine, fmt_buf);
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
            "" => Result::NoInput,
            _ => Result::UnrecognisedInput,
        }
    }

    fn print_prompt() {
        print!("> ");
        stdout().flush().unwrap();
    }
}
