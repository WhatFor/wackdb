use std::{
    env, fs,
    io::{stdin, stdout, Write},
    process::exit,
};

use cli_common::ParseError;
use lexer::Lexer;
use parser::Parser;

const FILE_EXT: &str = ".wak";

fn main() {
    println!("WackDB");

    let args: Vec<String> = env::args().collect();

    if args.len() <= 1 {
        repl();
    }

    // TODO: Probably swap this to a cmdline flag for safety (e.g. -f or -i)
    let looks_like_file = args[1].to_lowercase().ends_with(FILE_EXT);

    let _ = match looks_like_file {
        true => eval_file(&args[1]),
        false => eval_command(&args[1]),
    };
}

fn eval_command(input: &str) -> CommandResult {
    let input_str = input.to_string();
    let lexer = Lexer::new(&input_str);
    let lex_result = lexer.lex();

    let mut parser = Parser::new(lex_result.tokens, &input_str);
    let parse_result = parser.parse();

    dbg!(&parse_result);

    match parse_result {
        Ok(_) => CommandResult::Ok,
        Err(e) => CommandResult::Error(e),
    }
}

fn eval_file(file: &str) -> CommandResult {
    match fs::read_to_string(file) {
        Ok(file_content) => eval_command(&file_content),
        Err(_) => CommandResult::Failed(String::from("Failed to open file.")),
    }
}

fn repl() {
    loop {
        print_prompt();

        let mut buf = String::new();
        match stdin().read_line(&mut buf) {
            Ok(_) => {
                let command_status = handle_repl_command(buf);

                match command_status {
                    ReplResult::Ok(command_result) => match command_result {
                        CommandResult::_UnrecognisedCommand => {
                            println!("Error! Unrecognised command.");
                        }
                        CommandResult::Failed(err) => {
                            println!("Program Error: {err}");
                        }
                        CommandResult::Error(err) => {
                            for e in err {
                                let message = e.kind;
                                let pos = e.position;
                                println!("Syntax Error: {message} (Position {pos})");
                            }
                        }
                        CommandResult::Ok => {}
                    },
                    ReplResult::Help => {
                        println!("Sorry, you're on your own.");
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

///
/// Handle user input via REPL. Input is assumed
/// to be validated as a command by this point.
/// This will either eval a command or
/// short-circuit for a meta command.
fn handle_repl_command(buf: String) -> ReplResult {
    let fmt_buf = buf.trim();
    let is_meta = is_meta_command(&fmt_buf);

    match is_meta {
        true => handle_meta_command(&fmt_buf),
        false => {
            let command_result = eval_command(&fmt_buf);
            ReplResult::Ok(command_result)
        }
    }
}

fn is_meta_command(buf: &str) -> bool {
    buf.starts_with(".")
}

fn handle_meta_command(buf: &str) -> ReplResult {
    match buf.to_lowercase().as_ref() {
        ".exit" => ReplResult::Exit,
        ".help" => ReplResult::Help,
        "" => ReplResult::NoInput,
        _ => ReplResult::UnrecognisedInput,
    }
}

fn print_prompt() {
    print!("> ");
    stdout().flush().unwrap();
}

#[derive(Debug)]
enum ReplResult {
    Exit,
    Help,
    NoInput,
    UnrecognisedInput,
    Ok(CommandResult),
}

#[derive(Debug)]
enum CommandResult {
    _UnrecognisedCommand,
    Error(Vec<ParseError>),
    Failed(String),
    Ok,
}
