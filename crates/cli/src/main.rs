use env_logger::Env;
use repl::Repl;
use std::env::args;

mod repl;

fn init_logger() {
    let env = Env::default().default_filter_or("TRACE");

    env_logger::Builder::from_env(env)
        .format_target(false)
        .init();
}

const FILE_EXT: &str = ".wak";

fn main() {
    init_logger();

    log::info!("Welcome to WackDB");
    log::info!("-----------------");

    let args: Vec<String> = args().collect();
    let repl = Repl::new();

    if args.len() <= 1 {
        repl.run();
    }

    // TODO: Probably swap this to a cmdline flag for safety (e.g. -f or -i)
    let looks_like_file = args[1].to_lowercase().ends_with(FILE_EXT);

    if looks_like_file {
        repl.eval_file(&args[1])
    } else {
        repl.eval_command(&args[1])
    };
}
