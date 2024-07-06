use repl::Repl;
use std::env::args;

mod repl;

fn main() {
    println!("WackDB");

    let args: Vec<String> = args().collect();
    let repl = Repl::new();

    if args.len() <= 1 {
        repl.run();
    }

    // TODO: Probably swap this to a cmdline flag for safety (e.g. -f or -i)
    const FILE_EXT: &str = ".wak";
    let looks_like_file = args[1].to_lowercase().ends_with(FILE_EXT);

    let _ = match looks_like_file {
        true => repl.eval_file(&args[1]),
        false => repl.eval_command(&args[1]),
    };
}
