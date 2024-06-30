use cli_common::ExecuteError;
use parser::ast::Program;

pub fn execute(prog: Program) -> Result<(), ExecuteError> {
    dbg!(prog);

    Ok(())
}
