use cli_common::ExecuteError;
use parser::ast::{Program, ServerStatement, UserStatement};

mod server;
use server::CreateDatabaseError;

#[derive(Debug)]
pub struct ExecuteResult {
    pub results: Vec<StatementResult>,
    pub errors: Vec<StatementError>,
}

#[derive(Debug)]
pub struct StatementResult {}

#[derive(Debug)]
pub enum StatementError {
    CreateDatabase(CreateDatabaseError),
}

pub fn execute(prog: &Program) -> Result<ExecuteResult, ExecuteError> {
    let mut results = vec![];
    let mut errors = vec![];

    match prog {
        Program::Statements(statements) => {
            // TODO: We're looping through distinct statements,
            // which if we supported transactions would need some care here.

            for statement in statements {
                let result = match statement {
                    parser::ast::Statement::User(user_statement) => {
                        execute_user_statement(user_statement)
                    }
                    parser::ast::Statement::Server(server_statement) => {
                        execute_server_statement(server_statement)
                    }
                };

                match result {
                    Ok(statement_result) => results.push(statement_result),
                    Err(statement_error) => errors.push(statement_error),
                }
            }
        }
        Program::Empty => {
            println!("Warning: No statements found.");
        }
    }

    Ok(ExecuteResult { results, errors })
}

/// Userland statements. For example, SELECT, INSERT, etc.
pub fn execute_user_statement(
    statement: &UserStatement,
) -> Result<StatementResult, StatementError> {
    dbg!(&statement);
    Ok(StatementResult {})
}

/// Serverland statements. For example, CREATE DATABASE.
pub fn execute_server_statement(
    statement: &ServerStatement,
) -> Result<StatementResult, StatementError> {
    match statement {
        ServerStatement::CreateDatabase(s) => server::create_database(s),
    }
}
