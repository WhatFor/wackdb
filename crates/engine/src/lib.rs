use cli_common::ExecuteError;
use parser::ast::{Program, ServerStatement, UserStatement};

mod master;
mod page;
mod paging;
mod server;
use server::CreateDatabaseError;

pub struct Engine {}

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

impl Engine {
    pub fn new() -> Self {
        Engine {}
    }

    /// Initialise the entire DB server
    pub fn init(&self) {
        // We want to:
        //  - ensure system level stuff exists and is valid, like the master table.
        self.ensure_system_databases_initialised();
        //  - spin up any components we might want, like buffers, etc.
        self.ensure_system_components_initialised();
    }

    // TODO: Maybe move me
    /// Ensures the system infra is initialised
    ///  - Create system tables
    ///  - Whatever else that might be needed :)
    pub fn ensure_system_databases_initialised(&self) {
        let master_exists = server::master_database_exists();

        match master_exists {
            true => println!("Master database exists."),
            false => {
                println!("Creating master database...");
                let master_db_create_result = server::create_master_database();

                match master_db_create_result {
                    Ok(_) => {
                        println!("Master database created successfully.");
                    }
                    Err(_) => {
                        panic!("Failed to create master database.");
                    }
                }
            }
        }

        let master_validate = server::validate_master_database();

        match master_validate {
            Ok(_) => {
                println!("Master database validated successfully.");
            }
            Err(_) => {
                panic!("Failed to validate master database.");
            }
        }
    }

    // TODO: Maybe move me
    /// Ensures the system components are initialised
    pub fn ensure_system_components_initialised(&self) {
        // We:
        //  Do whatever we need to do :)
    }

    pub fn execute(&self, prog: &Program) -> Result<ExecuteResult, ExecuteError> {
        let mut results = vec![];
        let mut errors = vec![];

        match prog {
            Program::Statements(statements) => {
                // TODO: We're looping through distinct statements, which if we supported transactions would need some care here.
                for statement in statements {
                    let result = match statement {
                        parser::ast::Statement::User(user_statement) => {
                            self.execute_user_statement(user_statement)
                        }
                        parser::ast::Statement::Server(server_statement) => {
                            self.execute_server_statement(server_statement)
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
        &self,
        statement: &UserStatement,
    ) -> Result<StatementResult, StatementError> {
        dbg!(&statement);
        Ok(StatementResult {})
    }

    /// Serverland statements. For example, CREATE DATABASE.
    pub fn execute_server_statement(
        &self,
        statement: &ServerStatement,
    ) -> Result<StatementResult, StatementError> {
        match statement {
            ServerStatement::CreateDatabase(s) => server::create_user_database(s),
        }
    }
}
