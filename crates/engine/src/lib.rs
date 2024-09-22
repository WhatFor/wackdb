use cli_common::ExecuteError;
use parser::ast::{Program, ServerStatement, UserStatement};

use page_cache::PageCache;

mod db;
mod lru;
mod page;
mod page_cache;
mod persistence;
mod server;
mod util;
use server::CreateDatabaseError;

/// System wide Consts
pub const DATA_FILE_EXT: &str = ".wak";
pub const LOG_FILE_EXT: &str = ".wal";
pub const CURRENT_DATABASE_VERSION: u8 = 1;

//pub const PAGE_CACHE_CAPACITY: usize = 131_072; // 1GB
pub const PAGE_CACHE_CAPACITY: usize = 10; // Test

pub const PAGE_SIZE_BYTES: u16 = 8192; // 2^13
pub const PAGE_SIZE_BYTES_USIZE: usize = 8192; // 2^13

pub const PAGE_HEADER_SIZE_BYTES: u16 = 32;
pub const PAGE_HEADER_SIZE_BYTES_USIZE: usize = 32;

pub const WACK_DIRECTORY: &str = "data"; // TODO: Hardcoded for now. See /docs/assumptions.

pub struct Engine {
    pub page_cache: PageCache,
}

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
        Engine {
            page_cache: PageCache::new(PAGE_CACHE_CAPACITY),
        }
    }

    pub fn init(&self) {
        server::ensure_system_databases_initialised();
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
            ServerStatement::CreateDatabase(s) => server::create_user_database(s)
                .map_err(|e| StatementError::CreateDatabase(e))
                .map(|_| StatementResult {}),
        }
    }
}
