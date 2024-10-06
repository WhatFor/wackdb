use std::{cell::RefCell, fs::File, io::Error, rc::Rc};

use cli_common::ExecuteError;
use db::{DatabaseId, DatabaseInfo, FileType, DATABASE_INFO_PAGE_INDEX};
use fm::{FileId, FileManager, IdentifiedFile};
use page::PageDecoder;
use parser::ast::{Program, ServerStatement, UserStatement};

use page_cache::PageCache;

mod db;
mod fm;
mod lru;
mod page;
mod page_cache;
mod persistence;
mod server;
mod util;
use server::{CreateDatabaseError, OpenDatabaseError, OpenDatabaseResult, MASTER_DB_ID};

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
    pub file_manager: Rc<RefCell<FileManager>>,
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
        let file_manager = Rc::new(RefCell::new(FileManager::new()));
        let page_cache = PageCache::new(PAGE_CACHE_CAPACITY, Rc::clone(&file_manager));

        Engine {
            page_cache,
            file_manager,
        }
    }

    pub fn init(&self) {
        let master_db_result = server::open_or_create_master_db();

        match master_db_result {
            Ok(x) => {
                self.file_manager
                    .borrow_mut()
                    .add(FileId::new(MASTER_DB_ID, db::FileType::Primary), x.dat);

                self.file_manager
                    .borrow_mut()
                    .add(FileId::new(MASTER_DB_ID, db::FileType::Log), x.log);
            }
            Err(error) => match error {
                CreateDatabaseError::DatabaseExists(_) => {
                    println!("Master database already exists. Continuing.")
                }
                CreateDatabaseError::UnableToCreateFile(error) => {
                    println!("Unable to create database file. See: {:?}", error)
                }
                CreateDatabaseError::UnableToWrite(page_encoder_error) => {
                    println!(
                        "Unable to write to database file. See: {:?}",
                        page_encoder_error
                    )
                }
            },
        }

        match self.open_user_dbs() {
            Ok(user_dbs) => {
                for user_db in user_dbs {
                    println!("Database loaded. ID: {}", user_db.id);

                    self.file_manager
                        .borrow_mut()
                        .add(FileId::new(user_db.id, db::FileType::Primary), user_db.dat);

                    self.file_manager
                        .borrow_mut()
                        .add(FileId::new(user_db.id, db::FileType::Log), user_db.log);
                }
            }
            Err(err) => {
                panic!("Unable to open user databases. See: {:?}", err)
            }
        }

        self.validate_files();
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
            ServerStatement::CreateDatabase(s) => {
                let next_id = self.next_id();

                server::create_user_database(s, next_id)
                    .map_err(|e| StatementError::CreateDatabase(e))
                    .map(|result| {
                        self.file_manager
                            .borrow_mut()
                            .add(FileId::new(result.id, db::FileType::Primary), result.dat);

                        self.file_manager
                            .borrow_mut()
                            .add(FileId::new(result.id, db::FileType::Log), result.log);

                        // Revalidate all files
                        self.validate_files();

                        StatementResult {}
                    })
            }
        }
    }

    /// For all files in self.file_manager, validate them
    fn validate_files(&self) {
        let fm = self.file_manager.borrow();

        fm.get_all()
            .filter(|file| file.id.ty != FileType::Log)
            .for_each(|file| self.validate_file(file));
    }

    fn validate_file(&self, identifiable_file: IdentifiedFile) {
        match db::validate_data_file(identifiable_file.file) {
            Ok(_) => {
                println!(
                    "Database {}:{:?} validated successfully.",
                    identifiable_file.id.id, identifiable_file.id.ty
                );
            }
            Err(err) => match err {
                db::ValidationError::FileInfoChecksumIncorrect(checksum_result) => {
                    println!(
                        "ERR: Checksum failed for DB {}:{:?}. Expected: {:?}. Actual: {:?}.",
                        identifiable_file.id.id,
                        identifiable_file.id.ty,
                        checksum_result.expected,
                        checksum_result.actual,
                    )
                }
                db::ValidationError::FailedToOpenFileInfo => {
                    println!("Failed to open file_info")
                }
            },
        };
    }

    pub fn open_user_dbs(
        &self,
    ) -> Result<Box<impl Iterator<Item = OpenDatabaseResult> + '_>, OpenDatabaseError> {
        match persistence::find_user_databases() {
            Ok(dbs) => {
                let results = dbs.map(|db| {
                    let user_db = persistence::open_db(&db);
                    let id = self.get_db_id(&user_db.dat);

                    if id.is_err() {
                        panic!("I have no idea");
                    }

                    println!("Opening user DB: {:?}", db);

                    OpenDatabaseResult {
                        id: id.unwrap(),
                        dat: user_db.dat,
                        log: user_db.log,
                    }
                });

                Ok(Box::new(results))
            }
            Err(_) => {
                return Err(OpenDatabaseError::Err());
            }
        }
    }

    fn next_id(&self) -> DatabaseId {
        self.file_manager.borrow().next_id()
    }

    pub fn get_db_id(&self, file: &File) -> Result<DatabaseId, Error> {
        //Circumvent the page cache - can't use it until we have the db_id
        let page_bytes = persistence::read_page(&file, DATABASE_INFO_PAGE_INDEX)?;

        let page = PageDecoder::from_bytes(&page_bytes);

        let db_info = page.try_read::<DatabaseInfo>(0);

        match db_info {
            Ok(info) => Ok(info.database_id),
            Err(err) => {
                println!("Failed to read database info page. See: {:?}", err);
                Err(Error::from(std::io::Error::from(
                    std::io::ErrorKind::InvalidData,
                )))
            }
        }
    }
}
