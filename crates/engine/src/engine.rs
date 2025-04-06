use crate::db::{self, DatabaseId, DatabaseInfo, FileType, DATABASE_INFO_PAGE_INDEX};
use crate::fm::{FileId, FileManager, IdentifiedFile};
use crate::index_pager::IndexPager;
use crate::page::PageDecoder;
use crate::page_cache::PageCache;
use crate::persistence;
use crate::server::{self, OpenDatabaseResult, MASTER_DB_ID, MASTER_NAME};
use crate::vm::VirtualMachine;

use anyhow::Result;
use parser::ast::{Program, ServerStatement, UserStatement};
use std::fmt::Display;
use std::{cell::RefCell, fs::File, rc::Rc};
use tabled::Tabled;

/// System wide Consts
pub const DATA_FILE_EXT: &str = "wak";
pub const LOG_FILE_EXT: &str = "wal";
pub const CURRENT_DATABASE_VERSION: u8 = 1;

//pub const PAGE_CACHE_CAPACITY: usize = 131_072; // 1GB
pub const PAGE_CACHE_CAPACITY: usize = 10; // Test

pub const PAGE_SIZE_BYTES: u16 = 8192; // 2^13
pub const PAGE_SIZE_BYTES_USIZE: usize = 8192; // 2^13

pub const PAGE_HEADER_SIZE_BYTES: u16 = 32;
pub const PAGE_HEADER_SIZE_BYTES_USIZE: usize = 32;

pub const WACK_DIRECTORY: &str = "data"; // TODO: Hardcoded for now. See /docs/assumptions.

pub struct Engine {
    page_cache: Rc<RefCell<PageCache>>,
    file_manager: Rc<RefCell<FileManager>>,
    vm: VirtualMachine,
    index_pager: Rc<RefCell<IndexPager>>,
}

#[derive(Debug)]
pub struct ExecuteResult {
    pub results: Vec<StatementResult>,
    pub errors: Vec<anyhow::Error>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct StatementResult {
    pub result_set: ResultSet,
}

impl Default for StatementResult {
    fn default() -> Self {
        StatementResult {
            result_set: ResultSet { columns: vec![] },
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ResultSet {
    pub columns: Vec<ColumnResult>,
}

#[derive(Debug, PartialEq, Clone, Tabled)]
pub struct ColumnResult {
    pub name: String,
    pub value: ExprResult,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExprResult {
    Int(u32),
    Byte(u8),
    Bool(bool),
    String(String),
    Null,
}

impl Display for ExprResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExprResult::Int(x) => write!(f, "{}", x),
            ExprResult::Byte(x) => write!(f, "{}", x),
            ExprResult::Bool(x) => write!(f, "{}", x),
            ExprResult::String(x) => write!(f, "{}", x),
            ExprResult::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug)]
pub enum OpenDatabaseError {
    Err(),
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Self {
        let file_manager = Rc::new(RefCell::new(FileManager::new()));

        let page_cache = Rc::new(RefCell::new(PageCache::new(
            PAGE_CACHE_CAPACITY,
            Rc::clone(&file_manager),
        )));

        let index_pager = Rc::new(RefCell::new(IndexPager::new(Rc::clone(&page_cache))));
        let vm = VirtualMachine::new(Rc::clone(&file_manager), Rc::clone(&index_pager));

        Engine {
            page_cache,
            file_manager,
            vm,
            index_pager,
        }
    }

    pub fn init(&self) {
        let master_db_result = server::open_or_create_master_db();

        match master_db_result {
            Ok(x) => {
                let mut fm = self.file_manager.borrow_mut();

                fm.add(
                    FileId::new(MASTER_DB_ID, MASTER_NAME.into(), db::FileType::Primary),
                    x.dat,
                    x.allocated_page_count,
                );

                fm.add(
                    FileId::new(MASTER_DB_ID, MASTER_NAME.into(), db::FileType::Log),
                    x.log,
                    0,
                );
            }
            Err(error) => {
                log::error!("Error creating/reading master: {:?}", error);
                return;
            }
        }

        if let Err(e) = server::ensure_master_tables_exist(self.file_manager.borrow_mut()) {
            log::error!("Error initialising master tables: {:?}", e);
            return;
        }

        match self.open_user_dbs() {
            Ok(user_dbs) => {
                for user_db in user_dbs {
                    log::info!(
                        "Database {} loaded, containing {} pages.",
                        user_db.id,
                        user_db.allocated_page_count
                    );

                    let mut fm = self.file_manager.borrow_mut();

                    fm.add(
                        FileId::new(
                            user_db.id,
                            user_db.name.clone().into(),
                            db::FileType::Primary,
                        ),
                        user_db.dat,
                        user_db.allocated_page_count,
                    );

                    fm.add(
                        FileId::new(user_db.id, user_db.name.into(), db::FileType::Log),
                        user_db.log,
                        0,
                    );
                }
            }
            Err(err) => {
                log::error!("Error opening user databases: {:?}", err);
                return;
            }
        }

        self.validate_files();
    }

    pub fn execute(&self, prog: &Program) -> Result<ExecuteResult> {
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
                log::warn!("Warning: No statements found.");
            }
        }

        Ok(ExecuteResult { results, errors })
    }

    /// Userland statements. For example, SELECT, INSERT, etc.
    pub fn execute_user_statement(&self, statement: &UserStatement) -> Result<StatementResult> {
        dbg!(&statement);
        match statement {
            UserStatement::Select(select_expression_body) => {
                log::info!("Selecting: {:?}", select_expression_body);
                self.vm.execute_user_statement(statement)
            }
            UserStatement::Update => {
                log::info!("Updating");
                Ok(StatementResult::default())
            }
            UserStatement::Insert => {
                log::info!("Inserting");
                Ok(StatementResult::default())
            }
            UserStatement::Delete => {
                log::info!("Deleting");
                Ok(StatementResult::default())
            }
            UserStatement::CreateTable(_create_table_body) => {
                log::info!("Creating Table");
                Ok(StatementResult::default())
            }
        }
    }

    /// Serverland statements. For example, CREATE DATABASE.
    pub fn execute_server_statement(&self, statement: &ServerStatement) -> Result<StatementResult> {
        match statement {
            ServerStatement::CreateDatabase(s) => {
                let next_id = self.next_id();

                let result = server::create_user_database(s, next_id)?;

                self.file_manager.borrow_mut().add(
                    FileId::new(result.id, result.name.clone(), db::FileType::Primary),
                    result.dat,
                    result.allocated_page_count,
                );

                self.file_manager.borrow_mut().add(
                    FileId::new(result.id, result.name, db::FileType::Log),
                    result.log,
                    0,
                );

                // Revalidate all files
                self.validate_files();

                Ok(StatementResult::default())
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
                log::info!(
                    "Database {}:{:?} validated successfully.",
                    identifiable_file.id.id,
                    identifiable_file.id.ty
                );
            }
            Err(err) => log::error!(
                "Database {}:{:?} failed validation: {:?}",
                identifiable_file.id.id,
                identifiable_file.id.ty,
                err
            ),
        };
    }

    pub fn open_user_dbs(&self) -> Result<Box<impl Iterator<Item = OpenDatabaseResult> + '_>> {
        let dbs = persistence::find_user_databases()?;

        let results = dbs.map(|db| {
            let user_db = persistence::open_db(&db);
            let allocated_page_count = persistence::get_allocated_page_count(&user_db.dat);
            let id = self.get_db_id(&user_db.dat);

            if id.is_err() {
                panic!("I have no idea");
            }

            log::info!("Opening user DB: {:?}", db);

            OpenDatabaseResult {
                id: id.unwrap(),
                name: db,
                dat: user_db.dat,
                log: user_db.log,
                allocated_page_count,
            }
        });

        Ok(Box::new(results))
    }

    fn next_id(&self) -> DatabaseId {
        self.file_manager.borrow().next_id()
    }

    pub fn get_db_id(&self, file: &File) -> Result<DatabaseId> {
        //Circumvent the page cache - can't use it until we have the db_id
        let page_bytes = persistence::read_page(file, DATABASE_INFO_PAGE_INDEX)?;

        let page = PageDecoder::from_bytes(&page_bytes);

        let db_info = page.try_read::<DatabaseInfo>(0)?;

        Ok(db_info.database_id)
    }
}
