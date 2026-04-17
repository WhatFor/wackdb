use crate::db::{self, DatabaseId, DatabaseInfo, FileType, DATABASE_INFO_PAGE_INDEX};
use crate::fm::{FileId, FileManager, IdentifiedFile};
use crate::page::PageDecoder;
use crate::page_cache::PageCache;
use crate::persistence;
use crate::server::{self, OpenDatabaseResult, MASTER_DB_ID, MASTER_NAME};
use crate::vm::VirtualMachine;

use anyhow::Result;
use cli_common::{ExecuteResult, StatementResult};
use parser::ast::{Program, Statement};
use std::fs::File;

pub struct Engine {
    vm: VirtualMachine,
    storage: Storage,
}

pub struct Storage {
    pub page_cache: PageCache,
    pub file_manager: FileManager,
}

impl Default for Engine {
    fn default() -> Self {
        let vm = VirtualMachine::default();
        let page_cache = PageCache::default();
        let file_manager = FileManager::default();

        Engine {
            vm,
            storage: Storage {
                page_cache,
                file_manager,
            },
        }
    }
}

impl Engine {
    pub fn init(&mut self) {
        let master_db_result = server::open_or_create_master_db();

        match master_db_result {
            Ok(x) => {
                self.storage.file_manager.add(
                    FileId::new(MASTER_DB_ID, MASTER_NAME.into(), db::FileType::Primary),
                    x.dat,
                    x.allocated_page_count,
                );

                self.storage.file_manager.add(
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

        if let Err(e) = server::ensure_master_tables_exist(&mut self.storage.file_manager) {
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

                    self.storage.file_manager.add(
                        FileId::new(
                            user_db.id,
                            user_db.name.clone().into(),
                            db::FileType::Primary,
                        ),
                        user_db.dat,
                        user_db.allocated_page_count,
                    );

                    self.storage.file_manager.add(
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

    pub fn execute(&mut self, prog: &Program) -> Result<ExecuteResult> {
        let mut results = vec![];
        let mut errors = vec![];

        match prog {
            Program::Statements(statements) => {
                // TODO: We're looping through distinct statements, which if we supported transactions would need some care here.
                for statement in statements {
                    match self.execute_statement(statement) {
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

    // TODO: This is weird - we have arms for different status types but then just pass to `vm.execute_statement`.
    pub fn execute_statement(&mut self, statement: &Statement) -> Result<StatementResult> {
        dbg!(&statement);
        match statement {
            Statement::Select(select_expression_body) => {
                log::info!("Selecting: {:?}", select_expression_body);
                self.vm.execute_statement(statement, &mut self.storage)
            }
            Statement::Update => {
                log::info!("Updating");
                Ok(StatementResult::default())
            }
            Statement::Insert => {
                log::info!("Inserting");
                Ok(StatementResult::default())
            }
            Statement::Delete => {
                log::info!("Deleting");
                Ok(StatementResult::default())
            }
            Statement::CreateTable(_create_table_body) => {
                log::info!("Creating Table");
                Ok(StatementResult::default())
            }
            Statement::CreateDatabase(s) => {
                let next_id = self.next_id();

                let result = server::create_user_database(s, next_id)?;

                self.storage.file_manager.add(
                    FileId::new(result.id, result.name.clone(), db::FileType::Primary),
                    result.dat,
                    result.allocated_page_count,
                );

                self.storage.file_manager.add(
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

    fn validate_files(&self) {
        self.storage
            .file_manager
            .get_all()
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

    pub fn open_user_dbs(&self) -> Result<Vec<OpenDatabaseResult>> {
        let dbs = persistence::find_user_databases()?;

        let results = dbs
            .map(|db| {
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
            })
            .collect();

        Ok(results)
    }

    fn next_id(&self) -> DatabaseId {
        self.storage.file_manager.next_file_id()
    }

    pub fn get_db_id(&self, file: &File) -> Result<DatabaseId> {
        //Circumvent the page cache - can't use it until we have the db_id
        let page_bytes = persistence::read_page(file, DATABASE_INFO_PAGE_INDEX)?;

        let page = PageDecoder::from_bytes(&page_bytes);

        let db_info = page.try_read::<DatabaseInfo>(0)?;

        Ok(db_info.database_id)
    }
}
