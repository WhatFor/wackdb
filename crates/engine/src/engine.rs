use crate::catalog::{MASTER_DB_ID, MASTER_NAME};
use crate::file_format::{FileType, FILE_INFO_PAGE_INDEX};
use crate::fm::{FileId, FileManager, IdentifiedFile};
use crate::page::PageDecoder;
use crate::page_cache::PageCache;
use crate::persistence::ValidationError;
use crate::vm::VirtualMachine;
use crate::{bootstrap, persistence};

use anyhow::Result;
use cli_common::{ExecuteResult, StatementResult};
use parser::ast::{Program, Statement};

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
        let master_db_result = persistence::open_or_create_master_db();

        match master_db_result {
            Ok(x) => {
                self.storage.file_manager.add(
                    FileId::new(MASTER_DB_ID, MASTER_NAME.into(), FileType::Primary),
                    x.dat,
                    x.allocated_page_count,
                );

                self.storage.file_manager.add(
                    FileId::new(MASTER_DB_ID, MASTER_NAME.into(), FileType::Log),
                    x.log,
                    0,
                );
            }
            Err(error) => {
                log::error!("Error creating/reading master: {:?}", error);
                return;
            }
        }

        if let Err(e) = bootstrap::ensure_master_tables_exist(&mut self.storage.file_manager) {
            log::error!("Error initialising master tables: {:?}", e);
            return;
        }

        match persistence::open_user_dbs() {
            Ok(user_dbs) => {
                for user_db in user_dbs {
                    log::info!(
                        "Database {} loaded, containing {} pages.",
                        user_db.id,
                        user_db.allocated_page_count
                    );

                    self.storage.file_manager.add(
                        FileId::new(user_db.id, user_db.name.clone().into(), FileType::Primary),
                        user_db.dat,
                        user_db.allocated_page_count,
                    );

                    self.storage.file_manager.add(
                        FileId::new(user_db.id, user_db.name.into(), FileType::Log),
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

        if let Err(e) = self.validate_all_data_files() {
            panic!("Failed to validate file: {:?}", e);
        }
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
                let next_id = self.storage.file_manager.next_file_id();

                let db_name = s.database_name.value.as_str();
                let result = persistence::create_database(db_name, next_id, false)?;

                self.storage.file_manager.add(
                    FileId::new(result.id, result.name.clone(), FileType::Primary),
                    result.dat,
                    result.allocated_page_count,
                );

                self.storage.file_manager.add(
                    FileId::new(result.id, result.name, FileType::Log),
                    result.log,
                    0,
                );

                // Revalidate all files
                if let Err(e) = self.validate_all_data_files() {
                    panic!("Failed to validate file: {:?}", e);
                };

                Ok(StatementResult::default())
            }
        }
    }

    fn validate_all_data_files(&self) -> Result<()> {
        self.storage
            .file_manager
            .get_all()
            .filter(|file| file.id.ty != FileType::Log)
            .map(|file| self.validate_data_file(file))
            .collect()
    }

    fn validate_data_file(&self, identifiable_file: IdentifiedFile) -> Result<()> {
        let file_info_page = persistence::read_page(identifiable_file.file, FILE_INFO_PAGE_INDEX)?;

        let page = PageDecoder::from_bytes(&file_info_page);
        let checksum_pass = page.check();

        match checksum_pass.pass {
            true => Ok(()),
            false => Err(ValidationError::FileInfoChecksumIncorrect(checksum_pass).into()),
        }
    }
}
