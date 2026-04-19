use crate::bootstrap;
use crate::buffer_pool::{BufferPool, FilePageId};
use crate::catalog::{MASTER_DB_ID, MASTER_NAME};
use crate::file_format::{FileType, FILE_INFO_PAGE_INDEX};
use crate::fm::{FileId, FileManager};
use crate::page::PageDecoder;
use crate::persistence::ValidationError;
use crate::sm::SchemaManager;
use crate::vm::VirtualMachine;

use anyhow::{bail, Result};
use cli_common::{ExecuteResult, StatementResult};
use parser::ast::{Program, Statement};

pub struct Engine {
    vm: VirtualMachine,
    storage: Storage,
    sm: SchemaManager,
}

pub struct Storage {
    pub buffer_pool: BufferPool,
    pub file_manager: FileManager,
}

impl Default for Engine {
    fn default() -> Self {
        let vm = VirtualMachine::default();
        let buffer_pool = BufferPool::default();
        let file_manager = FileManager::default();

        let mut storage = Storage {
            buffer_pool,
            file_manager,
        };

        let master_db_result = bootstrap::open_or_create_master_db();

        match master_db_result {
            Ok(x) => {
                storage.file_manager.add(
                    FileId::new(MASTER_DB_ID, MASTER_NAME.into(), FileType::Primary),
                    Box::new(x.files.dat),
                    x.allocated_page_count,
                );

                storage.file_manager.add(
                    FileId::new(MASTER_DB_ID, MASTER_NAME.into(), FileType::Log),
                    Box::new(x.files.log),
                    0,
                );
            }
            Err(error) => {
                log::error!("Error creating/reading master: {:?}", error);
                panic!();
            }
        }

        if let Err(e) = bootstrap::ensure_master_tables_exist(&storage) {
            log::error!("Error initialising master tables: {:?}", e);
            panic!();
        }

        let sm = SchemaManager::new(&storage);

        // TODO: Clean this up
        if sm.is_err() {
            log::error!("Failed to build SchemaManager. See: {:?}", sm.unwrap_err());
            panic!("Couldn't build Schema info. Critically borked.",);
        }

        let sm_u = sm.unwrap();

        match bootstrap::open_user_dbs(&sm_u) {
            Ok(user_dbs) => {
                for user_db in user_dbs {
                    log::info!(
                        "Database {} loaded, containing {} pages.",
                        user_db.id,
                        user_db.allocated_page_count
                    );

                    storage.file_manager.add(
                        FileId::new(user_db.id, user_db.name.clone().into(), FileType::Primary),
                        Box::new(user_db.files.dat),
                        user_db.allocated_page_count,
                    );

                    storage.file_manager.add(
                        FileId::new(user_db.id, user_db.name.into(), FileType::Log),
                        Box::new(user_db.files.log),
                        0,
                    );
                }
            }
            Err(err) => {
                log::error!("Error opening user databases: {:?}", err);
                panic!();
            }
        }

        if let Err(e) = validate_all_data_files(&storage) {
            panic!("Failed to validate file: {:?}", e);
        }

        Engine {
            vm,
            sm: sm_u,
            storage,
        }
    }
}

impl Engine {
    pub fn execute(&self, prog: &Program) -> Result<ExecuteResult> {
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
    pub fn execute_statement(&self, statement: &Statement) -> Result<StatementResult> {
        dbg!(&statement);
        match statement {
            Statement::Select(select_expression_body) => {
                log::info!("Selecting: {:?}", select_expression_body);
                self.vm
                    .execute_statement(statement, &self.storage, &self.sm)
            }
            Statement::Update => {
                log::info!("Updating");
                Ok(StatementResult::default())
            }
            Statement::Insert(insert_statement_body) => {
                log::info!("Inserting: {:?}", insert_statement_body);
                self.vm
                    .execute_statement(statement, &self.storage, &self.sm)
            }
            Statement::Delete => {
                log::info!("Deleting");
                Ok(StatementResult::default())
            }
            Statement::CreateTable(_create_table_body) => {
                log::info!("Creating Table");
                Ok(StatementResult::default())
            }
            Statement::CreateDatabase(_) => {
                todo!("This CreateDatabase branch is probably going to get moved into the VM or something.");
                /*
                let next_id = self.storage.file_manager.next_file_id();

                let db_name = s.database_name.value.as_str();
                let result = persistence::create_database(db_name, next_id, false)?;

                self.storage.file_manager.add(
                    FileId::new(result.id, result.name.clone(), FileType::Primary),
                    Box::new(result.files.dat),
                    result.allocated_page_count,
                );

                self.storage.file_manager.add(
                    FileId::new(result.id, result.name, FileType::Log),
                    Box::new(result.files.log),
                    0,
                );

                // Revalidate all files
                if let Err(e) = self.validate_all_data_files() {
                    panic!("Failed to validate file: {:?}", e);
                };

                Ok(StatementResult::default())
                */
            }
        }
    }
}

fn validate_all_data_files(storage: &Storage) -> Result<()> {
    storage
        .file_manager
        .get_all()
        .filter(|file| file.id.ty != FileType::Log)
        .map(|file| {
            let file_info_page = storage.buffer_pool.get_page(
                &FilePageId {
                    db_id: file.id.id,
                    page_index: FILE_INFO_PAGE_INDEX,
                },
                &storage.file_manager,
            );

            match file_info_page {
                Some(info_page) => {
                    let page = PageDecoder::from_bytes(&info_page);
                    let checksum_pass = page.check();

                    match checksum_pass.pass {
                        true => Ok(()),
                        false => {
                            Err(ValidationError::FileInfoChecksumIncorrect(checksum_pass).into())
                        }
                    }
                }
                None => bail!("Errrr"), // TODO: Do better.
            }
        })
        .collect()
}
