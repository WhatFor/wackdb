use std::fs::File;

use crate::{
    db::{self, DatabaseId, FileType},
    page::PageEncoderError,
    persistence, util,
};
use derive_more::derive::From;
use parser::ast::CreateDatabaseBody;

pub const MASTER_NAME: &str = "master";
pub const MASTER_DB_ID: u16 = 0;

pub type Result<T> = std::result::Result<T, CreateDatabaseError>;

#[derive(Debug, From)]
pub enum CreateDatabaseError {
    #[from]
    DatabaseExists(String),
    #[from]
    UnableToWrite(PageEncoderError),
    #[from]
    UnableToCreateFile(util::Error),
    #[from]
    DiskError(persistence::Error),
    #[from]
    DbError(db::Error),
}

pub struct OpenDatabaseResult {
    pub id: DatabaseId,
    pub dat: File,
    pub log: File,
}

pub fn open_or_create_master_db() -> Result<OpenDatabaseResult> {
    let exists = persistence::check_db_exists(MASTER_NAME, FileType::Primary)?;

    if exists {
        let db = persistence::open_db(&MASTER_NAME.to_owned());

        log::info!("Opened existing master DB.");

        return Ok(OpenDatabaseResult {
            id: MASTER_DB_ID,
            dat: db.dat,
            log: db.log,
        });
    }

    create_database(MASTER_NAME, MASTER_DB_ID)
}

pub fn create_user_database(
    statement: &CreateDatabaseBody,
    db_id: DatabaseId,
) -> Result<OpenDatabaseResult> {
    let db_name = statement.database_name.value.as_str();

    create_database(db_name, db_id)
}

pub fn create_database(db_name: &str, db_id: DatabaseId) -> Result<OpenDatabaseResult> {
    let data_exists = persistence::check_db_exists(db_name, FileType::Primary)?;
    let log_exists = persistence::check_db_exists(db_name, FileType::Log)?;

    if data_exists || log_exists {
        return Err(CreateDatabaseError::DatabaseExists(String::from(db_name)));
    }

    let data_file = db::create_db_data_file(db_name, db_id)?;
    let log_file = db::create_db_log_file(db_name)?;

    return Ok(OpenDatabaseResult {
        id: db_id,
        dat: data_file,
        log: log_file,
    });
}
