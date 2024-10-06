use std::fs::File;

use crate::{
    db::{self, DatabaseId, FileType},
    page::PageEncoderError,
    persistence,
};
use parser::ast::CreateDatabaseBody;

const MASTER_NAME: &str = "master";
pub const MASTER_DB_ID: u16 = 0;

#[derive(Debug)]
pub enum CreateDatabaseError {
    DatabaseExists(String),
    UnableToCreateFile(std::io::Error),
    UnableToWrite(PageEncoderError),
}

pub struct OpenDatabaseResult {
    pub id: DatabaseId,
    pub dat: File,
    pub log: File,
}

#[derive(Debug)]
pub enum OpenDatabaseError {
    Err(),
}

pub fn open_master_db() -> Result<OpenDatabaseResult, CreateDatabaseError> {
    create_database(MASTER_NAME, MASTER_DB_ID)
}

pub fn open_user_dbs() -> Result<Vec<OpenDatabaseResult>, OpenDatabaseError> {
    let dbs_r = persistence::find_user_databases();

    println!("Opening user DBs: {:?}", dbs_r);

    match dbs_r {
        Ok(dbs) => dbs
            .into_iter()
            .map(|db| {
                let user_db = persistence::open_user_db(&db);
                let id = db::get_db_id(&user_db.dat);

                if id.is_err() {
                    panic!("I have no idea");
                }

                Ok(OpenDatabaseResult {
                    id: id.unwrap(),
                    dat: user_db.dat,
                    log: user_db.log,
                })
            })
            .collect(),
        Err(_) => {
            return Err(OpenDatabaseError::Err());
        }
    }
}

pub fn create_user_database(
    create_database_statement: &CreateDatabaseBody,
    db_id: DatabaseId,
) -> Result<OpenDatabaseResult, CreateDatabaseError> {
    let db_name = create_database_statement.database_name.value.as_str();

    create_database(db_name, db_id)
}

pub fn create_database(
    db_name: &str,
    db_id: DatabaseId,
) -> Result<OpenDatabaseResult, CreateDatabaseError> {
    let data_exists = persistence::check_db_exists(db_name, FileType::Primary);
    let log_exists = persistence::check_db_exists(db_name, FileType::Log);

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
