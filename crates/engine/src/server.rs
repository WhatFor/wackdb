use crate::{
    db::{self, FileType},
    page::PageEncoderError,
    persistence,
};
use parser::ast::CreateDatabaseBody;

const MASTER_NAME: &str = "master";

#[derive(Debug)]
pub enum CreateDatabaseError {
    DatabaseExists(String),
    UnableToCreateFile(std::io::Error),
    UnableToWrite(PageEncoderError),
}

pub fn ensure_system_databases_initialised() {
    let _master_create = create_database(MASTER_NAME);
}

pub fn create_user_database(
    create_database_statement: &CreateDatabaseBody,
) -> Result<(), CreateDatabaseError> {
    let db_name = create_database_statement.database_name.value.as_str();

    create_database(db_name)
}

pub fn create_database(db_name: &str) -> Result<(), CreateDatabaseError> {
    let data_exists = persistence::check_db_exists(db_name, FileType::Primary);
    let log_exists = persistence::check_db_exists(db_name, FileType::Log);

    if data_exists || log_exists {
        return Err(CreateDatabaseError::DatabaseExists(String::from(db_name)));
    }

    let _datafile = db::create_db_data_file(db_name)?;
    let _logfile = db::create_db_log_file(db_name)?;

    print_database_validation(db_name);

    return Ok(());
}

fn print_database_validation(db_name: &str) {
    let validation_result = db::validate_db_data_file(db_name);

    match validation_result {
        Ok(_) => {
            println!("Database validated successfully.");
        }
        Err(err) => match err {
            db::ValidationError::FileInfoChecksumIncorrect(checksum_result) => {
                println!(
                    "ERR: Checksum failed. Expected: {:?}. Actual: {:?}.",
                    checksum_result.expected, checksum_result.actual
                )
            }
            db::ValidationError::FileNotExists => {
                println!("File does not exist.")
            }
            db::ValidationError::FailedToOpenFile(err) => {
                println!("Failed to open file: {:?}", err)
            }
            db::ValidationError::FailedToOpenFileInfo => {
                println!("Failed to open file_info")
            }
        },
    }
}
