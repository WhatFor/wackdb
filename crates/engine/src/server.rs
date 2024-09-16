use crate::{master, util};
use parser::ast::CreateDatabaseBody;
use std::{io::Error, path::Path};

#[derive(Debug)]
pub enum CreateDatabaseError {
    DatabaseExists(String),
    UnableToCreateFile(Error),
    UnableToWrite(Error),
}

pub fn ensure_system_databases_initialised() {
    let master_exists = master::master_database_exists();

    match master_exists {
        true => println!("Master database exists."),
        false => {
            println!("Creating master database...");
            let master_db_create_result = master::create_master_database();

            match master_db_create_result {
                Ok(_) => {
                    println!("Master database created successfully.");
                }
                Err(err) => {
                    panic!("Failed to create master database. Error: {err:?}");
                }
            }
        }
    }

    let master_validate = master::validate_master_database();

    match master_validate {
        Ok(_) => {
            println!("Master database validated successfully.");
        }
        Err(err) => match err {
            master::ValidationError::FileInfoChecksumIncorrect(checksum_result) => {
                println!(
                    "ERR: Checksum failed. Expected: {:?}. Actual: {:?}.",
                    checksum_result.expected, checksum_result.actual
                )
            }
            master::ValidationError::FileNotExists => {
                println!("File does not exist.")
            }
            master::ValidationError::FailedToOpenFile(err) => {
                println!("Failed to open file: {:?}", err)
            }
            master::ValidationError::FailedToOpenFileInfo => {
                println!("Failed to open file_info")
            }
        },
    }
}

/// Create a new user database.
/// This process will create both a data file (.wak) and a log file (.wal), if needed.
pub fn create_user_database(
    create_database_statement: &CreateDatabaseBody,
) -> Result<(), CreateDatabaseError> {
    let base_path = util::get_base_path();
    let data_path = Path::join(&base_path, Path::new(crate::WACK_DIRECTORY));

    util::ensure_path_exists(&data_path);

    let data_file = String::from(data_path.to_str().unwrap())
        + &create_database_statement.database_name.value
        + crate::DATA_FILE_EXT;

    if util::file_exists(&data_file) {
        return Err(CreateDatabaseError::DatabaseExists(data_file.to_string()));
    }

    let log_file = String::from(data_path.to_str().unwrap())
        + &create_database_statement.database_name.value
        + crate::LOG_FILE_EXT;

    if util::file_exists(&log_file) {
        return Err(CreateDatabaseError::DatabaseExists(log_file.to_string()));
    }

    let _data_file_result = initialise_data_file(&data_file)?;
    let _log_file_result = initialise_log_file(&log_file)?;

    Ok(())
}

/// Initialise a data file, e.g. `my_database.wak`.
fn initialise_data_file(path: &String) -> Result<(), CreateDatabaseError> {
    let _file = util::create_file(&path)?;
    Ok(())
}

/// Initialise a WAL file, e.g. `my_database.wal`.
fn initialise_log_file(path: &String) -> Result<(), CreateDatabaseError> {
    let _file = util::create_file(&path)?;
    Ok(())
}
