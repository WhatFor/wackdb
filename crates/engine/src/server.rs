use std::{io::Error, path::PathBuf};

use parser::ast::CreateDatabaseBody;

use crate::{StatementError, StatementResult};

const DATA_FILE_EXT: &str = ".wak";
const LOG_FILE_EXT: &str = ".wal";

// TODO: Hardcoded for now. See /docs/assumptions.
const WACK_DIRECTORY: &str = "data\\";

#[derive(Debug)]
pub enum CreateDatabaseError {
    DatabaseExists(String),
    UnableToCreateFile(Error),
}

/// Create a new database.
///
/// This process will create both a data file (.wak) and a log file (.wal), if needed.
pub fn create_database(
    create_database_statement: &CreateDatabaseBody,
) -> Result<StatementResult, StatementError> {
    let base_path = get_base_path();
    let data_path = std::path::Path::join(&base_path, std::path::Path::new(WACK_DIRECTORY));

    ensure_path_exists(&data_path);

    let data_file = String::from(data_path.to_str().unwrap())
        + &create_database_statement.database_name.value
        + DATA_FILE_EXT;

    if file_exists(&data_file) {
        return Err(StatementError::CreateDatabase(
            CreateDatabaseError::DatabaseExists(data_file.to_string()),
        ));
    }

    let log_file = String::from(data_path.to_str().unwrap())
        + &create_database_statement.database_name.value
        + LOG_FILE_EXT;

    if file_exists(&log_file) {
        return Err(StatementError::CreateDatabase(
            CreateDatabaseError::DatabaseExists(log_file.to_string()),
        ));
    }

    let _data_file_result = initialise_data_file(&data_file)?;
    let _log_file_result = initialise_log_file(&log_file)?;

    Ok(StatementResult {})
}

/// Get the path for data files.
/// Currently, this is the executable path.
fn get_base_path() -> PathBuf {
    match std::env::current_exe() {
        Ok(mut path) => {
            path.pop();
            path
        }
        Err(err) => panic!("Error: Unable to read filesystem. See: {}", err),
    }
}

/// Check if a file exists.
/// Returns a boolean. May panic.
fn file_exists(path: &String) -> bool {
    let path_obj = std::path::Path::new(&path);

    match std::path::Path::try_exists(path_obj) {
        Ok(exists) => exists,
        Err(err) => panic!("Error: Unable to read filesystem. See: {}", err),
    }
}

/// Ensure a path exists.
fn ensure_path_exists(path: &PathBuf) {
    match std::fs::create_dir_all(path) {
        Err(err) => panic!("Error: Unable to write filesystem. See: {}", err),
        _ => {}
    }
}

/// Create a file, given a path.
/// Returns the file, or a StatementError.
fn create_file(path: &String) -> Result<std::fs::File, StatementError> {
    let file = std::fs::File::create(path);

    match file {
        Ok(file_result) => Ok(file_result),
        Err(err) => Err(StatementError::CreateDatabase(
            CreateDatabaseError::UnableToCreateFile(err),
        )),
    }
}

/// Initialise a data file, e.g. `my_database.wak`.
fn initialise_data_file(path: &String) -> Result<StatementResult, StatementError> {
    let file = create_file(&path)?;

    Ok(StatementResult {})
}

/// Initialise a WAL file, e.g. `my_database.wal`.
fn initialise_log_file(path: &String) -> Result<StatementResult, StatementError> {
    let file = create_file(&path)?;

    Ok(StatementResult {})
}

#[cfg(test)]
mod server_engine_tests {
    use crate::*;

    /// TODO NOTES
    ///
    /// Testing FS operations is tricky. I can mock out the FS, but that seems like a lot of work
    /// and doesn't simulate a real world scenario.
    /// Alternatively, I can use a temp dir to do all of the work (maybe even in a ram-backed virtual FS),
    /// but that means switching logic based on test/not-test (specifically the filepath).
    /// If I do want to do the virtual FS with the tempdir crate or something, I'll have to pass in the filepath
    /// into the engine, which will have to come from the cli at the moment which feels wrong.
    /// Additionally, I'll have to start tracking state on the engine (which, granted I'll probably have to do eventually anyway).
    /// Maybe I need to think about this server-level configuration and sort that out first. It makes sense to be able
    /// to configure things on the server like data paths, which would mean I'd have a sensible place to be overriding that path
    /// for testing reasons.

    #[test]
    fn empty() {}
}
