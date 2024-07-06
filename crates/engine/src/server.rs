use parser::ast::CreateDatabaseBody;
use std::{
    fs::{OpenOptions, Permissions},
    io::{Error, Read, Seek, Write},
    os::windows::fs::OpenOptionsExt,
    path::PathBuf,
};

use crate::{
    paging::{read_page, write_page},
    StatementError, StatementResult,
};

const DATA_FILE_EXT: &str = ".wak";
const LOG_FILE_EXT: &str = ".wal";
const MASTER_NAME: &str = "master";
pub const PAGE_SIZE_BYTES: usize = 8192; // 2^13

// TODO: Hardcoded for now. See /docs/assumptions.
const WACK_DIRECTORY: &str = "data\\";

#[derive(Debug)]
pub enum CreateDatabaseError {
    DatabaseExists(String),
    UnableToCreateFile(Error),
}

/// Returns true if the master database exists.
pub fn master_database_exists() -> bool {
    let master_path = get_master_path();
    file_exists(&master_path)
}

// TODO: Should the Result types be different?
// TODO: Should this function live in a different place?
/// Create a new master database.
pub fn create_master_database() -> Result<StatementResult, StatementError> {
    let master_path = get_master_path();

    if file_exists(&master_path) {
        panic!("Master database file exists");
    }

    let _file = create_file(&master_path)?;

    // Now the file exists and we have a handle, we can write some header info.
    // The master.wak file is ultimately no different to any other database, it's just system managed and stores system info.
    // The master DB will contain things like a list of DBs, their schema info, (and in a 'real' db, which this is not,
    // stuff like auth, config, etc).
    // All data is stored in pages - regardless of if it's data rows or system info. So, we'd expect to write at least 1 page here.
    // Each page will also have a header, of a preset size (TBD).
    // I think all data pages will be slotted (seems sensible) but I don't know if system pages should be. Might as well for uniformity.
    // Though I'm not 100% sure how slotted pages work - if there's only 1 slot, how do we know when the slot ends (as cant assume it ends
    // at the next slot offset! UNLESS the offset is pointing to the END of the record. That'd be a super cute way to calc length too).
    // Slotted pages are cool cos they let us reclaim freed space in the page without breaking external references (like indexes) that
    // point to data on the page, as the slot indexes can be preserved (i.e. slot 1 remains slot 1, even if compacted and moved to a new offset).
    // The header can contain info about what's in the page, like a TYPE enum, an ID, a page checksum, etc.
    // Because ALL data is stored in pages, we'd probably just write a page of a certain type, that contains db info - a FILE_INFO page.

    // need to decide on the format of:
    //  a page header, as we're still writing a page here.
    //     - this will contain info about the page:
    //         - page id
    //         - header version
    //         - type
    //         - bit flags (CAN_COMPACT)
    //         - checksum
    //         - allocated slot count
    //         - info about allocated and free space in the file
    //  a FILE_INFO page type, as that's what we're going to write as page 0 on every file.
    //     - this will contain info about the file:
    //         - file id (maybe not needed?)
    //         - file type (data file or log file)
    //         - file flags (not sure what kinda flags we want, but)
    //         - sector size (might be handy for optimising I/O)
    //         - created date
    //         - whatever else might relate to a FILE
    //  a DB_INFO page type that exists at the start of every DB file at page 1.
    //     - this will contain info about the database:
    //         - database name (128 bytes, string)
    //         - version (2 bytes, u16)
    //         - database id (2 bytes, u16)
    //         - created date (no idea)
    // All of this will be a certain offsets, e.g. a page header will always be the same size and contain
    // the same data in the same memory locations of the page. A FILE_INFO page will always exist in the
    // same page location (page 0) of a file, and after the header contain a structure similar to a page
    // header - the same values in the same locations every time. The same for the DB_INFO page type.
    // Because we can assume that a master db file will always exist, and always have the same content,
    // this is where we're going to store loooots of info about the system.

    // steps:
    //  - create a file_info strucure, write it to the start (after the header) of a page. write it to a specific page index.
    //  - do the same for a db_info structure.

    // page header:
    //   - page id (4 bytes, u32)
    //   - header version (2 bytes, u16)
    //   - type (1 byte, u8)
    //   - checksum (2 bytes, u16)
    //   - bit flags (2 bytes, u16, essentially supporting 16 different boolean toggles)
    //       - 0: CAN_COMPACT .. Means a slot has been freed and de-referenced.
    //   - slot count (2 bytes, u16) (if a row is super small, could be 1000s of rows so u8 is too small to hold max value)
    //   - free space (2 bytes, u16) (has a max value of 8kb, so can't fit in a u8)
    //   - free space start offset (2 bytes, u16)
    //   - free space end offset (2 bytes, u16)
    //   - allocated space (2 bytes, u16) (same constraint as free space)
    // total: 21 bytes. Will allocate 32 bytes to be safe.

    let file_info_page_bytes = vec![0; PAGE_SIZE_BYTES];
    let file_info_page_write = write_page(&_file, &file_info_page_bytes, 0);

    // TODO: Remove me! this is test code
    let info_page_bytes_read = read_page(&_file, 0);

    match file_info_page_write {
        Ok(_) => println!("Wrote FILE_INFO page."),
        Err(err) => {
            // TODO: Fix this error - just random stuff to keep the compiler happy, not correct
            return Err(StatementError::CreateDatabase(
                CreateDatabaseError::UnableToCreateFile(err),
            ));
        }
    }

    Ok(StatementResult {})
}

// TODO: return type - should it be different?
// TODO: Should this function live in a different place?
/// Validate the master database is okay.
pub fn validate_master_database() -> Result<StatementResult, StatementError> {
    Ok(StatementResult {})
}

/// Get the path to the master database.
/// Equal to: base + data directory + 'master.wak'
fn get_master_path() -> String {
    let base_path = get_base_path();
    let data_path = std::path::Path::join(&base_path, std::path::Path::new(WACK_DIRECTORY));

    String::from(data_path.to_str().unwrap()) + MASTER_NAME + DATA_FILE_EXT
}

/// Create a new user database.
/// This process will create both a data file (.wak) and a log file (.wal), if needed.
pub fn create_user_database(
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
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(0x80000000) // FILE_FLAG_WRITE_THROUGH
        .open(path);

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
