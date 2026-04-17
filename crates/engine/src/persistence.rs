use std::{
    ffi::OsStr,
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::Result;
use derive_more::derive::From;
use thiserror::Error;

use crate::{
    file::{DatabaseFile, DatabaseFileId},
    file_format::FileType,
    page::PageId,
    util,
};

pub const DATA_FILE_EXT: &str = "wak";
pub const LOG_FILE_EXT: &str = "wal";
pub const WACK_DIRECTORY: &str = "data"; // TODO: Hardcoded for now. See /docs/assumptions.

#[derive(Debug, From, Error)]
pub enum DbError {
    #[error("Deku Error: {0}")]
    Deku(deku::error::DekuError),
    #[error("Persistence Error: {0}")]
    Persistence(PersistenceError),
    #[error("Validation Error: {0}")]
    Validation(ValidationError),
    #[error("Page Encoder Error: {0}")]
    PageEncoder(crate::page::PageEncoderError),
}

#[derive(Debug, From, Error)]
pub enum ValidationError {
    #[error("Failed to open file info page.")]
    #[allow(dead_code)]
    FailedToOpenFileInfo,
    #[error("Checksum failed for file info page. Expected: {0:?}")]
    FileInfoChecksumIncorrect(crate::page::ChecksumResult),
    #[error("Persistence error: {0}")]
    PersistenceError(PersistenceError),
}

#[derive(Debug, From, Error)]
pub enum CreateDatabaseError {
    #[error("Database already exists: {0}")]
    DatabaseExists(String),
    #[error("Unable to create database: {0}")]
    UnableToWrite(crate::page::PageEncoderError),
    #[error("Unable to create database: {0}")]
    UnableToCreateFile(util::Error),
    #[error("Unable to create database: {0}")]
    DiskError(PersistenceError),
    #[error("Unable to create database: {0}")]
    DbError(DbError),
}

#[derive(Debug, From, Error)]
pub enum PersistenceError {
    #[error("IO Error: {0}")]
    Io(util::Error),
    #[error("IO Error: {0}")]
    StdIo(std::io::Error),
    #[error("Failed to seek to page index.")]
    PageSeekFailed,
}

pub struct OpenDatabaseResult {
    pub id: DatabaseFileId,
    pub name: String,
    pub files: DatabaseFilePair,
    pub allocated_page_count: PageId,
}

pub struct DatabaseFilePair {
    pub dat: DatabaseFile,
    pub log: DatabaseFile,
}

pub fn create_database(
    db_name: &str,
    db_id: DatabaseFileId,
    is_master: bool,
) -> Result<OpenDatabaseResult> {
    let data_exists = db_exists(db_name, FileType::Primary)?;
    let log_exists = db_exists(db_name, FileType::Log)?;

    if data_exists || log_exists {
        return Err(CreateDatabaseError::DatabaseExists(String::from(db_name)).into());
    }

    let data_file = create_db_data_file(db_name, db_id, is_master)?;
    let log_file = create_db_log_file(db_name)?;

    Ok(OpenDatabaseResult {
        id: db_id,
        name: db_name.into(),
        allocated_page_count: 3,
        files: DatabaseFilePair {
            dat: data_file,
            log: log_file,
        },
    })
}

fn create_db_data_file(
    db_name: &str,
    db_id: DatabaseFileId,
    is_master: bool,
) -> Result<DatabaseFile> {
    let mut file = create_empty_db_file(db_name, FileType::Primary)?;

    file.write_file_info()?;
    file.write_db_info(db_name, db_id)?;

    if is_master {
        file.write_schema_info()?;
    }

    Ok(file)
}

fn create_db_log_file(db_name: &str) -> Result<DatabaseFile> {
    create_empty_db_file(db_name, FileType::Log)
}

pub fn db_exists(db_name: &str, file_type: FileType) -> Result<bool> {
    let path = get_db_path(db_name, file_type);
    util::file_exists(&path)
}

pub fn create_empty_db_file(db_name: &str, file_type: FileType) -> Result<DatabaseFile> {
    let master_path = get_db_path(db_name, file_type);

    util::file_exists(&master_path)?;
    util::ensure_path_exists(&master_path)?;
    util::create_file(&master_path).map(|f| DatabaseFile::new(f))
}

// Get a PathBuf to a file with the given name and extension
pub fn get_db_path(db_name: &str, file_type: FileType) -> PathBuf {
    let ext = match file_type {
        FileType::Primary => DATA_FILE_EXT,
        FileType::Log => LOG_FILE_EXT,
    };

    let base_path = util::get_base_path();
    let mut data_path = Path::join(&base_path, std::path::Path::new(WACK_DIRECTORY));

    let file_name = db_name.to_owned() + "." + ext;
    PathBuf::push(&mut data_path, file_name);

    data_path
}

pub fn is_wack_file(extension: &OsStr) -> bool {
    extension.eq_ignore_ascii_case(DATA_FILE_EXT) || extension.eq_ignore_ascii_case(LOG_FILE_EXT)
}

pub fn open_db(database_name: &str) -> DatabaseFilePair {
    let dat = open_db_of_type(database_name, FileType::Primary);
    let log = open_db_of_type(database_name, FileType::Log);

    DatabaseFilePair {
        dat: DatabaseFile::new(dat),
        log: DatabaseFile::new(log),
    }
}

fn open_db_of_type(database_name: &str, file_type: FileType) -> File {
    let path = get_db_path(database_name, file_type);
    util::open_file(&path).expect("Failed to open database.")
}

#[cfg(test)]
mod persistence_tests {
    use crate::{file::DatabaseFile, page::PAGE_SIZE_BYTES};

    use std::{env::temp_dir, fs::OpenOptions, path::PathBuf};
    use uuid::Uuid;

    fn temp_dir_path() -> std::path::PathBuf {
        let mut dir = temp_dir();
        let id = Uuid::new_v4().to_string();
        dir.push(id + ".tmp");

        dir
    }

    fn get_temp_file() -> (DatabaseFile, PathBuf) {
        let path = temp_dir_path();

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .expect("Failed to create temp file");

        (DatabaseFile::new(file), path)
    }

    #[test]
    fn test_write_page() {
        let (mut temp_file, temp_path) = get_temp_file();
        let data = vec![1, 2, 0];

        let result = temp_file.write_page(&data, 0);

        assert!(result.is_ok());

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_read_page() {
        let (mut temp_file, temp_path) = get_temp_file();

        // Create a page-sized buffer
        let mut buffer = vec![0; PAGE_SIZE_BYTES as usize];
        buffer[0] = 1;

        // Act
        let _ = temp_file.write_page(&buffer, 0);

        // Read
        let result = temp_file.read_page(0);
        let read_bytes = result.unwrap();

        // Assert
        assert_eq!(read_bytes[0], 1);
        assert_eq!(read_bytes[1], 0);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_page_seek() {
        let (mut temp_file, temp_path) = get_temp_file();

        // Create 2 page-sized buffers
        let buffer1 = vec![0; PAGE_SIZE_BYTES as usize];
        let mut buffer2 = vec![0; PAGE_SIZE_BYTES as usize];

        // Write a byte at the start of the 2nd page
        buffer2[0] = 1;

        // Act
        let _ = temp_file.write_page(&buffer1, 0);
        let _ = temp_file.write_page(&buffer2, 1);

        // Read
        let result = temp_file.read_page(1);
        let read_bytes = result.unwrap();

        // Assert
        assert_eq!(read_bytes[0], 1);
        assert_eq!(read_bytes[1], 0);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }
}
