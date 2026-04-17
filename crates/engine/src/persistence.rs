use std::{
    ffi::OsStr,
    fs::File,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::Result;
use derive_more::derive::From;
use thiserror::Error;

use crate::{
    catalog::{MASTER_DB_ID, MASTER_NAME},
    file_format::{
        DatabaseInfo, FileInfo, FileType, SchemaInfo, CURRENT_DATABASE_VERSION,
        DATABASE_INFO_PAGE_INDEX, FILE_INFO_PAGE_INDEX, SCHEMA_INFO_PAGE_INDEX,
    },
    fm::DatabaseFileId,
    page::{PageDecoder, PageEncoder, PageHeader, PageId, PageType, PAGE_SIZE_BYTES},
    page_cache::PageBytes,
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
    pub dat: File,
    pub log: File,
    pub allocated_page_count: PageId,
}

pub fn open_or_create_master_db() -> Result<OpenDatabaseResult> {
    let exists = check_db_exists(MASTER_NAME, FileType::Primary)?;

    if exists {
        let db = open_db(MASTER_NAME);
        let allocated_page_count = get_allocated_page_count(&db.dat);

        log::info!(
            "Opened existing master DB, containing {} pages.",
            allocated_page_count
        );

        return Ok(OpenDatabaseResult {
            id: MASTER_DB_ID,
            name: MASTER_NAME.into(),
            dat: db.dat,
            log: db.log,
            allocated_page_count,
        });
    }

    create_database(MASTER_NAME, MASTER_DB_ID, true)
}

pub fn create_database(
    db_name: &str,
    db_id: DatabaseFileId,
    is_master: bool,
) -> Result<OpenDatabaseResult> {
    let data_exists = check_db_exists(db_name, FileType::Primary)?;
    let log_exists = check_db_exists(db_name, FileType::Log)?;

    if data_exists || log_exists {
        return Err(CreateDatabaseError::DatabaseExists(String::from(db_name)).into());
    }

    let data_file = create_db_data_file(db_name, db_id, is_master)?;
    let log_file = create_db_log_file(db_name)?;

    Ok(OpenDatabaseResult {
        id: db_id,
        name: db_name.into(),
        dat: data_file,
        log: log_file,
        allocated_page_count: 3,
    })
}

fn create_db_data_file(db_name: &str, db_id: DatabaseFileId, is_master: bool) -> Result<File> {
    let file = create_db_file_empty(db_name, FileType::Primary)?;

    write_file_info(&file)?;
    write_db_info(&file, db_name, db_id)?;

    if is_master {
        write_schema_info(&file)?;
    }

    Ok(file)
}

fn create_db_log_file(db_name: &str) -> Result<File> {
    create_db_file_empty(db_name, FileType::Log)
}

// TODO: The following 3 functions write pages to files
//       Next up to do is figure out how this should go through the page cache
//       Maybe just a .put on the cache, and the cache should have a .flush function
//       to force the write to disk.
//       Then, subsequent queries for the data can hit the cache instead of disk.
//       That does mean the page cache needs to know about files and have access
//       to the file handles, so now's the time to figure that out.

/// Write a FILE_INFO page to the correct page index, FILE_INFO_PAGE_INDEX.
fn write_file_info(file: &std::fs::File) -> Result<()> {
    let header = PageHeader::new(PageType::FileInfo);
    let mut page = PageEncoder::new(header);

    let created_date = SystemTime::now();
    let body = FileInfo::new(FileType::Primary, created_date);

    page.add_slot(body)?;
    let collected = page.collect();

    write_page(file, &collected, FILE_INFO_PAGE_INDEX)
}

/// Write a DATABASE_INFO page to the correct page index, DATABASE_INFO_PAGE_INDEX.
fn write_db_info(file: &std::fs::File, db_name: &str, db_id: DatabaseFileId) -> Result<()> {
    let header = PageHeader::new(PageType::DatabaseInfo);
    let mut page = PageEncoder::new(header);

    let body = DatabaseInfo::new(db_name, db_id, CURRENT_DATABASE_VERSION);

    page.add_slot(body)?;
    let collected = page.collect();

    write_page(file, &collected, DATABASE_INFO_PAGE_INDEX)
}

/// Write a SCHEMA_INFO page to the correct page index, SCHEMA_INFO_PAGE_INDEX.
fn write_schema_info(file: &std::fs::File) -> Result<()> {
    let header = PageHeader::new(PageType::SchemaInfo);
    let mut page = PageEncoder::new(header);

    let body = SchemaInfo {
        databases_root_page_id: 0,
        tables_root_page_id: 0,
        columns_root_page_id: 0,
        indexes_root_page_id: 0,
    };

    page.add_slot(body)?;
    let collected = page.collect();

    write_page(file, &collected, SCHEMA_INFO_PAGE_INDEX)
}

// Returns true if the given file exists
pub fn check_db_exists(db_name: &str, file_type: FileType) -> Result<bool> {
    let path = get_db_path(db_name, file_type);
    util::file_exists(&path)
}

/// Create a database file, empty.
pub fn create_db_file_empty(db_name: &str, file_type: FileType) -> Result<File> {
    let master_path = get_db_path(db_name, file_type);

    util::file_exists(&master_path)?;
    util::ensure_path_exists(&master_path)?;

    util::create_file(&master_path)
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

/// Seek to a specific page index in the file and write the given data
pub fn write_page(mut file: &std::fs::File, data: &[u8], page_index: u32) -> Result<()> {
    seek_page_index(file, page_index)?;
    file.write_all(data)?;

    // This ensures the write ACTUALLY writes
    Ok(file.sync_data()?)
}

/// Seek to a specific page index in the file and read the entire page
pub fn read_page(mut file: &std::fs::File, page_index: u32) -> Result<PageBytes> {
    seek_page_index(file, page_index)?;

    let mut buf = [0; PAGE_SIZE_BYTES as usize];
    file.read_exact(&mut buf)?;

    Ok(buf)
}

/// Seek to a given page index on a given File.
pub fn seek_page_index(mut file: &std::fs::File, page_index: u32) -> Result<()> {
    let offset = (page_index * PAGE_SIZE_BYTES as u32) as u64;
    let offset_from_start = std::io::SeekFrom::Start(offset);
    let pos = file.seek(offset_from_start)?;

    if pos == offset {
        Ok(())
    } else {
        Err(PersistenceError::PageSeekFailed.into())
    }
}

pub fn open_user_dbs() -> Result<Vec<OpenDatabaseResult>> {
    let dbs = find_user_databases()?;

    let results = dbs
        .map(|db| {
            let user_db = open_db(&db);
            let allocated_page_count = get_allocated_page_count(&user_db.dat);
            let id = get_db_id(&user_db.dat);

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

fn get_db_id(file: &File) -> Result<DatabaseFileId> {
    //Circumvent the page cache - can't use it until we have the db_id
    let page_bytes = read_page(file, DATABASE_INFO_PAGE_INDEX)?;

    let page = PageDecoder::from_bytes(&page_bytes);

    let db_info = page.try_read::<DatabaseInfo>(0)?;

    Ok(db_info.database_id)
}

pub fn find_user_databases() -> Result<Box<impl Iterator<Item = String>>> {
    let base_path = util::get_base_path();
    let data_path = Path::join(&base_path, std::path::Path::new(WACK_DIRECTORY));

    let files = std::fs::read_dir(data_path);

    let unique_file_names = files?.filter_map(|entry| {
        let entry = entry.ok()?;
        let path = entry.path();

        if path.is_dir() {
            return None;
        }

        if let Some(filename) = path.file_stem() {
            if filename == MASTER_NAME {
                return None;
            }
        }

        path.extension()
            .filter(|e| is_wack_file(e))
            .and_then(|_| path.file_stem().and_then(OsStr::to_str).map(str::to_owned))
    });

    Ok(Box::new(unique_file_names))
}

fn is_wack_file(extension: &OsStr) -> bool {
    extension.eq_ignore_ascii_case(DATA_FILE_EXT) || extension.eq_ignore_ascii_case(LOG_FILE_EXT)
}

pub struct OpenDatabaseResultX {
    pub dat: File,
    pub log: File,
}

pub fn get_allocated_page_count(file: &File) -> PageId {
    let metadata = file.metadata();

    match metadata {
        Ok(md) => (md.len() / PAGE_SIZE_BYTES as u64) as u32,
        Err(_) => 0,
    }
}

pub fn open_db(database_name: &str) -> OpenDatabaseResultX {
    let dat = open_db_of_type(database_name, FileType::Primary);
    let log = open_db_of_type(database_name, FileType::Log);

    OpenDatabaseResultX { dat, log }
}

fn open_db_of_type(database_name: &str, file_type: FileType) -> File {
    let path = get_db_path(database_name, file_type);
    util::open_file(&path).expect("Failed to open database.")
}

#[cfg(test)]
mod persistence_tests {
    use crate::{page::PAGE_SIZE_BYTES, *};

    use persistence::{read_page, write_page};
    use std::{
        env::temp_dir,
        fs::{File, OpenOptions},
        path::PathBuf,
    };
    use uuid::Uuid;

    fn temp_dir_path() -> std::path::PathBuf {
        let mut dir = temp_dir();
        let id = Uuid::new_v4().to_string();
        dir.push(id + ".tmp");

        dir
    }

    fn get_temp_file() -> (File, PathBuf) {
        let path = temp_dir_path();

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .expect("Failed to create temp file");

        (file, path)
    }

    #[test]
    fn test_write_page() {
        let (temp_file, temp_path) = get_temp_file();
        let data = vec![1, 2, 0];

        let result = write_page(&temp_file, &data, 0);

        assert!(result.is_ok());

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_read_page() {
        let (temp_file, temp_path) = get_temp_file();

        // Create a page-sized buffer
        let mut buffer = vec![0; PAGE_SIZE_BYTES as usize];
        buffer[0] = 1;

        // Act
        let _ = write_page(&temp_file, &buffer, 0);

        // Read
        let result = read_page(&temp_file, 0);
        let read_bytes = result.unwrap();

        // Assert
        assert_eq!(read_bytes[0], 1);
        assert_eq!(read_bytes[1], 0);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_page_seek() {
        let (temp_file, temp_path) = get_temp_file();

        // Create 2 page-sized buffers
        let buffer1 = vec![0; PAGE_SIZE_BYTES as usize];
        let mut buffer2 = vec![0; PAGE_SIZE_BYTES as usize];

        // Write a byte at the start of the 2nd page
        buffer2[0] = 1;

        // Act
        let _ = write_page(&temp_file, &buffer1, 0);
        let _ = write_page(&temp_file, &buffer2, 1);

        // Read
        let result = read_page(&temp_file, 1);
        let read_bytes = result.unwrap();

        // Assert
        assert_eq!(read_bytes[0], 1);
        assert_eq!(read_bytes[1], 0);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }
}
