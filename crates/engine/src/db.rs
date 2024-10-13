#![allow(non_snake_case)]

use anyhow::Result;
use deku::ctx::Endian;
use deku::prelude::{DekuRead, DekuWrite};
use derive_more::derive::From;
use std::{fs::File, time::SystemTime};
use thiserror::Error;

use crate::engine::CURRENT_DATABASE_VERSION;
use crate::{
    page::{PageDecoder, PageEncoder, PageHeader, PageType},
    persistence,
};

#[derive(Debug, From, Error)]
pub enum DbError {
    #[error("Deku Error: {0}")]
    Deku(deku::error::DekuError),
    #[error("Persistence Error: {0}")]
    Persistence(persistence::PersistenceError),
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
    PersistenceError(persistence::PersistenceError),
}

/// The constant page index of the FILE_INFO page.
pub const FILE_INFO_PAGE_INDEX: u32 = 0;

/// The constant page index of the DATABASE_INFO page.
pub const DATABASE_INFO_PAGE_INDEX: u32 = 1;

#[derive(DekuRead, DekuWrite, Debug, PartialEq, Eq, Hash)]
#[deku(
    id_type = "u8",
    endian = "endian",
    ctx = "endian: deku::ctx::Endian",
    ctx_default = "Endian::Big"
)]
pub enum FileType {
    #[deku(id = 0)]
    Primary,
    #[deku(id = 1)]
    Log,
}

/// Information describing a database file.
#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(endian = "big")]
pub struct FileInfo {
    #[deku(bytes = 4)]
    magic_string: [u8; 4],

    #[deku]
    file_type: FileType,

    #[deku(bytes = 2)]
    sector_size_bytes: u16,

    #[deku(bytes = 2)]
    created_date_unix: u16,
}

impl FileInfo {
    pub fn new(file_type: FileType, time: SystemTime) -> Self {
        FileInfo {
            magic_string: [0, 1, 6, 1],
            file_type,
            sector_size_bytes: 0, // TODO: Find this value
            created_date_unix: time
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u16,
        }
    }
}

pub type DatabaseId = u16;

/// Information describing a database.
/// There will only ever be one of these pages in a single file.
#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(endian = "big")]
pub struct DatabaseInfo {
    #[deku(bytes = 1)]
    pub database_name_len: u8,

    #[deku(bytes = 128, count = "database_name_len")]
    pub database_name: Vec<u8>,

    #[deku(bytes = 1)]
    pub database_version: u8,

    #[deku(bytes = 2)]
    pub database_id: DatabaseId,
}

impl DatabaseInfo {
    pub fn new(database_name: &str, database_id: DatabaseId, version: u8) -> Self {
        if database_name.len() >= 256 {
            panic!("db name too long");
        }

        DatabaseInfo {
            database_name_len: database_name.len() as u8,
            database_name: database_name.to_owned().into_bytes(),
            database_version: version,
            database_id,
        }
    }
}

pub fn create_db_data_file(db_name: &str, db_id: DatabaseId) -> Result<File> {
    let file = persistence::create_db_file_empty(db_name, FileType::Primary)?;

    write_file_info(&file)?;
    write_db_info(&file, db_name, db_id)?;

    Ok(file)
}

pub fn create_db_log_file(db_name: &str) -> Result<File> {
    persistence::create_db_file_empty(db_name, FileType::Log)
}

pub fn validate_data_file(file: &File) -> Result<()> {
    let file_info_page = persistence::read_page(file, FILE_INFO_PAGE_INDEX)?;

    let page = PageDecoder::from_bytes(&file_info_page);
    let checksum_pass = page.check();

    match checksum_pass.pass {
        true => Ok(()),
        false => Err(ValidationError::FileInfoChecksumIncorrect(checksum_pass).into()),
    }
}

// TODO: The following 2 functions write pages to files
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

    persistence::write_page(
        file,
        &collected,
        FILE_INFO_PAGE_INDEX,
    )
}

/// Write a DATABASE_INFO page to the correct page index, DATABASE_INFO_PAGE_INDEX.
fn write_db_info(file: &std::fs::File, db_name: &str, db_id: DatabaseId) -> Result<()> {
    let header = PageHeader::new(PageType::DatabaseInfo);
    let mut page = PageEncoder::new(header);

    let body = DatabaseInfo::new(db_name, db_id, CURRENT_DATABASE_VERSION);

    page.add_slot(body)?;
    let collected = page.collect();

    persistence::write_page(
        file,
        &collected,
        DATABASE_INFO_PAGE_INDEX,
    )
}

#[cfg(test)]
mod master_engine_tests {
    use db::{FileInfo, FileType};
    use deku::DekuContainerWrite;
    use std::time::SystemTime;

    use crate::*;

    // #[test]
    // fn test_validate_master_database() {
    //     let now = SystemTime::now();
    //     let page = master::write_master_file_info_page_internal(now).expect("Failed");
    //     let validate = master::validate_master_file_info(&page);

    //     assert_eq!(validate.is_ok(), true);
    // }

    #[test]
    fn test_read_write_binary_filetype_primary() {
        let file_type = FileType::Primary;
        let bytes = file_type.to_bytes().unwrap();

        assert_eq!(bytes, [0]);
    }

    #[test]
    fn test_read_write_binary_filetype_log() {
        let file_type = FileType::Log;
        let bytes = file_type.to_bytes().unwrap();

        assert_eq!(bytes, [1]);
    }

    #[test]
    fn test_read_write_binary_fileinfo_of_type_primary() {
        // continue writing this test - trying to get deku to serialise FileInfo.
        let time = SystemTime::now();
        let file_info = FileInfo::new(FileType::Primary, time);
        let bytes = file_info.to_bytes().unwrap();

        let time_bytes = time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u16;

        let expected = vec![
            // Magic string
            0,
            1,
            6,
            1,
            // File Type
            0,
            0,
            // Sector Size
            0,
            // Date Created
            (time_bytes >> 8) as u8,
            (time_bytes & 0xFF) as u8,
        ];

        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_read_write_binary_fileinfo_of_type_log() {
        let time = SystemTime::now();
        let file_info = FileInfo::new(FileType::Log, time);
        let bytes = file_info.to_bytes().unwrap();

        let time_bytes = time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u16;

        let time_l = (time_bytes >> 8) as u8;
        let time_h = (time_bytes & 0xFF) as u8;

        let expected = vec![
            0, 1, 6, 1, // Magic string
            1, // File Type
            0, 0, // Sector Size
            time_l, time_h, // Created
        ];

        assert_eq!(bytes, expected);
    }
}
