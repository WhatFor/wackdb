use std::time::SystemTime;

use deku::ctx::Endian;
use deku::prelude::*;

use crate::{
    page::{PageDecoder, PageEncoder, PageEncoderError, PageHeader, PageType},
    paging,
    server::CreateDatabaseError,
    util,
};

/// Master specific Consts
const MASTER_NAME: &str = "master";

/// The constant page index of the FILE_INFO page.
const FILE_INFO_PAGE_INDEX: u32 = 0;

#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
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

/// Information describing a database.
/// There will only ever be one of these pages in a single file.
#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(endian = "big")]
pub struct DatabaseInfo {
    #[deku(bytes = 1)]
    database_name_len: u8,

    #[deku(bytes = 128, count = "database_name_len")]
    database_name: Vec<u8>,

    #[deku(bytes = 1)]
    database_version: u8,

    #[deku(bytes = 2)]
    database_id: u16,
}

impl DatabaseInfo {
    pub fn new(database_name: String, version: u8) -> Self {
        if database_name.len() >= 256 {
            panic!("db name too long");
        }

        DatabaseInfo {
            database_name_len: database_name.len() as u8,
            database_name: database_name.into_bytes(),
            database_version: version,
            database_id: 0, // TODO
        }
    }
}

/// Returns true if the master database exists.
pub fn master_database_exists() -> bool {
    let master_path = get_master_path();
    crate::util::file_exists(&master_path)
}

/// Get the path to the master database.
/// Equal to: base + data directory + 'master.wak'
pub fn get_master_path() -> String {
    let base_path = util::get_base_path();
    let data_path = std::path::Path::join(&base_path, std::path::Path::new(crate::WACK_DIRECTORY));

    String::from(data_path.to_str().unwrap()) + MASTER_NAME + crate::DATA_FILE_EXT
}

/// Write a FILE_INFO page to the correct page index, FILE_INFO_PAGE_INDEX.
pub fn write_master_file_info_page(file: &std::fs::File) -> std::io::Result<()> {
    let try_page = write_master_file_info_page_internal(SystemTime::now());
    match try_page {
        Ok(page) => paging::write_page(&file, &page, FILE_INFO_PAGE_INDEX),
        Err(_) => {
            todo!("handle error")
        }
    }
}

fn write_master_file_info_page_internal(
    created_date: SystemTime,
) -> Result<Vec<u8>, PageEncoderError> {
    let header = PageHeader::new(PageType::FileInfo);
    let mut page = PageEncoder::new(header);

    let body = FileInfo::new(crate::master::FileType::Primary, created_date);
    page.add_slot(body)?;

    Ok(page.collect())
}

#[derive(Debug)]
pub enum ValidationError {
    FileNotExists,
    FailedToOpenFile(std::io::Error),
    FailedToOpenFileInfo,
    FileInfoChecksumIncorrect(crate::page::ChecksumResult),
}

/// Validate the master database is okay.
pub fn validate_master_database() -> Result<(), ValidationError> {
    let path = get_master_path();

    if !util::file_exists(&path) {
        return Err(ValidationError::FileNotExists);
    }

    let open_file = util::open_file(&path);

    match open_file {
        Ok(file) => {
            let file_info_page = paging::read_page(&file, FILE_INFO_PAGE_INDEX);

            match file_info_page {
                Ok(page_bytes) => validate_master_file_info(page_bytes),
                Err(_) => Err(ValidationError::FailedToOpenFileInfo),
            }
        }
        Err(err) => Err(ValidationError::FailedToOpenFile(err)),
    }
}

fn validate_master_file_info(bytes: Vec<u8>) -> Result<(), ValidationError> {
    let page = PageDecoder::from_bytes(&bytes);

    let checksum_pass = page.check();

    match checksum_pass.pass {
        true => Ok(()),
        false => Err(ValidationError::FileInfoChecksumIncorrect(checksum_pass)),
    }
}

//  a FILE_INFO page type, as that's what we're going to write as page 0 on every file.
//     - this will contain info about the file:
//         - a magic string to represent wak
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

/// Create a new master database
pub fn create_master_database() -> Result<(), CreateDatabaseError> {
    let master_path = get_master_path();

    if util::file_exists(&master_path) {
        panic!("Master database file exists");
    }

    let file = util::create_file(&master_path)?;

    // Write FILE_INFO Page
    let file_info_page_write = write_master_file_info_page(&file);

    match file_info_page_write {
        Ok(_) => println!("Wrote FILE_INFO page."),
        Err(err) => {
            return Err(CreateDatabaseError::UnableToWrite(err));
        }
    }

    Ok(())
}

#[cfg(test)]
mod master_engine_tests {
    use deku::DekuContainerWrite;
    use master::{FileInfo, FileType};
    use std::time::SystemTime;

    use crate::*;

    #[test]
    fn test_write_master_page_file_info() {
        let start: usize = PAGE_HEADER_SIZE_BYTES.into();
        let end: usize = start + 9;
        let range = start..end;

        let now = SystemTime::now();
        let file_info_page = master::write_master_file_info_page_internal(now).expect("Failed");
        println!("file info page bytes: {:?}", file_info_page);
        let actual = &file_info_page[range];

        let time_bytes = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u16;

        let expected = vec![
            // Magic String
            0,
            1,
            6,
            1,
            // File Type - Primary
            0,
            0,
            // Sector Size
            0,
            // Created Time
            (time_bytes >> 8) as u8,
            (time_bytes & 0xFF) as u8,
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_validate_master_database() {
        let now = SystemTime::now();
        let page = master::write_master_file_info_page_internal(now).expect("Failed");
        let validate = master::validate_master_file_info(page);

        assert_eq!(validate.is_ok(), true);
    }

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
