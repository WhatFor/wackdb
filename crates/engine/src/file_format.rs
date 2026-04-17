use std::time::SystemTime;

use deku::ctx::Endian;
use deku::prelude::{DekuRead, DekuWrite};

use crate::fm::DatabaseFileId;
use crate::page::PageId;
use crate::util::time_bytes;

/// The latest version of the database file structure.
/// Needed to support backwards compatability of the file structure.
/// Not currently used.
pub const CURRENT_DATABASE_VERSION: u8 = 1;

/// The constant page index of the FILE_INFO page.
pub const FILE_INFO_PAGE_INDEX: u32 = 0;

/// The constant page index of the DATABASE_INFO page.
pub const DATABASE_INFO_PAGE_INDEX: u32 = 1;

/// The constant page index of the SCHEMA_INFO page.
/// This page only exists in the master databse file
/// as an entry-point into reading all user-db schema info.
pub const SCHEMA_INFO_PAGE_INDEX: u32 = 2;

#[derive(DekuRead, DekuWrite, Debug, PartialEq, Eq, Hash, Clone, Copy)]
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
            created_date_unix: time_bytes(time),
        }
    }
}

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
    pub database_id: DatabaseFileId,
}

impl DatabaseInfo {
    pub fn new(database_name: &str, database_id: DatabaseFileId, version: u8) -> Self {
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

/// Information describing how to find schema information.
/// This only exists in the master database, and works as
/// a starting point to find all schema information from
/// the schema tables.
#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(endian = "big")]
pub struct SchemaInfo {
    #[deku(bytes = 4)]
    pub databases_root_page_id: PageId,

    #[deku(bytes = 4)]
    pub tables_root_page_id: PageId,

    #[deku(bytes = 4)]
    pub columns_root_page_id: PageId,

    #[deku(bytes = 4)]
    pub indexes_root_page_id: PageId,
}

#[cfg(test)]
mod file_format_tests {
    use deku::DekuContainerWrite;
    use std::time::SystemTime;
    use util::time_bytes;

    use crate::{
        file_format::{FileInfo, FileType},
        *,
    };

    // #[test]
    // fn test_validate_master_database() {
    //     let now = SystemTime::now();
    //     let page = master::write_master_file_info_page_internal(now).expect("Failed");
    //     let validate = master::validate_master_file_info(&page);

    //     assert_eq!(validate.is_ok(), true);
    // }

    #[test]
    fn test_read_write_binary_fileinfo_of_type_primary() {
        // continue writing this test - trying to get deku to serialise FileInfo.
        let time = SystemTime::now();
        let file_info = FileInfo::new(FileType::Primary, time);
        let bytes = file_info.to_bytes().unwrap();

        let time_bytes = time_bytes(time);

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

        let time_bytes = time_bytes(time);

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
