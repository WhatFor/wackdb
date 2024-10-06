use std::{
    fs::File,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
};

use crate::{db::FileType, page_cache::PageBytes, server::CreateDatabaseError, util, DATA_FILE_EXT, LOG_FILE_EXT};

// Returns true if the given file exists
pub fn check_db_exists(db_name: &str, file_type: FileType) -> bool {
    let path = get_db_path(db_name, file_type);
    util::file_exists(&path)
}

/// Create a database file, empty.
pub fn create_db_file_empty(
    db_name: &str,
    file_type: FileType,
) -> Result<File, CreateDatabaseError> {
    let master_path = get_db_path(db_name, file_type);

    if util::file_exists(&master_path) {
        return core::result::Result::Err(CreateDatabaseError::DatabaseExists(String::from(
            "Database exists",
        )));
    }

    util::ensure_path_exists(&master_path);

    util::create_file(&master_path)
}

// Get a PathBuf to a file with the given name and extension
pub fn get_db_path(db_name: &str, file_type: FileType) -> PathBuf {
    let ext = match file_type {
        FileType::Primary => crate::DATA_FILE_EXT,
        FileType::Log => crate::LOG_FILE_EXT,
    };

    let base_path = util::get_base_path();
    let mut data_path = Path::join(&base_path, std::path::Path::new(crate::WACK_DIRECTORY));

    let file_name = db_name.to_owned() + ext;
    PathBuf::push(&mut data_path, file_name);

    data_path
}

/// Seek to a specific page index in the file and write the given data
pub fn write_page(mut file: &std::fs::File, data: &[u8], page_index: u32) -> std::io::Result<()> {
    seek_page_index(file, page_index)?;
    file.write_all(data)?;

    // This ensures the write ACTUALLY writes
    file.sync_data()
}

/// Seek to a specific page index in the file and read the entire page
pub fn read_page(mut file: &std::fs::File, page_index: u32) -> std::io::Result<PageBytes> {
    seek_page_index(file, page_index)?;

    let mut buf = [0; crate::PAGE_SIZE_BYTES_USIZE];
    file.read_exact(&mut buf)?;

    Ok(buf)
}

/// Seek to a given page index on a given File.
pub fn seek_page_index(mut file: &std::fs::File, page_index: u32) -> std::io::Result<()> {
    let page_size: u32 = crate::PAGE_SIZE_BYTES.try_into().unwrap();
    let offset: u64 = (page_index * page_size).into();
    let offset_from_start = std::io::SeekFrom::Start(offset);

    let pos = file.seek(offset_from_start)?;

    if pos != offset {
        panic!("Failed to seek file.");
    }

    Ok(())
}

pub fn find_user_databases() -> std::io::Result<Vec<String>> {
    let base_path = util::get_base_path();
    let data_path = Path::join(&base_path, std::path::Path::new(crate::WACK_DIRECTORY));

    let files = std::fs::read_dir(data_path);
    let mut unique_file_names = Vec::new();

    for entry in files? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            continue;
        }

        if let Some(ext) = path.extension(){
            if ext != DATA_FILE_EXT || ext != LOG_FILE_EXT {
                continue;
            }
        }

        // todo: spicy unwraps
        let file_name = path.file_stem().unwrap().to_str().unwrap().to_owned();

        if unique_file_names.contains(&file_name) {
            continue;
        }

        unique_file_names.push(file_name);
    }

    Ok(unique_file_names)
}

pub struct OpenDatabaseResult {
    pub dat: File,
    pub log: File,
}

pub fn open_user_db(database_name: &String) -> OpenDatabaseResult {
    let dat = open_user_db_of_type(database_name, FileType::Primary);
    let log = open_user_db_of_type(database_name, FileType::Log);

    OpenDatabaseResult { dat, log }
}

fn open_user_db_of_type(database_name: &String, file_type: FileType) -> File {
    let path = get_db_path(database_name, file_type);
    util::open_file(&path).expect("Failed to open user database.")
}

#[cfg(test)]
mod persistence_tests {
    use crate::*;

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
            .open(&path)
            .expect("Failed to create temp file");

        (file, path)
    }

    #[test]
    fn test_write_page() {
        let (temp_file, temp_path) = get_temp_file();
        let data = vec![1, 2, 0];

        let result = write_page(&temp_file, &data, 0);

        assert_eq!(result.is_ok(), true);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_read_page() {
        let (temp_file, temp_path) = get_temp_file();

        // Create a page-sized buffer
        let mut buffer = vec![0; PAGE_SIZE_BYTES.into()];
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
        let buffer1 = vec![0; PAGE_SIZE_BYTES.into()];
        let mut buffer2 = vec![0; PAGE_SIZE_BYTES.into()];

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
