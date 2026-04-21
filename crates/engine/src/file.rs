use anyhow::Result;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::sync::Mutex;
use std::time::SystemTime;

use crate::buffer_pool::PageBytes;
use crate::file_format::{
    FileInfo, FileType, SchemaInfo, CURRENT_DATABASE_VERSION, FILE_INFO_PAGE_INDEX,
    SCHEMA_INFO_PAGE_INDEX,
};
use crate::page::{PageEncoder, PageHeader, PageType};
use crate::{
    file_format::{DatabaseInfo, DATABASE_INFO_PAGE_INDEX},
    page::{PageDecoder, PageId, PAGE_SIZE_BYTES},
};

/// An ID for an individual database file.
/// Note: Not an 'id to be used in a DB table' or otherwise.
pub type DatabaseFileId = u16;

pub trait PagedFile {
    fn read_page(&self, page_index: u32) -> Result<PageBytes>;
    fn read_raw(&self, offset: u64, len: usize) -> Result<Vec<u8>>;
    fn write_page(&self, data: &[u8], page_index: u32) -> Result<()>;
    fn allocated_page_count(&self) -> Result<PageId>;

    fn db_id(&mut self) -> Result<DatabaseFileId> {
        //Circumvent the buffer pool - can't use it until we have the db_id
        let page_bytes = self.read_page(DATABASE_INFO_PAGE_INDEX)?;
        let page = PageDecoder::from_bytes(&page_bytes);
        let db_info = page.try_read::<DatabaseInfo>(0)?;

        Ok(db_info.database_id)
    }

    /// Write a FILE_INFO page to the correct page index, FILE_INFO_PAGE_INDEX.
    fn write_file_info(&self) -> Result<()> {
        let header = PageHeader::new(PageType::FileInfo);
        let mut page = PageEncoder::new(header);

        let created_date = SystemTime::now();
        let body = FileInfo::new(FileType::Primary, created_date);

        page.add_slot(body)?;
        let collected = page.collect();

        self.write_page(&collected, FILE_INFO_PAGE_INDEX)
    }

    /// Write a DATABASE_INFO page to the correct page index, DATABASE_INFO_PAGE_INDEX.
    fn write_db_info(&self, db_name: &str, db_id: DatabaseFileId) -> Result<()> {
        let header = PageHeader::new(PageType::DatabaseInfo);
        let mut page = PageEncoder::new(header);

        let body = DatabaseInfo::new(db_name, db_id, CURRENT_DATABASE_VERSION);

        page.add_slot(body)?;
        let collected = page.collect();

        self.write_page(&collected, DATABASE_INFO_PAGE_INDEX)
    }

    /// Write a SCHEMA_INFO page to the correct page index, SCHEMA_INFO_PAGE_INDEX.
    fn write_schema_info(&self) -> Result<()> {
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

        self.write_page(&collected, SCHEMA_INFO_PAGE_INDEX)
    }
}

pub struct DiskFile {
    pub file: Mutex<File>,
}

impl DiskFile {
    pub fn new(file: File) -> Self {
        DiskFile {
            file: Mutex::new(file),
        }
    }
}

impl PagedFile for DiskFile {
    fn write_page(&self, data: &[u8], page_index: u32) -> Result<()> {
        let offset = (page_index * PAGE_SIZE_BYTES as u32) as u64;
        let offset_from_start = std::io::SeekFrom::Start(offset);

        let mut file = self.file.lock().unwrap();
        file.seek(offset_from_start)?;

        file.write_all(data)?;

        // This ensures the write ACTUALLY writes
        Ok(file.sync_data()?)
    }

    fn read_page(&self, page_index: u32) -> Result<PageBytes> {
        let offset = (page_index * PAGE_SIZE_BYTES as u32) as u64;
        let offset_from_start = std::io::SeekFrom::Start(offset);

        let mut file = self.file.lock().unwrap();
        file.seek(offset_from_start)?;

        let mut buf = [0; PAGE_SIZE_BYTES as usize];
        file.read_exact(&mut buf)?;

        Ok(buf)
    }

    fn read_raw(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let offset_from_start = std::io::SeekFrom::Start(offset);
        let mut file = self.file.lock().unwrap();
        file.seek(offset_from_start)?;

        let mut buf = Vec::with_capacity(len);
        buf.resize(len, 0);
        file.read_exact(&mut buf)?;

        Ok(buf)
    }

    fn allocated_page_count(&self) -> Result<PageId> {
        let metadata = self.file.lock().unwrap().metadata()?;

        Ok((metadata.len() / PAGE_SIZE_BYTES as u64) as u32)
    }
}

#[cfg(test)]
pub struct MemoryFile {
    pub data: Mutex<Vec<u8>>,
}

#[cfg(test)]
impl MemoryFile {
    pub fn new(data: Vec<u8>) -> Self {
        MemoryFile {
            data: Mutex::new(data),
        }
    }
}

#[cfg(test)]
impl PagedFile for MemoryFile {
    fn write_page(&self, data: &[u8], page_index: u32) -> Result<()> {
        let offset = (page_index as usize) * PAGE_SIZE_BYTES as usize;
        let end = offset + PAGE_SIZE_BYTES as usize;

        let mut current = self.data.lock().unwrap();

        if current.len() < end {
            current.resize(end, 0);
        }

        current[offset..offset + data.len()].copy_from_slice(data);

        Ok(())
    }

    fn read_page(&self, page_index: u32) -> Result<PageBytes> {
        let offset = (page_index as usize) * PAGE_SIZE_BYTES as usize;
        let end = offset + PAGE_SIZE_BYTES as usize;

        let current = self.data.lock().unwrap(); // TODO: don't like locking when reading

        if current.len() < end {
            anyhow::bail!("page {} not written yet", page_index);
        }

        let mut buf = [0u8; PAGE_SIZE_BYTES as usize];
        buf.copy_from_slice(&current[offset..end]);

        Ok(buf)
    }

    fn read_raw(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let offset = offset as usize;
        let end = offset + len;

        let file = self.data.lock().unwrap();

        if file.len() < end {
            anyhow::bail!("Out of range read");
        }

        let mut buf = Vec::with_capacity(len);
        buf.resize(len, 0);
        buf.copy_from_slice(&file[offset..end]);

        Ok(buf)
    }

    fn allocated_page_count(&self) -> Result<PageId> {
        let current = self.data.lock().unwrap();
        Ok((current.len() / PAGE_SIZE_BYTES as usize) as u32)
    }
}

#[cfg(test)]
mod disk_file_tests {
    use std::env::temp_dir;
    use std::fs::OpenOptions;
    use std::path::PathBuf;
    use uuid::Uuid;

    use crate::file::{DiskFile, PagedFile};
    use crate::page::PAGE_SIZE_BYTES;

    fn temp_dir_path() -> std::path::PathBuf {
        let mut dir = temp_dir();
        let id = Uuid::new_v4().to_string();
        dir.push(id + ".tmp");

        dir
    }

    fn get_temp_file() -> (DiskFile, PathBuf) {
        let path = temp_dir_path();

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .expect("Failed to create temp file");

        (DiskFile::new(file), path)
    }

    #[test]
    fn test_write_page() {
        let (file, temp_path) = get_temp_file();
        let data = vec![1, 2, 0];

        let result = file.write_page(&data, 0);

        assert!(result.is_ok());

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_read_page() {
        let (file, temp_path) = get_temp_file();

        // Create a page-sized buffer
        let mut buffer = vec![0; PAGE_SIZE_BYTES as usize];
        buffer[0] = 1;

        // Act
        let _ = file.write_page(&buffer, 0);

        // Read
        let result = file.read_page(0);
        let read_bytes = result.unwrap();

        // Assert
        assert_eq!(read_bytes[0], 1);
        assert_eq!(read_bytes[1], 0);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_page_seek() {
        let (file, temp_path) = get_temp_file();

        // Create 2 page-sized buffers
        let buffer1 = vec![0; PAGE_SIZE_BYTES as usize];
        let mut buffer2 = vec![0; PAGE_SIZE_BYTES as usize];

        // Write a byte at the start of the 2nd page
        buffer2[0] = 1;

        // Act
        let _ = file.write_page(&buffer1, 0);
        let _ = file.write_page(&buffer2, 1);

        // Read
        let result = file.read_page(1);
        let read_bytes = result.unwrap();

        // Assert
        assert_eq!(read_bytes[0], 1);
        assert_eq!(read_bytes[1], 0);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }
}

#[cfg(test)]
mod memory_file_tests {
    use crate::file::{MemoryFile, PagedFile};
    use crate::page::PAGE_SIZE_BYTES;

    #[test]
    fn test_write_page() {
        let file = MemoryFile::new(vec![]);
        let data = vec![1, 2, 0];

        let result = file.write_page(&data, 0);

        assert!(result.is_ok());
    }

    #[test]
    fn test_read_page() {
        let file = MemoryFile::new(vec![]);

        // Create a page-sized buffer
        let mut buffer = vec![0; PAGE_SIZE_BYTES as usize];
        buffer[0] = 1;

        // Act
        let _ = file.write_page(&buffer, 0);

        // Read
        let result = file.read_page(0);
        let read_bytes = result.unwrap();

        // Assert
        assert_eq!(read_bytes[0], 1);
        assert_eq!(read_bytes[1], 0);
    }

    #[test]
    fn test_page_seek() {
        let file = MemoryFile::new(vec![]);

        // Create 2 page-sized buffers
        let buffer1 = vec![0; PAGE_SIZE_BYTES as usize];
        let mut buffer2 = vec![0; PAGE_SIZE_BYTES as usize];

        // Write a byte at the start of the 2nd page
        buffer2[0] = 1;

        // Act
        let _ = file.write_page(&buffer1, 0);
        let _ = file.write_page(&buffer2, 1);

        // Read
        let result = file.read_page(1);
        let read_bytes = result.unwrap();

        // Assert
        assert_eq!(read_bytes[0], 1);
        assert_eq!(read_bytes[1], 0);
    }
}
