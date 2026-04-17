use anyhow::Result;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::time::SystemTime;

use crate::file_format::{
    FileInfo, FileType, SchemaInfo, CURRENT_DATABASE_VERSION, FILE_INFO_PAGE_INDEX,
    SCHEMA_INFO_PAGE_INDEX,
};
use crate::page::{PageEncoder, PageHeader, PageType};
use crate::page_cache::PageBytes;
use crate::persistence::PersistenceError;
use crate::{
    file_format::{DatabaseInfo, DATABASE_INFO_PAGE_INDEX},
    page::{PageDecoder, PageId, PAGE_SIZE_BYTES},
};

/// An ID for an individual database file.
/// Note: Not an 'id to be used in a DB table' or otherwise.
pub type DatabaseFileId = u16;

pub struct DatabaseFile {
    pub file: File,
}

impl DatabaseFile {
    pub fn new(file: File) -> Self {
        DatabaseFile { file }
    }

    pub fn write_page(&mut self, data: &[u8], page_index: u32) -> Result<()> {
        self.seek_page_index(page_index)?;
        self.file.write_all(data)?;

        // This ensures the write ACTUALLY writes
        Ok(self.file.sync_data()?)
    }

    /// Seek to a specific page index in the file and read the entire page
    pub fn read_page(&mut self, page_index: u32) -> Result<PageBytes> {
        self.seek_page_index(page_index)?;

        let mut buf = [0; PAGE_SIZE_BYTES as usize];
        self.file.read_exact(&mut buf)?;

        Ok(buf)
    }

    /// Seek to a given page index.
    pub fn seek_page_index(&mut self, page_index: u32) -> Result<()> {
        let offset = (page_index * PAGE_SIZE_BYTES as u32) as u64;
        let offset_from_start = std::io::SeekFrom::Start(offset);
        let pos = self.file.seek(offset_from_start)?;

        if pos == offset {
            Ok(())
        } else {
            Err(PersistenceError::PageSeekFailed.into())
        }
    }

    pub fn allocated_page_count(&self) -> Result<PageId> {
        let metadata = self.file.metadata()?;

        Ok((metadata.len() / PAGE_SIZE_BYTES as u64) as u32)
    }

    pub fn db_id(&mut self) -> Result<DatabaseFileId> {
        //Circumvent the page cache - can't use it until we have the db_id
        let page_bytes = self.read_page(DATABASE_INFO_PAGE_INDEX)?;
        let page = PageDecoder::from_bytes(&page_bytes);
        let db_info = page.try_read::<DatabaseInfo>(0)?;

        Ok(db_info.database_id)
    }

    /// Write a FILE_INFO page to the correct page index, FILE_INFO_PAGE_INDEX.
    pub fn write_file_info(&mut self) -> Result<()> {
        let header = PageHeader::new(PageType::FileInfo);
        let mut page = PageEncoder::new(header);

        let created_date = SystemTime::now();
        let body = FileInfo::new(FileType::Primary, created_date);

        page.add_slot(body)?;
        let collected = page.collect();

        self.write_page(&collected, FILE_INFO_PAGE_INDEX)
    }

    /// Write a DATABASE_INFO page to the correct page index, DATABASE_INFO_PAGE_INDEX.
    pub fn write_db_info(&mut self, db_name: &str, db_id: DatabaseFileId) -> Result<()> {
        let header = PageHeader::new(PageType::DatabaseInfo);
        let mut page = PageEncoder::new(header);

        let body = DatabaseInfo::new(db_name, db_id, CURRENT_DATABASE_VERSION);

        page.add_slot(body)?;
        let collected = page.collect();

        self.write_page(&collected, DATABASE_INFO_PAGE_INDEX)
    }

    /// Write a SCHEMA_INFO page to the correct page index, SCHEMA_INFO_PAGE_INDEX.
    pub fn write_schema_info(&mut self) -> Result<()> {
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
