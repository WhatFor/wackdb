use anyhow::{bail, Result};
use std::sync::Mutex;

use crate::{
    file::{DatabaseFileId, ManagedFile},
    file_format::FileType,
    fm::FileManager,
    lru::LRUCache,
    page::PageId,
};

pub type PageBytes = [u8; 8192];

//pub const BUFFER_POOL_CAPACITY: usize = 131_072; // 1GB
pub const BUFFER_POOL_CAPACITY: usize = 10; // Test

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FilePageId {
    pub db_id: DatabaseFileId,
    pub page_index: PageId,
}

impl FilePageId {
    pub fn new(db_id: u16, page_index: PageId) -> Self {
        FilePageId { db_id, page_index }
    }
}

pub type PageBufferPool = LRUCache<FilePageId, PageBytes>;

pub struct BufferPool {
    lru_cache: Mutex<PageBufferPool>,
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(BUFFER_POOL_CAPACITY)
    }
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        let lru_cache = PageBufferPool::new(capacity);

        BufferPool {
            lru_cache: Mutex::new(lru_cache),
        }
    }

    pub fn get_page_as<'a, T>(&self, id: &FilePageId, file_manager: &FileManager) -> Result<T>
    where
        T: deku::DekuContainerRead<'a> + std::fmt::Debug,
    {
        let bytes = self.get_page(&id, file_manager);

        match bytes {
            Some(page_bytes) => {
                let page = crate::page::PageDecoder::from_bytes(&page_bytes);
                let bytes = page.try_read::<T>(0)?;

                Ok(bytes)
            }
            None => bail!("Page not found."), // TODO: Do better :)
        }
    }

    pub fn get_page(&self, id: &FilePageId, file_manager: &FileManager) -> Option<PageBytes> {
        let mut lru = self.lru_cache.lock().unwrap();

        if let Some(page) = lru.get(id) {
            return Some(*page);
        }

        let file = file_manager.get_from_id(id.db_id, FileType::Primary);

        match file {
            Some(ManagedFile::Paged(file_handle)) => {
                let disk_page = file_handle.read_page(id.page_index);

                match disk_page {
                    Ok(disk_page_ok) => {
                        lru.put(id, disk_page_ok);

                        if let Some(created) = lru.get(id) {
                            return Some(*created);
                        }

                        None
                    }
                    Err(_err) => None,
                }
            }
            Some(crate::file::ManagedFile::Raw(_)) => todo!("Shouldn't happen."),
            None => None,
        }
    }

    pub fn add_page(
        &self,
        id: DatabaseFileId,
        data: PageBytes,
        file_manager: &FileManager,
    ) -> Result<PageId> {
        let file = file_manager.get_from_id(id, FileType::Primary);

        match file {
            Some(ManagedFile::Paged(db_file)) => {
                let next_page_id = db_file.allocated_page_count()? + 1;

                self.put_page(
                    &FilePageId {
                        db_id: id,
                        page_index: next_page_id,
                    },
                    data,
                    file_manager,
                )?;

                Ok(next_page_id)
            }
            Some(crate::file::ManagedFile::Raw(_)) => todo!("Shouldn't happen."),
            None => bail!("File not found!"), // TODO: Do better :)
        }
    }

    pub fn put_page(
        &self,
        id: &FilePageId,
        data: PageBytes,
        file_manager: &FileManager,
    ) -> Result<()> {
        // TODO:
        // need to start tracking what's been flushed and what hasn't, and bulk flushing.
        // for now, just write everything to disk. this is obviously very slow and jank.
        let file = file_manager.get_from_id(id.db_id, FileType::Primary);

        match file {
            Some(ManagedFile::Paged(db_file)) => {
                db_file.write_page(&data, id.page_index)?;

                let mut lru = self.lru_cache.lock().unwrap();
                lru.put(id, data);

                Ok(())
            }
            Some(crate::file::ManagedFile::Raw(_)) => todo!("Shouldn't happen."),
            None => bail!("File not found!"), // TODO: Do better :)
        }
    }
}

#[cfg(test)]
mod buffer_pool_tests {
    use super::{BufferPool, PageBytes};

    use crate::{
        buffer_pool::FilePageId,
        file::{ManagedFile, MemoryFile},
        file_format::FileType,
        fm::{FileId, FileManager},
    };

    use anyhow::Result;

    #[test]
    fn test_put_and_get() -> Result<()> {
        let mut fm = FileManager::new();

        fm.add(
            FileId::new(1, String::from("File 1"), FileType::Primary),
            ManagedFile::Paged(Box::new(MemoryFile::new(vec![]))),
            0,
        );

        let buffer_pool = BufferPool::new(3);
        let page: PageBytes = [1; 8192];

        let ix = FilePageId::new(1, 1);
        buffer_pool.add_page(ix.db_id, page, &fm)?;
        let read_value = buffer_pool.get_page(&ix, &fm);

        assert_eq!(read_value.unwrap(), page);

        Ok(())
    }

    #[test]
    fn test_capacity() -> Result<()> {
        let mut fm = FileManager::new();
        let db = 1;

        fm.add(
            FileId::new(db, String::from("File 1"), FileType::Primary),
            ManagedFile::Paged(Box::new(MemoryFile::new(vec![]))),
            0,
        );

        let buffer_pool = BufferPool::new(3);

        let page_1: PageBytes = [1; 8192];
        buffer_pool.put_page(&FilePageId::new(db, 1), page_1, &fm)?;
        let page_2: PageBytes = [2; 8192];
        buffer_pool.put_page(&FilePageId::new(db, 2), page_2, &fm)?;
        let page_3: PageBytes = [3; 8192];
        buffer_pool.put_page(&FilePageId::new(db, 3), page_3, &fm)?;
        let page_4: PageBytes = [4; 8192];
        buffer_pool.put_page(&FilePageId::new(db, 4), page_4, &fm)?;

        // This file was evicted from the LRU cache (capacity: 3), but should still return as it can be read from the file.
        let read_value_evicted = buffer_pool.get_page(&FilePageId::new(db, 1), &fm);
        assert_eq!(read_value_evicted.unwrap(), page_1);

        let read_value_exists = buffer_pool.get_page(&FilePageId::new(db, 2), &fm);
        assert_eq!(read_value_exists.unwrap(), page_2);

        Ok(())
    }
}
