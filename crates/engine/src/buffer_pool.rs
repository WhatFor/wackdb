use std::sync::Mutex;

use crate::{file_format::FileType, fm::FileManager, lru::LRUCache, page::PageId};

pub type PageBytes = [u8; 8192];

//pub const BUFFER_POOL_CAPACITY: usize = 131_072; // 1GB
pub const BUFFER_POOL_CAPACITY: usize = 10; // Test

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FilePageId {
    pub db_id: u16,
    pub page_index: u32,
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

    pub fn get_page(&self, id: &FilePageId, file_manager: &FileManager) -> Option<PageBytes> {
        let mut lru = self.lru_cache.lock().unwrap();

        if let Some(page) = lru.get(id) {
            return Some(*page);
        }

        let file = file_manager.get_from_id(id.db_id, FileType::Primary);

        match file {
            Some(file_handle) => {
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
            None => None,
        }
    }

    pub fn put_page(&self, id: &FilePageId, data: PageBytes) {
        // TODO: This probably needs to do a lot more than just put it into the cache.
        let mut lru = self.lru_cache.lock().unwrap();
        lru.put(id, data);
    }
}

#[cfg(test)]
mod buffer_pool_tests {
    use super::{BufferPool, PageBytes};
    use crate::{buffer_pool::FilePageId, fm::FileManager};

    #[test]
    fn test_put_and_get() {
        let fm = FileManager::new();
        let buffer_pool = BufferPool::new(3);

        let mut page: PageBytes = [0; 8192];
        page[0] = 5;

        let ix = FilePageId::new(0, 1);
        buffer_pool.put_page(&ix, page);
        let read_value = buffer_pool.get_page(&ix, &fm);

        assert_eq!(read_value.unwrap(), page);
    }

    #[test]
    fn test_capacity() {
        let fm = FileManager::new();
        let buffer_pool = BufferPool::new(3);

        let page: PageBytes = [0; 8192];

        buffer_pool.put_page(&FilePageId::new(0, 1), page);
        buffer_pool.put_page(&FilePageId::new(0, 2), page);
        buffer_pool.put_page(&FilePageId::new(0, 3), page);
        buffer_pool.put_page(&FilePageId::new(0, 4), page);

        let read_value_evicted = buffer_pool.get_page(&FilePageId::new(0, 1), &fm);
        assert_eq!(read_value_evicted, None);

        let read_value_exists = buffer_pool.get_page(&FilePageId::new(0, 2), &fm);
        assert_eq!(read_value_exists.unwrap(), page);
    }
}
