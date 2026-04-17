use crate::{file_format::FileType, fm::FileManager, lru::LRUCache, page::PageId, persistence};

pub type PageBytes = [u8; 8192];

//pub const PAGE_CACHE_CAPACITY: usize = 131_072; // 1GB
pub const PAGE_CACHE_CAPACITY: usize = 10; // Test

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

pub type FilePageCache = LRUCache<FilePageId, PageBytes>;

pub struct PageCache {
    lru_cache: FilePageCache,
}

impl Default for PageCache {
    fn default() -> Self {
        Self::new(PAGE_CACHE_CAPACITY)
    }
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        let lru_cache = FilePageCache::new(capacity);

        PageCache { lru_cache }
    }

    pub fn get_page(&mut self, id: &FilePageId, file_manager: &FileManager) -> Option<PageBytes> {
        if let Some(page) = self.lru_cache.get(id) {
            return Some(*page);
        }

        let file = file_manager.get_from_id(id.db_id, FileType::Primary);

        match file {
            Some(file_handle) => {
                let disk_page = persistence::read_page(file_handle, id.page_index);

                match disk_page {
                    Ok(disk_page_ok) => {
                        self.lru_cache.put(id, disk_page_ok);

                        if let Some(created) = self.lru_cache.get(id) {
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

    pub fn put_page(&mut self, id: &FilePageId, data: PageBytes) {
        // TODO: This probably needs to do a lot more than just put it into the cache.
        self.lru_cache.put(id, data);
    }
}

#[cfg(test)]
mod page_cache_tests {
    use super::{PageBytes, PageCache};
    use crate::{fm::FileManager, page_cache::FilePageId};

    #[test]
    fn test_put_and_get() {
        let fm = FileManager::new();
        let mut page_cache = PageCache::new(3);

        let mut page: PageBytes = [0; 8192];
        page[0] = 5;

        let ix = FilePageId::new(0, 1);
        page_cache.put_page(&ix, page);
        let read_value = page_cache.get_page(&ix, &fm);

        assert_eq!(read_value.unwrap(), page);
    }

    #[test]
    fn test_capacity() {
        let fm = FileManager::new();
        let mut page_cache = PageCache::new(3);

        let page: PageBytes = [0; 8192];

        page_cache.put_page(&FilePageId::new(0, 1), page);
        page_cache.put_page(&FilePageId::new(0, 2), page);
        page_cache.put_page(&FilePageId::new(0, 3), page);
        page_cache.put_page(&FilePageId::new(0, 4), page);

        let read_value_evicted = page_cache.get_page(&FilePageId::new(0, 1), &fm);
        assert_eq!(read_value_evicted, None);

        let read_value_exists = page_cache.get_page(&FilePageId::new(0, 2), &fm);
        assert_eq!(read_value_exists.unwrap(), page);
    }
}
