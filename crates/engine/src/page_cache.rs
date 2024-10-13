use crate::{
    db::FileType,
    fm::{FileId, FileManager},
    lru::LRUCache,
    persistence,
};
use std::{cell::RefCell, rc::Rc};

pub type PageBytes = [u8; 8192];

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FilePageId {
    db_id: u16,
    page_index: u32,
}

impl FilePageId {
    pub fn new(db_id: u16, page_index: u32) -> Self {
        FilePageId { db_id, page_index }
    }
}

pub type FilePageCache = LRUCache<FilePageId, PageBytes>;

pub struct PageCache {
    lru_cache: Rc<RefCell<FilePageCache>>,
    file_manager: Rc<RefCell<FileManager>>,
}

impl PageCache {
    pub fn new(capacity: usize, file_manager: Rc<RefCell<FileManager>>) -> Self {
        let lru_cache = Rc::new(RefCell::new(FilePageCache::new(capacity)));

        PageCache {
            lru_cache,
            file_manager,
        }
    }

    pub fn get_page(&mut self, id: &FilePageId) -> Option<PageBytes> {
        if let Some(page) = self.lru_cache.borrow_mut().get(id) {
            return Some(*page);
        }

        let fm_borrow = self.file_manager.borrow();

        let file = fm_borrow.get(&FileId {
            id: id.db_id,
            ty: FileType::Primary,
        });

        match file {
            Some(file_handle) => {
                let disk_page = persistence::read_page(file_handle, id.page_index);

                match disk_page {
                    Ok(disk_page_ok) => {
                        self.lru_cache.borrow_mut().put(id, disk_page_ok);

                        if let Some(created) = self.lru_cache.borrow_mut().get(id) {
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
        self.lru_cache.borrow_mut().put(id, data);
    }
}

#[cfg(test)]
mod page_cache_tests {
    use std::{cell::RefCell, rc::Rc};

    use crate::{fm::FileManager, page_cache::FilePageId};

    use super::{PageBytes, PageCache};

    #[test]
    fn test_put_and_get() {
        let fm = Rc::new(RefCell::new(FileManager::new()));
        let mut page_cache = PageCache::new(3, Rc::clone(&fm));

        let mut page: PageBytes = [0; 8192];
        page[0] = 5;

        let ix = FilePageId::new(0, 1);
        page_cache.put_page(&ix, page);
        let read_value = page_cache.get_page(&ix);

        assert_eq!(read_value.unwrap(), page);
    }

    #[test]
    fn test_capacity() {
        let fm = Rc::new(RefCell::new(FileManager::new()));
        let mut page_cache = PageCache::new(3, Rc::clone(&fm));

        let page: PageBytes = [0; 8192];

        page_cache.put_page(&FilePageId::new(0, 1), page);
        page_cache.put_page(&FilePageId::new(0, 2), page);
        page_cache.put_page(&FilePageId::new(0, 3), page);
        page_cache.put_page(&FilePageId::new(0, 4), page);

        let read_value_evicted = page_cache.get_page(&FilePageId::new(0, 1));
        assert_eq!(read_value_evicted, None);

        let read_value_exists = page_cache.get_page(&FilePageId::new(0, 2));
        assert_eq!(read_value_exists.unwrap(), page);
    }
}
