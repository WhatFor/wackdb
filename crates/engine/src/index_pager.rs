use std::{
    cell::{RefCell, RefMut},
    rc::Rc,
};

use crate::{
    page::{PageDecoder, PageId},
    page_cache::{FilePageId, PageCache},
};

#[derive(Clone)]
pub struct IndexPager {
    page_cache: Rc<RefCell<PageCache>>,
}

impl IndexPager {
    pub fn new(page_cache: Rc<RefCell<PageCache>>) -> Self {
        IndexPager { page_cache }
    }

    pub fn create_pager(&self, index_root_file_page_id: FilePageId) -> IndexPagerIterator {
        IndexPagerIterator::new(index_root_file_page_id, Rc::clone(&self.page_cache))
    }
}

pub struct IndexPagerIterator {
    file_id: u16,
    current_page: PageId,
    current_page_slot: u16,
    page_cache: Rc<RefCell<PageCache>>,
}

impl IndexPagerIterator {
    pub fn new(index_root_file_page_id: FilePageId, page_cache: Rc<RefCell<PageCache>>) -> Self {
        IndexPagerIterator {
            file_id: index_root_file_page_id.db_id,
            current_page: index_root_file_page_id.page_index,
            current_page_slot: 0,
            page_cache,
        }
    }
}

impl Iterator for IndexPagerIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let page_cache = self.page_cache.borrow_mut();

        let page_bytes = page_cache.get_page(&FilePageId::new(self.file_id, self.current_page))?;
        let page = PageDecoder::from_bytes(&page_bytes);

        // TODO: what do we read?
        let slot = page.try_read(self.current_page_slot);

        self.current_page_slot += 1;

        todo!()
    }
}
