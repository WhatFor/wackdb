use std::{
    cell::{RefCell, RefMut},
    rc::Rc,
};

use crate::{
    page::PageId,
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

pub struct IndexPagerIterator<'a> {
    root_page: PageId,
    file_id: u16,
    current_page: PageId,
    page_cache: RefMut<'a, PageCache>,
}

impl<'a> IndexPagerIterator<'a> {
    pub fn new(index_root_file_page_id: FilePageId, page_cache: Rc<RefCell<PageCache>>) -> Self {
        IndexPagerIterator {
            current_page: index_root_file_page_id.page_index,
            root_page: index_root_file_page_id.page_index,
            file_id: index_root_file_page_id.db_id,
            page_cache: page_cache.borrow_mut(),
        }
    }
}

impl<'a> Iterator for IndexPagerIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let _curr_page = self
            .page_cache
            .get_page(&FilePageId::new(self.file_id, self.current_page));

        todo!()
    }
}
