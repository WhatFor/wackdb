use crate::{
    engine::Storage,
    page::{PageDecoder, PageId},
    page_cache::FilePageId,
};

pub struct IndexPager<'a> {
    file_id: u16,
    current_page: PageId,
    current_page_slot_index: u16,
    storage: &'a mut Storage,
}

impl<'a> IndexPager<'a> {
    pub fn new(index_root_file_page_id: FilePageId, storage: &'a mut Storage) -> Self {
        IndexPager {
            file_id: index_root_file_page_id.db_id,
            current_page: index_root_file_page_id.page_index,
            current_page_slot_index: 0,
            storage,
        }
    }
}

impl<'a> Iterator for IndexPager<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let page_bytes = self.storage.page_cache.get_page(
            &FilePageId::new(self.file_id, self.current_page),
            &mut self.storage.file_manager,
        )?;

        // TODO: This feels like quite a costly operation - can we instead cache the parsed page instead of just the bytes?
        //       It's decoding the page for EVERY slot we iter over!
        let page = PageDecoder::from_bytes(&page_bytes);

        let slot = page.try_read_bytes(self.current_page_slot_index);

        if let Err(err) = slot {
            log::debug!("IndexPagerIterator error: {}", err);
            return None;
        }

        self.current_page_slot_index += 1;

        if self.current_page_slot_index == (page.header().allocated_slot_count + 1) {
            self.current_page += 1;
            self.current_page_slot_index = 0;
        }

        Some(slot.unwrap())
    }
}
