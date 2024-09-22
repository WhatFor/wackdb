use crate::lru::LRUCache;

type Page = [u8; 8192];

pub struct PageCache {
    cache: LRUCache<u32, Page>,
}

impl<'a> PageCache {
    pub fn new(capacity: usize) -> Self {
        PageCache {
            cache: LRUCache::<u32, Page>::new(capacity),
        }
    }

    pub fn get_page(&'a mut self, page_index: u32) -> Option<&'a Page> {
        self.cache.get(&page_index)
    }

    pub fn put_page(&'a mut self, page_index: u32, data: Page) {
        self.cache.put(page_index, data);
    }
}

#[cfg(test)]
mod page_cache_tests {
    use super::{Page, PageCache};

    #[test]
    fn test_put_and_get() {
        let mut page_cache = PageCache::new(3);

        let mut page: Page = [0; 8192];
        page[0] = 5;

        page_cache.put_page(1, page);

        let read_value = page_cache.get_page(1);

        assert_eq!(*read_value.unwrap(), page);
    }

    #[test]
    fn test_capacity() {
        let mut page_cache = PageCache::new(3);

        let page: Page = [0; 8192];

        page_cache.put_page(1, page);
        page_cache.put_page(2, page);
        page_cache.put_page(3, page);
        page_cache.put_page(4, page);

        let read_value_evicted = page_cache.get_page(1);
        assert_eq!(read_value_evicted, None);

        let read_value_exists = page_cache.get_page(2);
        assert_eq!(*read_value_exists.unwrap(), page);
    }
}
