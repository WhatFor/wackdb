use crate::lru::LRUCache;

pub type PageBytes = [u8; 8192];

pub struct PageCache {
    cache: LRUCache<u32, PageBytes>,
}

impl<'a> PageCache {
    pub fn new(capacity: usize) -> Self {
        PageCache {
            cache: LRUCache::<u32, PageBytes>::new(capacity),
        }
    }

    pub fn get_page(&'a mut self, page_index: u32) -> Option<&'a PageBytes> {
        let page = self.cache.get(&page_index);

        match page {
            Some(_) => page,
            None => {
                // TODO: need to figure out how to get page handle
                None
                // let disk_page = persistence::read_page(file, page_index);

                // match disk_page {
                //     Ok(disk_page_ok) => {
                //         self.put_page(page_index, disk_page_ok);
                //         Some(&disk_page_ok)
                //     }
                //     Err(_err) => None,
                // }
            }
        }
    }

    pub fn put_page(&'a mut self, page_index: u32, data: PageBytes) {
        // TODO: This probably needs to do a lot more than just put it into the cache.
        self.cache.put(page_index, data);
    }
}

#[cfg(test)]
mod page_cache_tests {
    use super::{PageBytes, PageCache};

    #[test]
    fn test_put_and_get() {
        let mut page_cache = PageCache::new(3);

        let mut page: PageBytes = [0; 8192];
        page[0] = 5;

        page_cache.put_page(1, page);

        let read_value = page_cache.get_page(1);

        assert_eq!(*read_value.unwrap(), page);
    }

    #[test]
    fn test_capacity() {
        let mut page_cache = PageCache::new(3);

        let page: PageBytes = [0; 8192];

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
