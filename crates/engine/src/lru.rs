use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
};

pub struct LRUCache<TKey, TValue> {
    capacity: usize,
    items: HashMap<TKey, TValue>,
    order: RefCell<VecDeque<TKey>>,
}

impl<TKey : Eq + std::hash::Hash + Clone, TValue> LRUCache<TKey, TValue> {
    pub fn new(capacity: usize) -> Self {
        LRUCache { 
            capacity,
            items: HashMap::with_capacity(capacity),
            order: RefCell::new(VecDeque::new()),
        }
    }

    pub fn get(&self, key: &TKey) -> Option<&TValue> {
        if self.items.contains_key(key) {
            let mut order = self.order.borrow_mut();
            order.retain(|i| i != key);
            order.push_back(key.to_owned());

            return self.items.get(key);
        } else {
            None
        }
    }

    pub fn put(&mut self, key: &TKey, value: TValue) {
        let mut order = self.order.borrow_mut();

        if self.items.contains_key(key) {
            order.retain(|i| i != key);
        }
        else if self.items.len() == self.capacity {
            if let Some(oldest_item_key) = order.pop_front() {
                self.items.remove(&oldest_item_key);
            }
        }

        order.push_back(key.to_owned());
        self.items.insert(key.to_owned(), value);
    }
}

#[cfg(test)]
mod lru_tests {
    use super::LRUCache;

    #[test]
    fn test() {
        let mut lru = LRUCache::<usize, usize>::new(3);

        lru.put(&1, 1);

        // Item 1 should be added
        let index_1 = lru.get(&1);
        assert_eq!(*index_1.unwrap(), 1);

        // 1 should be be at the start of the order
        {
            let order = lru.order.borrow();
            assert_eq!(order[0], 1);
        }

        lru.put(&2, 2);

        // Item 2 should be added
        let index_2 = lru.get(&2);
        assert_eq!(*index_2.unwrap(), 2);

        // Adding 2 more elements should exceed our capacity,
        // pushing 1 out of the LUR
        lru.put(&3, 3);
        lru.put(&4, 4);

        assert_eq!(lru.items.len(), 3);

        let mut values: Vec<_> = lru.items.values().cloned().collect();
        values.sort();
        assert_eq!(values, [2, 3, 4]);
    }
}
