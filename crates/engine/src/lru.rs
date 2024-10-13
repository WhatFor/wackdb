use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
};

pub struct LRUCache<K, V> {
    capacity: usize,
    map: HashMap<K, V>,
    order: RefCell<VecDeque<K>>,
}

impl<K: std::hash::Hash + Eq + Clone, V> LRUCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        LRUCache {
            capacity,
            map: HashMap::new(),
            order: RefCell::new(VecDeque::new()),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.map.contains_key(key) {
            let mut order = self.order.borrow_mut();
            order.retain(|k| k != key);
            order.push_back(key.clone());

            self.map.get(key)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: &K, value: V) {
        let mut order = self.order.borrow_mut();

        if self.map.contains_key(key) {
            order.retain(|k| k != key);
        } else if self.map.len() == self.capacity {
            if let Some(old_key) = order.pop_front() {
                self.map.remove(&old_key);
            }
        }

        order.push_back(key.clone());
        self.map.insert(key.to_owned(), value);
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

        assert_eq!(lru.map.len(), 3);

        let mut values: Vec<_> = lru.map.values().cloned().collect();
        values.sort();
        assert_eq!(values, [2, 3, 4]);
    }
}
