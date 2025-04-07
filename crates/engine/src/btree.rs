/// B-Tree implementation
///
/// A B-tree is a data structure that stores sorted data that is quick to search (O(log n)).
/// Interior nodes store keys and pointers to other nodes, while leaf nodes store keys and values.
///

/// The maximum number of keys a node can have
const MAX_KEYS: usize = 4;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct KeyValuePair<K, V> {
    pub key: K,
    pub value: V,
}

type Key = u32;
type Value = Vec<u8>;

type InteriorItem = KeyValuePair<Key, NodeType>;
type LeafItem = KeyValuePair<Key, Value>;

impl<K, V> KeyValuePair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        KeyValuePair { key, value }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum NodeType {
    Interior(Vec<InteriorItem>),
    Leaf(Vec<LeafItem>),
}

pub struct BTree {
    pub root: NodeType,
}

impl BTree {
    pub fn new() -> Self {
        let root = NodeType::Leaf(Vec::new());

        BTree { root }
    }

    pub fn add(&mut self, key: Key, value: Value) {
        let mut current_node = &mut self.root;

        loop {
            match current_node {
                NodeType::Interior(vec) => {
                    // find the correct child node
                    let mut i = 0;

                    while i < vec.len() && key > vec[i].key {
                        i += 1;
                    }

                    // Update the current node to the child node
                    current_node = &mut vec[i].value;
                }
                NodeType::Leaf(vec) => {
                    let mut i = 0;

                    while i < vec.len() && key > vec[i].key {
                        i += 1;
                    }

                    let kv = KeyValuePair::new(key, value.clone());
                    vec.insert(i, kv);

                    if vec.len() > MAX_KEYS {
                        //split
                    }

                    break;
                }
            }
        }
    }
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod btree_tests {
    use super::*;

    #[test]
    fn new_btree() {
        let actual = BTree::new();

        assert_eq!(actual.root, NodeType::Leaf(Vec::new()));
    }

    #[test]
    fn b_tree_add_key() {
        let mut actual = BTree::new();
        actual.add(1, vec![2]);

        assert_eq!(
            actual.root,
            NodeType::Leaf(vec![KeyValuePair::new(1, vec![2])])
        )
    }

    #[test]
    fn b_tree_add_keys_sorted() {
        let mut actual = BTree::new();

        actual.add(10, vec![10]);
        actual.add(5, vec![5]);
        actual.add(8, vec![8]);

        assert_eq!(
            actual.root,
            NodeType::Leaf(vec![
                KeyValuePair::new(5, vec![5]),
                KeyValuePair::new(8, vec![8]),
                KeyValuePair::new(10, vec![10]),
            ])
        )
    }

    #[test]
    fn b_tree_deep() {
        let mut actual = BTree::new();

        actual.root = NodeType::Interior(vec![
            KeyValuePair::new(10, NodeType::Leaf(vec![KeyValuePair::new(10, vec![10])])),
            KeyValuePair::new(20, NodeType::Leaf(vec![KeyValuePair::new(20, vec![20])])),
        ]);

        actual.add(15, vec![15]);

        let expected = NodeType::Interior(vec![
            KeyValuePair::new(10, NodeType::Leaf(vec![KeyValuePair::new(10, vec![10])])),
            KeyValuePair::new(
                20,
                NodeType::Leaf(vec![
                    KeyValuePair::new(15, vec![15]),
                    KeyValuePair::new(20, vec![20]),
                ]),
            ),
        ]);

        assert_eq!(actual.root, expected);
    }
}
