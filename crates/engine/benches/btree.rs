use engine::btree::BTree;

extern crate engine;

fn main() {
    divan::main();
}

#[divan::bench(args = [1, 32, 64, 128, 256, 512, 1024, 8192])]
fn write_nodes(n: u16) {
    let mut tree = BTree::new();

    for _ in 0..n {
        let k = 0;
        let v = vec![0];
        tree.add(k, v);
    }
}
