extern crate engine;

use engine::page::{self, PageEncoder, PageHeader};

fn main() {
    divan::main();
}

#[divan::bench(args = [1, 2, 4, 8, 16, 32])]
fn write_slots(n: u64) {
    let header = PageHeader::new(page::PageType::DatabaseInfo);
    let mut encoder = PageEncoder::new(header);

    for _ in 0..n {
        let slot = vec![0; 32];
        if let Err(e) = encoder.add_slot_bytes(slot) {
            panic!("Error adding slot: {:?}", e);
        }
    }

    encoder.collect();
}
