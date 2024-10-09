extern crate engine;

use engine::page::{self, PageEncoder, PageHeader};

fn main() {
    divan::main();
}

#[divan::bench(args = [1, 2, 4, 8, 16, 32, 64, 128, 240])]
fn write_slots(n: u16) {
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
