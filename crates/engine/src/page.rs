use bincode::{Decode, Encode};

pub enum PageType {
    // 0
    FileInfo,
    // 1
    DatabaseInfo,
}

/// A general purpose Page header.
/// True length: 22 bytes.
/// Allocated length: 32 bytes.
#[derive(Encode, Decode, Debug)]
pub struct PageHeader {
    /// Offset: 0. Length: 4.
    page_id: u32,
    /// Offset: 4. Length: 1.
    header_version: u8,
    /// Offset: 5. Length: 1.
    page_type: u8,
    /// Offset: 6. Length: 2.
    checksum: u16,
    /// Offset: 8. Length: 2.
    flags: u16,
    /// Offset: 10. Length: 2.
    allocated_slot_count: u16,
    /// Offset: 12. Length: 2.
    free_space: u16,
    /// Offset: 14. Length: 2.
    free_space_start_offset: u16,
    /// Offset: 16. Length: 2.
    free_space_end_offset: u16,
    /// Offset: 20. Length: 2.
    total_allocated_bytes: u16,
}

impl PageHeader {
    pub fn new(page_type: PageType, header_version: u8, checksum: u16, flags: u16) -> Self {
        PageHeader {
            page_id: 0, // TODO
            header_version,
            page_type: PageHeader::u8_from_page_type(page_type),
            checksum,
            flags,
            allocated_slot_count: 4,    // TODO
            free_space: 5,              // TODO
            free_space_start_offset: 6, // TODO
            free_space_end_offset: 7,   // TODO
            total_allocated_bytes: 8,   // TODO
        }
    }

    pub fn u8_from_page_type(page_type: PageType) -> u8 {
        match page_type {
            PageType::FileInfo => 0,
            PageType::DatabaseInfo => 1,
        }
    }

    pub fn page_type_from_u8(page_type: u8) -> PageType {
        match page_type {
            0 => PageType::FileInfo,
            1 => PageType::DatabaseInfo,
            _ => panic!(""), // TODO
        }
    }
}
