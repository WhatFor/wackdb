use bincode::{
    config::{BigEndian, Configuration, Fixint},
    Decode, Encode,
};

use crate::{PAGE_HEADER_SIZE_BYTES, PAGE_SIZE_BYTES};

/// The max, current version number for the Page Header record
pub const CURRENT_HEADER_VERSION: u8 = 1;

/// The amount of bytes needed to store a slot pointer in the page.
pub const SLOT_POINTER_SIZE: u16 = 2;

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
    pub fn new(page_type: PageType) -> Self {
        let free_space = PAGE_SIZE_BYTES - PAGE_HEADER_SIZE_BYTES;

        PageHeader {
            page_id: 0, // TODO
            header_version: CURRENT_HEADER_VERSION,
            page_type: PageHeader::u8_from_page_type(page_type),
            checksum: 0, // Not calc'd until collected
            flags: 0,    // Not set
            allocated_slot_count: 0,
            free_space,
            free_space_start_offset: PAGE_HEADER_SIZE_BYTES,
            free_space_end_offset: PAGE_SIZE_BYTES,
            total_allocated_bytes: PAGE_HEADER_SIZE_BYTES,
        }
    }

    pub fn u8_from_page_type(page_type: PageType) -> u8 {
        match page_type {
            PageType::FileInfo => 0,
            PageType::DatabaseInfo => 1,
        }
    }

    pub fn _page_type_from_u8(page_type: u8) -> PageType {
        match page_type {
            0 => PageType::FileInfo,
            1 => PageType::DatabaseInfo,
            _ => panic!("Unmatched PageType."),
        }
    }
}

pub struct PageEncoder {
    header: PageHeader,
    slots: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub enum PageEncoderError {
    NotEnoughSpace,
}

#[derive(Debug)]
pub struct AddSlot {
    pointer_index: u16,
}

impl PageEncoder {
    pub fn new(header: PageHeader) -> Self {
        PageEncoder {
            header,
            slots: vec![],
        }
    }

    pub fn has_space_for(&self, len: u16) -> bool {
        // Verify if the page has space for the slot and it's pointer
        self.header.free_space >= (len + SLOT_POINTER_SIZE)
    }

    pub fn add_slot(&mut self, slot: Vec<u8>) -> Result<AddSlot, PageEncoderError> {
        let length = slot.len() as u16;
        let has_space = self.has_space_for(length);

        match has_space {
            true => {
                self.slots.push(slot);

                self.header.allocated_slot_count += 1;
                self.header.free_space -= length;
                self.header.total_allocated_bytes += length;
                self.header.free_space_start_offset += length;
                self.header.free_space_end_offset -= SLOT_POINTER_SIZE;

                let pointer_index = self.header.allocated_slot_count - 1;
                Ok(AddSlot { pointer_index })
            }
            false => Err(PageEncoderError::NotEnoughSpace),
        }
    }

    /// Complete operations on the page and fetch the bytes.
    /// Computes the page hash.
    /// No other operations should be performed on the page after this function is called!
    pub fn collect(&mut self) -> Vec<u8> {
        let try_collect = self.collect_internal();

        match try_collect {
            Some(mut bytes) => {
                // Only run checksum on the body
                let body_bytes = &bytes[PAGE_HEADER_SIZE_BYTES.into()..];
                let body_checksum = check(body_bytes);
                let _ = &bytes[6..8].copy_from_slice(&body_checksum);

                bytes
            }
            None => {
                panic!("TODO")
            }
        }
    }

    fn collect_internal(&mut self) -> Option<Vec<u8>> {
        let mut bytes = vec![0; crate::PAGE_SIZE_BYTES.into()];

        // Write Header
        let config = header_bincode_config();
        let header_result = bincode::encode_into_slice(&self.header, &mut bytes, config);

        for slot in &self.slots {
            // TODO: Write slot and slot pointer
        }

        match header_result {
            Ok(_) => Some(bytes),
            Err(_) => None,
        }
    }
}

fn check(bytes: &[u8]) -> [u8; 2] {
    let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
    crc.checksum(&bytes).to_be_bytes()
}

pub struct PageDecoder<'a> {
    bytes: &'a Vec<u8>,
    header: PageHeader,
}

#[derive(Debug)]
pub struct ChecksumResult {
    pub pass: bool,
    pub expected: [u8; 2],
    pub actual: [u8; 2],
}

impl<'a> PageDecoder<'a> {
    pub fn from_bytes(bytes: &'a Vec<u8>) -> Self {
        let config = header_bincode_config();

        let header_slice = &bytes[0..PAGE_HEADER_SIZE_BYTES.into()];
        let header_decode = bincode::decode_from_slice::<PageHeader, _>(header_slice, config);

        println!("{header_decode:?}");

        match header_decode {
            Ok((header, _)) => PageDecoder { header, bytes },
            Err(err) => {
                // TODO
                panic!("{err}")
            }
        }
    }

    pub fn check(&self) -> ChecksumResult {
        let body_bytes = &self.bytes[PAGE_HEADER_SIZE_BYTES.into()..];

        let expected = self.header.checksum.to_be_bytes();
        let actual = check(body_bytes);

        let pass = expected == actual;

        ChecksumResult {
            pass,
            expected,
            actual,
        }
    }
}

fn header_bincode_config() -> Configuration<BigEndian, Fixint> {
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_big_endian()
}

#[cfg(test)]
mod page_encoder_tests {
    use crate::*;
    use page::{PageEncoder, PageEncoderError, PageHeader};

    #[test]
    fn test_page_encoder_header_only() {
        let header = PageHeader::new(page::PageType::DatabaseInfo);
        let mut encoder = PageEncoder::new(header);
        let bytes = encoder.collect();

        let actual_header_bytes = &bytes[0..PAGE_HEADER_SIZE_BYTES.into()];

        // The Free Space should be the page size, less the entire header.
        let fs = (PAGE_SIZE_BYTES - PAGE_HEADER_SIZE_BYTES).to_be_bytes();

        // The Free Space start should point to after the header.
        let fs_st = PAGE_HEADER_SIZE_BYTES.to_be_bytes();

        // The Free Space end should point to the end of the file.
        let fs_end = PAGE_SIZE_BYTES.to_be_bytes();

        // Should allocate only the header
        let aloc = PAGE_HEADER_SIZE_BYTES.to_be_bytes();

        // Should create using the current version.
        let ver = page::CURRENT_HEADER_VERSION;

        // Checksum
        let body_bytes = &bytes[PAGE_HEADER_SIZE_BYTES.into()..];
        let cs = page::check(body_bytes);

        // Ensure the body is as expected
        assert_eq!(
            body_bytes.len(),
            (PAGE_SIZE_BYTES - PAGE_HEADER_SIZE_BYTES).into()
        );

        // Multibyte values should be BigEndian
        let expected_header_bytes = vec![
            0, 0, 0, 0,   // ID - Currently not implemented
            ver, // Version
            1,   // Page Type - DatabaseInfo
            cs[0], cs[1], // Checksum
            0, 0, // Flags - None set
            0, 0, // Allocated Slot Count
            fs[0], fs[1], // Free Space
            fs_st[0], fs_st[1], // Free Space Start Offset
            fs_end[0], fs_end[1], // Free Space End Offset
            aloc[0], aloc[1], // Total Allocated Bytes
            0, 0, 0, 0, 0, 0, // Reserved space - 6 bytes
            0, 0, 0, 0, 0, 0, // Reserved space - 6 bytes
        ];

        assert_eq!(actual_header_bytes, expected_header_bytes);
        assert_eq!(bytes.len(), PAGE_SIZE_BYTES.into());
    }

    #[test]
    fn test_page_has_space_for_full_body() {
        let header = PageHeader::new(page::PageType::DatabaseInfo);
        let encoder = PageEncoder::new(header);

        // Try to fill the entire body (less 2 bytes for the slot pointer)
        let body_length = PAGE_SIZE_BYTES - PAGE_HEADER_SIZE_BYTES - 2;

        let actual = encoder.has_space_for(body_length);
        let expected = true;

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_page_add_slot_success() {
        let header = PageHeader::new(page::PageType::DatabaseInfo);
        let mut encoder = PageEncoder::new(header);

        let slot1 = vec![1, 2];
        let slot2 = vec![1, 2];

        // Expect allocated a header (32 bytes) and 2 pages each of length 2.
        let expected_len = PAGE_HEADER_SIZE_BYTES + 2 + 2;

        let slot_result_1 = encoder.add_slot(slot1.clone());
        let slot_result_2 = encoder.add_slot(slot2.clone());

        // Verify Result
        assert_eq!(slot_result_1.is_ok(), true);
        assert_eq!(slot_result_1.expect("Failed to add slot.").pointer_index, 0);

        assert_eq!(slot_result_2.is_ok(), true);
        assert_eq!(slot_result_2.expect("Failed to add slot.").pointer_index, 1);

        // Verify Internals
        assert_eq!(encoder.slots[0], slot1);
        assert_eq!(encoder.slots[1], slot2);
        assert_eq!(encoder.header.allocated_slot_count, 2);
        assert_eq!(encoder.header.total_allocated_bytes, expected_len);
    }

    #[test]
    fn test_page_add_slot_fail() {
        let header = PageHeader::new(page::PageType::DatabaseInfo);
        let mut encoder = PageEncoder::new(header);
        let slot = vec![0; (PAGE_SIZE_BYTES + 1) as usize];

        let slot_result = encoder.add_slot(slot.clone());

        // Verify Result
        assert_eq!(slot_result.is_err(), true);
        assert_eq!(slot_result.err().unwrap(), PageEncoderError::NotEnoughSpace);
    }

    #[test]
    fn test_page_encoder_body() {
        // let header = PageHeader::new(page::PageType::DatabaseInfo);
        // let encoder_r = PageEncoder::new(header);
        // let mut encoder = encoder_r.expect("Failed to build page encoder.");

        // let body = FileInfo::new(master::FileType::Primary);
        // let _actual = encoder.body(&body);

        // // TODO: need to be able to read slots!
    }
}
