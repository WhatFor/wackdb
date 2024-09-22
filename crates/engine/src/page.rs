use core::fmt;

use deku::ctx::Endian;
use deku::prelude::*;

use crate::{page_cache::PageBytes, PAGE_HEADER_SIZE_BYTES, PAGE_SIZE_BYTES};

/// The max, current version number for the Page Header record
pub const CURRENT_HEADER_VERSION: u8 = 1;

/// The amount of bytes needed to store a slot pointer in the page.
pub const SLOT_POINTER_SIZE: u16 = 2;

#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(
    id_type = "u8",
    endian = "endian",
    ctx = "endian: deku::ctx::Endian",
    ctx_default = "Endian::Big"
)]
pub enum PageType {
    #[deku(id = 0)]
    FileInfo,
    #[deku(id = 1)]
    DatabaseInfo,
}

/// A general purpose Page header.
/// Allocated length: 32 bytes.
#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(endian = "big")]
pub struct PageHeader {
    #[deku(bytes = 4)]
    page_id: u32,

    #[deku(bytes = 1)]
    header_version: u8,

    #[deku]
    page_type: PageType,

    #[deku(bytes = 2)]
    checksum: u16,

    #[deku(bytes = 2)]
    flags: u16, // todo: need to add these. Know for sure I want a CAN_COMPACT flag.

    #[deku(bytes = 2)]
    allocated_slot_count: u16,

    #[deku(bytes = 2)]
    free_space: u16,

    #[deku(bytes = 2)]
    free_space_start_offset: u16,

    #[deku(bytes = 2)]
    free_space_end_offset: u16,

    #[deku(bytes = 2)]
    total_allocated_bytes: u16,
}

impl PageHeader {
    pub fn new(page_type: PageType) -> Self {
        let free_space = PAGE_SIZE_BYTES - PAGE_HEADER_SIZE_BYTES;

        PageHeader {
            page_id: 0, // TODO
            header_version: CURRENT_HEADER_VERSION,
            page_type: page_type,
            checksum: 0, // Not calc'd until collected
            flags: 0,    // Not set
            allocated_slot_count: 0,
            free_space,
            free_space_start_offset: PAGE_HEADER_SIZE_BYTES,
            free_space_end_offset: PAGE_SIZE_BYTES,
            total_allocated_bytes: PAGE_HEADER_SIZE_BYTES,
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
    FailedToSerialise(DekuError),
}

impl fmt::Display for PageEncoderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PageEncoderError::FailedToSerialise(e) => write!(f, "Failed to serialise: {}", e),
            PageEncoderError::NotEnoughSpace => write!(f, "Not enough space"),
        }
    }
}

impl std::error::Error for PageEncoderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PageEncoderError::FailedToSerialise(e) => Some(e),
            PageEncoderError::NotEnoughSpace => None,
        }
    }
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

    #[allow(dead_code)] // Used for testing
    pub fn add_slot_bytes(&mut self, slot: Vec<u8>) -> Result<AddSlot, PageEncoderError> {
        self.add_slot_internal(slot)
    }

    pub fn add_slot<T>(&mut self, slot: T) -> Result<AddSlot, PageEncoderError>
    where
        T: DekuContainerWrite,
    {
        let bytes = slot.to_bytes();

        match bytes {
            Ok(bytes_ok) => {
                let add_slot = self.add_slot_internal(bytes_ok);

                match &add_slot {
                    Ok(ok) => println!("Added slot. Index: {:?}", ok.pointer_index),
                    Err(_) => {}
                }

                add_slot
            }
            Err(e) => Err(PageEncoderError::FailedToSerialise(e)),
        }
    }

    fn add_slot_internal(&mut self, slot: Vec<u8>) -> Result<AddSlot, PageEncoderError> {
        let length = slot.len() as u16;
        let has_space = self.has_space_for(length);

        match has_space {
            true => {
                self.slots.push(slot);

                self.header.allocated_slot_count += 1;
                self.header.free_space -= length;
                self.header.total_allocated_bytes += length;

                // TODO: These are maintained during collect. Can't really maintain them here else we can't write
                // self.header.free_space_start_offset += length;
                // self.header.free_space_end_offset -= SLOT_POINTER_SIZE;

                let pointer_index = self.header.allocated_slot_count - 1;
                Ok(AddSlot { pointer_index })
            }
            false => Err(PageEncoderError::NotEnoughSpace),
        }
    }

    /// Complete operations on the page and fetch the bytes.
    /// Computes the page hash.
    /// No other operations should be performed on the page after this function is called!
    pub fn collect(&mut self) -> PageBytes {
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

    fn collect_internal(&mut self) -> Option<PageBytes> {
        let mut full_page_vec = [0; crate::PAGE_SIZE_BYTES_USIZE];

        let header_bytes = self.header.to_bytes();

        match header_bytes {
            Ok(header) => {
                // Write the header bytes into the page vec;
                // We could specifically write 32 bytes (our header length), but that would mean
                // we'd have to pad out the `header_bytes` from it's current size for no real win.
                full_page_vec[0..header.len() as usize].copy_from_slice(&header);

                for slot in &self.slots {
                    // Calculate the new start position of the free space,
                    // including the bytes we're writing
                    let slot_end_pointer = self.header.free_space_start_offset + slot.len() as u16;

                    // Write the bytes
                    let _ = &full_page_vec
                        [self.header.free_space_start_offset.into()..slot_end_pointer.into()]
                        .copy_from_slice(&slot);

                    // Set the new start of free space
                    self.header.free_space_start_offset = slot_end_pointer;

                    // Write the pointer
                    let pointer = slot_end_pointer.to_be_bytes();
                    let pointer_length = pointer.len() as u16;
                    let pointer_start = (self.header.free_space_end_offset - pointer_length).into();

                    let _ = &full_page_vec[pointer_start..self.header.free_space_end_offset.into()]
                        .copy_from_slice(&pointer);

                    // Set the new end of the free space
                    let free_space_end = self.header.free_space_end_offset - pointer_length;
                    self.header.free_space_end_offset = free_space_end;
                }

                Some(full_page_vec)
            }
            Err(_) => None,
        }
    }
}

fn check(bytes: &[u8]) -> [u8; 2] {
    let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
    crc.checksum(&bytes).to_be_bytes()
}

pub struct PageDecoder<'a> {
    bytes: &'a PageBytes,
    header: PageHeader,
}

#[derive(Debug)]
pub struct ChecksumResult {
    pub pass: bool,
    pub expected: [u8; 2],
    pub actual: [u8; 2],
}

impl<'a> PageDecoder<'a> {
    pub fn from_bytes(bytes: &'a PageBytes) -> Self {
        let mut cursor = std::io::Cursor::new(bytes);
        let mut reader = deku::reader::Reader::new(&mut cursor);
        let header = PageHeader::from_reader_with_ctx(&mut reader, ()).unwrap();

        println!("DBG: Loaded page from bytes.");
        println!("   |      Page Type: {:?}", header.page_type);
        println!("   |        Page ID: {:?}", header.page_id);
        println!("   | Header version: {:?}", header.header_version);
        println!("   |     Free space: {:?} bytes", header.free_space);
        println!("   |          Flags: {:?}", header.flags);
        println!("   |       Checksum: {:?}", header.checksum);
        println!("   |   Alloc. slots: {:?}", header.allocated_slot_count);

        PageDecoder { header, bytes }
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

#[cfg(test)]
mod page_encoder_tests {
    use crate::*;
    use deku::prelude::*;
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

        let slot_result_1 = encoder.add_slot_bytes(slot1.clone());
        let slot_result_2 = encoder.add_slot_bytes(slot2.clone());

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

    #[derive(DekuRead, DekuWrite, Debug, PartialEq)]
    #[deku(endian = "big")]
    struct TooBigForAPage {
        #[deku(bytes = 2)]
        len: u16,
        // Page size (8192) - Header size (32) - slot index (2) - len field (2) + 1 to overflow = 8157
        #[deku(bytes = 8157, count = "len")]
        id: Vec<u8>,
    }

    #[test]
    fn test_page_add_slot_fail() {
        let header = PageHeader::new(page::PageType::DatabaseInfo);
        let mut encoder = PageEncoder::new(header);

        let data = vec![0; 8157];
        let len = data.len() as u16;
        let slot = TooBigForAPage { id: data, len: len };

        let slot_result = encoder.add_slot(slot);

        // Verify Result
        assert_eq!(slot_result.is_err(), true);
        assert_eq!(slot_result.err().unwrap(), PageEncoderError::NotEnoughSpace);
    }

    // #[test]
    // fn test_page_encoder_body() {
    //     let header = PageHeader::new(page::PageType::DatabaseInfo);
    //     let encoder = PageEncoder::new(header);

    //     let body = FileInfo::new(master::FileType::Primary);
    //     let _actual = encoder.body(&body);

    //     // TODO: need to be able to read slots!
    // }
}
