use anyhow::Result;
use deku::ctx::Endian;
use deku::prelude::*;
use thiserror::Error;

use crate::engine::{PAGE_HEADER_SIZE_BYTES, PAGE_SIZE_BYTES, PAGE_SIZE_BYTES_USIZE};
use crate::page_cache::PageBytes;

/// The max, current version number for the Page Header record
pub const CURRENT_HEADER_VERSION: u8 = 1;

/// The amount of bytes needed to store a slot pointer in the page.
pub const SLOT_POINTER_SIZE: u16 = 2;

pub type SlotPointer = u16;

pub type PageId = u32;

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
    #[deku(id = 2)]
    SchemaInfo,
    #[deku(id = 3)]
    Data,
    #[deku(id = 4)]
    Index,
}

/// A general purpose Page header.
/// Allocated length: 32 bytes.
#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(endian = "big")]
pub struct PageHeader {
    #[deku(bytes = 4)]
    pub page_id: PageId,

    #[deku(bytes = 1)]
    pub header_version: u8,

    #[deku]
    pub page_type: PageType,

    #[deku(bytes = 2)]
    pub checksum: u16,

    #[deku(bytes = 2)]
    pub flags: u16, // todo: need to add these. Know for sure I want a CAN_COMPACT flag.

    #[deku(bytes = 2)]
    pub allocated_slot_count: u16,

    #[deku(bytes = 2)]
    pub free_space: u16,

    #[deku(bytes = 2)]
    pub free_space_start_offset: u16,

    #[deku(bytes = 2)]
    pub free_space_end_offset: u16,

    #[deku(bytes = 2)]
    pub total_allocated_bytes: u16,
}

impl PageHeader {
    pub fn new(page_type: PageType) -> Self {
        let free_space = PAGE_SIZE_BYTES - PAGE_HEADER_SIZE_BYTES;

        PageHeader {
            page_id: 0, // TODO
            header_version: CURRENT_HEADER_VERSION,
            page_type,
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

#[derive(Debug, PartialEq, Error)]
pub enum PageEncoderError {
    #[error("Not enough space for slot")]
    NotEnoughSpace,
    #[error("Failed to serialise: {0}")]
    #[allow(dead_code)]
    FailedToSerialise(DekuError),
}

#[derive(Debug)]
pub struct AddSlot {
    #[allow(dead_code)]
    pointer_index: SlotPointer,
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
    pub fn add_slot_bytes(&mut self, slot: Vec<u8>) -> Result<AddSlot> {
        self.add_slot_internal(slot)
    }

    pub fn add_slot<T>(&mut self, slot: T) -> Result<AddSlot>
    where
        T: DekuContainerWrite,
    {
        let bytes = slot.to_bytes()?;
        self.add_slot_internal(bytes)
    }

    fn add_slot_internal(&mut self, slot: Vec<u8>) -> Result<AddSlot> {
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
            false => Err(PageEncoderError::NotEnoughSpace.into()),
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
        let mut full_page_vec = [0; PAGE_SIZE_BYTES_USIZE];

        let header_bytes = self.header.to_bytes();

        match header_bytes {
            Ok(header) => {
                // Write the header bytes into the page vec;
                // We could specifically write 32 bytes (our header length), but that would mean
                // we'd have to pad out the `header_bytes` from it's current size for no real win.
                full_page_vec[0..header.len()].copy_from_slice(&header);

                for slot in &self.slots {
                    // Calculate the new start position of the free space,
                    // including the bytes we're writing
                    let slot_end_pointer = self.header.free_space_start_offset + slot.len() as u16;

                    // Write the bytes
                    let _ = &full_page_vec
                        [self.header.free_space_start_offset.into()..slot_end_pointer.into()]
                        .copy_from_slice(slot);

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
    crc.checksum(bytes).to_be_bytes()
}

pub struct PageDecoder<'a> {
    bytes: &'a PageBytes,
    header: PageHeader,
    slots: Vec<&'a [u8]>,
}

#[derive(Debug, PartialEq, Error)]
pub enum PageDecoderError {
    #[error("Slot index out of range")]
    SlotOutOfRange,
    #[error("Failed to deserialise: {0}")]
    FailedToDeserialise(DekuError),
}

#[derive(Debug)]
pub struct ChecksumResult {
    pub pass: bool,
    #[allow(dead_code)]
    pub expected: [u8; 2],
    #[allow(dead_code)]
    pub actual: [u8; 2],
}

impl<'a> PageDecoder<'a> {
    pub fn from_bytes(bytes: &'a PageBytes) -> Self {
        let mut cursor = std::io::Cursor::new(bytes);
        let mut reader = deku::reader::Reader::new(&mut cursor);
        let header = PageHeader::from_reader_with_ctx(&mut reader, ()).unwrap();

        log::trace!("Loaded page from bytes.");
        log::trace!("|        Page Type: {:?}", header.page_type);
        log::trace!("|          Page ID: {:?}", header.page_id);
        log::trace!("|   Header version: {:?}", header.header_version);
        log::trace!("|       Free space: {:?} bytes", header.free_space);
        log::trace!("|            Flags: {:?}", header.flags);
        log::trace!("|         Checksum: {:?}", header.checksum);
        log::trace!("|     Alloc. slots: {:?}", header.allocated_slot_count);

        let slot_count = header.allocated_slot_count;

        PageDecoder {
            header,
            bytes,
            slots: Self::read_slots(slot_count, bytes),
        }
    }

    pub fn header(&self) -> &PageHeader {
        &self.header
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

    pub fn try_read_bytes(&self, slot_index: u16) -> Result<Vec<u8>, PageDecoderError> {
        if slot_index as usize >= self.slots.len() {
            return Err(PageDecoderError::SlotOutOfRange);
        }

        Ok(self.slots[slot_index as usize].to_vec())
    }

    pub fn try_read<T>(&self, slot_index: u16) -> Result<T, PageDecoderError>
    where
        T: DekuContainerRead<'a> + std::fmt::Debug,
    {
        if slot_index as usize >= self.slots.len() {
            return Err(PageDecoderError::SlotOutOfRange);
        }

        let slot = self.slots[slot_index as usize];
        let mut cursor = std::io::Cursor::new(slot);
        let mut reader = deku::reader::Reader::new(&mut cursor);

        match T::from_reader_with_ctx(&mut reader, ()) {
            Ok(slot_t) => Ok(slot_t),
            Err(e) => Err(PageDecoderError::FailedToDeserialise(e)),
        }
    }

    fn read_slots(slot_count: u16, bytes: &PageBytes) -> Vec<&[u8]> {
        // a slot pointer is 2 bytes, and are stored at the end of the page.
        // slots are at the start of the page, after the header.
        // a pointer points to the end of the slot.
        // a slot can be found by reading from the end of the previous slot to the pointer.

        fn read_pointer(index: u16, bytes: &PageBytes) -> usize {
            let pointer_end = PAGE_SIZE_BYTES - (index * SLOT_POINTER_SIZE);
            let pointer_start = pointer_end - SLOT_POINTER_SIZE;
            let pointer_bytes = &bytes[pointer_start.into()..pointer_end.into()];

            u16::from_be_bytes([pointer_bytes[0], pointer_bytes[1]]).into()
        }

        let mut slots = Vec::with_capacity(slot_count.into());

        for i in 0..slot_count {
            let slot_end = read_pointer(i, bytes);

            let slot_start = if i == 0 {
                PAGE_HEADER_SIZE_BYTES as usize
            } else {
                read_pointer(i + 1, bytes)
            };

            let range = slot_start..slot_end;

            log::trace!("Reading slot from page.");
            log::trace!("|   Slot Index: {:?}", i);
            log::trace!("|        Range: {:?}", range);
            log::trace!("|         Size: {:?}", range.len());

            let slot_bytes = &bytes[range];
            slots.push(slot_bytes);
        }

        slots
    }
}

#[cfg(test)]
mod page_encoder_tests {
    use crate::*;
    use deku::prelude::*;
    use engine::{PAGE_HEADER_SIZE_BYTES, PAGE_SIZE_BYTES};
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
        assert!(slot_result_1.is_ok());
        assert_eq!(slot_result_1.expect("Failed to add slot.").pointer_index, 0);

        assert!(slot_result_2.is_ok());
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
        let slot = TooBigForAPage { id: data, len };

        let slot_result = encoder.add_slot(slot);

        // Verify Result
        assert!(slot_result.is_err());
        if let Err(e) = slot_result {
            assert_eq!(e.to_string(), PageEncoderError::NotEnoughSpace.to_string());
        }
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
