use super::server::PAGE_SIZE_BYTES;

use std::io::{Read, Seek, Write};

/// Seek to a specific page index in the file and write the given data
pub fn write_page(mut file: &std::fs::File, data: &[u8], page_index: u32) -> std::io::Result<()> {
    seek_page_index(file, page_index)?;
    file.write_all(data)
}

/// Seek to a specific page index in the file and read the entire page
pub fn read_page(mut file: &std::fs::File, page_index: u32) -> std::io::Result<Vec<u8>> {
    seek_page_index(file, page_index)?;

    let mut buf: Vec<u8> = vec![0; PAGE_SIZE_BYTES];
    // todo: returning PermissionDenied!
    file.read_exact(&mut buf)?;

    Ok(buf)
}

/// Seek to a given page index on a given File.
pub fn seek_page_index(mut file: &std::fs::File, page_index: u32) -> std::io::Result<()> {
    let page_size: u32 = PAGE_SIZE_BYTES.try_into().unwrap();
    let offset: u64 = (page_index * page_size).into();
    let offset_from_start = std::io::SeekFrom::Start(offset);

    let pos = file.seek(offset_from_start)?;

    if pos != offset {
        panic!("Failed to seek file.");
    }

    Ok(())
}
