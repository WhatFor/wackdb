use bincode::{Decode, Encode};

#[derive(Encode, Decode, Debug)]
pub enum FileType {
    Primary,
    Log,
}

/// Information describing a database file.
#[derive(Encode, Decode, Debug)]
pub struct FileInfo {
    /// Offset: 0. Length: 1.
    magic_string_0: u8,
    /// Offset: 1. Length: 1.
    magic_string_1: u8,
    /// Offset: 2. Length: 1.
    magic_string_2: u8,
    /// Offset: 3. Length: 1.
    magic_string_3: u8,
    /// Offset: 4. Length: 1.
    file_type: FileType,
    /// Offset: 5. Length: 2.
    sector_size_bytes: u16,
    /// Offset: 7. Length: 1.
    created_date: u8, // TODO: Type
}

impl FileInfo {
    pub fn new(file_type: FileType) -> Self {
        FileInfo {
            magic_string_0: b'0',
            magic_string_1: b'1',
            magic_string_2: b'6',
            magic_string_3: b'1',
            file_type,
            sector_size_bytes: 0, // TODO: Find this value
            created_date: 0,      // TODO: Populate this value
        }
    }
}

/// Information describing a database.
/// There will only ever be one of these pages in a single file.
pub struct DatabaseInfo {
    /// Offset: 0. Length: 128.
    database_name: String,
    /// Offset: 128. Length: 1.
    database_version: u8,
    /// Offset: 129. Length: 2.
    database_id: u16,
    /// Offset: 131. Length: 1.
    created_date: u8, // TODO: Type
}

impl DatabaseInfo {
    pub fn new(database_name: String, version: u8) -> Self {
        DatabaseInfo {
            database_name,
            database_version: version,
            database_id: 0,  // TODO
            created_date: 0, // TODO
        }
    }
}
