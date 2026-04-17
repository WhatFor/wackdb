use deku::{ctx::Endian, DekuRead, DekuWrite};

use crate::db::DatabaseId;
use crate::file_format::CURRENT_DATABASE_VERSION;
use crate::page::PageId;
use crate::types::{DbByte, DbDate, DbInt, DbLong, DbShort};
use crate::util::now_bytes;

pub const MASTER_NAME: &str = "master";
pub const MASTER_DB_ID: u16 = 0;

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct Database {
    #[deku(bytes = 4)]
    pub id: DbInt,
    #[deku(bytes = 1)]
    pub name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    pub name: Vec<u8>,
    #[deku(bytes = 1)]
    pub database_version: u8,
    #[deku(bytes = 2)]
    pub created_date: DbDate,
}

impl Database {
    pub fn new(id: DatabaseId, name: String) -> Self {
        Database {
            id: id.into(),
            name: name.to_string().into_bytes(),
            name_len: name.len() as u8,
            database_version: CURRENT_DATABASE_VERSION,
            created_date: now_bytes(),
        }
    }
}

#[derive(DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct Table {
    #[deku(bytes = 4)]
    pub id: DbInt,
    #[deku(bytes = 4)]
    pub database_id: DbInt,
    #[deku(bytes = 1)]
    pub name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    pub name: Vec<u8>,
    #[deku(bytes = 2)]
    pub created_date: DbDate,
}

impl Table {
    pub fn new(id: DbInt, database_id: DatabaseId, name: String) -> Self {
        Table {
            id,
            database_id: database_id.into(),
            name: name.clone().into_bytes(),
            name_len: name.len() as u8,
            created_date: now_bytes(),
        }
    }
}

#[derive(DekuRead, DekuWrite, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[deku(
    id_type = "u8",
    endian = "endian",
    ctx = "endian: deku::ctx::Endian",
    ctx_default = "Endian::Big"
)]
pub enum ColumnType {
    #[deku(id = 0)]
    Bit,
    #[deku(id = 1)]
    Byte,
    #[deku(id = 2)]
    Short,
    #[deku(id = 3)]
    Int,
    #[deku(id = 4)]
    Long,
    #[deku(id = 5)]
    String,
    #[deku(id = 6)]
    Boolean,
    #[deku(id = 7)]
    Date,
    #[deku(id = 8)]
    DateTime,
}

#[derive(DekuRead, DekuWrite, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[deku(endian = "big")]
pub struct Column {
    #[deku(bytes = 4)]
    pub id: DbInt,
    #[deku(bytes = 4)]
    pub table_id: DbInt,
    #[deku(bytes = 1)]
    pub name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    pub name: Vec<u8>,
    #[deku(bytes = 1)]
    pub position: DbByte,
    #[deku(bytes = 1)]
    pub is_nullable: bool,
    #[deku(bytes = 1)]
    pub default_value_len: u8,
    #[deku(bytes = 128, count = "default_value_len")]
    pub default_value: Vec<u8>,
    #[deku]
    pub data_type: ColumnType,
    #[deku(bytes = 2)]
    pub max_str_length: DbShort,
    #[deku(bytes = 1)]
    pub num_precision: DbByte,
    #[deku(bytes = 2)]
    pub created_date: DbDate,
}

impl Column {
    pub fn new(
        id: DbInt,
        table_id: DbInt,
        name: String,
        position: u8,
        is_nullable: bool,
        default_value: Option<String>,
        data_type: ColumnType,
        max_str_length: Option<u16>,
        num_precision: Option<u8>,
    ) -> Self {
        Column {
            id,
            table_id,
            name_len: name.len() as u8,
            name: name.to_string().into_bytes(),
            position,
            is_nullable,
            default_value_len: match default_value {
                None => 0,
                Some(ref x) => x.len() as u8,
            },
            default_value: match default_value {
                None => Vec::new(),
                Some(ref s) => s.to_string().into_bytes(),
            },
            data_type,
            max_str_length: match max_str_length {
                Some(v) => v,
                // TODO: this sucks
                None => 128,
            },
            num_precision: match num_precision {
                Some(v) => v,
                None => 0,
            },
            created_date: now_bytes(),
        }
    }
}

#[derive(DekuRead, DekuWrite, PartialEq, Eq)]
#[deku(
    id_type = "u8",
    endian = "endian",
    ctx = "endian: deku::ctx::Endian",
    ctx_default = "Endian::Big"
)]
pub enum IndexType {
    #[deku(id = 0)]
    PK,
    #[deku(id = 1)]
    FK,
    #[deku(id = 2)]
    Index,
}

#[derive(DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct Index {
    #[deku(bytes = 4)]
    pub id: DbInt,
    #[deku(bytes = 4)]
    pub table_id: DbInt,
    #[deku(bytes = 1)]
    pub name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    pub name: Vec<u8>,
    #[deku]
    pub index_type: IndexType,
    #[deku(bytes = 1)]
    pub is_unique: bool,
    #[deku(bytes = 8)]
    pub root_page_id: DbLong,
    #[deku(bytes = 2)]
    pub created_date: DbDate,
}

impl Index {
    pub fn new(
        id: DbInt,
        table_id: DbInt,
        name: String,
        index_type: IndexType,
        is_unique: bool,
        root_page_id: PageId,
    ) -> Self {
        Index {
            id,
            table_id,
            name_len: name.len() as u8,
            name: name.to_string().into_bytes(),
            index_type,
            is_unique,
            root_page_id: root_page_id.into(),
            created_date: now_bytes(),
        }
    }
}
