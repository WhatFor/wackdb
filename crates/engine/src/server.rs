use anyhow::Result;
use deku::{ctx::Endian, DekuContainerWrite, DekuRead, DekuWrite};
use derive_more::derive::From;
use parser::ast::CreateDatabaseBody;
use std::{cell::RefMut, fs::File};
use thiserror::Error;

use crate::{
    btree::BTree,
    db::{self, DatabaseId, FileType, SchemaInfo, SCHEMA_INFO_PAGE_INDEX},
    engine::CURRENT_DATABASE_VERSION,
    fm::{FileId, FileManager},
    page::{PageDecoder, PageEncoder, PageEncoderError, PageHeader, PageId, PageType},
    persistence,
    util::{self, now_bytes},
};

pub const MASTER_NAME: &str = "master";
pub const MASTER_DB_ID: u16 = 0;

#[derive(Debug, From, Error)]
pub enum CreateDatabaseError {
    #[error("Database already exists: {0}")]
    DatabaseExists(String),
    #[error("Unable to create database: {0}")]
    UnableToWrite(PageEncoderError),
    #[error("Unable to create database: {0}")]
    UnableToCreateFile(util::Error),
    #[error("Unable to create database: {0}")]
    DiskError(persistence::PersistenceError),
    #[error("Unable to create database: {0}")]
    DbError(db::DbError),
}

pub struct OpenDatabaseResult {
    pub id: DatabaseId,
    pub name: String,
    pub dat: File,
    pub log: File,
    pub allocated_page_count: PageId,
}

pub fn open_or_create_master_db() -> Result<OpenDatabaseResult> {
    let exists = persistence::check_db_exists(MASTER_NAME, FileType::Primary)?;

    if exists {
        let db = persistence::open_db(MASTER_NAME);
        let allocated_page_count = persistence::get_allocated_page_count(&db.dat);

        log::info!(
            "Opened existing master DB, containing {} pages.",
            allocated_page_count
        );

        return Ok(OpenDatabaseResult {
            id: MASTER_DB_ID,
            name: MASTER_NAME.into(),
            dat: db.dat,
            log: db.log,
            allocated_page_count,
        });
    }

    create_database(MASTER_NAME, MASTER_DB_ID, true)
}

pub fn create_user_database(
    statement: &CreateDatabaseBody,
    db_id: DatabaseId,
) -> Result<OpenDatabaseResult> {
    let db_name = statement.database_name.value.as_str();

    create_database(db_name, db_id, false)
}

pub fn create_database(
    db_name: &str,
    db_id: DatabaseId,
    is_master: bool,
) -> Result<OpenDatabaseResult> {
    let data_exists = persistence::check_db_exists(db_name, FileType::Primary)?;
    let log_exists = persistence::check_db_exists(db_name, FileType::Log)?;

    if data_exists || log_exists {
        return Err(CreateDatabaseError::DatabaseExists(String::from(db_name)).into());
    }

    let data_file = db::create_db_data_file(db_name, db_id, is_master)?;
    let log_file = db::create_db_log_file(db_name)?;

    Ok(OpenDatabaseResult {
        id: db_id,
        name: db_name.into(),
        dat: data_file,
        log: log_file,
        allocated_page_count: 3,
    })
}

#[derive(Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct Database {
    #[deku(bytes = 2)]
    id: DatabaseId,
    #[deku(bytes = 1)]
    name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    name: Vec<u8>,
    #[deku(bytes = 1)]
    database_version: u8,
    #[deku(bytes = 2)]
    created_date: u16,
}

impl Database {
    pub fn new(id: DatabaseId, name: String) -> Self {
        Database {
            id,
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
    #[deku(bytes = 2)]
    id: DatabaseId,
    #[deku(bytes = 2)]
    database_id: DatabaseId,
    #[deku(bytes = 2)]
    name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    name: Vec<u8>,
    #[deku(bytes = 2)]
    created_date: u16,
}

impl Table {
    pub fn new(id: DatabaseId, database_id: DatabaseId, name: String) -> Self {
        Table {
            id,
            database_id,
            name: name.clone().into_bytes(),
            name_len: name.len() as u8,
            created_date: now_bytes(),
        }
    }
}

#[derive(DekuRead, DekuWrite)]
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
    Int,
    #[deku(id = 3)]
    String,
    #[deku(id = 4)]
    Boolean,
    #[deku(id = 5)]
    Date,
    #[deku(id = 6)]
    DateTime,
}

#[derive(DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct Column {
    #[deku(bytes = 2)]
    id: DatabaseId,
    #[deku(bytes = 2)]
    table_id: DatabaseId,
    #[deku(bytes = 1)]
    name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    name: Vec<u8>,
    #[deku(bytes = 1)]
    position: u8,
    #[deku(bytes = 1)]
    is_nullable: bool,
    #[deku(bytes = 1)]
    default_value_len: u8,
    #[deku(bytes = 128, count = "default_value_len")]
    default_value: Option<Vec<u8>>,
    #[deku]
    data_type: ColumnType,
    #[deku(bytes = 2)]
    max_str_length: Option<u16>,
    #[deku(bytes = 1)]
    num_precision: Option<u8>,
    #[deku(bytes = 2)]
    created_date: u16,
}

impl Column {
    pub fn new(
        id: DatabaseId,
        table_id: DatabaseId,
        name: String,
        position: u8,
        is_nullable: bool,
        default_value: Option<String>,
        data_type: ColumnType,
        max_str_length: Option<u16>,
        num_precision: Option<u8>,
    ) -> Self {
        let default_value_len = match default_value {
            None => 0,
            Some(ref x) => x.len() as u8,
        };

        let default_value_v = match default_value {
            None => None,
            Some(ref s) => Some(s.to_string().into_bytes()),
        };

        Column {
            id,
            table_id,
            name_len: name.len() as u8,
            name: name.to_string().into_bytes(),
            position,
            is_nullable,
            default_value_len,
            default_value: default_value_v,
            data_type,
            max_str_length,
            num_precision,
            created_date: now_bytes(),
        }
    }
}

#[derive(DekuRead, DekuWrite)]
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
    #[deku(bytes = 2)]
    id: DatabaseId,
    #[deku(bytes = 2)]
    table_id: DatabaseId,
    #[deku(bytes = 1)]
    name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    name: Vec<u8>,
    #[deku]
    index_type: IndexType,
    #[deku(bytes = 1)]
    is_unique: bool,
    #[deku(bytes = 4)]
    root_page_id: PageId,
    #[deku(bytes = 2)]
    created_date: u16,
}
#[derive(Debug, From, Error)]
pub enum SchemaCreationError {
    #[error("Failed to open file.")]
    FailedToOpenFile,
}

const DATABASES_TABLE: &str = "databases";
const TABLES_TABLE: &str = "tables";
const COLUMNS_TABLE: &str = "columns";
const INDEXES_TABLE: &str = "indexes";

pub fn ensure_master_tables_exist(file_manager: RefMut<FileManager>) -> Result<()> {
    // create a databases table
    // id, name, created_date, database_version
    // id = primary key for index
    // this lists all databases tracked (including self).
    // create an indexes table?

    let database = Database::new(MASTER_DB_ID, MASTER_NAME.into());
    let mut databases_index = BTree::new();
    let database_bytes = database.to_bytes()?;
    databases_index.add(database.id.into(), database_bytes);

    // TODO: This only builds one page (if it's a leaf page, which it will be) of the index...
    let header = PageHeader::new(PageType::Index);
    let mut page = PageEncoder::new(header);

    match databases_index.root {
        crate::btree::NodeType::Interior(_) => todo!(), // this needs to make new pages for each interior. probably recursive.
        crate::btree::NodeType::Leaf(leaf) => {
            for key in leaf {
                page.add_slot_bytes(key.value)?;
            }
        }
    }

    let page_bytes = page.collect();

    let root_page_id = file_manager
        .next_page_id_by_id(MASTER_DB_ID, FileType::Primary)
        .unwrap();

    let master_db_file = file_manager.get_from_id(MASTER_DB_ID, FileType::Primary);

    if let Some(file) = master_db_file {
        // write the index to the master db file
        persistence::write_page(file, &page_bytes, *root_page_id)?;

        // read out the schema info page
        // TODO: should use page cache
        let file_info_page = persistence::read_page(file, SCHEMA_INFO_PAGE_INDEX)?;
        let page = PageDecoder::from_bytes(&file_info_page);
        let mut schema_info = page.try_read::<SchemaInfo>(0)?;

        schema_info.databases_root_page_id = root_page_id.to_owned();
        schema_info.columns_root_page_id = 99;
        schema_info.indexes_root_page_id = 98;
        schema_info.tables_root_page_id = 97;

        // write schema info back
        // TODO: this is building a whole new page to write a single number... how do I want to do this better?
        let schema_header = PageHeader::new(PageType::SchemaInfo);
        let mut schema_page = PageEncoder::new(schema_header);
        let schema_info_bytes = schema_info.to_bytes()?;
        schema_page.add_slot_bytes(schema_info_bytes)?;
        let schema_page_bytes = schema_page.collect();
        persistence::write_page(file, &schema_page_bytes, SCHEMA_INFO_PAGE_INDEX)?;
    } else {
        return Result::Err(SchemaCreationError::FailedToOpenFile.into());
    }

    let tables = [
        Table::new(0, MASTER_DB_ID, DATABASES_TABLE.to_string()),
        Table::new(0, MASTER_DB_ID, TABLES_TABLE.to_string()),
        Table::new(0, MASTER_DB_ID, COLUMNS_TABLE.to_string()),
        Table::new(0, MASTER_DB_ID, INDEXES_TABLE.to_string()),
    ];

    let mut tables_index = BTree::new();

    for table in tables {
        let table_bytes = table.to_bytes()?;
        tables_index.add(table.id.into(), table_bytes);
    }

    let database_table_columns = [
        Column::new(
            0,
            0,
            "id".to_string(),
            0,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "name".to_string(),
            1,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "database_version".to_string(),
            2,
            false,
            None,
            ColumnType::Byte,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "created_date".to_string(),
            3,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
    ];

    let tables_table_columns = [
        Column::new(
            0,
            0,
            "id".to_string(),
            0,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "database_id".to_string(),
            1,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "name".to_string(),
            2,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "created_date".to_string(),
            3,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
    ];

    let columns_table_columns = [
        Column::new(
            0,
            0,
            "id".to_string(),
            0,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "table_id".to_string(),
            1,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "name".to_string(),
            2,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "position".to_string(),
            3,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "is_nullable".to_string(),
            4,
            false,
            None,
            ColumnType::Boolean,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "default_value".to_string(),
            5,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "data_type".to_string(),
            6,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "max_str_length".to_string(),
            7,
            false,
            Some(i32::MAX.to_string()),
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "num_precision".to_string(),
            8,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "created_date".to_string(),
            9,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
    ];

    let indexes_table_columns = [
        Column::new(
            0,
            0,
            "id".to_string(),
            0,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "table_id".to_string(),
            1,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "name".to_string(),
            2,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "type".to_string(),
            3,
            false,
            None,
            ColumnType::Byte,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "is_unique".to_string(),
            4,
            false,
            Some(String::from("false")),
            ColumnType::Boolean,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "root_page_id".to_string(),
            5,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            0,
            0,
            "created_date".to_string(),
            6,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
    ];

    let mut columns_index = BTree::new();

    for col in database_table_columns {
        let col_bytes = col.to_bytes()?;
        columns_index.add(col.id.into(), col_bytes);
    }

    for col in tables_table_columns {
        let col_bytes = col.to_bytes()?;
        columns_index.add(col.id.into(), col_bytes);
    }

    for col in columns_table_columns {
        let col_bytes = col.to_bytes()?;
        columns_index.add(col.id.into(), col_bytes);
    }

    for col in indexes_table_columns {
        let col_bytes = col.to_bytes()?;
        columns_index.add(col.id.into(), col_bytes);
    }

    Ok(())
}
