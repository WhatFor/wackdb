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
    fm::{FileManager, IdMapKey},
    page::{PageEncoder, PageEncoderError, PageHeader, PageId, PageType},
    page_cache::PageBytes,
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
    pub id: DatabaseId,
    #[deku(bytes = 1)]
    pub name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    pub name: Vec<u8>,
    #[deku(bytes = 1)]
    pub database_version: u8,
    #[deku(bytes = 2)]
    pub created_date: u16,
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
    pub id: DatabaseId,
    #[deku(bytes = 2)]
    pub database_id: DatabaseId,
    #[deku(bytes = 2)]
    pub name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    pub name: Vec<u8>,
    #[deku(bytes = 2)]
    pub created_date: u16,
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

#[derive(DekuRead, DekuWrite, PartialEq, Eq)]
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
    pub id: DatabaseId,
    #[deku(bytes = 2)]
    pub table_id: DatabaseId,
    #[deku(bytes = 1)]
    pub name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    pub name: Vec<u8>,
    #[deku(bytes = 1)]
    pub position: u8,
    #[deku(bytes = 1)]
    pub is_nullable: bool,
    #[deku(bytes = 1)]
    pub default_value_len: u8,
    #[deku(bytes = 128, count = "default_value_len")]
    pub default_value: Vec<u8>,
    #[deku]
    pub data_type: ColumnType,
    #[deku(bytes = 2)]
    pub max_str_length: u16,
    #[deku(bytes = 1)]
    pub num_precision: u8,
    #[deku(bytes = 2)]
    pub created_date: u16,
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
    #[deku(bytes = 2)]
    pub id: DatabaseId,
    #[deku(bytes = 2)]
    pub table_id: DatabaseId,
    #[deku(bytes = 1)]
    pub name_len: u8,
    #[deku(bytes = 128, count = "name_len")]
    pub name: Vec<u8>,
    #[deku]
    pub index_type: IndexType,
    #[deku(bytes = 1)]
    pub is_unique: bool,
    #[deku(bytes = 4)]
    pub root_page_id: PageId,
    #[deku(bytes = 2)]
    pub created_date: u16,
}

impl Index {
    pub fn new(
        id: DatabaseId,
        table_id: DatabaseId,
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
            root_page_id,
            created_date: now_bytes(),
        }
    }
}

#[derive(Debug, From, Error)]
pub enum SchemaCreationError {
    #[error("Failed to open file.")]
    FailedToOpenFile,
}

fn initialise_databases_table() -> Result<PageBytes> {
    let database = Database::new(MASTER_DB_ID, MASTER_NAME.into());
    let mut databases_index = BTree::new();
    let database_bytes = database.to_bytes()?;
    databases_index.add(database.id.into(), database_bytes);

    // TODO: This only builds one page (if it's a leaf page, which it will be) of the index...
    let header = PageHeader::new(PageType::Index);
    let mut page = PageEncoder::new(header);

    // TODO: this is duplicated a lot
    match databases_index.root {
        crate::btree::NodeType::Interior(_) => todo!(), // this needs to make new pages for each interior. probably recursive.
        crate::btree::NodeType::Leaf(leaf) => {
            for key in leaf {
                page.add_slot_bytes(key.value)?;
            }
        }
    }

    Ok(page.collect())
}

const DATABASES_TABLE: &str = "databases";
const TABLES_TABLE: &str = "tables";
const COLUMNS_TABLE: &str = "columns";
const INDEXES_TABLE: &str = "indexes";

const DATABASES_TABLE_ID: DatabaseId = 1;
const TABLES_TABLE_ID: DatabaseId = 2;
const COLUMNS_TABLE_ID: DatabaseId = 3;
const INDEXES_TABLE_ID: DatabaseId = 4;

fn initialise_tables_table() -> Result<PageBytes> {
    let tables = [
        Table::new(
            DATABASES_TABLE_ID,
            MASTER_DB_ID,
            DATABASES_TABLE.to_string(),
        ),
        Table::new(TABLES_TABLE_ID, MASTER_DB_ID, TABLES_TABLE.to_string()),
        Table::new(COLUMNS_TABLE_ID, MASTER_DB_ID, COLUMNS_TABLE.to_string()),
        Table::new(INDEXES_TABLE_ID, MASTER_DB_ID, INDEXES_TABLE.to_string()),
    ];

    let mut index = BTree::new();

    for table in tables {
        let table_bytes = table.to_bytes()?;
        index.add(table.id.into(), table_bytes);
    }

    let header = PageHeader::new(PageType::Index);
    let mut page = PageEncoder::new(header);

    // TODO: this is duplicated a lot
    match index.root {
        crate::btree::NodeType::Interior(_) => todo!(), // this needs to make new pages for each interior. probably recursive.
        crate::btree::NodeType::Leaf(leaf) => {
            for key in leaf {
                page.add_slot_bytes(key.value)?;
            }
        }
    }

    Ok(page.collect())
}

fn initialise_columns_table() -> Result<PageBytes> {
    let database_table_columns = [
        Column::new(
            1,
            DATABASES_TABLE_ID,
            "id".to_string(),
            0,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            2,
            DATABASES_TABLE_ID,
            "name".to_string(),
            1,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            3,
            DATABASES_TABLE_ID,
            "database_version".to_string(),
            2,
            false,
            None,
            ColumnType::Byte,
            None,
            None,
        ),
        Column::new(
            4,
            DATABASES_TABLE_ID,
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
            5,
            TABLES_TABLE_ID,
            "id".to_string(),
            0,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            6,
            TABLES_TABLE_ID,
            "database_id".to_string(),
            1,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            7,
            TABLES_TABLE_ID,
            "name".to_string(),
            2,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            8,
            TABLES_TABLE_ID,
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
            9,
            COLUMNS_TABLE_ID,
            "id".to_string(),
            0,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            10,
            COLUMNS_TABLE_ID,
            "table_id".to_string(),
            1,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            11,
            COLUMNS_TABLE_ID,
            "name".to_string(),
            2,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            12,
            COLUMNS_TABLE_ID,
            "position".to_string(),
            3,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            13,
            COLUMNS_TABLE_ID,
            "is_nullable".to_string(),
            4,
            false,
            None,
            ColumnType::Boolean,
            None,
            None,
        ),
        Column::new(
            14,
            COLUMNS_TABLE_ID,
            "default_value".to_string(),
            5,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            15,
            COLUMNS_TABLE_ID,
            "data_type".to_string(),
            6,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            16,
            COLUMNS_TABLE_ID,
            "max_str_length".to_string(),
            7,
            false,
            Some(i32::MAX.to_string()),
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            17,
            COLUMNS_TABLE_ID,
            "num_precision".to_string(),
            8,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            18,
            COLUMNS_TABLE_ID,
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
            19,
            INDEXES_TABLE_ID,
            "id".to_string(),
            0,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            20,
            INDEXES_TABLE_ID,
            "table_id".to_string(),
            1,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            21,
            INDEXES_TABLE_ID,
            "name".to_string(),
            2,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
        Column::new(
            22,
            INDEXES_TABLE_ID,
            "type".to_string(),
            3,
            false,
            None,
            ColumnType::Byte,
            None,
            None,
        ),
        Column::new(
            23,
            INDEXES_TABLE_ID,
            "is_unique".to_string(),
            4,
            false,
            Some(String::from("false")),
            ColumnType::Boolean,
            None,
            None,
        ),
        Column::new(
            24,
            INDEXES_TABLE_ID,
            "root_page_id".to_string(),
            5,
            false,
            None,
            ColumnType::Int,
            None,
            None,
        ),
        Column::new(
            25,
            INDEXES_TABLE_ID,
            "created_date".to_string(),
            6,
            false,
            None,
            ColumnType::String,
            None,
            None,
        ),
    ];

    let mut index = BTree::new();

    for col in database_table_columns {
        let col_bytes = col.to_bytes()?;
        index.add(col.id.into(), col_bytes);
    }

    for col in tables_table_columns {
        let col_bytes = col.to_bytes()?;
        index.add(col.id.into(), col_bytes);
    }

    for col in columns_table_columns {
        let col_bytes = col.to_bytes()?;
        index.add(col.id.into(), col_bytes);
    }

    for col in indexes_table_columns {
        let col_bytes = col.to_bytes()?;
        index.add(col.id.into(), col_bytes);
    }

    let header = PageHeader::new(PageType::Index);
    let mut page = PageEncoder::new(header);

    // TODO: this is duplicated a lot
    match index.root {
        crate::btree::NodeType::Interior(_) => todo!(), // this needs to make new pages for each interior. probably recursive.
        crate::btree::NodeType::Leaf(leaf) => {
            for key in leaf {
                page.add_slot_bytes(key.value)?;
            }
        }
    }

    Ok(page.collect())
}

fn initialise_indexes_table(
    databases_root_id: PageId,
    tables_root_id: PageId,
    columns_root_id: PageId,
    indexes_root_id: PageId,
) -> Result<PageBytes> {
    // TODO
    let indexes = [
        Index::new(
            1,
            DATABASES_TABLE_ID,
            String::from("PK_Databases"),
            IndexType::PK,
            true,
            databases_root_id,
        ),
        Index::new(
            2,
            TABLES_TABLE_ID,
            String::from("PK_Tables"),
            IndexType::PK,
            true,
            tables_root_id,
        ),
        Index::new(
            3,
            COLUMNS_TABLE_ID,
            String::from("PK_Columns"),
            IndexType::PK,
            true,
            columns_root_id,
        ),
        Index::new(
            4,
            INDEXES_TABLE_ID,
            String::from("PK_Indexes"),
            IndexType::PK,
            true,
            indexes_root_id,
        ),
    ];

    let mut index = BTree::new();

    for index_record in indexes {
        let bytes = index_record.to_bytes()?;
        index.add(index_record.id.into(), bytes);
    }

    let header = PageHeader::new(PageType::Index);
    let mut page = PageEncoder::new(header);

    // TODO: this is duplicated a lot
    match index.root {
        crate::btree::NodeType::Interior(_) => todo!(), // this needs to make new pages for each interior. probably recursive.
        crate::btree::NodeType::Leaf(leaf) => {
            for key in leaf {
                page.add_slot_bytes(key.value)?;
            }
        }
    }

    Ok(page.collect())
}

pub fn ensure_master_tables_exist(mut file_manager: RefMut<FileManager>) -> Result<()> {
    let master_id = &IdMapKey::new(MASTER_DB_ID, FileType::Primary);

    // read out the schema info page
    // TODO: should use page cache
    let mut schema = file_manager.read_page_as::<SchemaInfo>(master_id, SCHEMA_INFO_PAGE_INDEX)?;

    if schema.databases_root_page_id != 0 {
        log::debug!("SchemaInfo Page exists. Skipping initialisation.");
        return Ok(());
    }

    // Write DB page
    let databases_page_id = file_manager
        .next_page_id_by_id(MASTER_DB_ID, FileType::Primary)
        .unwrap();

    let databases_page_bytes = initialise_databases_table()?;

    // TODO: handle Result
    let _ = file_manager.write_page(master_id, &databases_page_bytes, databases_page_id);

    log::debug!("Wrote Databases index to pageID {}", databases_page_id);

    // Write Tables pages
    let tables_page_id = file_manager
        .next_page_id_by_id(MASTER_DB_ID, FileType::Primary)
        .unwrap();

    let tables_page_bytes = initialise_tables_table()?;

    // TODO: handle Result
    let _ = file_manager.write_page(master_id, &tables_page_bytes, tables_page_id);

    log::debug!("Wrote Tables index to pageID {}", tables_page_id);

    // Write Columns pages
    let columns_page_id = file_manager
        .next_page_id_by_id(MASTER_DB_ID, FileType::Primary)
        .unwrap();

    let columns_page_bytes = initialise_columns_table()?;

    // TODO: handle Result
    let _ = file_manager.write_page(master_id, &columns_page_bytes, columns_page_id);

    log::debug!("Wrote Columns index to pageID {}", columns_page_id);

    // Write Indexes pages
    let indexes_page_id = file_manager
        .next_page_id_by_id(MASTER_DB_ID, FileType::Primary)
        .unwrap();

    let indexes_page_bytes = initialise_indexes_table(
        databases_page_id,
        tables_page_id,
        columns_page_id,
        indexes_page_id,
    )?;

    // TODO: handle Result
    let _ = file_manager.write_page(master_id, &indexes_page_bytes, indexes_page_id);

    log::debug!("Wrote Indexes index to pageID {}", indexes_page_id);

    schema.databases_root_page_id = databases_page_id.to_owned();
    schema.tables_root_page_id = tables_page_id.to_owned();
    schema.columns_root_page_id = columns_page_id.to_owned();
    schema.indexes_root_page_id = indexes_page_id.to_owned();

    // write schema info back
    // TODO: this is building a whole new page to write a few numbers... how do I want to do this better?
    let schema_header = PageHeader::new(PageType::SchemaInfo);
    let mut schema_page = PageEncoder::new(schema_header);
    let schema_info_bytes = schema.to_bytes()?;
    schema_page.add_slot_bytes(schema_info_bytes)?;
    let schema_page_bytes = schema_page.collect();

    file_manager.write_page(master_id, &schema_page_bytes, SCHEMA_INFO_PAGE_INDEX)?;

    Ok(())
}
