use anyhow::Result;
use deku::DekuContainerWrite;

use crate::{
    btree::BTree,
    catalog::{Column, ColumnType, Database, Index, IndexType, Table, MASTER_DB_ID, MASTER_NAME},
    db::{FileType, SchemaInfo, SCHEMA_INFO_PAGE_INDEX},
    fm::{FileManager, IdMapKey},
    page::{PageEncoder, PageHeader, PageId, PageType},
    page_cache::PageBytes,
    types::DbInt,
};

pub fn ensure_master_tables_exist(file_manager: &mut FileManager) -> Result<()> {
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

const DATABASES_TABLE_ID: DbInt = 1;
const TABLES_TABLE_ID: DbInt = 2;
const COLUMNS_TABLE_ID: DbInt = 3;
const INDEXES_TABLE_ID: DbInt = 4;

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
            ColumnType::Date,
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
            ColumnType::Date,
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
            ColumnType::Byte,
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
            ColumnType::Byte,
            None,
            None,
        ),
        Column::new(
            16,
            COLUMNS_TABLE_ID,
            "max_str_length".to_string(),
            7,
            false,
            Some(u16::MAX.to_string()),
            ColumnType::Short,
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
            ColumnType::Byte,
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
            ColumnType::Date,
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
            ColumnType::Long,
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
            ColumnType::Date,
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
