use crate::{
    buffer_pool::FilePageId,
    catalog::{Column, Database, DbInt, Index, Table, MASTER_DB_ID},
    engine::Storage,
    file_format::{SchemaInfo, SCHEMA_INFO_PAGE_INDEX},
    index_pager::IndexPager,
};

use anyhow::Result;
use deku::DekuReader;

#[derive(Debug)]
struct Schema {
    pub databases: Vec<SchemaDatabase>,
}

#[derive(Debug)]
struct SchemaDatabase {
    pub id: DbInt,
    pub name: String,
    pub tables: Vec<SchemaTable>,
}

#[derive(Clone, Debug)]
struct SchemaTable {
    pub id: DbInt,
    pub name: String,
    pub database_id: DbInt,
    pub columns: Vec<SchemaColumn>,
    pub indexes: Vec<SchemaIndex>,
}

#[derive(Clone, Debug)]
struct SchemaColumn {
    pub id: DbInt,
    pub name: String,
    pub table_id: DbInt,
    // TODO: Probably need more here!
}

#[derive(Clone, Debug)]
struct SchemaIndex {
    pub id: DbInt,
    pub name: String,
    pub table_id: DbInt,
    // TODO: Probably need more here!
}

#[derive(Debug)]
pub struct SchemaManager {
    schema: Schema,
}

impl SchemaManager {
    pub fn new(storage: &Storage) -> Result<Self> {
        Ok(SchemaManager {
            schema: init(storage)?,
        })
    }
}

fn init(storage: &Storage) -> Result<Schema> {
    let master_database_schema_info = &FilePageId {
        db_id: MASTER_DB_ID,
        page_index: SCHEMA_INFO_PAGE_INDEX,
    };

    let schema_info = storage
        .buffer_pool
        .get_page_as::<SchemaInfo>(master_database_schema_info, &storage.file_manager)?;

    let databases_page_iter = IndexPager::new(
        FilePageId::new(MASTER_DB_ID, schema_info.databases_root_page_id),
        storage,
    );

    let dbs = databases_page_iter.map(|item| {
        let mut cursor = std::io::Cursor::new(item);
        let mut reader = deku::reader::Reader::new(&mut cursor);

        return Database::from_reader_with_ctx(&mut reader, ());
    });

    let tables_page_iter = IndexPager::new(
        FilePageId::new(MASTER_DB_ID, schema_info.tables_root_page_id),
        storage,
    );

    let tables = tables_page_iter.map(|item| {
        let mut cursor = std::io::Cursor::new(item);
        let mut reader = deku::reader::Reader::new(&mut cursor);

        return Table::from_reader_with_ctx(&mut reader, ());
    });

    let indexes_page_iter = IndexPager::new(
        FilePageId::new(MASTER_DB_ID, schema_info.indexes_root_page_id),
        storage,
    );

    let indexes = indexes_page_iter.map(|item| {
        let mut cursor = std::io::Cursor::new(item);
        let mut reader = deku::reader::Reader::new(&mut cursor);

        return Index::from_reader_with_ctx(&mut reader, ());
    });

    let columns_page_iter = IndexPager::new(
        FilePageId::new(MASTER_DB_ID, schema_info.columns_root_page_id),
        storage,
    );

    let columns = columns_page_iter.map(|item| {
        let mut cursor = std::io::Cursor::new(item);
        let mut reader = deku::reader::Reader::new(&mut cursor);

        return Column::from_reader_with_ctx(&mut reader, ());
    });

    let columns: Vec<SchemaColumn> = columns
        .map(|column| column.unwrap())
        .map(|column| SchemaColumn {
            id: column.id,
            name: String::from_utf8(column.name).unwrap(),
            table_id: column.table_id,
        })
        .collect();

    let indexes: Vec<SchemaIndex> = indexes
        .map(|index| index.unwrap())
        .map(|index| SchemaIndex {
            id: index.id,
            name: String::from_utf8(index.name).unwrap(),
            table_id: index.table_id,
        })
        .collect();

    let tables: Vec<SchemaTable> = tables
        .map(|table| table.unwrap())
        .map(|table| SchemaTable {
            id: table.id,
            name: String::from_utf8(table.name).unwrap(),
            database_id: table.database_id,
            columns: columns
                .clone()
                .into_iter()
                .filter(|column| column.table_id == table.id)
                .collect(),
            indexes: indexes
                .clone()
                .into_iter()
                .filter(|index| index.table_id == table.id)
                .collect(),
        })
        .collect();

    let databases = dbs
        .map(|db| db.unwrap())
        .map(|db| SchemaDatabase {
            id: db.id,
            name: String::from_utf8(db.name).unwrap(),
            tables: tables
                .clone() // TODO: Clone is dumb - I know each row of tables exists only on 1 table. Can do better.
                .into_iter()
                .filter(|table| table.database_id == db.id)
                .collect(),
        })
        .collect();

    Ok(Schema { databases })
}
