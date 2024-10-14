use anyhow::Result;
use deku::{DekuRead, DekuWrite};
use derive_more::derive::From;
use parser::ast::CreateDatabaseBody;
use std::{fs::File, time::SystemTime};
use thiserror::Error;

use crate::{
    db::{self, DatabaseId, FileType},
    engine::CURRENT_DATABASE_VERSION,
    page::PageEncoderError,
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
    pub dat: File,
    pub log: File,
}

pub fn open_or_create_master_db() -> Result<OpenDatabaseResult> {
    let exists = persistence::check_db_exists(MASTER_NAME, FileType::Primary)?;

    if exists {
        let db = persistence::open_db(MASTER_NAME);

        log::info!("Opened existing master DB.");

        return Ok(OpenDatabaseResult {
            id: MASTER_DB_ID,
            dat: db.dat,
            log: db.log,
        });
    }

    create_database(MASTER_NAME, MASTER_DB_ID)
}

pub fn create_user_database(
    statement: &CreateDatabaseBody,
    db_id: DatabaseId,
) -> Result<OpenDatabaseResult> {
    let db_name = statement.database_name.value.as_str();

    create_database(db_name, db_id)
}

pub fn create_database(db_name: &str, db_id: DatabaseId) -> Result<OpenDatabaseResult> {
    let data_exists = persistence::check_db_exists(db_name, FileType::Primary)?;
    let log_exists = persistence::check_db_exists(db_name, FileType::Log)?;

    if data_exists || log_exists {
        return Err(CreateDatabaseError::DatabaseExists(String::from(db_name)).into());
    }

    let data_file = db::create_db_data_file(db_name, db_id)?;
    let log_file = db::create_db_log_file(db_name)?;

    Ok(OpenDatabaseResult {
        id: db_id,
        dat: data_file,
        log: log_file,
    })
}

#[derive(DekuRead, DekuWrite)]
pub struct Database {
    id: DatabaseId,
    name: String,
    database_version: u8,
    created_date: u16,
}

#[derive(DekuRead, DekuWrite)]
pub struct Table {
    id: DatabaseId,
    database_id: DatabaseId,
    name: String,
    created_date: u16,
}

#[derive(DekuRead, DekuWrite)]
pub struct Column {
    id: DatabaseId,
    table_id: DatabaseId,
    name: String,
    position: u8,
    is_nullable: bool,
    default_value: String,
    data_type: String,
    max_srt_length: u16,
    num_precision: u8,
    created_date: u16,
}

pub struct Index {
    id: DatabaseId,
    table_id: DatabaseId,
    name: String,
    created_date: u16,
}

const DATABASES_TABLE: &str = "databases";
const TABLES_TABLE: &str = "tables";
const COLUMNS_TABLE: &str = "columns";
const INDEXES_TABLE: &str = "indexes";

pub fn ensure_master_tables_exist() -> Result<()> {
    // create a databases table
    // id, name, created_date, database_version
    // id = primary key for index
    // this lists all databases tracked (including self).
    // create an indexes table?

    // Master DB
    let databases = vec![Database {
        id: MASTER_DB_ID,
        name: String::from(MASTER_NAME),
        created_date: util::now_bytes(),
        database_version: CURRENT_DATABASE_VERSION,
    }];

    let tables = vec![
        // Database table
        Table {
            id: 0, //todo
            database_id: MASTER_DB_ID,
            name: String::from(DATABASES_TABLE),
            created_date: now_bytes(),
        },
        // Tables table
        Table {
            id: 0, //todo
            database_id: MASTER_DB_ID,
            name: String::from(TABLES_TABLE),
            created_date: now_bytes(),
        },
        // Columns table
        Table {
            id: 0, //todo
            database_id: MASTER_DB_ID,
            name: String::from(COLUMNS_TABLE),
            created_date: now_bytes(),
        },
        // Indexes table
        Table {
            id: 0, //todo
            database_id: MASTER_DB_ID,
            name: String::from(INDEXES_TABLE),
            created_date: now_bytes(),
        },
    ];

    let columns = vec![Column {
        id: 0, //todo
        table_id: todo!(),
        name: todo!(),
        position: todo!(),
        is_nullable: todo!(),
        default_value: todo!(),
        data_type: todo!(),
        max_srt_length: todo!(),
        num_precision: todo!(),
        created_date: todo!(),
    }];

    Ok(())
}
