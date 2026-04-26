use anyhow::{bail, Result};
use deku::prelude::DekuSize;
use deku::{ctx::Endian, DekuRead, DekuWrite};
use thiserror::Error;

use crate::file::DatabaseFileId;
use crate::fm::FileManager;

/// The max, current version number for the Log Header record
pub const CURRENT_WAL_HEADER_VERSION: u8 = 1;

const MAGIC_STRING: [u8; 4] = [0, 1, 6, 2];

#[derive(Debug, PartialEq, Error)]
pub enum WalError {
    #[error("WAL Magic String invalid")]
    MagicStringInvalid,
}

#[derive(DekuRead, DekuWrite, DekuSize, Debug, PartialEq)]
#[deku(endian = "big")]
pub struct WalHeader {
    #[deku(bytes = 4)]
    pub magic_string: [u8; 4],

    #[deku(bytes = 1)]
    pub version: u8,

    #[deku(bytes = 4)]
    pub last_checkpoint_offset: u32,

    #[deku(bytes = 4)]
    pub last_flushed_offset: u32,
}

impl WalHeader {
    pub fn default() -> Self {
        WalHeader {
            magic_string: MAGIC_STRING,
            version: CURRENT_WAL_HEADER_VERSION,
            last_checkpoint_offset: 0,
            last_flushed_offset: 0,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic_string == MAGIC_STRING {
            Ok(())
        } else {
            Err(WalError::MagicStringInvalid.into())
        }
    }
}

#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(
    id_type = "u8",
    endian = "endian",
    ctx = "endian: deku::ctx::Endian",
    ctx_default = "Endian::Big"
)]
pub enum LogType {
    #[deku(id = 0)]
    Insert,
    #[deku(id = 1)]
    Update,
    #[deku(id = 2)]
    Delete,
    #[deku(id = 3)]
    Checkpoint,
    #[deku(id = 4)]
    Commit,
}

#[derive(DekuRead, DekuWrite, Debug, PartialEq)]
#[deku(endian = "big")]
pub struct WalLog {
    #[deku(bytes = 4)]
    pub lsn: u32,
    #[deku(bytes = 4)]
    pub prev_lsn: u32,
    #[deku(bytes = 8)]
    pub transaction_id: u64,
    #[deku]
    pub log_type: LogType,
    #[deku(bytes = 2)]
    pub checksum: u16, // Big endian
    #[deku(bytes = 1)]
    pub payload_len: u16,
    #[deku(bytes = 65_536, count = "payload_len")] // TODO: is 65kb enough?
    pub payload: Vec<u8>,
}

impl WalLog {
    pub fn new(
        lsn: u32,
        prev_lsn: Option<u32>,
        txn_id: Option<u64>,
        log_type: LogType,
        payload: Vec<u8>,
    ) -> Self {
        let crc = check(&payload);
        let checksum = u16::from_be_bytes(crc);

        WalLog {
            lsn,
            prev_lsn: prev_lsn.unwrap_or(0),
            transaction_id: txn_id.unwrap_or(0),
            log_type: log_type,
            checksum: checksum,
            payload_len: payload
                .len()
                .try_into()
                .expect("Max payload length of 65kb exceeded"),
            payload: payload,
        }
    }
}

fn check(bytes: &[u8]) -> [u8; 2] {
    let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
    crc.checksum(bytes).to_be_bytes()
}

pub struct Wal;

impl Wal {
    pub fn default() -> Self {
        Wal {}
    }

    pub fn log(&self, fm: &FileManager, database: &DatabaseFileId, log: WalLog) -> Result<()> {
        if let Some(log_file) = fm.get_from_id(*database, crate::file_format::FileType::Log) {
            // TODO: this isn't a paged file - so need to refactor FM
        } else {
            bail!("Unable to find .wal file.");
        };

        Ok(())
    }
}
