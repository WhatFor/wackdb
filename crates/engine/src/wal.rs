use anyhow::Result;
use deku::prelude::DekuSize;
use deku::{DekuRead, DekuWrite};
use thiserror::Error;

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
