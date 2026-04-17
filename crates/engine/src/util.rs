use anyhow::Result;
use derive_more::derive::From;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, From, Error)]
pub enum Error {
    #[error("IO Error: {0}")]
    Io(std::io::Error),
}

pub fn now_bytes() -> u16 {
    time_bytes(std::time::SystemTime::now())
}

pub fn time_bytes(time: std::time::SystemTime) -> u16 {
    time.duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u16
}

pub fn file_exists(path: &Path) -> Result<bool> {
    Ok(Path::try_exists(path)?)
}

pub fn ensure_path_exists(path: &Path) -> Result<()> {
    let dir = match path.is_dir() {
        true => path,
        false => path.parent().unwrap(),
    };

    std::fs::create_dir_all(dir)?;
    Ok(())
}

pub fn create_file(path: &PathBuf) -> Result<std::fs::File> {
    Ok(std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        // TODO: Only works on windows - Need multiplatform support.
        //.custom_flags(0x80000000) // FILE_FLAG_WRITE_THROUGH
        .open(path)?)
}

pub fn open_file(path: &PathBuf) -> Result<std::fs::File> {
    Ok(std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?)
}

pub fn get_base_path() -> std::path::PathBuf {
    match std::env::current_exe() {
        Ok(mut path) => {
            path.pop();
            path
        }
        Err(err) => panic!("Error: Unable to read filesystem. See: {}", err),
    }
}

#[cfg(test)]
mod util_tests {
    use crate::*;

    use std::{
        env::temp_dir,
        fs::{File, OpenOptions},
        path::PathBuf,
    };
    use util::{create_file, ensure_path_exists, file_exists, open_file};
    use uuid::Uuid;

    fn temp_dir_path() -> std::path::PathBuf {
        let mut dir = temp_dir();
        let id = Uuid::new_v4().to_string();
        dir.push(id + ".tmp");

        dir
    }

    fn get_temp_file() -> (File, PathBuf) {
        let path = temp_dir_path();

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .expect("Failed to create temp file");

        (file, path)
    }

    #[test]
    fn test_file_exists_when_true() {
        let (_, temp_path) = get_temp_file();
        let actual = file_exists(&temp_path).unwrap();

        assert!(actual);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_file_exists_when_false() {
        let temp_path = temp_dir_path();
        let actual = file_exists(&temp_path).unwrap();

        assert!(!actual);
    }

    #[test]
    fn test_ensure_path_exists() {
        let mut temp_dir = temp_dir();
        temp_dir.push("test.file");

        ensure_path_exists(&temp_dir).unwrap();
    }

    #[test]
    fn test_create_file() {
        let temp_path = temp_dir_path();
        let actual = create_file(&temp_path);

        assert!(actual.is_ok());

        let is_readonly = actual.unwrap().metadata().unwrap().permissions().readonly();

        // Should be writable
        assert!(!is_readonly);
    }

    #[test]
    fn test_open_file() {
        let temp_path = temp_dir_path();

        {
            create_file(&temp_path).expect("Unable to create test file.");
        }

        let actual = open_file(&temp_path);

        assert!(actual.is_ok());
        let is_readonly = actual.unwrap().metadata().unwrap().permissions().readonly();

        // Should be writable
        assert!(!is_readonly);
    }
}
