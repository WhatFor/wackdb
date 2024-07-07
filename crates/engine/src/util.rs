use std::os::windows::fs::OpenOptionsExt;

pub fn file_exists(path: &String) -> bool {
    let path_obj = std::path::Path::new(&path);

    match std::path::Path::try_exists(path_obj) {
        Ok(exists) => exists,
        Err(err) => panic!("Error: Unable to read filesystem. See: {}", err),
    }
}

pub fn ensure_path_exists(path: &std::path::PathBuf) {
    match std::fs::create_dir_all(path) {
        Err(err) => panic!("Error: Unable to write filesystem. See: {}", err),
        _ => {}
    }
}

pub fn create_file(path: &String) -> Result<std::fs::File, crate::CreateDatabaseError> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(0x80000000) // FILE_FLAG_WRITE_THROUGH
        .open(path);

    match file {
        Ok(file_result) => Ok(file_result),
        Err(err) => Err(crate::CreateDatabaseError::UnableToCreateFile(err)),
    }
}

pub fn open_file(path: &String) -> Result<std::fs::File, std::io::Error> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
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
            .open(&path)
            .expect("Failed to create temp file");

        (file, path)
    }

    #[test]
    fn test_file_exists_when_true() {
        let (_, temp_path) = get_temp_file();

        let actual = file_exists(&temp_path.to_str().unwrap().to_string());

        assert_eq!(actual, true);

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_file_exists_when_false() {
        let temp_path = temp_dir_path();

        let actual = file_exists(&temp_path.to_str().unwrap().to_string());

        assert_eq!(actual, false);
    }

    #[test]
    fn test_ensure_path_exists() {
        let mut temp_dir = temp_dir();
        temp_dir.push("/test");

        ensure_path_exists(&temp_dir);

        // Clean down
        std::fs::remove_dir(temp_dir).expect("Unable to clear down test.");
    }

    #[test]
    fn test_create_file() {
        let temp_path = temp_dir_path();

        let actual = create_file(&temp_path.to_str().unwrap().to_string());

        assert_eq!(actual.is_ok(), true);

        // Should be writable
        assert_eq!(
            actual.unwrap().metadata().unwrap().permissions().readonly(),
            false
        );

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }

    #[test]
    fn test_open_file() {
        let temp_path = temp_dir_path();

        {
            create_file(&temp_path.to_str().unwrap().to_string())
                .expect("Unable to create test file.");
        }

        let actual = open_file(&temp_path.to_str().unwrap().to_string());

        assert_eq!(actual.is_ok(), true);

        // Should be writable
        assert_eq!(
            actual.unwrap().metadata().unwrap().permissions().readonly(),
            false
        );

        // Clean down
        std::fs::remove_file(temp_path).expect("Unable to clear down test.");
    }
}
