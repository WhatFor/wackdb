use anyhow::Result;
use deku::DekuContainerRead;
use derive_more::derive::From;
use std::{collections::HashMap, hash::Hash};
use thiserror::Error;

use crate::{
    file::{DatabaseFileId, DatabaseStorage},
    file_format::FileType,
    page::{PageDecoder, PageId},
    page_cache::PageBytes,
};

#[derive(Debug, From, Error)]
enum FileError {
    #[error("File not found.")]
    FileIdNotMatched,
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct FileId {
    pub id: DatabaseFileId,
    pub name: String,
    pub ty: FileType,
}

impl FileId {
    pub fn new(id: DatabaseFileId, name: String, ty: FileType) -> Self {
        FileId { id, name, ty }
    }
}

#[derive(Eq, PartialEq, Hash, Clone)]
struct NameMapKey {
    name: String,
    ty: FileType,
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct IdMapKey {
    id: DatabaseFileId,
    ty: FileType,
}

impl IdMapKey {
    pub fn new(id: DatabaseFileId, ty: FileType) -> Self {
        IdMapKey { id, ty }
    }
}

pub struct IdentifiedFile<'a> {
    pub id: &'a FileId,
    pub file: &'a mut dyn DatabaseStorage,
}

#[derive(Default)]
pub struct FileManager {
    name_map: HashMap<NameMapKey, FileId>,
    id_map: HashMap<IdMapKey, FileId>,
    handles: HashMap<FileId, Box<dyn DatabaseStorage>>,
    allocated_page_count: HashMap<FileId, PageId>,
}

impl FileManager {
    pub fn new() -> Self {
        FileManager {
            name_map: HashMap::new(),
            id_map: HashMap::new(),
            handles: HashMap::new(),
            allocated_page_count: HashMap::new(),
        }
    }

    pub fn add(&mut self, id: FileId, file: Box<dyn DatabaseStorage>, page_count: PageId) {
        // Insert entries into the ID and Name maps to facilitate finding Files by either property
        self.id_map.insert(
            IdMapKey {
                id: id.id,
                ty: id.ty,
            },
            id.clone(),
        );

        self.name_map.insert(
            NameMapKey {
                name: id.name.clone(),
                ty: id.ty,
            },
            id.clone(),
        );

        self.handles.insert(id.clone(), file);
        self.allocated_page_count.insert(id.clone(), page_count);
    }

    pub fn get_from_id(
        &mut self,
        id: DatabaseFileId,
        ty: FileType,
    ) -> Option<&mut Box<dyn DatabaseStorage>> {
        let file_id = self.id_map.get(&IdMapKey { id, ty })?;
        self.handles.get_mut(file_id)
    }

    pub fn get_from_name(
        &mut self,
        name: String,
        ty: FileType,
    ) -> Option<&mut Box<dyn DatabaseStorage>> {
        let file_id = self.name_map.get(&NameMapKey { name, ty })?;
        self.handles.get_mut(file_id)
    }

    pub fn get_all<'a>(&'a mut self) -> Box<dyn Iterator<Item = IdentifiedFile<'a>> + 'a> {
        Box::new(self.handles.iter_mut().map(|(id, file)| IdentifiedFile {
            id,
            file: file.as_mut(),
        }))
    }

    pub fn next_page_id_by_id(&self, id: DatabaseFileId, ty: FileType) -> Option<PageId> {
        let file_id = self.id_map.get(&IdMapKey { id, ty })?;
        self.allocated_page_count.get(file_id).copied()
    }

    pub fn read_page_as<'a, T>(&mut self, file_id: &IdMapKey, page_index: PageId) -> Result<T>
    where
        T: DekuContainerRead<'a> + std::fmt::Debug,
    {
        let page_bytes = self.read_page(file_id, page_index)?;

        let page = PageDecoder::from_bytes(&page_bytes);
        let bytes = page.try_read::<T>(0)?;

        Ok(bytes)
    }

    pub fn read_page(&mut self, file_id: &IdMapKey, page_index: PageId) -> Result<PageBytes> {
        match self.id_map.get(file_id) {
            Some(id) => match self.handles.get_mut(id) {
                Some(file) => file.read_page(page_index),
                None => Err(FileError::FileIdNotMatched.into()),
            },
            None => Err(FileError::FileIdNotMatched.into()),
        }
    }

    pub fn write_page(&mut self, file_id: &IdMapKey, data: &[u8], page_index: u32) -> Result<()> {
        match self.id_map.get(file_id) {
            Some(id) => match self.handles.get_mut(id) {
                Some(file) => {
                    file.write_page(data, page_index)?;

                    let current_offset = self.allocated_page_count.get(id).unwrap();

                    self.allocated_page_count
                        .insert(id.clone(), current_offset + 1);

                    Ok(())
                }
                None => Err(FileError::FileIdNotMatched.into()),
            },
            None => Err(FileError::FileIdNotMatched.into()),
        }
    }
    pub fn next_file_id(&self) -> DatabaseFileId {
        self.handles.keys().map(|id| id.id).max().unwrap_or(0) + 1
    }
}

#[cfg(test)]
mod fm_tests {
    use crate::file::MemoryFile;

    use super::*;

    #[test]
    fn fm_can_add_and_get_file_by_id() {
        let mut fm = FileManager::new();

        let id = FileId::new(1, String::from("File 1"), FileType::Primary);
        let file = Box::new(MemoryFile::new(vec![]));

        fm.add(id, file, 0);

        let actual = fm.get_from_id(1, FileType::Primary);

        assert!(actual.is_some());
    }

    #[test]
    fn fm_can_add_and_get_file_by_name() {
        let mut fm = FileManager::new();

        let name = String::from("File 1");
        let id = FileId::new(1, name.clone(), FileType::Primary);
        let file = Box::new(MemoryFile::new(vec![]));

        fm.add(id, file, 0);

        let actual = fm.get_from_name(name, FileType::Primary);

        assert!(actual.is_some());
    }
}
