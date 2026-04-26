use std::{collections::HashMap, hash::Hash};

use crate::{
    file::{DatabaseFileId, ManagedFile},
    file_format::FileType,
    page::PageId,
};

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

pub struct IdentifiedFile<'a> {
    pub id: &'a FileId,
    pub file: &'a ManagedFile,
}

#[derive(Default)]
pub struct FileManager {
    name_map: HashMap<NameMapKey, FileId>,
    id_map: HashMap<IdMapKey, FileId>,
    handles: HashMap<FileId, ManagedFile>,
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

    pub fn add(&mut self, id: FileId, file: ManagedFile, page_count: PageId) {
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

    pub fn get_from_id(&self, id: DatabaseFileId, ty: FileType) -> Option<&ManagedFile> {
        let file_id = self.id_map.get(&IdMapKey { id, ty })?;
        self.handles.get(file_id)
    }

    pub fn get_from_name(&self, name: String, ty: FileType) -> Option<&ManagedFile> {
        let file_id = self.name_map.get(&NameMapKey { name, ty })?;
        self.handles.get(file_id)
    }

    pub fn get_all<'a>(&'a self) -> Box<dyn Iterator<Item = IdentifiedFile<'a>> + 'a> {
        Box::new(
            self.handles
                .iter()
                .map(|(id, file)| IdentifiedFile { id, file }),
        )
    }

    pub fn next_file_id(&self) -> DatabaseFileId {
        self.handles.keys().map(|id| id.id).max().unwrap_or(0) + 1
    }
}

#[cfg(test)]
mod fm_tests {
    use super::*;
    use crate::file::MemoryFile;

    #[test]
    fn fm_can_add_and_get_file_by_id() {
        let mut fm = FileManager::new();

        let id = FileId::new(1, String::from("File 1"), FileType::Primary);
        let file = Box::new(MemoryFile::new(vec![]));
        let file = ManagedFile::Paged(file);

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
        let file = ManagedFile::Paged(file);

        fm.add(id, file, 0);

        let actual = fm.get_from_name(name, FileType::Primary);

        assert!(actual.is_some());
    }
}
