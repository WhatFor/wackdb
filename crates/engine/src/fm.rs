use std::{collections::HashMap, fs::File, hash::Hash};

use crate::{
    db::{DatabaseId, FileType},
    page::PageId,
};

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct FileId {
    pub id: DatabaseId,
    pub name: String,
    pub ty: FileType,
}

#[derive(Eq, PartialEq, Hash, Clone)]
struct NameMapKey {
    name: String,
    ty: FileType,
}

#[derive(Eq, PartialEq, Hash, Clone)]
struct IdMapKey {
    id: DatabaseId,
    ty: FileType,
}

impl FileId {
    pub fn new(id: DatabaseId, name: String, ty: FileType) -> Self {
        FileId { id, name, ty }
    }
}

pub struct IdentifiedFile<'a> {
    pub id: &'a FileId,
    pub file: &'a File,
}

pub struct FileManager {
    name_map: HashMap<NameMapKey, FileId>,
    id_map: HashMap<IdMapKey, FileId>,
    handles: HashMap<FileId, File>,
    allocated_page_count: HashMap<FileId, PageId>,
}

impl Default for FileManager {
    fn default() -> Self {
        Self::new()
    }
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

    pub fn add(&mut self, id: FileId, file: File, page_count: PageId) {
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

    pub fn get_from_id(&self, id: DatabaseId, ty: FileType) -> Option<&File> {
        let file_id = self.id_map.get(&IdMapKey { id, ty })?;
        self.handles.get(file_id)
    }

    pub fn get_from_name(&self, name: String, ty: FileType) -> Option<&File> {
        let file_id = self.name_map.get(&NameMapKey { name, ty })?;
        self.handles.get(file_id)
    }

    pub fn get_all(&self) -> Box<dyn Iterator<Item = IdentifiedFile> + '_> {
        Box::new(
            self.handles
                .iter()
                .map(|(id, file)| IdentifiedFile { id, file }),
        )
    }

    pub fn next_page_id_by_id(&self, id: DatabaseId, ty: FileType) -> Option<&PageId> {
        let file_id = self.id_map.get(&IdMapKey { id, ty })?;
        self.allocated_page_count.get(file_id)
    }

    pub fn next_id(&self) -> DatabaseId {
        self.handles.keys().map(|id| id.id).max().unwrap_or(0) + 1
    }
}
