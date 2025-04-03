use std::{collections::HashMap, fs::File};

use crate::{
    db::{DatabaseId, FileType},
    page::PageId,
};

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub struct FileId {
    pub id: DatabaseId,
    pub ty: FileType,
}

impl FileId {
    pub fn new(id: DatabaseId, ty: FileType) -> Self {
        FileId { id, ty }
    }
}

pub struct IdentifiedFile<'a> {
    pub id: &'a FileId,
    pub file: &'a File,
}

pub struct FileManager {
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
            handles: HashMap::new(),
            allocated_page_count: HashMap::new(),
        }
    }

    pub fn add(&mut self, id: FileId, file: File, page_count: PageId) {
        self.handles.insert(id, file);
        self.allocated_page_count.insert(id, page_count);
    }

    pub fn get(&self, id: &FileId) -> Option<&File> {
        self.handles.get(id)
    }

    pub fn get_all(&self) -> Box<dyn Iterator<Item = IdentifiedFile> + '_> {
        Box::new(
            self.handles
                .iter()
                .map(|(id, file)| IdentifiedFile { id, file }),
        )
    }

    pub fn next_page_id(&self, id: &FileId) -> Option<&PageId> {
        self.allocated_page_count.get(id)
    }

    pub fn next_id(&self) -> DatabaseId {
        self.handles.keys().map(|id| id.id).max().unwrap_or(0) + 1
    }
}
