use super::Document;
use super::StorageError;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct Collection {
    name: String,
    pub documents: DashMap<String, Document>, // TODO: name is stored twice: in key and in value struct. remove one of them, this is a waste of space
}

impl Collection {
    pub fn new(name: String) -> Self {
        Collection {
            name,
            documents: DashMap::new(),
        }
    }

    pub fn add_document(&self, document: Document) -> Result<(), StorageError> {
        self.documents.insert(document.id.clone(), document);
        Ok(())
    }

    pub fn get_document(&self, id: &str) -> Result<Document, StorageError> {
        self.documents
            .get(id)
            .map(|doc| doc.clone())
            .ok_or(StorageError::DocumentNotFound)
    }

    pub fn delete_document(&self, id: &str) -> Result<(), StorageError> {
        self.documents
            .remove(id)
            .map(|_| ())
            .ok_or(StorageError::DocumentNotFound)
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }
}
