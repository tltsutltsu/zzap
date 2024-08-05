use super::Collection;
use super::Document;
use super::StorageError;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct Bucket {
    name: String,
    pub collections: DashMap<String, Collection>,
}

impl Bucket {
    pub fn new(name: String) -> Self {
        Bucket {
            name,
            collections: DashMap::new(),
        }
    }

    pub fn add_document(&self, collection_name: &str, document: Document) -> Result<(), StorageError> {
        self.collections
            .entry(collection_name.to_string())
            .or_insert_with(|| Collection::new(collection_name.to_string()))
            .add_document(document)
    }

    pub fn get_document(&self, collection_name: &str, id: &str) -> Result<Document, StorageError> {
        self.collections
            .get(collection_name)
            .ok_or(StorageError::CollectionNotFound)?
            .get_document(id)
    }

    pub fn delete_document(&self, collection_name: &str, id: &str) -> Result<(), StorageError> {
        let result = self.collections
            .get(collection_name)
            .ok_or(StorageError::CollectionNotFound)?
            .delete_document(id);

        // Clean up empty collections
        if result.is_ok() {
            if let Some(collection) = self.collections.get(collection_name) {
                if collection.is_empty() {
                    self.collections.remove(collection_name);
                }
            }
        }

        result
    }

    pub fn is_empty(&self) -> bool {
        self.collections.is_empty()
    }
}