use super::{Document, StorageError, StorageOperations};
use crate::storage::EntityType;
use std::collections::HashMap;
use std::sync::RwLock;

pub struct MockStorage(RwLock<HashMap<String, Document>>);
impl MockStorage {
    pub fn new() -> Self {
        MockStorage(RwLock::new(HashMap::new()))
    }
}
impl StorageOperations for MockStorage {
    fn get_document(
        &self,
        _bucket: &str,
        _collection: &str,
        id: &str,
    ) -> Result<Document, StorageError> {
        let res = self
            .0
            .read()
            .map_err(|_| StorageError::PoisonError)?
            .get(id)
            .cloned()
            .ok_or(StorageError::NotFound(EntityType::Item));
        res
    }
    fn add_document(
        &self,
        _bucket: &str,
        _collection: &str,
        document: Document,
    ) -> Result<(), StorageError> {
        self.0
            .write()
            .map_err(|_| StorageError::PoisonError)?
            .insert(document.id.clone(), document);
        Ok(())
    }
    fn delete_document(
        &self,
        _bucket: &str,
        _collection: &str,
        id: &str,
    ) -> Result<(), StorageError> {
        self.0
            .write()
            .map_err(|_| StorageError::PoisonError)?
            .remove(id);
        Ok(())
    }
    fn persist(&self) -> Result<(), StorageError> {
        Ok(())
    }
    fn load(&mut self) -> Result<(), StorageError> {
        Ok(())
    }
    fn initialize(&mut self) -> Result<(), StorageError> {
        Ok(())
    }
}
