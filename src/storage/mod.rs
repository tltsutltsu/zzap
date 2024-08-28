mod error;
pub mod mock;

pub use error::*;

use dashmap::{try_result::TryResult, DashMap};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    pub content: String,
}

impl Document {
    pub fn new(id: &str, content: &str) -> Self {
        Document {
            id: id.to_string(),
            content: content.to_string(),
        }
    }
}

// Bucket
// |
// Collection
// |
// Document, where the value is the content and the key is the id
type StorageInner = DashMap<String, DashMap<String, DashMap<String, String>>>;

pub struct Storage {
    pub store: Arc<StorageInner>,
    persistence_path: PathBuf,
}

pub trait StorageOperations {
    fn add_document(
        &self,
        bucket: &str,
        collection: &str,
        document: Document,
    ) -> Result<(), StorageError>;
    fn get_document(
        &self,
        bucket: &str,
        collection: &str,
        id: &str,
    ) -> Result<Document, StorageError>;
    fn delete_document(&self, bucket: &str, collection: &str, id: &str)
        -> Result<(), StorageError>;
    fn persist(&self) -> Result<(), StorageError>;
    fn load(&mut self) -> Result<(), StorageError>;
    fn initialize(&mut self) -> Result<(), StorageError>;
}

pub trait StorageOperationsInternal: StorageOperations {
    fn store(&self) -> Result<Arc<StorageInner>, StorageError>;
}

trait TryResultUnwrapStorageError<T> {
    fn unwrap_storage_error(self, entity_type: EntityType) -> Result<T, StorageError>;
}

impl<T> TryResultUnwrapStorageError<T> for TryResult<T> {
    fn unwrap_storage_error(self, entity_type: EntityType) -> Result<T, StorageError> {
        match self {
            TryResult::Present(item) => Ok(item),
            TryResult::Absent => Err(StorageError::NotFound(entity_type)),
            TryResult::Locked => Err(StorageError::Locked(entity_type)),
        }
    }
}

impl Storage {
    pub fn new<P: AsRef<Path>>(persistence_path: P) -> Self {
        Storage {
            store: Arc::new(DashMap::new()),
            persistence_path: persistence_path.as_ref().to_path_buf(),
        }
    }
}

impl StorageOperations for Storage {
    fn add_document(
        &self,
        bucket: &str,
        collection: &str,
        document: Document,
    ) -> Result<(), StorageError> {
        let _res = self
            .store
            .try_entry(bucket.to_string())
            .ok_or(StorageError::Locked(EntityType::Bucket))?
            .or_insert_with(|| DashMap::new())
            .try_entry(collection.to_string())
            .ok_or(StorageError::Locked(EntityType::Collection))?
            .or_insert_with(|| DashMap::new())
            .insert(document.id, document.content);

        Ok(())
    }

    fn get_document(
        &self,
        bucket: &str,
        collection: &str,
        id: &str,
    ) -> Result<Document, StorageError> {
        let bucket = self
            .store
            .try_get(bucket)
            .unwrap_storage_error(EntityType::Bucket)?;
        let collection = bucket
            .try_get(collection)
            .unwrap_storage_error(EntityType::Collection)?;
        let res = collection
            .try_get(id)
            .unwrap_storage_error(EntityType::Item)?;

        Ok(Document::new(id, &res))
    }

    fn delete_document(
        &self,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
    ) -> Result<(), StorageError> {
        let bucket = self
            .store
            .try_get(bucket_name)
            .unwrap_storage_error(EntityType::Bucket)?;
        let collection = bucket
            .try_get(collection_name)
            .unwrap_storage_error(EntityType::Collection)?;
        collection.remove(id);

        if collection.is_empty() {
            drop(collection);
            bucket.remove(collection_name);

            if bucket.is_empty() {
                drop(bucket);
                self.store.remove(bucket_name);
            }
        }

        Ok(())
    }

    fn persist(&self) -> Result<(), StorageError> {
        let tmp_path = self.persistence_path.with_extension("zzap_tmp"); // `zzap_tmp` is used to avoid situation where user would name database file with `tmp` extension

        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.store
            .serialize(&mut s)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let serialized = s.take_buffer();
        std::fs::write(&tmp_path, serialized)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        std::fs::rename(&tmp_path, &self.persistence_path)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        Ok(())
    }

    fn load(&mut self) -> Result<(), StorageError> {
        if !self.persistence_path.exists() {
            return Ok(());
        }

        let serialized = std::fs::read(&self.persistence_path)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let s = flexbuffers::Reader::get_root(&*serialized)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let store: StorageInner = Deserialize::deserialize(s)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        self.store = Arc::new(store);
        Ok(())
    }

    fn initialize(&mut self) -> Result<(), StorageError> {
        self.load()?;
        Ok(())
    }
}

impl StorageOperationsInternal for Storage {
    fn store(&self) -> Result<Arc<StorageInner>, StorageError> {
        Ok(self.store.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_persistence_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        const PERSISTENCE_PATH: &str = "test.db";
        let documents = vec![
            ("bucket", "collection", Document::new("id", "content")),
            (
                "other bucket",
                "other collection?",
                Document::new("id2", "content2"),
            ),
            (
                r#"bucket name with ascii non␍-prin␀␊tab␄le characters␄ and <html></html> and {"json": "object"} and "quoted string" and 'single quoted string' and `backticks`"#,
                r#"collection name with ascii non␍-prin␀␊tab␄le characters␄ and <html></html> and {"json": "object"} and "quoted string" and 'single quoted string' and `backticks`"#,
                Document::new(
                    r#"id with ascii non␍-prin␀␊tab␄le characters␄ and <html></html> and {"json": "object"} and "quoted string" and 'single quoted string' and `backticks`"#,
                    r#"content with ascii non␍-prin␀␊tab␄le characters␄ and <html></html> and {"json": "object"} and "quoted string" and 'single quoted string' and `backticks`"#,
                ),
            ),
        ];
        let mut storage = Storage::new(PERSISTENCE_PATH);
        storage.initialize()?;

        for (bucket, collection, document) in documents.clone() {
            storage.add_document(bucket, collection, document)?;
        }

        storage.persist()?;

        let mut storage = Storage::new(PERSISTENCE_PATH);
        storage.initialize()?;

        for (bucket, collection, document) in documents.clone() {
            let doc_from_storage = storage.get_document(bucket, collection, &document.id)?;
            assert_eq!(doc_from_storage.content, document.content);
        }

        for (bucket, collection, document) in documents.clone() {
            storage.delete_document(bucket, collection, &document.id)?;
        }

        storage.persist()?;
        let mut storage = Storage::new(PERSISTENCE_PATH);
        storage.initialize()?;

        for (bucket, collection, document) in documents.clone() {
            let res = storage.get_document(bucket, collection, &document.id);
            assert!(res.is_err());
            assert!(res.err().unwrap().is_not_found());
        }

        assert!(storage.store.is_empty());

        Ok(())
    }

    #[test]
    fn test_storage_load_without_persistence_path() -> Result<(), Box<dyn std::error::Error>> {
        let mut storage = Storage::new("");
        let res = storage.initialize();
        assert!(res.is_ok());
        Ok(())
    }
}
