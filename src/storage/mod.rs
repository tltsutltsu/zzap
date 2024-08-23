mod bucket;
mod collection;
mod error;

use bucket::Bucket;
use collection::Collection;
pub use error::*;

use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
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

type BucketStore = DashMap<String, Bucket>;

pub struct Storage {
    pub store: Arc<RwLock<BucketStore>>,
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
    fn load(&self) -> Result<(), StorageError>;
    fn initialize(&self) -> Result<(), StorageError>;
}

impl Storage {
    pub fn new<P: AsRef<Path>>(persistence_path: P) -> Self {
        Storage {
            store: Arc::new(RwLock::new(DashMap::new())),
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
        let store = self.store.read().map_err(|_| StorageError::PoisonError)?;
        let res = store
            .entry(bucket.to_string())
            .or_insert_with(|| Bucket::new(bucket.to_string()))
            .add_document(collection, document);

        res
    }

    fn get_document(
        &self,
        bucket: &str,
        collection: &str,
        id: &str,
    ) -> Result<Document, StorageError> {
        let store = self.store.read().map_err(|_| StorageError::PoisonError)?;
        let res = store
            .get(bucket)
            .ok_or(StorageError::BucketNotFound)?
            .get_document(collection, id);

        res
    }

    fn delete_document(
        &self,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
    ) -> Result<(), StorageError> {
        let store = self.store.write().map_err(|_| StorageError::PoisonError)?;
        let bucket = store.get(bucket_name).ok_or(StorageError::BucketNotFound)?;

        bucket.delete_document(&collection_name, id).and_then(|_| {
            if bucket.is_empty() {
                drop(bucket);
                store.remove(bucket_name);
            }
            Ok(())
        })?;

        Ok(())
    }

    fn persist(&self) -> Result<(), StorageError> {
        let store = self.store.read().map_err(|_| StorageError::PoisonError)?;
        let mut s = flexbuffers::FlexbufferSerializer::new();
        store
            .serialize(&mut s)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let serialized = s.take_buffer();
        std::fs::write(&self.persistence_path, serialized)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        Ok(())
    }

    fn load(&self) -> Result<(), StorageError> {
        if !self.persistence_path.exists() {
            return Ok(());
        }

        let serialized = std::fs::read(&self.persistence_path)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let s = flexbuffers::Reader::get_root(&*serialized)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let store: DashMap<String, Bucket> = Deserialize::deserialize(s)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        *self.store.write().map_err(|_| StorageError::PoisonError)? = store;
        Ok(())
    }

    fn initialize(&self) -> Result<(), StorageError> {
        self.load()?;
        Ok(())
    }
}
