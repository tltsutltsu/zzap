mod bucket;
mod collection;
mod error;

use bucket::Bucket;
use collection::Collection;
pub use error::StorageError;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, sync::Arc};
use tokio::sync::RwLock;
use async_trait::async_trait;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Document {
    pub id: String,
    pub content: String,
}

impl Document {
    pub fn new(id: &str, content: &str) -> Self {
        Document { id: id.to_string(), content: content.to_string() }
    }
}

type BucketStore = DashMap<String, Bucket>;

pub struct Storage {
    pub store: Arc<RwLock<BucketStore>>,
    persistence_path: PathBuf,
}

#[async_trait]
pub trait StorageOperations {
    async fn add_document(&self, bucket: &str, collection: &str, document: Document) -> Result<(), StorageError>;
    async fn get_document(&self, bucket: &str, collection: &str, id: &str) -> Result<Document, StorageError>;
    async fn delete_document(&self, bucket: &str, collection: &str, id: &str) -> Result<(), StorageError>;
    async fn persist(&self) -> Result<(), StorageError>;
    async fn load(&self) -> Result<(), StorageError>;
    async fn initialize(&self) -> Result<(), StorageError>;
}

impl Storage {
    pub fn new<P: AsRef<Path>>(persistence_path: P) -> Self {
        Storage {
            store: Arc::new(RwLock::new(DashMap::new())),
            persistence_path: persistence_path.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl StorageOperations for Storage {
    async fn add_document(&self, bucket: &str, collection: &str, document: Document) -> Result<(), StorageError> {
        let store = self.store.read().await;
        let res = store
            .entry(bucket.to_string())
            .or_insert_with(|| Bucket::new(bucket.to_string()))
            .add_document(collection, document);

        res
            .map_err(|e| StorageError::OperationFailed(e.to_string()))
    }

    async fn get_document(&self, bucket: &str, collection: &str, id: &str) -> Result<Document, StorageError> {
        let store = self.store.read().await;
        let res = store
            .get(bucket)
            .ok_or(StorageError::BucketNotFound)?
            .get_document(collection, id);

        res
            .map_err(|e| StorageError::OperationFailed(e.to_string()))
    }

    async fn delete_document(&self, bucket: &str, collection: &str, id: &str) -> Result<(), StorageError> {
        let store = self.store.write().await;
        let result = store
            .get(bucket)
            .ok_or(StorageError::BucketNotFound)?
            .delete_document(collection, id);

        // Clean up empty buckets
        if result.is_ok() {
            if let Some(bucket_entry) = store.get(bucket) {
                if bucket_entry.is_empty() {
                    store.remove(bucket);
                }
            }
        }

        result
    }

    async fn persist(&self) -> Result<(), StorageError> {
        let store = self.store.read().await;
        let mut s = flexbuffers::FlexbufferSerializer::new();
        store.serialize(&mut s).map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let serialized = s.take_buffer();
        std::fs::write(&self.persistence_path, serialized).map_err(|e| StorageError::SerializationError(e.to_string()))?;
        Ok(())
    }

    async fn load(&self) -> Result<(), StorageError> {
        if !self.persistence_path.exists() {
            return Ok(());
        }

        let serialized = std::fs::read(&self.persistence_path).map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let s = flexbuffers::Reader::get_root(&*serialized).map_err(|e| StorageError::SerializationError(e.to_string()))?;
        let store: DashMap<String, Bucket> = Deserialize::deserialize(s).map_err(|e| StorageError::SerializationError(e.to_string()))?;
        *self.store.write().await = store;
        Ok(())
    }

    async fn initialize(&self) -> Result<(), StorageError> {
        self.load().await?;
        Ok(())
    }
}