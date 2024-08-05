use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::storage::{Storage, StorageError};

type IndexStore = HashMap<String, HashMap<String, Vec<String>>>;

pub struct SearchEngine {
    index: Arc<RwLock<IndexStore>>,
}

impl SearchEngine {
    pub fn new() -> Self {
        SearchEngine {
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub async fn initialize(&self, storage: &Storage) -> Result<(), StorageError> {
        let store = storage.store.read().await;
        for bucket_ref in store.iter() {
            let bucket_name = bucket_ref.key();
            let bucket = bucket_ref.value();
            for collection_ref in bucket.collections.iter() {
                let collection_name = collection_ref.key();
                let collection = collection_ref.value();
                for document_ref in collection.documents.iter() {
                    let document_id = document_ref.key();
                    let document = document_ref.value();
                    self.index(bucket_name, collection_name, document_id, &document.content).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn index(&self, bucket: &str, collection: &str, id: &str, content: &str) -> Result<(), StorageError> {
        let mut index = self.index.write().await;
        let bucket_index = index.entry(bucket.to_string()).or_insert_with(HashMap::new);
        let collection_index = bucket_index.entry(collection.to_string()).or_insert_with(Vec::new);

        // Simple tokenization (split by whitespace)
        let tokens: Vec<String> = content.split_whitespace()
            .map(|s| s.to_lowercase())
            .collect();

        for token in tokens {
            collection_index.push(format!("{}:{}", token, id));
        }

        Ok(())
    }

    pub async fn search(&self, bucket: &str, collection: &str, query: &str) -> Result<Vec<String>, StorageError> {
        let index = self.index.read().await;
        let bucket_index = index.get(bucket).ok_or(StorageError::BucketNotFound)?;
        let collection_index = bucket_index.get(collection).ok_or(StorageError::CollectionNotFound)?;

        let query_tokens: Vec<String> = query.split_whitespace()
            .map(|s| s.to_lowercase())
            .collect();

        let mut results: HashMap<String, usize> = HashMap::new();

        for token in query_tokens {
            for indexed_token in collection_index.iter() {
                if indexed_token.starts_with(&token) {
                    let (_, id) = indexed_token.split_once(':').unwrap();
                    *results.entry(id.to_string()).or_insert(0) += 1;
                }
            }
        }

        let mut sorted_results: Vec<(String, usize)> = results.into_iter().collect();
        sorted_results.sort_by(|a, b| b.1.cmp(&a.1));

        Ok(sorted_results.into_iter().map(|(id, _)| id).collect())
    }

    pub async fn remove_from_index(&self, bucket: &str, collection: &str, id: &str) -> Result<(), StorageError> {
        let mut index = self.index.write().await;
        let bucket_index = index.get_mut(bucket).ok_or(StorageError::BucketNotFound)?;
        let collection_index = bucket_index.get_mut(collection).ok_or(StorageError::CollectionNotFound)?;

        collection_index.retain(|indexed_token| !indexed_token.ends_with(&format!(":{}", id)));

        Ok(())
    }
}