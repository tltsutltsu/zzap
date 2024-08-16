use std::sync::Arc;
use rayon::prelude::*;
use tokio::sync::RwLock;
use crate::storage::{Storage, StorageError};
use dashmap::DashMap;

type IndexStore = DashMap<String, DashMap<String, Vec<String>>>;

pub struct SearchEngine {
    index: Arc<RwLock<IndexStore>>,
}

impl SearchEngine {
    pub fn new() -> Self {
        SearchEngine {
            index: Arc::new(RwLock::new(DashMap::new())),
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
        let index = self.index.write().await;
        let bucket_index = index.entry(bucket.to_string()).or_insert_with(DashMap::new);
        let mut collection_index = bucket_index.entry(collection.to_string()).or_insert_with(Vec::new);

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

        let results: DashMap<String, usize> = DashMap::new();
        for token in query_tokens {
            collection_index.par_iter().for_each(|indexed_token| {
                if indexed_token.starts_with(&token) {
                    let (_, id) = indexed_token.split_once(':').unwrap();
                    results.entry(id.to_string()).and_modify(|count| *count += 1).or_insert(1);
                }
            });
        }

        let mut sorted_results: Vec<(String, usize)> = results.into_iter().collect();
        sorted_results.sort_by(|a, b| b.1.cmp(&a.1));

        Ok(sorted_results.into_iter().map(|(id, _)| id).collect())
    }

    pub async fn remove_from_index(&self, bucket: &str, collection: &str, id: &str) -> Result<(), StorageError> {
        let index = self.index.write().await;
        let bucket_index = index.get_mut(bucket).ok_or(StorageError::BucketNotFound)?;
        let mut collection_index = bucket_index.get_mut(collection).ok_or(StorageError::CollectionNotFound)?;

        collection_index.retain(|indexed_token| !indexed_token.ends_with(&format!(":{}", id)));

        Ok(())
    }
}