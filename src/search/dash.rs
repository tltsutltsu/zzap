use super::SearchEngine;
use crate::{
    lang,
    storage::{StorageError, StorageOperations},
};
use dashmap::DashMap;
use std::collections::HashSet;

// This is inverse index for search engine.
// It is a map of bucket+collection -> token -> document ids.

pub struct DashSearchEngine {
    index: DashMap<String, DashMap<String, HashSet<String>>>,
}

impl DashSearchEngine {
    pub fn new() -> Self {
        Self {
            index: DashMap::new(),
        }
    }
}

impl SearchEngine for DashSearchEngine {
    fn index(
        &self,
        storage: &crate::storage::Storage,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
        content: &str,
    ) -> Result<(), crate::storage::StorageError> {
        // let index_cleanup_result = self
        //     .remove_from_index(storage, bucket_name, collection_name, id)
        //     .await;

        // if let Err(e) = index_cleanup_result {
        //     if e != StorageError::BucketNotFound
        //         && e != StorageError::CollectionNotFound
        //         && e != StorageError::DocumentNotFound
        //     {
        //         return Err(e);
        //     }
        // }

        let tokens = lang::tokenize(content);

        let bucket_plus_collection = format!("{bucket_name}~ZZAP~{collection_name}");
        let collection = self
            .index
            .entry(bucket_plus_collection)
            .or_insert_with(DashMap::new);

        for token in tokens {
            let mut entry = collection.entry(token).or_insert_with(HashSet::new);
            entry.insert(id.to_string());
        }

        Ok(())
    }

    fn remove_from_index(
        &self,
        storage: &crate::storage::Storage,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
    ) -> Result<(), crate::storage::StorageError> {
        let content = storage.get_document(bucket_name, collection_name, id)?;
        let tokens = lang::tokenize(&content.content);

        let bucket_plus_collection = format!("{bucket_name}~ZZAP~{collection_name}");
        let collection = self
            .index
            .entry(bucket_plus_collection)
            .or_insert_with(DashMap::new);

        for token in tokens {
            let mut entry = collection.entry(token.clone()).or_insert_with(HashSet::new);
            entry.remove(id);

            if entry.is_empty() {
                collection.remove(&token);
            }
        }

        Ok(())
    }

    fn search(
        &self,
        bucket_name: &str,
        collection_name: &str,
        query: &str,
    ) -> Result<Vec<String>, StorageError> {
        let tokens = lang::tokenize(query);

        let bucket_plus_collection = format!("{bucket_name}~ZZAP~{collection_name}");
        let collection = self
            .index
            .entry(bucket_plus_collection)
            .or_insert_with(DashMap::new);

        let mut results: HashSet<String> = HashSet::new();

        for token in tokens {
            if let Some(ids) = collection.get(&token) {
                results.extend(ids.iter().map(|id| id.clone()));
            }
        }

        Ok(results
            .into_iter()
            .map(|id| id.as_str().to_string())
            .collect())
    }
}
