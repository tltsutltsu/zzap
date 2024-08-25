use super::SearchEngine;
use crate::{
    lang,
    storage::{StorageError, StorageOperations},
};
use std::{
    collections::{BTreeMap, HashSet},
    sync::RwLock,
};

// This is inverse index for search engine.
// It is a map of bucket+collection -> token -> document ids.

pub struct BTreeSearchEngine {
    index: RwLock<BTreeMap<String, HashSet<String>>>,
}

impl BTreeSearchEngine {
    pub fn new() -> Self {
        Self {
            index: RwLock::new(BTreeMap::new()),
        }
    }
}

impl SearchEngine for BTreeSearchEngine {
    fn index(
        &self,
        _storage: &dyn StorageOperations,
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

        let mut content = content.to_string();
        let tokens = lang::tokenize_iter(&mut content);

        let mut unlocked_index = self.index.write().unwrap();

        for token in tokens {
            let key = format!("{bucket_name}~ZZAP~{collection_name}~ZZAP~{token}");
            let mut entry = unlocked_index.get_mut(&key);
            if entry.is_none() {
                unlocked_index.insert(key.clone(), HashSet::new());
                entry = unlocked_index.get_mut(&key);
            }
            entry.unwrap().insert(id.to_string());
        }

        Ok(())
    }

    fn remove_from_index(
        &self,
        _storage: &dyn StorageOperations,
        _bucket_name: &str,
        _collection_name: &str,
        _id: &str,
    ) -> Result<(), crate::storage::StorageError> {
        // let content = storage.get_document(bucket_name, collection_name, id)?;
        // let tokens = lang::tokenize(&content.content);

        // for token in tokens {
        //     let key = format!("{bucket_name}~ZZAP~{collection_name}~ZZAP~{token}");
        //     let mut entry = self.index.entry(key.clone()).or_insert_with(HashSet::new);
        //     entry.remove(id);

        //     if entry.is_empty() {
        //         self.index.remove(&key);
        //     }
        // }

        Ok(())
    }

    fn search(
        &self,
        bucket_name: &str,
        collection_name: &str,
        query: &str,
    ) -> Result<Vec<String>, StorageError> {
        let tokens = lang::tokenize(query);

        let mut results: HashSet<String> = HashSet::new();

        let reader = self.index.read().unwrap();

        for token in tokens {
            let key = format!("{bucket_name}~ZZAP~{collection_name}~ZZAP~{token}");
            if let Some(ids) = reader.get(&key) {
                results.extend(ids.iter().map(|id| id.clone()));
            }
        }

        Ok(results
            .into_iter()
            .map(|id| id.as_str().to_string())
            .collect())
    }
}
