use super::SearchEngine;
use crate::storage::StorageOperations;
use crate::{
    lang,
    storage::{Storage, StorageError},
};
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

// IndexStore is a map of buckets, each containing a map of collections, each containing a map of tokens (as keys) and a vector of document ids (as values)
// The token is the single word.
// The document id is the id of the document it belongs to.
//                        Bucket
//                        |
//                        Collection
//                        |
//                        Token
//                        |
//                        Document IDs
type IndexStore = RwLock<HashMap<String, HashMap<String, HashMap<String, Vec<String>>>>>;

pub struct StdSearchEngine {
    index: Arc<IndexStore>,
}

impl StdSearchEngine {
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SearchEngine for StdSearchEngine {
    fn index(
        &self,
        storage: &Storage,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
        content: &str,
    ) -> Result<(), StorageError> {
        let index_cleanup_result =
            self.remove_from_index(storage, bucket_name, collection_name, id);
        if let Err(e) = index_cleanup_result {
            if e != StorageError::BucketNotFound
                && e != StorageError::CollectionNotFound
                && e != StorageError::DocumentNotFound
            {
                return Err(e);
            }
        }

        let tokens = lang::tokenize(content);

        let mut bucket = self.index.write().map_err(|_| StorageError::PoisonError)?;
        let bucket = bucket
            .entry(bucket_name.to_string())
            .or_insert_with(HashMap::new);

        let collection = bucket
            .entry(collection_name.to_string())
            .or_insert_with(HashMap::new);

        // if there is already tokens corresponding to the same id, remove the id from those tokens
        // and if the token is now empty, remove the token

        for token in tokens {
            let ids = collection.entry(token).or_insert_with(Vec::new);
            ids.push(id.to_string());
        }

        Ok(())
    }

    fn search(
        &self,
        bucket_name: &str,
        collection_name: &str,
        query: &str,
    ) -> Result<Vec<String>, StorageError> {
        // string, found times
        let found_ids: Mutex<HashMap<String, usize>> = Mutex::new(HashMap::new());

        let index = self.index.read().map_err(|_| StorageError::PoisonError)?;
        let bucket = index.get(bucket_name).ok_or(StorageError::BucketNotFound)?;
        let collection = bucket
            .get(collection_name)
            .ok_or(StorageError::CollectionNotFound)?;

        let tokens = lang::tokenize(query);

        for token in tokens {
            if let Some(ids) = collection.get(&token) {
                for id in ids {
                    *found_ids.lock().unwrap().entry(id.to_string()).or_insert(0) += 1;
                }
            }
        }

        // top 10
        let found_ids = found_ids.lock().expect("Mutex poisoned");
        let mut found_ids: Vec<(String, usize)> = found_ids.clone().into_iter().collect();
        found_ids.sort_by_key(|k| k.1);
        found_ids.reverse();
        found_ids.truncate(10);

        let found_ids = found_ids.clone().into_iter().map(|(id, _)| id).collect();

        Ok(found_ids)
    }

    fn remove_from_index(
        &self,
        storage: &Storage,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
    ) -> Result<(), StorageError> {
        // search all collection index entries (values) vectors for the id
        // if found, remove the id. if this was the last id, remove the entry
        // if not found, do nothing

        let document = storage.get_document(bucket_name, collection_name, id);

        if let Err(e) = document {
            match e {
                StorageError::DocumentNotFound => return Ok(()),
                StorageError::BucketNotFound => return Ok(()),
                StorageError::CollectionNotFound => return Ok(()),
                _ => return Err(e),
            }
        }

        let document = document.unwrap();

        let tokens = lang::tokenize(&document.content);

        let mut bucket = self.index.write().map_err(|_| StorageError::PoisonError)?;
        let bucket = bucket
            .get_mut(bucket_name)
            .ok_or(StorageError::BucketNotFound)?;
        let collection = bucket
            .get_mut(collection_name)
            .ok_or(StorageError::CollectionNotFound)?;

        for token in tokens {
            if let Some(ids) = collection.get_mut(&token) {
                ids.retain(|id| id != &id.to_string());

                if ids.is_empty() {
                    collection.remove(&token);
                }
            }
        }

        Ok(())
    }
}

/// Generate a token blacklist from the index
///
/// This is used to remove tokens that are too common, such as "the", "and", "is", etc,
/// therefore not adding much value to the search and increasing the size of the index.
///
/// This function iterates over the index and collects all the tokens, then filters out the most common (top 1%)
/// and returns them as a blacklist. It does not run if there are less than 1000 tokens in the index.
///
/// There may be a more sophisticated approach to this in the future, but for now this is a simple solution.
// TODO: When to run it? Need some kind of scheduler for this.
// async fn generate_token_blacklist(engine: &SearchEngine) -> Vec<String> {
//     unimplemented!();
//     // let mut blacklist = HashSet::new();
//     // for bucket in engine.index.iter() {
//     //     for collection in bucket.iter() {
//     //         for token in collection.iter() {
//     //             blacklist.insert(token.key().to_string());
//     //         }
//     //     }
//     // }
//     // blacklist.into_iter().collect()
// }

#[cfg(test)]
mod tests {
    use super::*;
}
