use super::SearchEngine;
use crate::storage::{EntityType, StorageOperations};
use crate::{lang, storage::StorageError};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
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
        storage: &dyn StorageOperations,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
        content: &str,
    ) -> Result<(), StorageError> {
        let index_cleanup_result =
            self.remove_from_index(storage, bucket_name, collection_name, id);
        if let Err(e) = index_cleanup_result {
            if !e.is_not_found() {
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
        let bucket = index
            .get(bucket_name)
            .ok_or(StorageError::NotFound(EntityType::Bucket))?;
        let collection = bucket
            .get(collection_name)
            .ok_or(StorageError::NotFound(EntityType::Collection))?;

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
        storage: &dyn StorageOperations,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
    ) -> Result<(), StorageError> {
        // search all collection index entries (values) vectors for the id
        // if found, remove the id. if this was the last id, remove the entry
        // if not found, do nothing

        let document = storage.get_document(bucket_name, collection_name, id);

        if let Err(e) = document {
            if e.is_not_found() {
                return Ok(());
            }
            return Err(e);
        }

        let document = document.unwrap();

        let tokens = lang::tokenize(&document.content);

        let mut bucket = self.index.write().map_err(|_| StorageError::PoisonError)?;
        let bucket = bucket
            .get_mut(bucket_name)
            .ok_or(StorageError::NotFound(EntityType::Bucket))?;
        let collection = bucket
            .get_mut(collection_name)
            .ok_or(StorageError::NotFound(EntityType::Collection))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{mock::MockStorage, Document};

    #[test]
    fn test_index_cleanups() {
        let engine = StdSearchEngine::new();
        let bucket_name = "test_bucket";
        let collection_name = "test_collection";
        let doc_id = "test_doc";

        let storage = MockStorage::new();

        // Initial indexing
        engine
            .index(
                &storage,
                bucket_name,
                collection_name,
                doc_id,
                "initial content",
            )
            .unwrap();

        storage
            .add_document(
                bucket_name,
                collection_name,
                Document::new(doc_id, "initial content (old)"),
            )
            .unwrap();

        // Re-index with new content
        engine
            .index(
                &storage,
                bucket_name,
                collection_name,
                doc_id,
                "new updated content",
            )
            .unwrap();

        // Check the index state
        let index = engine.index.read().unwrap();
        let bucket = index.get(bucket_name).unwrap();
        let collection = bucket.get(collection_name).unwrap();

        // Verify old token are removed
        assert!(!collection.contains_key("initial"));
        assert!(!collection.contains_key("old"));

        // Verify new tokens are added
        assert!(collection.contains_key("new"));
        assert!(collection.contains_key("updated"));
        assert!(collection.contains_key("content"));

        // Verify the document ID is associated with new tokens
        assert!(collection.get("new").unwrap().contains(&doc_id.to_string()));
        assert!(collection
            .get("updated")
            .unwrap()
            .contains(&doc_id.to_string()));
        assert!(collection
            .get("content")
            .unwrap()
            .contains(&doc_id.to_string()));

        // Verify no other unexpected tokens
        assert_eq!(collection.len(), 3);
    }

    #[test]
    fn test_index_single_document() {
        let storage = MockStorage::new();
        let engine = StdSearchEngine::new();
        let bucket_name = "test_bucket";
        let collection_name = "test_collection";
        let doc_id = "test_doc";

        engine
            .index(
                &storage,
                bucket_name,
                collection_name,
                doc_id,
                "test content",
            )
            .unwrap();

        let results = engine
            .search(bucket_name, collection_name, "content")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], doc_id);
    }

    #[test]
    fn test_search_non_existent_items() {
        let engine = StdSearchEngine::new();
        let storage = MockStorage::new();
        let bucket_name = "test_bucket";
        let collection_name = "test_collection";
        let doc_id = "test_doc";
        let content = "content";

        engine
            .index(&storage, bucket_name, collection_name, doc_id, content)
            .unwrap();

        let result = engine.search(bucket_name, collection_name, content);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], doc_id);

        let result = engine.search(bucket_name, collection_name, "non existent");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        let result = engine.search(bucket_name, "non existent collection", "content");
        assert!(result.is_err_and(|err| err == StorageError::NotFound(EntityType::Collection)));

        let result = engine.search("non existent bucket", collection_name, "content");
        assert!(result.is_err_and(|err| err == StorageError::NotFound(EntityType::Bucket)));
    }

    #[test]
    fn test_index_non_existent_items() {
        let engine = StdSearchEngine::new();
        let storage = MockStorage::new();
        let bucket_name = "test_bucket";
        let collection_name = "test_collection";
        let doc_id = "test_doc";
        let content = "content";

        let result = engine.index(&storage, bucket_name, collection_name, doc_id, content);
        assert!(result.is_ok());

        let result = engine.index(
            &storage,
            bucket_name,
            &(collection_name.to_string() + "non existent"),
            doc_id,
            content,
        );
        assert!(result.is_ok());
    }
}
