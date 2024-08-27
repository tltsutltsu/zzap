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
// It is a map of bucket+collection+ token -> document ids.

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
        storage: &dyn StorageOperations,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
        content: &str,
    ) -> Result<(), crate::storage::StorageError> {
        let index_cleanup_result =
            self.remove_from_index(storage, bucket_name, collection_name, id);

        if let Err(e) = index_cleanup_result
            && !e.is_not_found()
        {
            return Err(e);
        }

        let mut content = content.to_string();
        let tokens = lang::tokenize_iter(&mut content);

        let mut unlocked_index = self.index.write().unwrap();

        for token in tokens {
            let key = generate_key(bucket_name, collection_name, &token);
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
        storage: &dyn StorageOperations,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
    ) -> Result<(), crate::storage::StorageError> {
        let content = storage.get_document(bucket_name, collection_name, id)?;
        let tokens = lang::tokenize(&content.content);

        let mut unlocked_index = self.index.write().unwrap();

        for token in tokens {
            let key = generate_key(bucket_name, collection_name, &token);
            let entry = unlocked_index.get_mut(&key);
            if entry.is_none() {
                continue;
            }
            if let Some(set) = entry {
                set.remove(id);
                if set.is_empty() {
                    unlocked_index.remove(&key);
                }
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

        let mut results: HashSet<String> = HashSet::new();

        let reader = self.index.read().unwrap();

        for token in tokens {
            let key = generate_key(bucket_name, collection_name, &token);
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

fn generate_key(bucket_name: &str, collection_name: &str, token: &str) -> String {
    format!("{bucket_name}~ZZAP~{collection_name}~ZZAP~{token}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{mock::MockStorage, Document};

    #[test]
    fn test_index_cleanups() {
        let engine = BTreeSearchEngine::new();
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
                "initial content old",
            )
            .unwrap();

        storage
            .add_document(
                bucket_name,
                collection_name,
                Document::new(doc_id, "initial content old"),
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

        storage
            .add_document(
                bucket_name,
                collection_name,
                Document::new(doc_id, "new updated content"),
            )
            .unwrap();

        // Check the index state
        let index = engine.index.read().unwrap();

        // Verify old token are removed
        assert!(!index.contains_key(&generate_key(bucket_name, collection_name, "initial")));
        assert!(!index.contains_key(&generate_key(bucket_name, collection_name, "old")));

        // Verify new tokens are added
        assert!(index.contains_key(&generate_key(bucket_name, collection_name, "new")));
        assert!(index.contains_key(&generate_key(bucket_name, collection_name, "updated")));
        assert!(index.contains_key(&generate_key(bucket_name, collection_name, "content")));

        // Verify the document ID is associated with new tokens
        assert!(index
            .get(&generate_key(bucket_name, collection_name, "new"))
            .unwrap()
            .contains(&doc_id.to_string()));
        assert!(index
            .get(&generate_key(bucket_name, collection_name, "updated"))
            .unwrap()
            .contains(&doc_id.to_string()));
        assert!(index
            .get(&generate_key(bucket_name, collection_name, "content"))
            .unwrap()
            .contains(&doc_id.to_string()));

        // Verify no other unexpected tokens
        assert_eq!(index.len(), 3);
    }

    #[test]
    fn test_index_single_document() {
        let storage = MockStorage::new();
        let engine = BTreeSearchEngine::new();
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
        let engine = BTreeSearchEngine::new();
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
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        let result = engine.search("non existent bucket", collection_name, "content");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_index_non_existent_items() {
        let engine = BTreeSearchEngine::new();
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
