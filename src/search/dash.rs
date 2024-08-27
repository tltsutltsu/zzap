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

        let tokens = lang::tokenize(content);

        let bucket_plus_collection = generate_key(bucket_name, collection_name);
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
        storage: &dyn StorageOperations,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
    ) -> Result<(), crate::storage::StorageError> {
        let content = storage.get_document(bucket_name, collection_name, id)?;
        let tokens = lang::tokenize(&content.content);

        let bucket_plus_collection = generate_key(bucket_name, collection_name);
        let collection = self
            .index
            .entry(bucket_plus_collection)
            .or_insert_with(DashMap::new);

        for token in tokens {
            let mut entry = collection.entry(token.clone()).or_insert_with(HashSet::new);
            entry.remove(id);

            if entry.is_empty() {
                drop(entry);
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

        let bucket_plus_collection = generate_key(bucket_name, collection_name);
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

fn generate_key(bucket_name: &str, collection_name: &str) -> String {
    format!("{bucket_name}~ZZAP~{collection_name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{mock::MockStorage, Document};

    #[test]
    fn test_index_cleanups() {
        let engine = DashSearchEngine::new();
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
        let key = generate_key(bucket_name, collection_name);
        let collection = engine.index.get(&key).unwrap();

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
        let engine = DashSearchEngine::new();
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
        let engine = DashSearchEngine::new();
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
        let engine = DashSearchEngine::new();
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
