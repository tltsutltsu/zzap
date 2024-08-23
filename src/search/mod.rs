mod btree;
mod dash;
mod dash2;
mod std;

pub use {
    btree::BTreeSearchEngine, dash::DashSearchEngine, dash2::Dash2SearchEngine,
    std::StdSearchEngine,
};

use crate::storage::StorageOperations;
use crate::{
    lang,
    storage::{Storage, StorageError},
};

pub trait SearchEngine {
    fn initialize(&self, storage: &Storage) -> Result<(), StorageError> {
        let store = storage
            .store
            .read()
            .map_err(|_| StorageError::PoisonError)?;
        for bucket_ref in store.iter() {
            let bucket_name = bucket_ref.key();
            let bucket = bucket_ref.value();
            for collection_ref in bucket.collections.iter() {
                let collection_name = collection_ref.key();
                let collection = collection_ref.value();
                for document_ref in collection.documents.iter() {
                    let document_id = document_ref.key();
                    let document = document_ref.value();
                    self.index(
                        storage,
                        bucket_name,
                        collection_name,
                        document_id,
                        &document.content,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn index(
        &self,
        storage: &Storage,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
        content: &str,
    ) -> Result<(), StorageError>;

    fn search(
        &self,
        bucket_name: &str,
        collection_name: &str,
        query: &str,
    ) -> Result<Vec<String>, StorageError>;

    fn remove_from_index(
        &self,
        storage: &Storage,
        bucket_name: &str,
        collection_name: &str,
        id: &str,
    ) -> Result<(), StorageError>;

    fn batch_index(
        &self,
        storage: &Storage,
        bucket_name: &str,
        collection_name: &str,
        docs: Vec<(String, String)>,
    ) -> Result<(), StorageError> {
        for (id, content) in docs {
            self.index(storage, bucket_name, collection_name, &id, &content)?;
        }

        Ok(())
    }
}
