use crate::encryption::{Encryption, EncryptionError, MockEncryptor};
use crate::protocol::{request::Request, response::Response};
use crate::search::SearchEngine;
use crate::storage::{Document, Storage, StorageError, StorageOperations};
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) enum HandleError {
    Encryption(EncryptionError),
    Storage(StorageError),
}

impl fmt::Display for HandleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HandleError::Encryption(e) => write!(f, "Encryption error: {}", e),
            HandleError::Storage(e) => write!(f, "Storage error: {}", e),
        }
    }
}

pub async fn handle_request(
    request: Request,
    storage: &Arc<RwLock<Storage>>,
    encryption: &MockEncryptor,
    search_engine: &Arc<RwLock<SearchEngine>>,
) -> Result<Response, HandleError> {
    match request {
        Request::Set {
            bucket,
            collection,
            id,
            content,
            key,
        } => {
            let content = match key {
                Some(key) => encryption
                    .encrypt(&content, &key)
                    .map_err(HandleError::Encryption)?,
                None => content,
            };
            let document = Document::new(&id, &content);
            let storage = storage.read().await;
            let search_engine = search_engine.read().await;
            storage
                .add_document(&bucket, &collection, document)
                .await
                .map_err(HandleError::Storage)?;
            search_engine
                .index(&bucket, &collection, &id, &content)
                .await
                .map_err(HandleError::Storage)?;
            Ok(Response::Success)
        }

        Request::Search {
            bucket,
            collection,
            query,
        } => {
            let search_engine = search_engine.read().await;
            let results = search_engine
                .search(&bucket, &collection, &query)
                .await
                .map_err(HandleError::Storage)?;
            Ok(Response::Array(results))
        }

        Request::Get {
            bucket,
            collection,
            id,
            key,
        } => {
            let storage = storage.read().await;
            let encrypted_document = storage
                .get_document(&bucket, &collection, &id)
                .await
                .map_err(HandleError::Storage)?;
            Ok(Response::BulkString(match key {
                Some(key) => encryption
                    .decrypt(&encrypted_document.content, &key)
                    .map_err(HandleError::Encryption)?,
                None => encrypted_document.content,
            }))
        }

        Request::Remove {
            bucket,
            collection,
            id,
        } => {
            let storage = storage.read().await;
            let search_engine = search_engine.read().await;
            storage
                .delete_document(&bucket, &collection, &id)
                .await
                .map_err(HandleError::Storage)?;
            search_engine
                .remove_from_index(&bucket, &collection, &id)
                .await
                .map_err(HandleError::Storage)?;
            Ok(Response::Success)
        }

        Request::Ping => Ok(Response::Success),
    }
}
