use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{request::Request, response::Response};
use crate::storage::{Document, Storage, StorageOperations};
use crate::encryption::{Encryption, MockEncryptor};
use crate::search::SearchEngine;

pub async fn handle_request(
    request: Request,
    storage: &Arc<RwLock<Storage>>,
    encryption: &MockEncryptor,
    search_engine: &Arc<RwLock<SearchEngine>>
) -> Result<Response, Box<dyn std::error::Error>> {
    match request {
        Request::Set { bucket, collection, id, content, key } => {
            let content = match key {
                Some(key) => encryption.encrypt(&content, &key)?,
                None => content,
            };
            let document = Document::new(&id, &content);
            let storage = storage.read().await;
            let search_engine = search_engine.read().await;
            storage.add_document(&bucket, &collection, document).await?;
            search_engine.index(&bucket, &collection, &id, &content).await?;
            Ok(Response::Success)
        }

        Request::Search { bucket, collection, query } => {
            let search_engine = search_engine.read().await;
            let results = search_engine.search(&bucket, &collection, &query).await?;
            Ok(Response::Array(results))
        }

        Request::Get { bucket, collection, id, key } => {
            let storage = storage.read().await;
            let encrypted_document = storage.get_document(&bucket, &collection, &id).await?;
            Ok(Response::BulkString(match key {
                Some(key) => encryption.decrypt(&encrypted_document.content, &key)?,
                None => encrypted_document.content,
            }))
        }

        Request::Remove { bucket, collection, id } => {
            let storage = storage.read().await;
            let search_engine = search_engine.read().await;
            storage.delete_document(&bucket, &collection, &id).await?;
            search_engine.remove_from_index(&bucket, &collection, &id).await?;
            Ok(Response::Success)
        }

        Request::Ping => Ok(Response::Success),
    }
}