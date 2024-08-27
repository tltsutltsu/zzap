use crate::encryption::{Encryption, MockEncryptor};
use crate::protocol::{Message, Request, Response};
use crate::search::StdSearchEngine;
use crate::server::handler::{handle_request, HandleError};
use crate::storage::{EntityType, Storage, StorageError};
use std::sync::{Arc, RwLock};

#[track_caller]
async fn command_predicate(
    storage: &Arc<RwLock<Storage>>,
    encryptor: &MockEncryptor,
    search_engine: &Arc<RwLock<StdSearchEngine>>,
    command: &str,
    predicate: impl Fn(Result<Response, HandleError>) -> bool,
) {
    let request = Request::from_bytes(command.as_bytes()).unwrap();
    let result = handle_request(request, storage, encryptor, search_engine).await;

    assert!(predicate(result));
}

async fn command(
    storage: &Arc<RwLock<Storage>>,
    encryptor: &MockEncryptor,
    search_engine: &Arc<RwLock<StdSearchEngine>>,
    command: &str,
    expected: Result<Response, HandleError>,
) {
    let request = Request::from_bytes(command.as_bytes()).unwrap();
    let result = handle_request(request, storage, encryptor, search_engine).await;

    assert_eq!(result, expected);
}

#[tokio::test]
async fn simple() {
    let storage = Arc::new(RwLock::new(Storage::new("test.db")));
    let encryptor = MockEncryptor;
    let search_engine = Arc::new(RwLock::new(StdSearchEngine::new()));

    command(
        &storage,
        &encryptor,
        &search_engine,
        "PING",
        Ok(Response::Success),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SET default test_collection test_id 7:test123",
        Ok(Response::Success),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SEARCH default test_collection test123",
        Ok(Response::Array(vec!["test_id".to_string()])),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "GET default test_collection test_id",
        Ok(Response::BulkString("test123".to_string())),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "REMOVE default test_collection test_id",
        Ok(Response::Success),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "GET default test_collection test_id",
        Err(HandleError::Storage(StorageError::NotFound(
            EntityType::Bucket,
        ))),
    )
    .await;
}

#[tokio::test]
async fn index_cleans_properly() {
    let storage = Arc::new(RwLock::new(Storage::new("test.db")));
    let encryptor = MockEncryptor;
    let search_engine = Arc::new(RwLock::new(StdSearchEngine::new()));

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SET default articles 42 test_article",
        Ok(Response::Success),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SET default articles 42 other_word",
        Ok(Response::Success),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SEARCH default articles test_article",
        Ok(Response::Array(vec![])),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SEARCH default articles other_word",
        Ok(Response::Array(vec!["42".to_string()])),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "REMOVE default articles 42",
        Ok(Response::Success),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SEARCH default articles test_article",
        Ok(Response::Array(vec![])),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SEARCH default articles other_word",
        Ok(Response::Array(vec![])),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SET default articles 5 12:first second",
        Ok(Response::Success),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        "SET default articles 6 first",
        Ok(Response::Success),
    )
    .await;

    command_predicate(
        &storage,
        &encryptor,
        &search_engine,
        "SEARCH default articles first",
        |resp| {
            resp == Ok(Response::Array(vec!["5".to_string(), "6".to_string()]))
                || resp == Ok(Response::Array(vec!["6".to_string(), "5".to_string()]))
        },
    )
    .await;
}

#[tokio::test]
async fn with_encryption() {
    let storage = Arc::new(RwLock::new(Storage::new("test.db")));
    let encryptor = MockEncryptor;
    let search_engine = Arc::new(RwLock::new(StdSearchEngine::new()));

    let id = "1".to_string();
    let data = "test_article".to_string();
    let key = "42".to_string();
    let encrypted_data = encryptor.encrypt(&data, &key).unwrap();

    command(
        &storage,
        &encryptor,
        &search_engine,
        &format!("SET default articles {id} {encrypted_data}"),
        Ok(Response::Success),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        &format!("GET default articles {id}"),
        Ok(Response::BulkString(encrypted_data)),
    )
    .await;

    command(
        &storage,
        &encryptor,
        &search_engine,
        &format!("GET default articles {id} {key}"),
        Ok(Response::BulkString(data)),
    )
    .await;
}
