#![feature(try_trait_v2)]
#![feature(option_get_or_insert_default)]
#![feature(try_find)]
#![feature(trait_upcasting)]
#![feature(async_fn_track_caller)]
#![feature(let_chains)]

use crate::{encryption::Encryption, search::SearchEngine, storage::StorageOperations};
use std::net::SocketAddr;

pub mod encryption;
mod lang;
pub mod protocol;
pub mod search;
pub mod server;
pub mod storage;

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let mut storage = storage::Storage::new("storage.db");
    let encryption = encryption::MockEncryptor::new();
    let search_engine = search::StdSearchEngine::new();

    storage.initialize()?;
    search_engine.initialize(&storage)?;

    let addr = SocketAddr::from(([0, 0, 0, 0], 13413));
    let server = server::ZzapServer::new(addr, storage, encryption, search_engine);

    println!("zzap server starting on {}", addr);

    server.run().await?;

    Ok(())
}
