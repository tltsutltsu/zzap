use std::net::SocketAddr;
use tokio;

use crate::{encryption::Encryption, storage::StorageOperations};

mod server;
mod protocol;
mod storage;
mod encryption;
mod search;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = storage::Storage::new("storage.db");
    let encryption = encryption::MockEncryptor::new();
    let search_engine = search::SearchEngine::new();

    storage.initialize().await?;
    search_engine.initialize(&storage).await?;

    let addr = SocketAddr::from(([127, 0, 0, 1], 13413));
    let server = server::ZzapServer::new(addr, storage, encryption, search_engine);

    println!("zzap server starting on {}", addr);

    server.run().await?;

    Ok(())
}