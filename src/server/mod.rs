mod connection;
pub mod handler;

use crate::encryption::MockEncryptor;
use crate::search::StdSearchEngine;
use crate::search::SearchEngine;
use crate::storage::Storage;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock as SyncRwLock;
use tokio::net::TcpListener;
use tokio::sync::RwLock as AsyncRwLock;

pub struct ZzapServer {
    addr: SocketAddr,
    storage: Arc<SyncRwLock<Storage>>,
    encryption: Arc<MockEncryptor>,
    search_engine: Arc<SyncRwLock<StdSearchEngine>>,
}

impl ZzapServer {
    pub fn new(
        addr: SocketAddr,
        storage: Storage,
        encryption: MockEncryptor,
        search_engine: StdSearchEngine,
    ) -> Self {
        Self {
            addr,
            storage: Arc::new(SyncRwLock::new(storage)),
            encryption: Arc::new(encryption),
            search_engine: Arc::new(SyncRwLock::new(search_engine)),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.addr).await?;

        loop {
            let (socket, _) = listener.accept().await?;

            let socket = Arc::new(AsyncRwLock::new(socket));
            let storage = self.storage.clone();
            let encryption = self.encryption.clone();
            let search_engine = self.search_engine.clone();

            let mut conn = connection::Connection::new(socket, storage, encryption, search_engine);

            // TODO: double spawn?
            tokio::spawn(async move {
                if let Err(e) = conn.handle().await {
                    eprintln!("Error handling connection: {}", e);
                }
            });
        }
    }
}
