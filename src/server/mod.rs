mod connection;
pub mod handler;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use crate::storage::Storage;
use crate::encryption::MockEncryptor;
use crate::search::SearchEngine;

pub struct ZzapServer {
    addr: SocketAddr,
    storage: Arc<RwLock<Storage>>,
    encryption: Arc<MockEncryptor>,
    search_engine: Arc<RwLock<SearchEngine>>,
}

impl ZzapServer {
    pub fn new(addr: SocketAddr, storage: Storage, encryption: MockEncryptor, search_engine: SearchEngine) -> Self {
        Self {
            addr,
            storage: Arc::new(RwLock::new(storage)),
            encryption: Arc::new(encryption),
            search_engine: Arc::new(RwLock::new(search_engine)),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.addr).await?;

        loop {
            let (socket, _) = listener.accept().await?;

            let socket = Arc::new(RwLock::new(socket));
            let storage = self.storage.clone();
            let encryption = self.encryption.clone();
            let search_engine = self.search_engine.clone();

            let mut conn = connection::Connection::new(socket, storage, encryption, search_engine);
            
            tokio::spawn(async move {
                if let Err(e) = conn.handle().await {
                    eprintln!("Error handling connection: {}", e);
                }
            });
        }
    }
}