use std::sync::Arc;

use crate::encryption::MockEncryptor;
use crate::protocol::message::Message;
use crate::protocol::request::Request;
use crate::protocol::response::Response;
use crate::search::SearchEngine;
use crate::storage::Storage;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::task;

use super::handler::handle_request;

pub struct Connection {
    stream: Arc<RwLock<TcpStream>>,
    storage: Arc<RwLock<Storage>>,
    encryption: Arc<MockEncryptor>,
    search_engine: Arc<RwLock<SearchEngine>>,
}

impl Connection {
    pub fn new(
        stream: Arc<RwLock<TcpStream>>,
        storage: Arc<RwLock<Storage>>,
        encryption: Arc<MockEncryptor>,
        search_engine: Arc<RwLock<SearchEngine>>,
    ) -> Self {
        Self {
            stream,
            storage,
            encryption,
            search_engine,
        }
    }

    pub async fn handle(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let stream_clone = self.stream.clone();
            let storage_clone = self.storage.clone();
            let encryption_clone = self.encryption.clone();
            let search_engine_clone = self.search_engine.clone();

            let handle = task::spawn(async move {
                let mut buffer = Vec::new();
                let mut stream = stream_clone.write().await;
                let mut reader = tokio::io::BufReader::new(&mut *stream);
                if let Err(e) = reader.read_until(b'\n', &mut buffer).await {
                    eprintln!("Error reading from stream: {}", e);
                    return;
                }
                drop(stream);

                let req_str = String::from_utf8_lossy(&buffer);
                #[cfg(debug_assertions)]
                println!("Received request: {}", req_str);

                let request = match Request::from_bytes(&buffer) {
                    Ok(req) => req,
                    Err(e) => {
                        eprintln!("Error parsing request: {}", e);
                        let response = Response::from_decoding_error(e);
                        let mut stream = stream_clone.write().await;
                        if let Err(e) = stream.write_all(&response.to_bytes()).await {
                            eprintln!("Error writing response: {}", e);
                        }
                        return;
                    }
                };

                let response = match handle_request(
                    request,
                    &storage_clone,
                    &encryption_clone,
                    &search_engine_clone
                ).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        eprintln!("Error handling request: {}", e);
                        Response::from_handle_error(e)
                    }
                };

                #[cfg(debug_assertions)]
                println!("Sending response: {}", String::from_utf8_lossy(&response.to_bytes()));

                let mut stream = stream_clone.write().await;
                if let Err(e) = stream.write_all(&response.to_bytes()).await {
                    eprintln!("Error writing response: {}", e);
                }
            });

            // Await the task to ensure any errors are propagated
            handle.await?;

            // Break the loop if needed (e.g., client disconnects)
            if self.stream.read().await.peek(&mut [0; 1]).await? == 0 {
                break;
            }
        }

        Ok(())
    }
}
