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
        // Implement connection handling logic here
        // This would typically involve reading from the stream,
        // parsing the request, and writing the response

        loop {
            // Read stream until newline
            let mut buffer = Vec::new();
            let mut stream = self.stream.write().await;
            let mut reader = tokio::io::BufReader::new(&mut *stream);
            reader.read_until(b'\n', &mut buffer).await?;
            drop(stream);

            let req_str = String::from_utf8(buffer.clone())?;
            #[cfg(debug_assertions)]
            println!("Received request: {}", req_str);

            let request = Request::from_bytes(&buffer);

            if let Err(e) = request {
                eprintln!("Error parsing request: {}", e);
                let response = Response::from_error(e);
                let mut stream = self.stream.write().await;
                stream.write_all(&response.to_bytes()).await?;

                continue;
            }

            let response = handle_request(
                request.unwrap(),
                &self.storage,
                &self.encryption,
                &self.search_engine
            ).await?;

            #[cfg(debug_assertions)]
            println!("Sending response: {}", String::from_utf8(response.to_bytes())?);

            let mut stream = self.stream.write().await;
            stream.write_all(&response.to_bytes()).await?;

            // Break the loop if needed (e.g., client disconnects)
            if buffer.is_empty() {
                break;
            }
        }

        Ok(())
    }
}
