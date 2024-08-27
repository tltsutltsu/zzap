use std::sync::Arc;

use super::handler::handle_request;
use crate::encryption::MockEncryptor;
use crate::protocol::{Message, Request, Response};
use crate::search::StdSearchEngine;
use crate::storage::Storage;
use std::sync::RwLock as SyncRwLock;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::task;

pub struct Connection {
    stream: Arc<AsyncRwLock<TcpStream>>,
    storage: Arc<SyncRwLock<Storage>>,
    encryption: Arc<MockEncryptor>,
    search_engine: Arc<SyncRwLock<StdSearchEngine>>,
}

impl Connection {
    pub fn new(
        stream: Arc<AsyncRwLock<TcpStream>>,
        storage: Arc<SyncRwLock<Storage>>,
        encryption: Arc<MockEncryptor>,
        search_engine: Arc<SyncRwLock<StdSearchEngine>>,
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

            // TODO: double spawn?
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
                    &*encryption_clone,
                    &search_engine_clone,
                )
                .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        eprintln!("Error handling request: {}", e);
                        Response::from_handle_error(e)
                    }
                };

                #[cfg(debug_assertions)]
                println!(
                    "Sending response: {}",
                    String::from_utf8_lossy(&response.to_bytes())
                );

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Message, Request, Response};
    use std::net::SocketAddr;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::time::{sleep, Duration};

    const DEFAULT_STORAGE_PATH: &str = "test.db";

    async fn setup_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let storage = Arc::new(SyncRwLock::new(Storage::new(DEFAULT_STORAGE_PATH)));
        let encryption = Arc::new(MockEncryptor);
        let search_engine = Arc::new(SyncRwLock::new(StdSearchEngine::new()));

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = Arc::new(AsyncRwLock::new(stream));
            let mut connection = Connection::new(stream, storage, encryption, search_engine);
            connection.handle().await.unwrap();
        });

        addr
    }

    // TODO: change to common function to read response from stream
    async fn command(stream: &mut TcpStream, command: Request, expected: Response) {
        stream.write_all(&command.to_bytes()).await.unwrap();
        let mut buffer = Vec::new();
        let mut reader = tokio::io::BufReader::new(stream);
        reader.read_until(b'\n', &mut buffer).await.unwrap();
        let response = String::from_utf8(buffer).unwrap();

        // if response is number, parse it as int N and read N lines
        if let Ok(n) = response.trim().parse::<usize>() {
            let mut lines: Vec<String> = vec![response.clone()];
            for _ in 0..n {
                let mut buffer = Vec::new();
                reader.read_until(b'\n', &mut buffer).await.unwrap();
                lines.push(String::from_utf8(buffer).unwrap());
            }
            let response = format!("{}{}\n", response, lines.join(""));
            let response = Response::from_bytes(&response.as_bytes()).unwrap();

            assert_eq!(response, expected);

            return;
        }

        // if response starts with $, then we need to continue reading until we get the full string
        if response.starts_with("$") {
            let start = response.clone();
            let mut buffer = Vec::new();
            reader.read_until(b'\n', &mut buffer).await.unwrap();
            let response = String::from_utf8(buffer).unwrap();
            let response = format!("{}{}\n", start, response);
            let response = Response::from_bytes(&response.as_bytes()).unwrap();
            assert_eq!(response, expected);
            return;
        }

        let response = Response::from_bytes(&response.as_bytes()).unwrap();
        assert_eq!(response, expected);
    }

    async fn command_string(stream: &mut TcpStream, command: String, expected: Response) {
        let mut buffer = command.as_bytes().to_vec();
        buffer.push(b'\n');
        stream.write_all(&buffer).await.unwrap();
        let mut buffer = [0; 1024];
        let n = stream.read(&mut buffer).await.unwrap();
        let response = Response::from_bytes(&buffer[..n]).unwrap();
        assert_eq!(response, expected);
    }
    #[tokio::test]
    async fn test_set_and_get() {
        let addr = setup_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Test SET request
        let set_request = Request::Set {
            bucket: "b".into(),
            collection: "c".into(),
            id: "first_record".into(),
            content: "value1".into(),
            key: None,
        };

        command(&mut stream, set_request, Response::Success).await;

        // Test GET request
        let get_request = Request::Get {
            bucket: "b".into(),
            collection: "c".into(),
            id: "first_record".into(),
            key: None,
        };

        command(
            &mut stream,
            get_request,
            Response::BulkString("value1".into()),
        )
        .await;

        stream.shutdown().await.unwrap();
    }

    // tests passing error from request parser
    #[tokio::test]
    async fn test_invalid_request() {
        let addr = setup_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        command_string(
            &mut stream,
            "INVALID bucket collection id content".into(),
            Response::Error("Invalid command".into()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_large_payload() {
        const PAYLOAD_SIZE: usize = 10_000_000;
        let addr = setup_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Create a large payload
        let large_value = String::from_utf8(vec![b'a'; PAYLOAD_SIZE]).unwrap();
        let set_request = Request::Set {
            bucket: "b".into(),
            collection: "c".into(),
            id: "first_record".into(),
            content: large_value.clone(),
            key: None,
        };

        command(&mut stream, set_request, Response::Success).await;

        // Retrieve the large payload
        let get_request = Request::Get {
            bucket: "b".into(),
            collection: "c".into(),
            id: "first_record".into(),
            key: None,
        };

        command(&mut stream, get_request, Response::BulkString(large_value)).await;

        stream.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_client_do_not_listen_for_response() {
        let addr = setup_server().await;

        // Client
        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Send a valid request
        let set_request = Request::Set {
            bucket: "b".into(),
            collection: "c".into(),
            id: "first_record".into(),
            content: "value1".into(),
            key: None,
        };
        stream.write_all(&set_request.to_bytes()).await.unwrap();

        // Do not read the response

        // Simulate unexpected client disconnect
        drop(stream);

        // Give the server some time to process the disconnect
        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_client_disconnect_mid_command() {
        let addr = setup_server().await;

        // Client
        let mut stream = TcpStream::connect(addr).await.unwrap();

        let set_request = "SET b c fir";
        stream.write_all(&set_request.as_bytes()).await.unwrap();

        // Do not read the response

        // Simulate unexpected client disconnect
        drop(stream);

        // Give the server some time to process the disconnect
        sleep(Duration::from_millis(100)).await;
    }

    // tests passing error from handler
    #[tokio::test]
    async fn test_nonexistent_bucket() {
        let addr = setup_server().await;

        // Client
        let mut stream = TcpStream::connect(addr).await.unwrap();

        command(
            &mut stream,
            Request::Get {
                bucket: "non".into(),
                collection: "existent".into(),
                id: "1".into(),
                key: None,
            },
            Response::Error("Storage error: bucket not found".into()),
        )
        .await;
    }
}
