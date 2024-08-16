use crate::server::handler::HandleError;

use super::message::{DecodingError, Message};

pub enum Response {
    Success,
    Error(String),
    BulkString(String),
    Array(Vec<String>),
}

impl Message for Response {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Response::Success => b"+OK\n".to_vec(),
            Response::Error(message) => {
                let mut bytes = b"-ERR ".to_vec();
                bytes.extend_from_slice(message.as_bytes());
                bytes.push(b'\n');
                bytes
            },
            Response::BulkString(content) => {
                let mut bytes = format!("${}\n", content.len()).into_bytes();
                bytes.extend_from_slice(content.as_bytes());
                bytes.push(b'\n');
                bytes
            },
            Response::Array(items) => {
                let mut bytes = format!("{}\n", items.len()).into_bytes();
                for item in items {
                    bytes.extend_from_slice(item.as_bytes());
                    bytes.push(b'\n');
                }
                bytes
            },
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, DecodingError> {
        let input = String::from_utf8_lossy(bytes);
        let mut lines = input.lines();

        match lines.next() {
            Some(line) if line.starts_with("+OK") => Ok(Response::Success),
            Some(line) if line.starts_with("-ERR") => {
                let error_message = line.trim_start_matches("-ERR ").to_string();
                Ok(Response::Error(error_message))
            },
            Some(line) if line.starts_with("$") => {
                if line == "$-1" {
                    Ok(Response::BulkString(String::new())) // Represent null bulk string as empty string
                } else {
                    let content = lines.next().unwrap_or("").to_string();
                    Ok(Response::BulkString(content))
                }
            },
            Some(line) => {
                if let Ok(count) = line.parse::<usize>() {
                    let items: Vec<String> = lines.take(count).map(|s| s.to_string()).collect();
                    Ok(Response::Array(items))
                } else {
                    Err(DecodingError::InvalidResponseFormat)
                }
            },
            None => Err(DecodingError::EmptyResponse),
        }
    }
}

impl Response {
    pub fn from_decoding_error(error: DecodingError) -> Self {
        Response::Error(error.to_string())
    }

    pub fn from_handle_error(error: HandleError) -> Self {
        Response::Error(error.to_string())
    }
}
