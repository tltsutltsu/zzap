use crate::protocol::message::{DecodingError, Message};
use crate::server::handler::HandleError;

#[derive(Debug, PartialEq)]
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
            }
            Response::BulkString(content) => {
                let mut bytes = format!("${}\n", content.len()).into_bytes();
                bytes.extend_from_slice(content.as_bytes());
                bytes.push(b'\n');
                bytes
            }
            Response::Array(items) => {
                let mut bytes = format!("{}\n", items.len()).into_bytes();
                for item in items {
                    bytes.extend_from_slice(item.as_bytes());
                    bytes.push(b'\n');
                }
                bytes
            }
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
            }
            Some(line) if line.starts_with("$") => {
                if line == "$-1" {
                    Ok(Response::BulkString(String::new())) // Represent null bulk string as empty string
                } else {
                    let content = lines.next().unwrap_or("").to_string();
                    Ok(Response::BulkString(content))
                }
            }
            Some(line) => {
                if let Ok(count) = line.parse::<usize>() {
                    let items: Vec<String> = lines.take(count).map(|s| s.to_string()).collect();
                    Ok(Response::Array(items))
                } else {
                    Err(DecodingError::InvalidResponseFormat)
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encryption::EncryptionError, storage::EntityType, storage::StorageError};

    #[test]
    fn test_from_decoding_error() {
        let error = DecodingError::InvalidRequest("Invalid command".to_string());
        let response = Response::from_decoding_error(error);
        assert_eq!(response, Response::Error("Invalid command".to_string()));
    }

    #[test]
    fn test_from_handle_error() {
        let error = HandleError::Encryption(EncryptionError::DecryptionFailed("test".to_string()));
        let response = Response::from_handle_error(error);
        assert_eq!(
            response,
            Response::Error("Encryption error: Decryption failed: test".to_string())
        );

        let error = HandleError::Storage(StorageError::NotFound(EntityType::Bucket));
        let response = Response::from_handle_error(error);
        assert_eq!(
            response,
            Response::Error("Storage error: bucket not found".to_string())
        );
    }

    #[test]
    fn test_response_success_encode() {
        let response = Response::Success;
        assert_eq!(response.to_bytes(), b"+OK\n");
    }

    #[test]
    fn test_response_success_decode() {
        let response = Response::from_bytes(b"+OK\n").unwrap();
        assert_eq!(response, Response::Success);
    }

    #[test]
    fn test_response_error_encode_simple() {
        let response = Response::Error("Invalid command".to_string());
        assert_eq!(response.to_bytes(), b"-ERR Invalid command\n");
    }

    #[test]
    fn test_response_error_encode_empty() {
        let response = Response::Error("".to_string());
        assert_eq!(response.to_bytes(), b"-ERR \n");
    }

    #[test]
    fn test_response_error_encode_long() {
        let too_long_message = "Too long error message".repeat(100);
        let response = Response::Error(too_long_message.clone());
        assert_eq!(
            response.to_bytes(),
            format!("-ERR {}\n", too_long_message).as_bytes()
        );
    }

    #[test]
    fn test_response_error_decode_simple() {
        let response = Response::from_bytes(b"-ERR Invalid command\n").unwrap();
        assert_eq!(response, Response::Error("Invalid command".to_string()));
    }

    #[test]
    fn test_response_error_decode_empty() {
        let response = Response::from_bytes(b"-ERR \n").unwrap();
        assert_eq!(response, Response::Error("".to_string()));
    }

    #[test]
    fn test_response_error_decode_long() {
        let too_long_message = "Too long error message".repeat(100);
        let response =
            Response::from_bytes(format!("-ERR {}\n", too_long_message).as_bytes()).unwrap();
        assert_eq!(response, Response::Error(too_long_message));
    }

    #[test]
    fn test_response_bulk_string_encode() {
        let response = Response::BulkString("Hello, world!".to_string());
        assert_eq!(response.to_bytes(), b"$13\nHello, world!\n");
    }

    #[test]
    fn test_response_bulk_string_decode() {
        let response = Response::from_bytes(b"$13\nHello, world!\n").unwrap();
        assert_eq!(response, Response::BulkString("Hello, world!".to_string()));
    }

    #[test]
    fn test_response_array_encode() {
        let response = Response::Array(vec!["Hello".to_string(), "world".to_string()]);
        assert_eq!(response.to_bytes(), b"2\nHello\nworld\n");
    }

    #[test]
    fn test_response_array_decode() {
        let response = Response::from_bytes(b"2\nHello\nworld\n").unwrap();
        assert_eq!(
            response,
            Response::Array(vec!["Hello".to_string(), "world".to_string()])
        );
    }

    #[test]
    fn test_response_array_decode_empty() {
        let response = Response::from_bytes(b"0\n").unwrap();
        assert_eq!(response, Response::Array(vec![]));
    }

    #[test]
    fn test_response_array_encode_empty() {
        let response = Response::Array(vec![]);
        assert_eq!(response.to_bytes(), b"0\n");
    }

    #[test]
    fn test_response_bulk_string_encode_empty() {
        let response = Response::BulkString(String::new());
        assert_eq!(response.to_bytes(), b"$0\n\n");
    }

    #[test]
    fn test_response_bulk_string_encode_spaces() {
        let response = Response::BulkString(" ".to_string());
        assert_eq!(response.to_bytes(), b"$1\n \n");
    }

    #[test]
    fn test_response_bulk_string_decode_empty() {
        let response = Response::from_bytes(b"$-1\n").unwrap();
        assert_eq!(response, Response::BulkString(String::new()));
    }

    // TODO: these characters are now implemented incorrectly, and they would break the protocol
    // The test is now passing as a result of the incorrect implementation, and it should be fixed in protocol design first
    #[test]
    fn test_response_array_encode_special_characters() {
        let response = Response::Array(vec!["Hello\nworld".to_string()]);
        assert_eq!(response.to_bytes(), b"1\nHello\nworld\n");
    }

    #[test]
    fn test_response_empty_decode() {
        let response = Response::from_bytes(b"");
        assert_eq!(response, Err(DecodingError::EmptyResponse));
    }

    #[test]
    fn test_response_invalid_format_decode() {
        // does not start with + (success), - (error), $ (bulk string) or number (array)
        let response = Response::from_bytes(b"invalid format");
        assert_eq!(response, Err(DecodingError::InvalidResponseFormat));
    }
}
