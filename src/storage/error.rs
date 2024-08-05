use std::fmt;

#[derive(Debug)]
pub enum StorageError {
    BucketNotFound,
    CollectionNotFound,
    DocumentNotFound,
    OperationFailed(String),
    SerializationError(String),
    DeserializationError(String),
    IOError(std::io::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageError::BucketNotFound => write!(f, "Bucket not found"),
            StorageError::CollectionNotFound => write!(f, "Collection not found"),
            StorageError::DocumentNotFound => write!(f, "Document not found"),
            StorageError::OperationFailed(msg) => write!(f, "Operation failed: {}", msg),
            StorageError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            StorageError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            StorageError::IOError(err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::IOError(err)
    }
}

impl From<flexbuffers::DeserializationError> for StorageError {
    fn from(err: flexbuffers::DeserializationError) -> Self {
        StorageError::DeserializationError(err.to_string())
    }
}
