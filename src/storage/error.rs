use std::fmt;

#[derive(Debug, PartialEq)]
pub enum LockPlace {
    Bucket,
    Collection,
    Item,
}

impl fmt::Display for LockPlace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LockPlace::Bucket => write!(f, "bucket"),
            LockPlace::Collection => write!(f, "collection"),
            LockPlace::Item => write!(f, "item"),
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    Locked(LockPlace),
    BucketNotFound,
    CollectionNotFound,
    DocumentNotFound,
    OperationFailed(String),
    SerializationError(String),
    DeserializationError(String),
    IOError(std::io::Error),
    PoisonError,
}

impl StorageError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, StorageError::BucketNotFound)
            || matches!(self, StorageError::CollectionNotFound)
            || matches!(self, StorageError::DocumentNotFound)
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageError::Locked(place) => write!(f, "{} is locked", place),
            StorageError::BucketNotFound => write!(f, "Bucket not found"),
            StorageError::CollectionNotFound => write!(f, "Collection not found"),
            StorageError::DocumentNotFound => write!(f, "Document not found"),
            StorageError::OperationFailed(msg) => write!(f, "Operation failed: {}", msg),
            StorageError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            StorageError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            StorageError::IOError(err) => write!(f, "I/O error: {}", err),
            StorageError::PoisonError => write!(f, "Poison error"),
        }
    }
}

impl PartialEq for StorageError {
    fn eq(&self, other: &Self) -> bool {
        use StorageError::*;

        match (self, other) {
            (IOError(e), IOError(other_e)) => e.kind() == other_e.kind(),
            (Locked(a), Locked(b)) => a == b,
            (OperationFailed(a), OperationFailed(b)) => a == b,
            (SerializationError(a), SerializationError(b)) => a == b,
            (DeserializationError(a), DeserializationError(b)) => a == b,
            (BucketNotFound, BucketNotFound) => true,
            (CollectionNotFound, CollectionNotFound) => true,
            (DocumentNotFound, DocumentNotFound) => true,
            (PoisonError, PoisonError) => true,
            _ => false,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equality() {
        assert_eq!(StorageError::BucketNotFound, StorageError::BucketNotFound);
        assert_eq!(
            StorageError::CollectionNotFound,
            StorageError::CollectionNotFound
        );
        assert_eq!(
            StorageError::DocumentNotFound,
            StorageError::DocumentNotFound
        );
        assert_eq!(
            StorageError::IOError(std::io::Error::new(std::io::ErrorKind::Other, "test")),
            StorageError::IOError(std::io::Error::new(std::io::ErrorKind::Other, "test2"))
        );
        assert_eq!(
            StorageError::OperationFailed("test".to_string()),
            StorageError::OperationFailed("test".to_string())
        );
        assert_eq!(
            StorageError::SerializationError("test".to_string()),
            StorageError::SerializationError("test".to_string())
        );
        assert_eq!(
            StorageError::DeserializationError("test".to_string()),
            StorageError::DeserializationError("test".to_string())
        );
    }
}
