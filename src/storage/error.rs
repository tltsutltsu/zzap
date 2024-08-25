use std::fmt;

#[derive(Debug, PartialEq)]
pub enum EntityType {
    Bucket,
    Collection,
    Item,
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EntityType::Bucket => write!(f, "bucket"),
            EntityType::Collection => write!(f, "collection"),
            EntityType::Item => write!(f, "item"),
        }
    }
}

#[derive(Debug)]
pub enum StorageError {
    Locked(EntityType),
    NotFound(EntityType),
    OperationFailed(String),
    SerializationError(String),
    DeserializationError(String),
    IOError(std::io::Error),
    PoisonError,
}

impl StorageError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, StorageError::NotFound(_))
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageError::Locked(place) => write!(f, "{} is locked", place),
            StorageError::NotFound(entity_type) => write!(f, "{} not found", entity_type),
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
            (NotFound(a), NotFound(b)) => a == b,
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
        assert_eq!(
            StorageError::NotFound(EntityType::Bucket),
            StorageError::NotFound(EntityType::Bucket)
        );
        assert_eq!(
            StorageError::NotFound(EntityType::Collection),
            StorageError::NotFound(EntityType::Collection)
        );
        assert_eq!(
            StorageError::NotFound(EntityType::Item),
            StorageError::NotFound(EntityType::Item)
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
        assert_eq!(StorageError::PoisonError, StorageError::PoisonError);
        assert_eq!(
            StorageError::Locked(EntityType::Bucket),
            StorageError::Locked(EntityType::Bucket)
        );
        assert_eq!(
            StorageError::Locked(EntityType::Collection),
            StorageError::Locked(EntityType::Collection)
        );
        assert_eq!(
            StorageError::Locked(EntityType::Item),
            StorageError::Locked(EntityType::Item)
        );

        assert_ne!(
            StorageError::NotFound(EntityType::Bucket),
            StorageError::NotFound(EntityType::Collection)
        );
        assert_ne!(
            StorageError::NotFound(EntityType::Bucket),
            StorageError::NotFound(EntityType::Item)
        );
        assert_ne!(
            StorageError::NotFound(EntityType::Collection),
            StorageError::OperationFailed("test".to_string())
        );
        assert_ne!(
            StorageError::NotFound(EntityType::Bucket),
            StorageError::SerializationError("test".to_string())
        );
        assert_ne!(
            StorageError::NotFound(EntityType::Bucket),
            StorageError::DeserializationError("test".to_string())
        );
        assert_ne!(
            StorageError::NotFound(EntityType::Bucket),
            StorageError::PoisonError
        );
    }

    #[test]
    fn test_is_not_found() {
        assert!(StorageError::NotFound(EntityType::Bucket).is_not_found());
        assert!(StorageError::NotFound(EntityType::Collection).is_not_found());
        assert!(StorageError::NotFound(EntityType::Item).is_not_found());
        assert!(
            !StorageError::IOError(std::io::Error::new(std::io::ErrorKind::Other, "test"))
                .is_not_found()
        );

        assert!(!StorageError::Locked(EntityType::Bucket).is_not_found());
        assert!(!StorageError::Locked(EntityType::Collection).is_not_found());
        assert!(!StorageError::Locked(EntityType::Item).is_not_found());
    }

    #[test]
    fn test_display_lock_place() {
        assert_eq!(
            StorageError::Locked(EntityType::Bucket).to_string(),
            "bucket is locked"
        );
        assert_eq!(
            StorageError::Locked(EntityType::Collection).to_string(),
            "collection is locked"
        );
        assert_eq!(
            StorageError::Locked(EntityType::Item).to_string(),
            "item is locked"
        );
    }

    #[test]
    fn test_display_error() {
        assert_eq!(
            StorageError::NotFound(EntityType::Bucket).to_string(),
            "bucket not found"
        );
        assert_eq!(
            StorageError::NotFound(EntityType::Collection).to_string(),
            "collection not found"
        );
        assert_eq!(
            StorageError::NotFound(EntityType::Item).to_string(),
            "item not found"
        );
        assert_eq!(
            StorageError::OperationFailed("test".to_string()).to_string(),
            "Operation failed: test"
        );
        assert_eq!(
            StorageError::SerializationError("test".to_string()).to_string(),
            "Serialization error: test"
        );
        assert_eq!(
            StorageError::DeserializationError("test".to_string()).to_string(),
            "Deserialization error: test"
        );
        assert_eq!(
            StorageError::IOError(std::io::Error::new(std::io::ErrorKind::Other, "test"))
                .to_string(),
            "I/O error: test"
        );
        assert_eq!(StorageError::PoisonError.to_string(), "Poison error");
    }

    #[test]
    fn test_from_io_error() {
        let err = std::io::Error::new(std::io::ErrorKind::Other, "test");
        let err2 = std::io::Error::new(std::io::ErrorKind::Other, "test");
        assert_eq!(StorageError::from(err), StorageError::IOError(err2));
    }

    #[test]
    fn test_from_flexbuffers_deserialization_error() {
        let err = flexbuffers::DeserializationError::Serde("test".to_string());
        assert_eq!(
            StorageError::from(err),
            StorageError::DeserializationError("Serde Error: test".to_string())
        );
    }
}
