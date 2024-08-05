use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum EncryptionError {
    InvalidKey,
    EncryptionFailed,
    DecryptionFailed,
}

impl fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EncryptionError::InvalidKey => write!(f, "Invalid encryption key"),
            EncryptionError::EncryptionFailed => write!(f, "Encryption failed"),
            EncryptionError::DecryptionFailed => write!(f, "Decryption failed"),
        }
    }
}

impl Error for EncryptionError {}

pub trait Encryption {
    fn new() -> Self where Self: Sized;
    fn encrypt(&self, data: &str, key: &str) -> Result<String, EncryptionError>;
    fn decrypt(&self, data: &str, key: &str) -> Result<String, EncryptionError>;
}

pub struct MockEncryptor;

impl Encryption for MockEncryptor {
    fn new() -> Self {
        MockEncryptor
    }

    fn encrypt(&self, data: &str, key: &str) -> Result<String, EncryptionError> {
        if key.is_empty() {
            return Err(EncryptionError::InvalidKey);
        }

        // Simple mock encryption: reverse the string and append the key length
        let encrypted = format!("{}{}", data.chars().rev().collect::<String>(), key.len());
        Ok(encrypted)
    }

    fn decrypt(&self, data: &str, key: &str) -> Result<String, EncryptionError> {
        if key.is_empty() {
            return Err(EncryptionError::InvalidKey);
        }

        // Simple mock decryption: remove the key length and reverse the string
        let key_len_str = data.chars().rev().take(2).collect::<String>();
        let key_len = key_len_str.parse::<usize>().map_err(|_| EncryptionError::DecryptionFailed)?;

        if key_len != key.len() {
            return Err(EncryptionError::DecryptionFailed);
        }

        let decrypted = data.chars().rev().skip(2).collect::<String>();
        Ok(decrypted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_encryption() {
        let encryptor = MockEncryptor::new();
        let original = "Hello, World!";
        let key = "secret";

        let encrypted = encryptor.encrypt(original, key).unwrap();
        let decrypted = encryptor.decrypt(&encrypted, key).unwrap();

        assert_eq!(original, decrypted);
    }

    #[test]
    fn test_invalid_key() {
        let encryptor = MockEncryptor::new();
        let original = "Hello, World!";
        let key = "";

        assert!(matches!(
            encryptor.encrypt(original, key),
            Err(EncryptionError::InvalidKey)
        ));
    }

    #[test]
    fn test_decryption_failure() {
        let encryptor = MockEncryptor::new();
        let encrypted = "!dlroW ,olleH6";
        let wrong_key = "wrong";

        assert!(matches!(
            encryptor.decrypt(encrypted, wrong_key),
            Err(EncryptionError::DecryptionFailed)
        ));
    }
}
