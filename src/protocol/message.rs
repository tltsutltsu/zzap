#[derive(Debug, derive_more::Display, PartialEq, Eq)]
pub enum DecodingError {
    InvalidRequest(String),
    InvalidResponseFormat,
    EmptyResponse,
}

pub trait Message {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, DecodingError>
    where
        Self: Sized;
}