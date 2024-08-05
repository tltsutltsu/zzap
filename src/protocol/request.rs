use super::message::{DecodingError, Message};

#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    Ping,
    Set {
        bucket: String,
        collection: String,
        id: String,
        content: String,
        key: Option<String>,
    },
    Get {
        bucket: String,
        collection: String,
        id: String,
        key: Option<String>,
    },
    Search {
        bucket: String,
        collection: String,
        query: String,
    },
    Remove {
        bucket: String,
        collection: String,
        id: String,
    },
}

impl Message for Request {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Request::Ping => b"PING\n".to_vec(),
            Request::Set {
                bucket,
                collection,
                id,
                content,
                key,
            } => {
                let mut bytes = format!(
                    "SET {} {} {} {}:{}",
                    bucket,
                    collection,
                    id,
                    content.len(),
                    content
                )
                .into_bytes();
                if let Some(k) = key {
                    bytes.extend_from_slice(b" ");
                    bytes.extend_from_slice(k.as_bytes());
                }
                bytes.push(b'\n');
                bytes
            }
            Request::Get {
                bucket,
                collection,
                id,
                key,
            } => {
                let mut bytes = format!("GET {} {} {}", bucket, collection, id).into_bytes();
                if let Some(k) = key {
                    bytes.extend_from_slice(b" ");
                    bytes.extend_from_slice(k.as_bytes());
                }
                bytes.push(b'\n');
                bytes
            }
            Request::Search {
                bucket,
                collection,
                query,
            } => format!("SEARCH {} {} {}\n", bucket, collection, query).into_bytes(),
            Request::Remove {
                bucket,
                collection,
                id,
            } => format!("REMOVE {} {} {}\n", bucket, collection, id).into_bytes(),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, DecodingError> {
        let input = String::from_utf8_lossy(bytes);
        let parts = input.clone();
        let mut parts = parts.trim_end().split_whitespace();

        match parts.next() {
            Some("PING") => Ok(Request::Ping),
            Some("SET") => {
                let bucket = parts
                    .next()
                    .ok_or(DecodingError::InvalidRequest("Missing bucket".to_string()))?
                    .to_string();
                let collection = parts
                    .next()
                    .ok_or(DecodingError::InvalidRequest(
                        "Missing collection".to_string(),
                    ))?
                    .to_string();
                let id = parts
                    .next()
                    .ok_or(DecodingError::InvalidRequest("Missing id".to_string()))?
                    .to_string();

                let after_params = input
                    .replace("SET", "")
                    .replace(&format!("{} ", bucket), "")
                    .replace(&format!("{} ", collection), "")
                    .replace(&format!("{} ", id), "");

                let maybe_len = after_params.trim().find(':');
                let (content, key) = match maybe_len {
                    Some(len_pos) => {
                        // it is in form of "4:content [key]"
                        let len_pos = len_pos + 1;
                        let len = after_params[..len_pos].trim();
                        println!(
                            "SET, bucket: {}, collection: {}, id: {}, len: {}, mod_len: {}",
                            bucket,
                            collection,
                            id,
                            len,
                            after_params[..len_pos + 1].trim()
                        );
                        let len: Result<usize, _> = len.parse();

                        if let Err(_) = len {
                            return Err(DecodingError::InvalidRequest(
                                "Invalid content length".to_string(),
                            ));
                        }
                        let len = len.unwrap();

                        let content_end = len_pos + 1 + len;
                        if content_end > after_params.len() {
                            return Err(DecodingError::InvalidRequest(
                                "Content length exceeds input length".to_string(),
                            ));
                        }
                        let content = &after_params[len_pos + 1..content_end];
                        let key = after_params[content_end..].trim();
                        let key = if key.is_empty() {
                            None
                        } else {
                            Some(key.to_string())
                        };

                        (content.to_string(), key)
                    }
                    None => {
                        // it is in form of "content [key]"

                        let last_whitespace = after_params.rfind(|c: char| c.is_whitespace());

                        match last_whitespace {
                            Some(last_whitespace) => {
                                let content = after_params[..last_whitespace].trim();
                                let key = after_params[last_whitespace..].trim();

                                if content.is_empty() && !key.is_empty() {
                                    println!("content: '{}', key: '{}'", content, key);
                                    (key.to_string(), None)
                                } else if !content.is_empty() && key.is_empty() {
                                    println!("content: '{}', key: '{}'", content, key);
                                    (content.to_string(), None)
                                } else {
                                    println!("content: '{}', key: '{}'", content, key);
                                    (content.to_string(), Some(key.to_string()))
                                }
                            }
                            None => (after_params.to_string(), None),
                        }
                    }
                };

                Ok(Request::Set {
                    bucket,
                    collection,
                    id,
                    content: content.to_string(),
                    key,
                })
            }
            Some("GET") => {
                let bucket = parts.next().unwrap().to_string();
                let collection = parts.next().unwrap().to_string();
                let id = parts.next().unwrap().to_string();
                let key = parts.next().map(|s| s.to_string());

                Ok(Request::Get {
                    bucket,
                    collection,
                    id,
                    key,
                })
            }
            Some("SEARCH") => {
                let bucket = parts.next().unwrap().to_string();
                let collection = parts.next().unwrap().to_string();
                let query = parts.collect::<Vec<&str>>().join(" ");

                Ok(Request::Search {
                    bucket,
                    collection,
                    query,
                })
            }
            Some("REMOVE") => {
                let bucket = parts.next().unwrap().to_string();
                let collection = parts.next().unwrap().to_string();
                let id = parts.next().unwrap().to_string();

                Ok(Request::Remove {
                    bucket,
                    collection,
                    id,
                })
            }
            _ => Err(DecodingError::InvalidRequest("Invalid command".to_string())),
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn simple() {
        let request = Request::from_bytes(b"PING\n").unwrap();
        assert_eq!(request, Request::Ping);
    }

    #[test]
    fn test_set_command() {
        let one_mb_of_data = std::fs::read_to_string("assets/tests/1_MiB_of_data").unwrap();

        let very_long_key = "a".repeat(1000);

        let cases: Vec<(&str, Result<Request, DecodingError>)> = vec![
            // Basic functionality
            (
                "SET default users 1 4:test",
                Ok(Request::Set {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            (
                "SET myapp docs 123 13:Hello, World! mykey",
                Ok(Request::Set {
                    bucket: "myapp".into(),
                    collection: "docs".into(),
                    id: "123".into(),
                    content: "Hello, World!".into(),
                    key: Some("mykey".into()),
                }),
            ),
            (
                "SET default users 1 test",
                Ok(Request::Set {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            (
                "SET default users 1 username with spaces",
                Ok(Request::Set {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                    content: "username with".into(),
                    key: Some("spaces".into()),
                }),
            ),
            (
                "SET default users 1 username with %!/)!(#$)@*!( special characters",
                Ok(Request::Set {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                    content: "username with %!/)!(#$)@*!( special".into(),
                    key: Some("characters".into()),
                }),
            ),
            (
                "SET default users 1 username with ascii non␍-prin␀␊tab␄le characters␄",
                Ok(Request::Set {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                    content: "username with ascii non␍-prin␀␊tab␄le".into(),
                    key: Some("characters␄".into()),
                }),
            ),
            // Content variations
            (
                "SET b c i 0:",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "".into(),
                    key: None,
                }),
            ),
            (
                "SET b c i 11:Hello World",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "Hello World".into(),
                    key: None,
                }),
            ),
            (
                "SET b c i 11:Hello\nWorld",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "Hello\nWorld".into(),
                    key: None,
                }),
            ),
            (
                "SET b c i 4:!@#$",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "!@#$".into(),
                    key: None,
                }),
            ),
            (
                {
                    let s = format!("SET b c i {}:{}", one_mb_of_data.len(), one_mb_of_data);
                    Box::leak(s.into_boxed_str())
                },
                {
                    let content = one_mb_of_data.to_string();
                    Ok(Request::Set {
                        bucket: "b".into(),
                        collection: "c".into(),
                        id: "i".into(),
                        content: content.into(),
                        key: None,
                    })
                },
            ),
            // Key variations
            (
                "SET b c i 4:test ",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: Some("".into()),
                }),
            ),
            (
                "SET b c i 4:test key with spaces",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: Some("key with spaces".into()),
                }),
            ),
            (
                "SET b c i 4:test !@#$%^&*",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: Some("!@#$%^&*".into()),
                }),
            ),
            (
                "SET b c i 4:test [very long key...]",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: Some("[very long key...]".into()),
                }),
            ),
            // Bucket and collection variations
            (
                "SET  users 1 4:test",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length: 4".to_string(),
                )),
            ),
            (
                "SET default  1 4:test",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length: 4".to_string(),
                )),
            ),
            (
                "SET 'my bucket' users 1 4:test",
                Ok(Request::Set {
                    bucket: "'my".into(),
                    collection: "bucket'".into(),
                    id: "users".into(),
                    content: "1".into(),
                    key: None,
                }),
            ),
            (
                "SET [very long bucket] [very long collection] 1 4:test",
                Ok(Request::Set {
                    bucket: "[very".into(),
                    collection: "long".into(),
                    id: "bucket]".into(),
                    content: "[very".into(),
                    key: None,
                }),
            ),
            // ID variations
            (
                "SET b c  4:test",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length: 4".to_string(),
                )),
            ),
            (
                "SET b c 'id with spaces' 4:test",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "'id".into(),
                    content: "with".into(),
                    key: None,
                }),
            ),
            (
                {
                    let s = format!("SET b c {} 4:test", very_long_key);
                    Box::leak(s.into_boxed_str())
                },
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: very_long_key,
                    content: "long".into(),
                    key: None,
                }),
            ),
            // Edge cases
            (
                "SET b c i test:4",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length: test:4".to_string(),
                )),
            ),
            (
                "SET b c i 10:test",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length: 10:test".to_string(),
                )),
            ),
            (
                "SET b c i 4test",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length: 4test".to_string(),
                )),
            ),
            (
                "SET  b  c  i  4:test",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            (
                "SET b c",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length: ".to_string(),
                )),
            ),
            (
                "SET b c i 4:test extra args",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: Some("extra args".into()),
                }),
            ),
            // Protocol specifics
            (
                "SET b c i 4:test\n",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            (
                "SET b c i 4:test\r\n",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            (
                "SET b c i 4:test\nSET b c j 5:test2",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
        ];

        for (input, expected) in cases {
            let result = Request::from_bytes(input.as_bytes());
            assert_eq!(expected, result, "Failed on input: {}", input);
        }
    }
}
