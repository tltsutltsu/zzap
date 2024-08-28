use super::message::{DecodingError, Message};

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
                    .replacen("SET ", "", 1)
                    .replacen(&format!("{} ", bucket), "", 1)
                    .replacen(&format!("{} ", collection), "", 1)
                    .replacen(&format!("{} ", id), "", 1);

                let after_params = after_params.trim_start();

                let maybe_len = after_params.find(':');
                let (content, key) = match maybe_len {
                    Some(len_pos) => {
                        // it is in form of "4:content [key]"
                        let len = after_params[..len_pos].trim();
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
                        let position = len_pos + 1;
                        if !after_params.is_char_boundary(position)
                            || !after_params.is_char_boundary(content_end)
                        {
                            return Err(DecodingError::InvalidRequest(
                                "Invalid content length".to_string(),
                            ));
                        }
                        let content = &after_params[position..content_end];
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
                                    (key.to_string(), None)
                                } else if !content.is_empty() && key.is_empty() {
                                    (content.to_string(), None)
                                } else {
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
                let key = parts.collect::<Vec<&str>>().join(" ").trim().to_string();

                let key = if key.is_empty() { None } else { Some(key) };

                Ok(Request::Get {
                    bucket,
                    collection,
                    id,
                    key,
                })
            }
            Some("SEARCH") => {
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
                let query = parts.collect::<Vec<&str>>().join(" ");

                Ok(Request::Search {
                    bucket,
                    collection,
                    query,
                })
            }
            Some("REMOVE") => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let request = Request::from_bytes(b"PING\n").unwrap();
        assert_eq!(request, Request::Ping);
    }

    #[test]
    fn test_decode_set_command() {
        let binary_data = std::fs::read_to_string("assets/tests/binary_data").unwrap();
        let very_long_symbol = "a".repeat(1000);

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
                "SET b c i 4:abc",
                Err(DecodingError::InvalidRequest("Content length exceeds input length".to_string())),
            ),
            (
                {
                    let s = format!("SET b c i {}:{}", binary_data.len(), binary_data);
                    Box::leak(s.into_boxed_str())
                },
                {
                    let content = binary_data.to_string();
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
                    key: None,
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
                {
                    let s = format!("SET b c i 4:test {}", very_long_symbol);
                    Box::leak(s.into_boxed_str())
                },
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "test".into(),
                    key: Some(very_long_symbol.clone()),
                }),
            ),
            // Bucket and collection variations
            (
                "SET  users 1 4:test",
                Ok(Request::Set {
                    bucket: "users".into(),
                    collection: "1".into(),
                    id: "4:test".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            (
                "SET default  1 4:test",
                Ok(Request::Set {
                    bucket: "default".into(),
                    collection: "1".into(),
                    id: "4:test".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            (
                "SET 'my bucket' users 1 4:test",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length".to_string(),
                )),
            ),
            (
                "SET verylongbucketnameconsistsofmorethan256characters123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123 verylongcollectionnameconsistsofmorethan256characters123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123 1 4:test",
                Ok(Request::Set {
                    bucket: "verylongbucketnameconsistsofmorethan256characters123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123".into(),
                    collection: "verylongcollectionnameconsistsofmorethan256characters123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123123".into(),
                    id: "1".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            // ID variations
            (
                "SET b c  4:test",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "4:test".into(),
                    content: "test".into(),
                    key: None,
                }),
            ),
            (
                {
                    let s = format!("SET b c {} 4:test", very_long_symbol);
                    Box::leak(s.into_boxed_str())
                },
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: very_long_symbol,
                    content: "test".into(),
                    key: None,
                }),
            ),
            // Edge cases
            (
                "SET b c i test:4",
                Err(DecodingError::InvalidRequest(
                    "Invalid content length".to_string(),
                )),
            ),
            (
                "SET b c i 10:test",
                Err(DecodingError::InvalidRequest(
                    "Content length exceeds input length".to_string(),
                )),
            ),
            (
                "SET b c i 4test",
                Ok(Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "4test".into(),
                    key: None,
                }),
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
                    "Missing id".to_string(),
                )),
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
                    key: Some("SET b c j 5:test2".into()),
                }),
            ),
            ( // case from fuzzer: invalid utf8 boundary
                #[allow(invalid_from_utf8_unchecked)]
                unsafe {
                    std::str::from_utf8_unchecked(&[
                        83,
                        69,
                        84,
                        32,
                        50,
                        12,
                        58,
                        12,
                        229,
                    ])
                },
             Err(DecodingError::InvalidRequest("Invalid content length".to_string())),
            )
        ];

        for (input, expected) in cases {
            let result = Request::from_bytes(input.as_bytes());
            assert_eq!(expected, result, "Failed on input: {}", input);
        }
    }

    #[test]
    fn test_encode_set_command() {
        let cases = vec![
            // Basic SET command
            (
                Request::Set {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                    content: "test".into(),
                    key: None,
                },
                b"SET default users 1 4:test\n".to_vec(),
            ),
            // SET command with a key
            (
                Request::Set {
                    bucket: "myapp".into(),
                    collection: "docs".into(),
                    id: "123".into(),
                    content: "Hello, World!".into(),
                    key: Some("mykey".into()),
                },
                b"SET myapp docs 123 13:Hello, World! mykey\n".to_vec(),
            ),
            // SET command with empty content
            (
                Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "".into(),
                    key: None,
                },
                b"SET b c i 0:\n".to_vec(),
            ),
            // SET command with content containing spaces
            (
                Request::Set {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    id: "doc1".into(),
                    content: "This is a test".into(),
                    key: None,
                },
                b"SET bucket col doc1 14:This is a test\n".to_vec(),
            ),
            // SET command with content containing special characters
            (
                Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "!@#$%^&*".into(),
                    key: None,
                },
                b"SET b c i 8:!@#$%^&*\n".to_vec(),
            ),
            // SET command with a very long content
            (
                Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "a".repeat(1000),
                    key: None,
                },
                format!("SET b c i 1000:{}\n", "a".repeat(1000)).into_bytes(),
            ),
            // SET command with content containing newlines
            (
                Request::Set {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    content: "line1\nline2".into(),
                    key: None,
                },
                b"SET b c i 11:line1\nline2\n".to_vec(),
            ),
            // SET command with very long bucket, collection, and id names
            (
                Request::Set {
                    bucket: "very_long_bucket_name".into(),
                    collection: "very_long_collection_name".into(),
                    id: "very_long_id_name".into(),
                    content: "test".into(),
                    key: None,
                },
                b"SET very_long_bucket_name very_long_collection_name very_long_id_name 4:test\n"
                    .to_vec(),
            ),
        ];

        for (request, expected) in cases {
            let result = request.to_bytes();
            assert_eq!(result, expected, "Failed to encode: {:?}", request);
        }
    }

    #[test]
    fn test_encode_ping_command() {
        let request = Request::Ping;
        let expected = b"PING\n".to_vec();
        let result = request.to_bytes();
        assert_eq!(result, expected, "Failed to encode: {:?}", request);
    }

    #[test]
    fn test_decode_ping_command() {
        let variants: Vec<&[u8]> = vec![b"PING\n", b"PING\r\n", b"PING\r\n\r\n", b"\r\nPING\n"];
        for variant in variants {
            let request = Request::from_bytes(variant).unwrap();
            assert_eq!(request, Request::Ping);
        }
    }

    #[test]
    fn test_encode_get_command() {
        let cases = vec![
            // Basic GET command without key
            (
                Request::Get {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                    key: None,
                },
                b"GET default users 1\n".to_vec(),
            ),
            // GET command with a key
            (
                Request::Get {
                    bucket: "myapp".into(),
                    collection: "docs".into(),
                    id: "123".into(),
                    key: Some("mykey".into()),
                },
                b"GET myapp docs 123 mykey\n".to_vec(),
            ),
            // GET command with special characters in bucket, collection, and id
            (
                Request::Get {
                    bucket: "my-bucket".into(),
                    collection: "my_collection".into(),
                    id: "doc@123".into(),
                    key: None,
                },
                b"GET my-bucket my_collection doc@123\n".to_vec(),
            ),
            // GET command with spaces in key
            (
                Request::Get {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    key: Some("key with spaces".into()),
                },
                b"GET b c i key with spaces\n".to_vec(),
            ),
            // GET command with very long bucket, collection, and id names
            (
                Request::Get {
                    bucket: "very_long_bucket_name".into(),
                    collection: "very_long_collection_name".into(),
                    id: "very_long_id_name".into(),
                    key: None,
                },
                b"GET very_long_bucket_name very_long_collection_name very_long_id_name\n".to_vec(),
            ),
            // GET command with empty bucket, collection, or id (edge case)
            (
                Request::Get {
                    bucket: "".into(),
                    collection: "".into(),
                    id: "".into(),
                    key: None,
                },
                b"GET   \n".to_vec(),
            ),
        ];

        for (request, expected) in cases {
            let result = request.to_bytes();
            assert_eq!(result, expected, "Failed to encode: {:?}", request);
        }
    }

    #[test]
    fn test_decode_get_command() {
        let cases: Vec<(&[u8], Result<Request, DecodingError>)> = vec![
            // Basic GET command without key
            (
                b"GET default users 1\n",
                Ok(Request::Get {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                    key: None,
                }),
            ),
            // GET command with a key
            (
                b"GET myapp docs 123 mykey\n",
                Ok(Request::Get {
                    bucket: "myapp".into(),
                    collection: "docs".into(),
                    id: "123".into(),
                    key: Some("mykey".into()),
                }),
            ),
            // GET command with special characters in bucket, collection, and id
            (
                b"GET my-bucket my_collection doc@123\n",
                Ok(Request::Get {
                    bucket: "my-bucket".into(),
                    collection: "my_collection".into(),
                    id: "doc@123".into(),
                    key: None,
                }),
            ),
            // GET command with spaces in key
            (
                b"GET b c i key with spaces\n",
                Ok(Request::Get {
                    bucket: "b".into(),
                    collection: "c".into(),
                    id: "i".into(),
                    key: Some("key with spaces".into()),
                }),
            ),
            // GET command with very long bucket, collection, and id names
            (
                b"GET very_long_bucket_name very_long_collection_name very_long_id_name\n",
                Ok(Request::Get {
                    bucket: "very_long_bucket_name".into(),
                    collection: "very_long_collection_name".into(),
                    id: "very_long_id_name".into(),
                    key: None,
                }),
            ),
            // GET command with trailing whitespace
            (
                b"GET bucket col id   \n",
                Ok(Request::Get {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    id: "id".into(),
                    key: None,
                }),
            ),
            // GET command with different line endings
            (
                b"GET bucket col id\r\n",
                Ok(Request::Get {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    id: "id".into(),
                    key: None,
                }),
            ),
            // Invalid GET commands
            (
                b"GET\n",
                Err(DecodingError::InvalidRequest("Missing bucket".to_string())),
            ),
            (
                b"GET bucket\n",
                Err(DecodingError::InvalidRequest(
                    "Missing collection".to_string(),
                )),
            ),
            (
                b"GET bucket col\n",
                Err(DecodingError::InvalidRequest("Missing id".to_string())),
            ),
        ];

        for (input, expected) in cases {
            let result = Request::from_bytes(input);
            assert_eq!(
                result,
                expected,
                "Failed to decode: {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn test_encode_search_command() {
        let cases = vec![
            // Basic SEARCH command
            (
                Request::Search {
                    bucket: "default".into(),
                    collection: "users".into(),
                    query: "John".into(),
                },
                b"SEARCH default users John\n".to_vec(),
            ),
            // SEARCH command with multi-word query
            (
                Request::Search {
                    bucket: "myapp".into(),
                    collection: "docs".into(),
                    query: "Hello World".into(),
                },
                b"SEARCH myapp docs Hello World\n".to_vec(),
            ),
            // SEARCH command with special characters in query
            (
                Request::Search {
                    bucket: "b".into(),
                    collection: "c".into(),
                    query: "test@example.com".into(),
                },
                b"SEARCH b c test@example.com\n".to_vec(),
            ),
            // SEARCH command with very long bucket and collection names
            (
                Request::Search {
                    bucket: "very_long_bucket_name".into(),
                    collection: "very_long_collection_name".into(),
                    query: "test".into(),
                },
                b"SEARCH very_long_bucket_name very_long_collection_name test\n".to_vec(),
            ),
            // SEARCH command with empty query
            (
                Request::Search {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    query: "".into(),
                },
                b"SEARCH bucket col \n".to_vec(),
            ),
        ];

        for (request, expected) in cases {
            let result = request.to_bytes();
            assert_eq!(result, expected, "Failed to encode: {:?}", request);
        }
    }

    #[test]
    fn test_decode_search_command() {
        let cases: Vec<(&[u8], Result<Request, DecodingError>)> = vec![
            // Basic SEARCH command
            (
                b"SEARCH default users John\n",
                Ok(Request::Search {
                    bucket: "default".into(),
                    collection: "users".into(),
                    query: "John".into(),
                }),
            ),
            // SEARCH command with multi-word query
            (
                b"SEARCH myapp docs Hello World\n",
                Ok(Request::Search {
                    bucket: "myapp".into(),
                    collection: "docs".into(),
                    query: "Hello World".into(),
                }),
            ),
            // SEARCH command with special characters in query
            (
                b"SEARCH b c test@example.com\n",
                Ok(Request::Search {
                    bucket: "b".into(),
                    collection: "c".into(),
                    query: "test@example.com".into(),
                }),
            ),
            // SEARCH command with very long bucket and collection names
            (
                b"SEARCH very_long_bucket_name very_long_collection_name test\n",
                Ok(Request::Search {
                    bucket: "very_long_bucket_name".into(),
                    collection: "very_long_collection_name".into(),
                    query: "test".into(),
                }),
            ),
            // SEARCH command with empty query
            (
                b"SEARCH bucket col \n",
                Ok(Request::Search {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    query: "".into(),
                }),
            ),
            // SEARCH command with trailing whitespace
            (
                b"SEARCH bucket col query   \n",
                Ok(Request::Search {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    query: "query".into(),
                }),
            ),
            // SEARCH command with different line endings
            (
                b"SEARCH bucket col query\r\n",
                Ok(Request::Search {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    query: "query".into(),
                }),
            ),
            // Invalid SEARCH commands
            (
                b"SEARCH\n",
                Err(DecodingError::InvalidRequest("Missing bucket".to_string())),
            ),
            (
                b"SEARCH bucket\n",
                Err(DecodingError::InvalidRequest(
                    "Missing collection".to_string(),
                )),
            ),
        ];

        for (input, expected) in cases {
            let result = Request::from_bytes(input);
            assert_eq!(
                result,
                expected,
                "Failed to decode: {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn test_encode_remove_command() {
        let cases = vec![
            // Basic REMOVE command
            (
                Request::Remove {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                },
                b"REMOVE default users 1\n".to_vec(),
            ),
            // REMOVE command with special characters in bucket, collection, and id
            (
                Request::Remove {
                    bucket: "my-bucket".into(),
                    collection: "my_collection".into(),
                    id: "doc@123".into(),
                },
                b"REMOVE my-bucket my_collection doc@123\n".to_vec(),
            ),
            // REMOVE command with very long bucket, collection, and id names
            (
                Request::Remove {
                    bucket: "very_long_bucket_name".into(),
                    collection: "very_long_collection_name".into(),
                    id: "very_long_id_name".into(),
                },
                b"REMOVE very_long_bucket_name very_long_collection_name very_long_id_name\n"
                    .to_vec(),
            ),
            // REMOVE command with empty bucket, collection, or id (edge case)
            (
                Request::Remove {
                    bucket: "".into(),
                    collection: "".into(),
                    id: "".into(),
                },
                b"REMOVE   \n".to_vec(),
            ),
        ];

        for (request, expected) in cases {
            let result = request.to_bytes();
            assert_eq!(result, expected, "Failed to encode: {:?}", request);
        }
    }

    #[test]
    fn test_decode_remove_command() {
        let cases: Vec<(&[u8], Result<Request, DecodingError>)> = vec![
            // Basic REMOVE command
            (
                b"REMOVE default users 1\n",
                Ok(Request::Remove {
                    bucket: "default".into(),
                    collection: "users".into(),
                    id: "1".into(),
                }),
            ),
            // REMOVE command with special characters in bucket, collection, and id
            (
                b"REMOVE my-bucket my_collection doc@123\n",
                Ok(Request::Remove {
                    bucket: "my-bucket".into(),
                    collection: "my_collection".into(),
                    id: "doc@123".into(),
                }),
            ),
            // REMOVE command with very long bucket, collection, and id names
            (
                b"REMOVE very_long_bucket_name very_long_collection_name very_long_id_name\n",
                Ok(Request::Remove {
                    bucket: "very_long_bucket_name".into(),
                    collection: "very_long_collection_name".into(),
                    id: "very_long_id_name".into(),
                }),
            ),
            // REMOVE command with trailing whitespace
            (
                b"REMOVE bucket col id   \n",
                Ok(Request::Remove {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    id: "id".into(),
                }),
            ),
            // REMOVE command with different line endings
            (
                b"REMOVE bucket col id\r\n",
                Ok(Request::Remove {
                    bucket: "bucket".into(),
                    collection: "col".into(),
                    id: "id".into(),
                }),
            ),
            // Invalid REMOVE commands
            (
                b"REMOVE\n",
                Err(DecodingError::InvalidRequest("Missing bucket".to_string())),
            ),
            (
                b"REMOVE bucket\n",
                Err(DecodingError::InvalidRequest(
                    "Missing collection".to_string(),
                )),
            ),
            (
                b"REMOVE bucket col\n",
                Err(DecodingError::InvalidRequest("Missing id".to_string())),
            ),
        ];

        for (input, expected) in cases {
            let result = Request::from_bytes(input);
            assert_eq!(
                result,
                expected,
                "Failed to decode: {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn test_invalid_command() {
        let result = Request::from_bytes(b"INVALID 123");
        assert!(result.is_err(), "Expected error for invalid command");
        assert_eq!(
            result.unwrap_err(),
            DecodingError::InvalidRequest("Invalid command".to_string())
        );
    }
}
