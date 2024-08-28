#![no_main]

use derive_arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use zzap::protocol::{Message, Request, Response};

#[derive(Debug, Arbitrary)]
enum MessageType {
    Request(Vec<u8>),
    Response(Vec<u8>),
}

fuzz_target!(|data: MessageType| {
    match data {
        MessageType::Request(data) => {
            let _ = Request::from_bytes(&data);
        }
        MessageType::Response(data) => {
            let _ = Response::from_bytes(&data);
        }
    }
});
