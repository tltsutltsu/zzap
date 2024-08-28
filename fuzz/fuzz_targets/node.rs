#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::{Arc, RwLock};
use zzap::encryption::MockEncryptor;
use zzap::protocol::Request;
use zzap::search::StdSearchEngine;
use zzap::server::handler::handle_request;
use zzap::storage::Storage;

fuzz_target!(|requests: Vec<Request>| {
    let storage = Arc::new(RwLock::new(Storage::new("test.db")));
    let encryptor = MockEncryptor;
    let search_engine = Arc::new(RwLock::new(StdSearchEngine::new()));

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for req in requests {
            let _ = handle_request(req, &storage, &encryptor, &search_engine).await;
        }
    });
});
