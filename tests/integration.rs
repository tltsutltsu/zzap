use rand::{distributions::Alphanumeric, Rng};
use std::ops::Range;
use zzap::search::{Dash2SearchEngine, SearchEngine};
use zzap::storage::Storage;

#[test]
fn test_search() {
    const DOCUMENTS_COUNT: usize = 100_000;
    const ARTICLE_NAME_LENGTH: (usize, usize) = (10, 1000);
    const ARTICLE_ID_RANGE: Range<usize> = 0..100_000_000_000;

    let documents_100k: Vec<(String, String)> = (0..DOCUMENTS_COUNT)
        .map(|_| {
            let id = rand::thread_rng().gen_range(ARTICLE_ID_RANGE);
            let content: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(rand::thread_rng().gen_range(ARTICLE_NAME_LENGTH.0..ARTICLE_NAME_LENGTH.1))
                .map(|c| c as char)
                .collect();
            (id.to_string(), content)
        })
        .collect();

    let engine = Dash2SearchEngine::new();
    let storage = Storage::new("storage.db");

    loop {
        let index = rand::thread_rng().gen_range(0..documents_100k.len());
        let document = documents_100k.get(index).unwrap();
        engine
            .index(&storage, "bucket", "collection", &document.0, &document.1)
            .unwrap();
    }
}
