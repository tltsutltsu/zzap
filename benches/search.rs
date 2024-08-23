#![cfg(target_os = "linux")]

use csv::ReaderBuilder;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use rand::{distributions::Alphanumeric, seq::IteratorRandom, Rng};
use std::fs::File;
use std::hint::black_box;
use std::ops::Range;
use zzap::search::{
    BTreeSearchEngine, Dash2SearchEngine, DashSearchEngine, SearchEngine, StdSearchEngine,
};
use zzap::storage::Storage;

struct EngineSetup {
    engine: Box<dyn SearchEngine>,
    storage: Storage,
    documents: Vec<(String, String)>,
}

fn engine_setup(engine_type: &str) -> EngineSetup {
    let engine: Box<dyn SearchEngine> = match engine_type {
        "btree" => Box::new(BTreeSearchEngine::new()),
        "dash" => Box::new(DashSearchEngine::new()),
        "dash2" => Box::new(Dash2SearchEngine::new()),
        "std" => Box::new(StdSearchEngine::new()),
        _ => panic!("Unknown engine type"),
    };
    let storage = Storage::new("storage.db");

    let file = File::open("assets/tests/search_synthetic_dataset.csv").unwrap();
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(file);
    let documents: Vec<(String, String)> = reader
        .records()
        .map(|result| {
            let record = result.unwrap();
            let article_name = record[0].to_string();
            article_name
        })
        .enumerate()
        .map(|(id, article_name)| (id.to_string(), article_name))
        .collect();

    EngineSetup {
        engine,
        storage,
        documents,
    }
}

fn search_setup(engine_type: &str) -> EngineSetup {
    let setup = engine_setup(engine_type);
    setup
        .engine
        .batch_index(
            &setup.storage,
            "bucket",
            "collection",
            setup.documents.clone(),
        )
        .unwrap();
    setup
}

#[library_benchmark(setup = engine_setup)]
#[bench::btree("btree")]
#[bench::dash("dash")]
#[bench::dash2("dash2")]
#[bench::std("std")]
fn index(setup: EngineSetup) {
    black_box(
        setup
            .engine
            .batch_index(&setup.storage, "bucket", "collection", setup.documents)
            .unwrap(),
    );
}

#[library_benchmark(setup = search_setup)]
#[bench::btree("btree")]
#[bench::dash("dash")]
#[bench::dash2("dash2")]
#[bench::std("std")]
fn search(setup: EngineSetup) {
    black_box(
        setup
            .engine
            .search(
                "bucket",
                "collection",
                setup
                    .documents
                    .iter()
                    .map(|(_, content)| content)
                    .collect::<Vec<_>>()
                    .first()
                    .unwrap(),
            )
            .unwrap(),
    );
}

library_benchmark_group!(
    name = search_group;
    benchmarks = index, search
);

main!(library_benchmark_groups = search_group);
