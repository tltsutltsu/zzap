#![allow(unexpected_cfgs)] // to avoid warnings about missing tarpaulin cfg attributes
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::Duration;

const SERVER_PORT: u16 = 13413;
const RELEASE_MODE: bool = true;

struct TestNode {
    process: Child,
}

impl TestNode {
    const START_TIMEOUT: Duration = Duration::from_secs(3);

    fn new() -> Self {
        let mut args = vec!["build"];

        if RELEASE_MODE {
            args.push("--release")
        }

        let build_status = std::process::Command::new("cargo")
            .args(args)
            .spawn()
            .expect("Failed to build zzap")
            .wait()
            .expect("Failed to wait for build to finish");

        assert!(build_status.success(), "Failed to build zzap");

        let process = Command::new("./target/release/zzap")
            .spawn()
            .expect("Failed to start database");

        std::thread::sleep(Self::START_TIMEOUT);

        Self { process }
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.process
            .kill()
            .expect("Failed to kill database process");
    }
}

fn send_command(stream: &mut TcpStream, command: &str) -> Result<(), Box<dyn Error>> {
    stream.write_all(format!("{}\n", command).as_bytes())?;
    Ok(())
}

// TODO: change to common function to read response from stream
fn read_response(stream: &mut TcpStream) -> Result<String, Box<dyn Error>> {
    // read until newline
    let mut buffer = Vec::new();
    let mut reader = BufReader::new(&mut *stream);
    reader.read_until(b'\n', &mut buffer)?;
    let response = String::from_utf8(buffer)?;

    // if response is number, parse it as int N and read N lines
    if let Ok(n) = response.trim().parse::<usize>() {
        let mut lines: Vec<String> = vec![response.clone()];
        for _ in 0..n {
            let mut buffer = Vec::new();
            reader.read_until(b'\n', &mut buffer)?;
            lines.push(String::from_utf8(buffer)?);
        }
        return Ok(lines.join(""));
    }

    Ok(response)
}

/// Macro to send a command to the server and read the response
/// Parameters:
/// - `stream`: the stream to send the command to   
/// - `command`: the command to send
/// - `expected_response`: the expected response
macro_rules! command {
    ($stream:expr, $command:expr, $expected_response:expr) => {
        send_command($stream, $command)?;
        let resp = read_response($stream)?;
        assert_eq!(resp, $expected_response);
    };
}

macro_rules! command_predicate {
    ($stream:expr, $command:expr, $predicate:expr) => {
        send_command($stream, $command)?;
        let resp = read_response($stream)?;
        assert!($predicate(resp));
    };
}

// This test requires running zzap with the default config
#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
#[cfg_attr(not(feature = "e2e-tests"), ignore)]
async fn e2e_simple() -> Result<(), Box<dyn Error>> {
    // Connect to the server
    let _node = TestNode::new();

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", SERVER_PORT))?;

    println!("Connected to server");

    // Test PING
    command!(&mut stream, "PING", "+OK\n");

    // Set a key
    command!(
        &mut stream,
        "SET default test_collection test_id 7:test123",
        "+OK\n"
    );

    // Search for the key
    command!(
        &mut stream,
        "SEARCH default test_collection test123",
        "1\ntest_id\n"
    );

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "e2e-tests"), ignore)]
#[cfg_attr(tarpaulin, ignore)]
async fn e2e_index_cleans_properly() -> Result<(), Box<dyn Error>> {
    // Connect to the server
    let _node = TestNode::new();

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", SERVER_PORT))?;

    println!("Connected to server");

    command!(&mut stream, "SET default articles 42 test_article", "+OK\n");
    command!(&mut stream, "SET default articles 42 other_word", "+OK\n");

    command!(&mut stream, "SEARCH default articles test_article", "0\n");
    command!(&mut stream, "SEARCH default articles other_word", "1\n42\n");

    command!(&mut stream, "REMOVE default articles 42", "+OK\n");

    command!(&mut stream, "SEARCH default articles test_article", "0\n");
    command!(&mut stream, "SEARCH default articles other_word", "0\n");

    command!(&mut stream, "SET default articles 5 first second", "+OK\n");
    command!(&mut stream, "SET default articles 6 first", "+OK\n");

    command_predicate!(&mut stream, "SEARCH default articles first", |resp| {
        resp == "2\n5\n6\n" || resp == "2\n6\n5\n"
    });

    Ok(())
}

// This test is slow, but mostly bc it uses 1 client to send all the data
#[tokio::test]
#[cfg_attr(not(feature = "e2e-tests"), ignore)]
#[cfg_attr(tarpaulin, ignore)]
async fn e2e_lot_of_data() -> Result<(), Box<dyn Error>> {
    use csv::ReaderBuilder;

    // Connect to the server
    let _node = TestNode::new();

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", SERVER_PORT))?;

    println!("Connected to server");

    // load data from file "assets/tests/search_synthetic_dataset.csv"
    // format is "article name,search phrase 1, search phrase 2, search phrase 3"
    // for each line, send SET command

    let file = File::open("assets/tests/search_synthetic_dataset.csv")?;
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(file);
    let all_records: Vec<(String, Vec<String>)> = reader
        .records()
        .map(|result| {
            let record = result.unwrap();
            let article_name = record[0].to_string();
            let search_phrases = record.iter().skip(1).map(|s| s.to_string()).collect();
            (article_name, search_phrases)
        })
        .take(15_000) // roughly quater of the dataset, TODO: make this configurable
        .collect();
    println!("all_records: {:?}", all_records.len());
    for (id, (article_name, _search_phrases)) in all_records.iter().enumerate() {
        let cmd = format!(
            "SET default articles {} {}:{}",
            id,
            article_name.len(),
            article_name
        );
        command!(&mut stream, cmd.as_str(), "+OK\n");
    }

    // now try to search for each phrase
    let mut found = 0;
    let mut total = 0;
    for (id, (_article_name, search_phrases)) in all_records.iter().enumerate() {
        for search_phrase in search_phrases {
            total += 1;

            command_predicate!(
                &mut stream,
                format!("SEARCH default articles {}", search_phrase).as_str(),
                |resp: String| {
                    if resp.contains(id.to_string().as_str()) {
                        found += 1;
                    }

                    true
                }
            );
        }

        if id % 1000 == 0 {
            println!("processed {} records", id);
        }
    }

    println!("found {} documents of {} search phrases", found, total);

    Ok(())
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
#[cfg_attr(not(feature = "e2e-tests"), ignore)]
async fn e2e_lot_of_clients() -> Result<(), Box<dyn Error>> {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    const NUM_CLIENTS: usize = 100;
    const OPERATIONS_PER_CLIENT: usize = 1000;
    const ARTICLE_NAME_LENGTH: (usize, usize) = (10, 40);

    let _node = TestNode::new();

    let mut clients = Vec::new();

    for _ in 0..NUM_CLIENTS {
        let stream = TcpStream::connect(format!("127.0.0.1:{}", SERVER_PORT))?;
        clients.push(stream);
    }

    fn run_client(stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        send_command(stream, "PING")?;
        let resp = read_response(stream)?;
        assert_eq!(resp, "+OK\n");

        for _ in 0..OPERATIONS_PER_CLIENT {
            let article_id = rand::thread_rng().gen_range(0..1000000);
            let article_name: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(rand::thread_rng().gen_range(ARTICLE_NAME_LENGTH.0..ARTICLE_NAME_LENGTH.1))
                .map(|c| c as char)
                .collect();

            command!(
                stream,
                format!("SET default articles {} {}", article_id, article_name).as_str(),
                "+OK\n"
            );
            command!(
                stream,
                format!("SEARCH default articles {}", article_name).as_str(),
                format!("1\n{}\n", article_id)
            );
        }

        Ok(())
    }

    for mut client in clients {
        run_client(&mut client)?;
    }

    Ok(())
}
