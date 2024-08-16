use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

// This test requires running zzap with the default config
#[tokio::test]
async fn simple() -> Result<(), Box<dyn Error>> {
    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:13413")?;

    println!("Connected to server");

    // Test PING
    send_command(&mut stream, "PING")?;
    assert_eq!(read_response(&mut stream)?, "+OK\n");

    // Set a key
    send_command(&mut stream, "SET default test_collection test_id 7:test123")?;
    assert_eq!(read_response(&mut stream)?, "+OK\n");

    // Search for the key
    send_command(&mut stream, "SEARCH default test_collection test")?;
    assert_eq!(read_response(&mut stream)?, "1\ntest_id\n");

    Ok(())
}

#[tokio::test]
async fn lot_of_data() -> Result<(), Box<dyn Error>> {
    use csv::ReaderBuilder;

    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:13413")?;

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
        .collect();
    println!("all_records: {:?}", all_records.len());
    for (id, (article_name, _search_phrases)) in all_records.iter().enumerate() {
        let command = format!(
            "SET default articles {} {}:{}",
            id,
            article_name.len(),
            article_name
        );
        send_command(&mut stream, &command)?;
        let resp = read_response(&mut stream)?;
        assert_eq!(resp, "+OK\n");
    }

    // now try to search for each phrase
    let mut found = 0;
    let mut total = 0;
    for (id, (_article_name, search_phrases)) in
        all_records.iter().enumerate().take(all_records.len() / 100)
    {
        for search_phrase in search_phrases {
            total += 1;
            send_command(
                &mut stream,
                &format!("SEARCH default articles {}", search_phrase),
            )?;
            let resp = read_response(&mut stream)?;

            if resp.contains(id.to_string().as_str()) {
                found += 1;
            }
        }

        if id % 1000 == 0 {
            println!("processed {} records ({}%)", id, id * 100 / total);
        }
    }

    println!("found {} of {} items", found, total);

    Ok(())
}

#[tokio::test]
async fn lot_of_clients() -> Result<(), Box<dyn Error>> {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    const NUM_CLIENTS: usize = 100;
    const OPERATIONS_PER_CLIENT: usize = 1000;
    const ARTICLE_NAME_LENGTH: (usize, usize) = (10, 40);

    let mut clients = Vec::new();

    for _ in 0..NUM_CLIENTS {
        let stream = TcpStream::connect("127.0.0.1:13413")?;
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
            let command = format!("SET default articles {} {}", article_id, article_name);
            send_command(stream, &command)?;
            let resp = read_response(stream)?;
            assert_eq!(resp, "+OK\n");

            let search_phrase = article_name.clone();
            send_command(stream, &format!("SEARCH default articles {}", search_phrase))?;
            let resp = read_response(stream)?;
            assert_eq!(resp, format!("1\n{}\n", article_id));
        }

        Ok(())
    }

    for mut client in clients {
        run_client(&mut client)?;
    }

    Ok(())
}

fn send_command(stream: &mut TcpStream, command: &str) -> Result<(), Box<dyn Error>> {
    stream.write_all(format!("{}\n", command).as_bytes())?;
    Ok(())
}

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
