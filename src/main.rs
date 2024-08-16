#![warn(clippy::all)]

use zzap::start;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start().await
}