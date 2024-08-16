#![warn(clippy::all)]

use zzap::start;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start().await
}