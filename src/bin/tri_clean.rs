use anyhow::Result;
use clap::Parser;
use tri::client;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_clean", about = "remove completed task records")]
struct Args {}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = Args::parse();
    match client::local(&Request::Clean).await? {
        Response::Message(message) => println!("{message}"),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
