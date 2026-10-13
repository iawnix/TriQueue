use anyhow::Result;
use clap::Parser;
use tri::client;
use tri::output;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_node", about = "show tri nodes")]
struct Args {}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = Args::parse();
    match client::local(&Request::Nodes).await? {
        Response::Nodes(nodes) => output::nodes(&nodes),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
