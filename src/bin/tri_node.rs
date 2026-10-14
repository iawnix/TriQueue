use anyhow::Result;
use clap::Parser;
use tri::client;
use tri::output;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_node", about = "show tri nodes")]
struct Args {
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let response = match client::local(&Request::Nodes).await {
        Ok(response) => response,
        Err(error) if args.json => output::json_error(&error),
        Err(error) => return Err(error),
    };
    match response {
        Response::Nodes(nodes) if args.json => output::nodes_json(&nodes)?,
        Response::Nodes(nodes) => output::nodes(&nodes),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
