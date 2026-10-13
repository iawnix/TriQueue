use anyhow::Result;
use clap::Parser;
use tri::init_logging;

#[derive(Debug, Parser)]
#[command(name = "tri_top", about = "interactive tri dashboard")]
struct Args {}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = Args::parse();
    init_logging();
    tri::tui::run().await
}
