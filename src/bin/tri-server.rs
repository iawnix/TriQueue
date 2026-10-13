use anyhow::Result;
use tri::{data_dir, db_path, init_logging, server::Server, store::Store};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let store = Store::open(&db_path(), &data_dir())?;
    Server::new(store).serve().await
}
