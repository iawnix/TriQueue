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
    /// Validate the registered nodes' runtime capabilities.
    #[arg(long)]
    check: bool,
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
        Response::Nodes(nodes) => {
            if args.check
                && let Err(error) = check_nodes(&nodes, !args.json)
            {
                if args.json {
                    output::json_error(&error);
                }
                return Err(error);
            }
            if args.json {
                output::nodes_json(&nodes)?;
            } else {
                output::nodes(&nodes);
            }
        }
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}

fn check_nodes(nodes: &[tri::model::NodeInfo], print_ok: bool) -> Result<()> {
    anyhow::ensure!(!nodes.is_empty(), "no nodes are registered");
    let required = [
        "cgroup_v2",
        "cgroup:cpuset",
        "cgroup:cpu",
        "cgroup:memory",
        "resource_enforcement",
    ];
    let mut failed = false;
    for node in nodes {
        let mut missing = required
            .iter()
            .filter(|capability| !node.capabilities.iter().any(|item| item == **capability))
            .copied()
            .collect::<Vec<_>>();
        if !node
            .capabilities
            .iter()
            .any(|item| item == "systemd_user" || item == "systemd_system")
        {
            missing.push("systemd_manager");
        }
        if !node.online || !missing.is_empty() {
            failed = true;
            eprintln!(
                "{}: unavailable={} missing={}",
                node.name,
                !node.online,
                if missing.is_empty() {
                    "-".to_owned()
                } else {
                    missing.join(",")
                }
            );
        } else if print_ok {
            println!("{}: ok", node.name);
        }
    }
    anyhow::ensure!(!failed, "node capability check failed");
    Ok(())
}
