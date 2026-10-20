# TriQueue

TriQueue is a lightweight task queue for small Linux clusters, written in Rust.
Submit shell commands or job scripts, request CPU, memory, and GPU resources,
and follow their progress from the command line or a terminal dashboard.

A single `tri-server` stores the queue in SQLite. A `tri-client` on each compute
node starts tasks through systemd. The server can also run a compute client,
so you can start with one machine and add nodes later.

## Features

- CPU allocation, per-task memory limits, and GPU assignment.
- Job scripts with resource declarations, output paths, and runtime limits.
- Task dependencies and optional retries after a node's lease expires.
- Node-local scratch directories for temporary work.
- JSON output and submission identifiers for workflow integration.
- A terminal dashboard for inspecting the queue and requesting cancellation.

## Requirements and scope

TriQueue is intended for a trusted environment where tasks run as the account
selected for task execution (the compute client's account in user mode). It does not provide per-user isolation or transfer
scripts, software, inputs, or results between machines.

To run tasks, each compute node needs:

- Linux with systemd and cgroup v2 CPU, cpuset, and memory controllers.
- A [system-managed compute client](docs/deployment.md#system-managed-compute-client-recommended),
  or a user manager with delegated controllers and working device enforcement.
- The programs and files used by your jobs. For multiple nodes, use consistent
  absolute paths and shared storage such as NFS where needed.

GPU jobs additionally need compatible drivers, device access, and systemd
support for the configured device rules. The current GPU configuration uses
NVIDIA device paths and visibility variables.

TriQueue is at version 0.1.0. It originated on a two-node cluster; large-cluster
scalability and high availability are not established. See the
[deployment guide](docs/deployment.md) for configuration and operational limits.

## Install from source

Install a current stable Rust toolchain and a C compiler/linker, then run:

```bash
git clone https://github.com/iawnix/TriQueue.git
cd TriQueue
cargo install --path . --locked --bins
export PATH="$HOME/.cargo/bin:$PATH"
```

SQLite is bundled with the build; a separate database server is not required.

## Run your first task

This example runs a server and a CPU-only compute client on the same machine.
Use three terminals under the same Linux account, with the installed binaries
on `PATH`. First run `tri-client --check` to verify that this host can enforce
limits in user mode. If it fails, use the [system-managed deployment](docs/deployment.md#system-managed-compute-client-recommended).
It uses port 7400; choose another port consistently if it is in use.
Use an account that is not already running a TriQueue compute client: clients
under the same account share a systemd task-unit namespace, even across queues.

In the first terminal, start the server:

```bash
mkdir -p "$HOME/.local/state/tri-demo"
chmod 700 "$HOME/.local/state/tri-demo"
TRI_SOCKET="$HOME/.local/state/tri-demo/server.sock" \
TRI_DATA_DIR="$HOME/.local/state/tri-demo" \
TRI_DB="$HOME/.local/state/tri-demo/tri.db" \
TRI_SERVER_ADDR=127.0.0.1:7400 \
tri-server
```

In the second terminal, start a compute client advertising one CPU and 1 GiB
of memory:

```bash
TRI_SERVER_ADDR=127.0.0.1:7400 \
tri-client --name local --cpus 1 --memory 1G --gpus 0 \
  --scratch "$HOME/.local/state/tri-demo/scratch"
```

The demo uses the default unauthenticated TCP setup on loopback. Run it on a
trusted machine with `TRI_AUTH_TOKEN` unset in both terminals. For a shared
machine or a network deployment, [configure authentication](docs/deployment.md#connections-and-authentication).

In the third terminal, check the node and submit a task:

```bash
export TRI_SOCKET="$HOME/.local/state/tri-demo/server.sock"
tri_node --check
mkdir -p "$HOME/tri-demo-jobs"
cd "$HOME/tri-demo-jobs"
job_id=$(tri_sub --name hello --cpu 1 --mem 128M -- bash -c 'echo "Hello from TriQueue"')
tri_stat "$job_id"
tri_log "$job_id" --follow
tri_ls
```

Once the client registers, `tri_node --check` should report `local: ok`.
The task should reach `done`, and its log should contain `Hello from TriQueue`.
Output is also saved to `tri-<id>.out` in the job directory. If the node check
fails or the task ends in `fail`, see [troubleshooting](docs/deployment.md#troubleshooting).

Run `tri_top` to explore the dashboard. After the task finishes, stop the
foreground client and server with Ctrl-C in their respective terminals.

## Commands

| Command | Purpose |
| --- | --- |
| `tri_sub` | Submit a command or job script |
| `tri_ls` | List tasks |
| `tri_stat` | Show the queue or inspect a task |
| `tri_log` | Read or follow task output |
| `tri_del` | Request task cancellation |
| `tri_node` | List nodes and check runtime capabilities |
| `tri_top` | Open the terminal dashboard |
| `tri_clean` | Remove old task records |

Run a command with `--help` for its options.

## Documentation

- [Usage guide](docs/usage.md): jobs, resources, logs, dependencies, and scratch.
- [Deployment guide](docs/deployment.md): services, storage, authentication, and troubleshooting.
- [Reference](docs/reference.md): configuration, task states, JSON output, and recovery semantics.
- [Contributing](CONTRIBUTING.md): building, testing, and submitting changes.

## License

TriQueue is distributed under the [MIT License](LICENSE).
