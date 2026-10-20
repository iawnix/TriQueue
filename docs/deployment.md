# Deploying TriQueue

Use the [single-machine quick start](../README.md#run-your-first-task) to check
your environment before setting up persistent services.

## Choose a layout

Run one `tri-server` with its SQLite database on local storage. Run one
`tri-client` per compute node, using a unique node name. A compute client may
also run on the server host.

Run only one compute client per Linux account on a host, including clients for
different servers. Task-unit names contain task IDs and attempt numbers, not a
queue identifier; clients under the same account can otherwise interfere with
each other's units during reconciliation.

CLI tools normally run on the server host and connect through a Unix socket.
They can also connect through TCP when their configured socket does not exist.
Compute clients always use TCP.

TriQueue runs tasks as the compute client's account. Use it for trusted jobs
and trusted callers. The shared authentication token is not a per-user
permission system. The server is a single point of availability; this project
does not provide replicated control servers or database failover.

## Prepare storage and software

Job scripts, inputs, executables, and working directories must be available on
every node eligible to run the task. Use the same absolute paths on those
nodes. An NFS share is one option; TriQueue does not configure or require a
particular shared filesystem.

Log paths must also be accessible from the machine where you run `tri_log`,
which reads files directly. Store final outputs on shared or durable storage,
and use node-local scratch only for temporary files. Match account permissions
across hosts so jobs can read inputs and write results.

Keep `TRI_DB` on the server's local filesystem, outside NFS. `TRI_DATA_DIR`
is the server's data directory and fallback base for logs when a submission
has no working directory. Normal CLI submissions set a working directory.
Configure a separate local scratch directory on each compute node.

Install the binaries from the same revision on the server and compute nodes,
using the [source installation instructions](../README.md#install-from-source).

## Connections and authentication

Set the same nonempty random `TRI_AUTH_TOKEN` in the server and all compute
clients. Set it in CLI environments too when using TCP. For example, generate
a token locally with `openssl rand -hex 32`, then place it in the environment
files described below. Protect those files with mode `600`.

Set the server's `TRI_SERVER_ADDR` to its private interface and port, such as
`192.0.2.10:7400` (replace this documentation address). Clients use that same
server address. Permit the port only from intended compute nodes and remote
CLI hosts. The protocol uses plain TCP; use a trusted private network or a
protected tunnel when traffic crosses an untrusted network.

Unix-socket requests rely on filesystem access rather than the token. Keep
the socket inside a private runtime directory. If a configured socket exists
but cannot be opened, CLI tools report an error rather than falling back to TCP.

## System-managed compute client (recommended)

For reliable CPU, memory, and GPU device enforcement, run the compute client
as a root-owned system service and choose a non-root task account with
`TRI_TASK_USER`. The client asks the system manager to create task units with
`User=ACCOUNT`. Calculations, scratch preparation, cleanup, and log writes
run as that account. The server and CLI can continue running as ordinary users.
This remains a queue for trusted callers; it is not a multi-user security system.

Build and install matching CLI/server binaries first. Build the release client
on each compute node, then prepare a client environment file as shown in the
user-service section below:

```bash
cargo build --release --locked --bins
```

After draining active tasks, install the system client using the task account
and the existing client environment file. For example, replace `worker` and
the file path with your own:

```bash
sudo bash deploy/install-system-client.sh worker /home/worker/.config/tri/client.env
```

The installer checks actual resource enforcement from a temporary system
service before switching services,
installs a root-owned binary at `/usr/local/bin/tri-client`, copies the
configuration to `/etc/tri/client.env` with mode `600`, sets `TRI_TASK_USER`,
and installs [`deploy/system/tri-client.service`](../deploy/system/tri-client.service).
It refuses to switch while task units exist, stops and disables old compute
user services, and starts the system compute client. It leaves the server
and user manager running. For an existing system client, drain tasks before
rerunning the installer; it stops the client after the new check passes and
rechecks for tasks before replacing it. Review local service
customizations before replacing an existing unit.

On SELinux hosts, the installer stages the check under `/usr/local/bin` and
restores the default file labels before execution. SELinux remains enabled;
the installer does not add policy exceptions for files in home directories.

The source environment file must use systemd `EnvironmentFile` syntax and must
be trusted by the administrator. Each compute host needs its own unique node
name, actual capacities, scratch path, server address, and authentication token.
Use one compute client per host/system manager to avoid task-unit name collisions.

Check the host independently of the queue with:

```bash
sudo /usr/local/bin/tri-client --task-user worker --check
systemctl status tri-client.service
journalctl -u tri-client.service -n 100
```

Then run `tri_node --check` and a representative task through the queue.
Normal interactive shell commands remain outside task cgroups.

## Install systemd user services (requires delegated controls)

User mode requires both delegated CPU/memory controls and effective device
access enforcement. Some hosts cannot apply GPU device policies from a user
manager; use the system-managed mode above when the probe fails. Run
`tri-client --check` before enabling a user client. CPU-only nodes still require
cpuset delegation; GPU-equipped nodes must deny GPU access to CPU-only jobs.

The generic templates in [`deploy/user/`](../deploy/user/) run under the current
account and expect Cargo-installed binaries at `%h/.cargo/bin/`. Adjust
`ExecStart` if you install elsewhere. Run the commands below from the repository
root on each relevant host.

```bash
mkdir -p "$HOME/.config/tri" "$HOME/.config/systemd/user"
chmod 700 "$HOME/.config/tri"
```

On the server host, create local state storage and install the server template:

```bash
mkdir -p "$HOME/.local/state/tri"
chmod 700 "$HOME/.local/state/tri"
cp deploy/user/tri-server.service "$HOME/.config/systemd/user/"
```

Create `~/.config/tri/server.env`, replacing the address and token:

```ini
TRI_SERVER_ADDR=192.0.2.10:7400
TRI_AUTH_TOKEN=replace-with-your-generated-token
```

The server template uses `%t/tri/server.sock` for the socket and
`%h/.local/state/tri/tri.db` for the database. These match the CLI's default
socket location when `XDG_RUNTIME_DIR` is set. To override storage or socket
paths, add `TRI_DATA_DIR`, `TRI_DB`, or `TRI_SOCKET` to `server.env`, using
absolute paths. Environment files do not expand shell variables such as `$HOME`.
Configure CLI environments to match any socket override.

On each compute node, install the client template:

```bash
cp deploy/user/tri-client.service "$HOME/.config/systemd/user/"
```

Create `~/.config/tri/client.env`. This is an example for a CPU-only node;
replace the address, token, resource counts, and scratch path:

```ini
TRI_SERVER_ADDR=192.0.2.10:7400
TRI_AUTH_TOKEN=replace-with-your-generated-token
TRI_NODE=worker-1
TRI_NODE_CPUS=4
TRI_NODE_MEMORY=8G
TRI_NODE_GPUS=0
TRI_NODE_SCRATCH=/absolute/local/scratch/tri
```

Use a distinct `TRI_NODE` on each host. The scratch path must be writable by
the account running the client. On a host that also runs the server, install
both templates and create both environment files.

Protect each environment file you created, then start the appropriate service:

```bash
# Server host:
chmod 600 "$HOME/.config/tri/server.env"
systemctl --user daemon-reload
systemctl --user enable --now tri-server.service

# Each compute node:
chmod 600 "$HOME/.config/tri/client.env"
systemctl --user daemon-reload
systemctl --user enable --now tri-client.service
```

To keep user services running after logout, an administrator may need to enable
lingering for the service account:

```bash
sudo loginctl enable-linger "$USER"
```

Check registration and runtime capabilities from the server host:

```bash
tri_node --check
tri_node --json
```

`tri_node --check` requires reported controller availability and a successful
runtime resource probe. Each task also verifies its actual limits before its
command starts. Run representative jobs to validate hardware-specific behavior.

## Compute-node configuration

CPU and memory capacities are configured values, not automatically detected
hardware totals. Advertise resources the node can actually provide, leaving
capacity for the OS and other work. The scheduler allocates CPU indexes from
zero, so the advertised range must be usable by the client account.

For an NVIDIA GPU node, set `TRI_NODE_GPUS` to the device count. The default
device paths are `/dev/nvidia0`, `/dev/nvidia1`, and so on. Override these with
a comma-separated `TRI_NODE_GPU_DEVICES` list when needed. The list must contain
at least as many entries as the configured GPU count.

Tasks run in transient systemd units named `tri-task-<id>-<attempt>` in the
selected user or system manager.
The client applies CPU sets with `AllowedCPUs`, memory limits with `MemoryMax`
and `MemorySwapMax=0`, and timeouts with `RuntimeMaxSec`. GPU jobs also configure
device rules. Controller delegation and device-rule support depend on the host;
check job and systemd logs if a unit cannot apply these properties. GPU capability
paths are expanded to concrete device nodes; filesystem wildcards are not used
in `DeviceAllow` entries.

## Upgrades and recovery

Back up the local database before upgrades, using SQLite's backup facilities
or a copy made with the server stopped. Deploy server, CLI, and compute-client
binaries from the same revision. The server migrates existing database schemas
on startup. Drain jobs before adopting guarded execution: active legacy units
without a verification guard will be stopped on reconciliation, and old queued
tasks with zero memory must be cancelled and resubmitted with a positive memory
reservation. Old zero-memory idempotency fingerprints do not match the new
1 GiB default; inspect the original task rather than blindly retrying submissions
with changed identifiers. Protocol negotiation rejects version mismatches, but matching
protocol versions alone do not guarantee every optional capability is present.

On startup or reconnect, a client reconciles existing task units with the
server. It can adopt a still-running current attempt and stops units that no
longer match. Stopping a client service alone does not necessarily stop its task
units. To shut down deliberately, stop submitting work and let jobs finish or
request cancellation and verify confirmation before stopping the services.

See the [reference](reference.md#leases-and-resource-enforcement) for lease
expiry, retry behavior, and cancellation migration details.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| CLI cannot connect | Server status, matching `TRI_SOCKET`, or TCP address and firewall when no socket exists |
| Authentication fails | Identical nonempty `TRI_AUTH_TOKEN` values on both ends of TCP connections |
| No nodes are registered | Client service logs, server reachability, and authentication |
| `tri_node --check` fails | Run `tri-client --check` on that node; inspect actual delegation and device enforcement, or use system-managed execution |
| Task stays in `wait` | Requested node, resource capacity, prerequisites, and node availability |
| Task fails to start | Task `error`, unit logs, working directory, executable paths, and permissions |
| Task exits with code 78 | Task stderr explains which resource limit failed verification; the command was not started |
| Logs cannot be read | The recorded path exists and is accessible on the CLI host |
| Protocol version mismatch | Install matching revisions of the server, clients, and CLI tools |

Useful service diagnostics:

```bash
systemctl --user status tri-server.service
systemctl --user status tri-client.service
journalctl --user -u tri-server.service -n 100
journalctl --user -u tri-client.service -n 100
```

For a task, inspect `tri_stat TASK_ID` and its output. If needed, inspect the
corresponding `tri-task-TASK_ID-ATTEMPT` unit in the selected manager's journal.
For system-managed tasks, omit `--user` from service diagnostic commands. Set
`RUST_LOG=tri=debug` in the service environment to increase application logging.

## Original cluster examples

The files directly under [`deploy/`](../deploy/README.md) describe the original
FED1/FED2 deployment, including fixed usernames, network addresses, and NFS
paths. They are site-specific examples, not the installation procedure above.
