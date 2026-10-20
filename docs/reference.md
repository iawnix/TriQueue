# Configuration and behavior reference

See the [usage guide](usage.md) for examples and the
[deployment guide](deployment.md) for host setup.

## Environment variables

### Connections and server storage

| Variable | Default | Purpose |
| --- | --- | --- |
| `TRI_SOCKET` | `$XDG_RUNTIME_DIR/tri/server.sock`, or `/tmp/tri.sock` if unset | Server Unix socket; preferred CLI connection |
| `TRI_SERVER_ADDR` | `127.0.0.1:7400` | Server TCP bind address; TCP destination for clients and CLI fallback |
| `TRI_AUTH_TOKEN` | Unset | Shared token for TCP authentication |
| `TRI_DATA_DIR` | `./data` | Server data directory and fallback log base |
| `TRI_DB` | `TRI_DATA_DIR/tri.db` | SQLite database path |
| `RUST_LOG` | `tri=info` | Logging filter for server and compute client |

Use absolute paths for persistent services. The compute client always uses
TCP. CLI tools use the Unix socket when it exists and otherwise try TCP.
Unix-socket requests do not require the authentication token.

### Compute client

Command-line flags override their corresponding environment variables.

| Variable | Flag | Default |
| --- | --- | --- |
| `TRI_NODE` | `--name` | Required |
| `TRI_NODE_ADDRESS` | `--address` | `unknown` (informational node address) |
| `TRI_NODE_CPUS` | `--cpus` | `1` |
| `TRI_NODE_MEMORY` | `--memory` | `1G` |
| `TRI_NODE_GPUS` | `--gpus` | `0` |
| `TRI_NODE_GPU_DEVICES` | `--gpu-devices` | `/dev/nvidia0` through the configured GPU count |
| `TRI_NODE_SCRATCH` | `--scratch` | `/tmp/tri-scratch` |

Memory sizes accept integer byte counts and case-insensitive `K`, `M`, `G`,
`T`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, and `TiB` suffixes. All
suffixes use powers of 1024. GPU device lists are comma-separated.

### Task environment

| Variable | Meaning |
| --- | --- |
| `TRI_TASK_ID` | Queue task ID |
| `TRI_SCRATCH` | Node-local temporary directory for this task |
| `TRI_JOB_CWD` | Original job working directory, when supplied |
| `CUDA_VISIBLE_DEVICES` | Assigned GPU indexes for GPU tasks |
| `NVIDIA_VISIBLE_DEVICES` | Assigned GPU indexes for GPU tasks |

Use `tri_sub --env KEY=VALUE` for additional variables. `TMPDIR` is not set
automatically; a script can set it to `TRI_SCRATCH`.

## Task states

| State | Meaning |
| --- | --- |
| `wait` | Queued for resources or prerequisites |
| `run` | Assigned to a node as an active attempt |
| `done` | Completed successfully |
| `fail` | Failed to start or execute, including memory-limit failures |
| `cancel` | Cancellation requested or completed; inspect confirmation fields |
| `lost` | The attempt's lease expired with no retry remaining |
| `blocked` | A prerequisite did not succeed or its record is missing |
| `timed_out` | Exceeded the configured runtime limit |

A task ID is a monotonically increasing integer. An `attempt` identifies an
execution attempt under that task ID; retrying does not create a new task ID.
Timestamps in task JSON are Unix seconds encoded as strings, or `null` when
not recorded.

## JSON output and workflow integration

`tri_sub`, `tri_ls`, `tri_stat`, `tri_node`, `tri_del`, and `tri_clean` support
`--json`. `tri_log` emits task output without a JSON wrapper.

| Command | JSON result |
| --- | --- |
| `tri_sub --json` | Object with `task`, `created`, and `deduplicated` |
| `tri_ls --json` | Array of tasks |
| `tri_stat --json` | Array of tasks in the queue view |
| `tri_stat ID --json` | One task object |
| `tri_stat --external-id ID --json` | One task object |
| `tri_del ID --json` | Updated task object |
| `tri_node --json` | Array of node objects |
| `tri_clean --json` | Object with `removed` count |

Task objects include `id`, `spec`, `state`, `node`, resource assignments,
`attempt`, `lease_id`, `lease_expires_at`, timestamps, `exit_code`, `error`,
`stdout_path`, `stderr_path`, `external_id`, `cancel_confirmed`, and
`completion_reason`. `spec` contains the submitted command and resource
requirements. Node objects report capacities, reservations, running tasks,
online status, and capabilities.

Handled request errors in JSON mode return an `error` object with `code`,
`message`, and `retryable`, and exit with status 1. Argument parsing and local
preparation errors may still be ordinary diagnostics on standard error; callers
should always check the process exit status.

Use `--external-id` for a stable workflow task identity and `--request-id`
(or `--idempotency-key`) for submissions that may be retried after a lost
response:

```bash
tri_sub --external-id workflow-42-step-1 --request-id submission-42-step-1 \
  --json -- python3 calculation.py
tri_stat --external-id workflow-42-step-1 --json
```

Repeating either identifier with the same task specification returns the
original task. Reusing it with a different specification is an error. Keep
working directories and other submission options consistent across retries.
Cleaning the task record also removes its submission receipt, so retrying an
old identifier after cleanup can create a new task.

## Cancellation and resource reservations

Cancelling a queued task finishes it immediately. Cancelling a running task
keeps its CPU, memory, and GPU reservations until the node reports completion,
unless the lease expires first. Scheduling, `tri_node`, and `tri_top` derive
reservations from unfinished attempts with a lease. Historical `node`,
`assigned_cpus`, and `assigned_gpus` fields alone do not reserve capacity.

The task state remains `cancel` in all of these cases:

| Situation | `cancel_confirmed` | `completion_reason` |
| --- | --- | --- |
| Requested; node has not acknowledged completion | `false` | `null` |
| Cancelled while queued | `true` | `cancelled_before_start` |
| Node reported completion after cancellation | `true` | `node_completion` |
| Cancelled attempt's lease expired | `false` | `lease_expired` |

Lease expiry closes the scheduler's reservation but does not prove the process
stopped. Keep execution status unresolved in that case. A delayed completion
for the same cancelled attempt can subsequently confirm stopping.

Cancellation heartbeats renew the lease while the node is still stopping the
task. Failed stop requests are retried. The client retains successful stop
acknowledgements until the server acknowledges them, before cleaning up units.

Existing databases migrate automatically. Historical cancellations without
recorded confirmation remain unconfirmed; migration does not manufacture stop
evidence. Closed historical records no longer reduce available capacity.
Deploy updated server, CLI, and client binaries together when adopting these
fields and acknowledgement behavior.

## Leases and resource enforcement

Each active attempt has a lease renewed by node heartbeats. If renewal stops,
the attempt becomes `lost`, or returns to `wait` if `--max-attempts` permits
another attempt. The default is one attempt. Ordinary task failures and runtime
timeouts do not trigger this lease-based retry mechanism.

On startup and reconnect, the client reconciles existing systemd task units
against the server's current attempts. It adopts matching active attempts and
stops obsolete units. Lease expiry and retries do not provide exactly-once
execution: an unreachable node may still be running an old process.

CPU assignments use systemd `AllowedCPUs`. Tasks with `--mem` use `MemoryMax`
and disable swap for the task cgroup; without `--mem`, there is no task-specific
memory limit. GPU assignments use device rules and visibility variables.
Systemd `RuntimeMaxSec` enforces a requested runtime limit.

## Protocol compatibility

Every connection starts with a `Hello` exchange that checks the protocol
version and returns server capabilities. A version mismatch produces an
explicit error. Capability names cover features such as submission
idempotency, dependencies, reconciliation, and cancellation confirmation.

The wire types are defined in [`src/protocol.rs`](../src/protocol.rs), task and
node fields in [`src/model.rs`](../src/model.rs), and CLI JSON formatting in
[`src/output.rs`](../src/output.rs). Keep components on the same revision;
these source definitions are the reference for custom integrations.
