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
| `TRI_NODE` | `--name` | Required, except with `--check` |
| `TRI_TASK_USER` | `--task-user` | Unset: user manager; set: root client with system units running as this non-root account |
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

`TMPDIR` is set to `TRI_SCRATCH`. Use `tri_sub --env KEY=VALUE` for additional
variables. Task-supplied variables are applied after resource verification;
GPU visibility variables are set from the scheduler's assignment.

## Task states

| State | Meaning |
| --- | --- |
| `wait` | Queued for resources or prerequisites |
| `run` | Assigned to a node as an active attempt |
| `unknown` | Renewal stopped; execution is unresolved and resources remain reserved |
| `done` | Completed successfully |
| `fail` | Failed to start or execute, including memory-limit failures |
| `cancel` | Cancellation requested or completed; inspect confirmation fields |
| `lost` | Execution outcome unavailable; operator confirmed the old execution stopped (legacy records may only indicate lease expiry) |
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
| `tri_stat ID --history --json` or `--events --json` | Object with `attempts` and `events` |
| `tri_stat --external-id ID --json` | One task object |
| `tri_del ID --json` | Updated task object |
| `tri_node --json` | Array of node objects |
| `tri_clean --json` | Object with `removed` count |

Task objects include `id`, `spec`, `state`, `node`, resource assignments,
`attempt`, `lease_id`, `lease_expires_at`, timestamps, `exit_code`, `error`,
`stdout_path`, `stderr_path`, `external_id`, `cancel_confirmed`, and
`completion_reason`, and `recovery_pending`. `spec` contains the submitted command and resource
requirements. Node objects report capacities, reservations, running tasks,
online status, `quarantined`, and capabilities. `tri_node` displays quarantined
nodes as `hold`; `tri_top` labels them `quarantined`. They receive no new tasks.

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
keeps its CPU, memory, and GPU reservations until stopping is confirmed,
including when the lease expires. Scheduling, `tri_node`, and `tri_top` derive
reservations from unfinished attempts with a lease. Historical `node`,
`assigned_cpus`, and `assigned_gpus` fields alone do not reserve capacity.

The task state remains `cancel` in all of these cases:

| Situation | `cancel_confirmed` | `completion_reason` |
| --- | --- | --- |
| Requested; node has not acknowledged completion | `false` | `null` |
| Cancelled while queued | `true` | `cancelled_before_start` |
| Node reported completion after cancellation | `true` | `node_completion` |
| Cancelled attempt's lease expired (`recovery_pending=true`) | `false` | `null` |
| Operator confirmed the old execution stopped | `true` | `operator_confirmed_stopped` |

Lease expiry retains the reservation and quarantines the node. It does not
prove the process stopped. A delayed completion for the same cancelled attempt
can subsequently confirm stopping. `tri_clean` never removes unfinished
cancellations or unresolved attempts.

Cancellation heartbeats renew the lease while the node is still stopping the
task. Failed stop requests are retried. The client retains successful stop
acknowledgements until the server acknowledges them, before cleaning up units.

Existing databases migrate automatically. Historical cancellations without
recorded confirmation remain unconfirmed; migration does not manufacture stop
evidence. Closed historical records no longer reduce available capacity.
Deploy updated server, CLI, and client binaries together when adopting these
fields and acknowledgement behavior.

## Leases and resource enforcement

Each active attempt has a 60-second lease renewed by node heartbeats roughly
every two seconds. Heartbeats run independently of task startup, inspection,
stopping, and cleanup, and include pending starts and completions awaiting
acknowledgement. TCP requests time out after five seconds. If renewal stops,
the attempt becomes `unknown` (`cancel` keeps its cancellation intent), with
`recovery_pending=true`. Its CPU, memory, GPU, and lease identity remain reserved;
the whole node is quarantined, even if spare resources exist. Other nodes can
continue unrelated tasks. Dependencies on an `unknown` task keep waiting.
Quarantine survives server restart and node registration. A matching lease
heartbeat from the assigned node restores the same execution; a matching
completion releases its resources. Neither a missing heartbeat nor absence
from a unit listing authorizes another execution.

There is no automatic retry on lease expiry. If recovery is impossible, first
verify the old execution is stopped and cannot launch later (for example,
fence the old node and disable pending launches). Then record that assertion:

```bash
tri_stat 123 --confirm-stopped --attempt 1 --reason "old node fenced; pending launches disabled"
# Add --retry only if another execution is desired and max_attempts permits it.
tri_stat 124 --confirm-stopped --attempt 1 --reason "old execution stopped and launches disabled" --retry
```

The exact attempt, unresolved status, and nonempty reason are required. A
recovered attempt rejects stale operator resolution. Resolution records a
terminal `lost` attempt (or confirmed `cancel`), then optionally requeues it
atomically. Cancelled tasks cannot retry; ordinary failures/timeouts do not
automatically retry either. `--max-attempts` defaults to one and bounds explicit
retries after a confirmed stop. The command trusts the operator's assertion;
it does not power off a node or inspect remote processes for you.

A task whose systemd unit disappears is reported as `fail` with an unavailable
exit status and a specific error. Before reporting any terminal execution, the
client waits for systemd stop to settle, including control-group cleanup. This
does not terminate the compute client.
Temporary inspection errors retain the allocation and are retried; a failed
inspection alone is not evidence that the task has stopped. Lease expiry and
reconciliation stops are logged with task identities for diagnosis.

On startup and reconnect, the client reconciles existing systemd task units
against the server's current attempts. It adopts matching active attempts and
stops obsolete units. This avoids automatic duplicate execution during network
partitions; it is not an exactly-once guarantee for external side effects.

CPU assignments prefer a contiguous free range. If free CPUs are fragmented,
the scheduler selects enough distinct free IDs (for example `0,2,4`) and uses
systemd `AllowedCPUs` to enforce that set. GPU IDs use the same selection rule. Every new task reserves memory and
uses `MemoryMax` with swap disabled; omitted or zero memory requests become
1 GiB at submission, before scheduling and idempotency fingerprinting. GPU
assignments use device rules and visibility variables. The device allowlist
also applies to tasks with zero GPUs. Systemd `RuntimeMaxSec` enforces a
requested runtime limit.

A guard runs inside the task unit before executing the command. It verifies
`cpuset.cpus.effective`, `memory.max`, and `memory.swap.max`, checks access to
assigned GPU devices, and verifies that unallocated GPU device nodes cannot
be opened. Verification failure prevents the command from running and returns
exit code 78; details are in task stderr. Task commands should avoid reserving
exit code 78 for unrelated errors, since it is reported as an enforcement failure.

`tri-client --check` runs a short-lived CPU-only probe through the configured
manager. Only a successful probe advertises `resource_enforcement`, which is
required by `tri_node --check`. With `--task-user ACCOUNT`, the probe uses a
system unit running under that account. Otherwise it uses a user unit, and
controller detection reads the user manager's delegated cgroup, not the host
root. Restart the client to refresh capabilities after host configuration changes.

On reconciliation, active units must have been started through the guard and
must still have matching CPU and memory constraints. Unsafe legacy units are
stopped, with their reservation retained until stopping is acknowledged.

## Execution history

```bash
tri_stat 123 --history
tri_stat 123 --events
tri_stat 123 --history --events
tri_stat 123 --history --json
```

Attempts retain the command, resource allocation, node, lease identity, times,
exit code, and completion reason. Requeuing does not overwrite the previous
attempt. Events record submission, claims, expiry, recovery, cancellation,
confirmed stops, completion, and requeue in monotonically increasing event-ID
order. Routine renewals do not create events. Attempt snapshots update at these
transitions; use the current task object for the latest lease expiry.

History is persisted atomically with task state changes. Migration imports the
latest available attempt and a `baseline` event; it cannot reconstruct older
attempts already overwritten by previous releases. Log paths are recorded, but
history does not make separate copies of log files. Cleaning a terminal task
also removes its history and submission receipts.

## Protocol compatibility

This release uses protocol version **2**. Back up the SQLite database with its
SQLite backup mechanism (or stop the server before copying the database and
WAL files). Upgrade the server, all clients, and CLI tools together. Version 1
clients cannot renew leases against a version 2 server; a partial upgrade can
leave nodes quarantined. Database migration happens automatically on startup.
Do not roll back only the server binary against the migrated database.

Every connection starts with a `Hello` exchange that checks the protocol
version and returns server capabilities. A version mismatch produces an
explicit error. Capability names cover features such as submission
idempotency, dependencies, reconciliation, and cancellation confirmation.

The wire types are defined in [`src/protocol.rs`](../src/protocol.rs), task and
node fields in [`src/model.rs`](../src/model.rs), and CLI JSON formatting in
[`src/output.rs`](../src/output.rs). Keep components on the same revision;
these source definitions are the reference for custom integrations.
