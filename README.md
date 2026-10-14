# tri

`tri` is a small Rust task queue for the two-node 3w cluster. It keeps the
control plane on `FED1`, runs a small client on each compute node, and leaves
software/data sharing to NFS.

## Commands

```text
tri_sub       submit a task or job script
tri_ls        list tasks
tri_stat      show queue status (or inspect one task by ID)
tri_log       read stdout/stderr
tri_del       cancel a task
tri_node      list nodes
tri_top       terminal dashboard
tri_clean     remove finished task records
```

`tri_sub`, `tri_ls`, `tri_stat`, `tri_node`, `tri_del`, and `tri_clean` support
`--json` for scripts and agents. The JSON contains queue metadata such as task
state, attempt, lease, exit code, and log paths. `tri_log` keeps stdout and
stderr as raw task output.

`tri_ls` includes a `TIME` column. It shows elapsed runtime as `HH:MM:SS`
(or `D-HH:MM:SS` for jobs lasting more than a day); running jobs update on
each invocation, while waiting jobs show `--`.

The task ID is a monotonically increasing integer. A submitted command is
stored in SQLite on the server. By default, stdout and stderr are merged into
`tri-<id>.out` in the job working directory. Use `--output PATH` and
`--error PATH` to choose separate files; `%j` expands to the task ID. Relative
paths are resolved from `--chdir` (and `--cwd` remains a compatibility alias),
while absolute paths are used as written.

For a reusable job script, put resource declarations in its header and pass
the script directly to `tri_sub`:

```bash
#!/usr/bin/env bash
# tri_name: gaussian-water
# tri_node: fed2
# tri_cpu: 1
# tri_mem: 512M
# tri_gpu: 1
# tri_scratch: true
# tri_output: tri-%j.out
# tri_error: tri-%j.err

set -Eeuo pipefail

scratch=${TRI_SCRATCH:?tri-client did not provide TRI_SCRATCH}
export TMPDIR="$scratch"

exec /home/iaw/soft/gaussian/bin/gaussian16-run water.gjf water.log
```

Submit it with `tri_sub --chdir JOB_DIRECTORY -- ./job.sh`. The supported
declarations are `tri_name`, `tri_node`, `tri_cpu`, `tri_mem`, `tri_gpu`, `tri_scratch`, `tri_output`,
and `tri_error`; command-line options override values from the script header.
The short aliases `tri_out` and `tri_err` are also accepted. `-o/--output` and
`-e/--error` on the command line override the corresponding script settings.

For example:

```bash
tri_sub --chdir "$PWD" --gpu 1 --scratch-cwd --output result.out --error result.err -- ./job.sh
```

Use `--after TASK_ID` to wait for an earlier task to finish successfully;
repeat it for multiple prerequisites. A prerequisite that fails, is cancelled,
or is lost blocks the dependent task. Use `--max-attempts N` to requeue a task
after a node lease expires. The default is one attempt, so jobs with side
effects are not duplicated unless requested.

A complete scratch-based job can look like this:

```bash
#!/usr/bin/env bash
# tri_name: gpu-example
# tri_node: fed1
# tri_cpu: 4
# tri_mem: 8G
# tri_gpu: 1
# tri_scratch: true

set -Eeuo pipefail

scratch=${TRI_SCRATCH:?tri-client did not provide TRI_SCRATCH}
job_dir=${TRI_JOB_CWD:?tri-client did not provide TRI_JOB_CWD}
trap 'rm -rf -- "$scratch"' EXIT

cp "$job_dir/input.dat" "$scratch/"
cd "$scratch"

# Replace this with the actual calculation command.
/home/iaw/soft/my-program input.dat result.dat

cp result.dat "$job_dir/"
```

Submit it from the directory containing `input.dat`:

```bash
tri_sub --chdir "$PWD" --output result.out --error result.err -- ./job.sh
```

Each running task receives a node-local temporary directory in `TRI_SCRATCH`,
for example `/home/iaw/scratch/<id>`. Use `--scratch-cwd` or `# tri_scratch: true`
to start the task in that directory; otherwise the task starts in `--chdir`.
The original `--chdir` value remains available as `TRI_JOB_CWD`. A script can
copy inputs into scratch, run there, copy final results back to the shared job
directory, and remove scratch when it is finished. The client also performs
best-effort cleanup after the task exits. Do not hardcode the FED1 or FED2
scratch path because the scheduler may choose either node. Final results and
logs should remain in the shared job directory or in the paths selected with
`--output` and `--error`.

`tri_top` is the live queue view. It puts aggregate CPU and memory usage at
the top, shows each node as a small resource card, keeps active jobs above
history, and shows the selected job's output path at the bottom. Use `j/k` or
the arrow keys to move, `c` to request cancellation, `r` to refresh, and `q`
to quit.

## Local smoke test

Build the binaries:

```bash
cargo build --bins
```

Start the server in one terminal:

```bash
TRI_SOCKET=$PWD/tri.sock \
TRI_SERVER_ADDR=127.0.0.1:7400 \
TRI_DATA_DIR=$PWD/data \
TRI_DB=$PWD/tri.db \
target/debug/tri-server
```

Start a client in a second terminal:

```bash
TRI_SERVER_ADDR=127.0.0.1:7400 \
TRI_NODE=fed2 \
TRI_NODE_CPUS=8 \
TRI_NODE_MEMORY=23G \
TRI_NODE_GPUS=0 \
TRI_NODE_SCRATCH=$PWD/scratch \
target/debug/tri-client
```

Then submit and inspect a task from a third terminal:

```bash
TRI_SOCKET=$PWD/tri.sock target/debug/tri_sub --name hello --node fed2 --cpu 2 -- bash -lc 'echo start; sleep 1; echo done'
TRI_SOCKET=$PWD/tri.sock target/debug/tri_stat
TRI_SOCKET=$PWD/tri.sock target/debug/tri_ls
TRI_SOCKET=$PWD/tri.sock target/debug/tri_stat --json
TRI_SOCKET=$PWD/tri.sock target/debug/tri_top
```

The scheduler allocates CPU sets and GPU indexes. GPU jobs receive
`CUDA_VISIBLE_DEVICES` and `NVIDIA_VISIBLE_DEVICES` with their assigned indexes.
Tasks with `--mem` run in their own systemd cgroup with `MemoryMax` set to the
requested value and swap disabled for that cgroup. Exceeding the limit ends the
task with a memory-limit error; tasks without `--mem` have no task-specific
memory limit. Memory and GPU usage are also tracked by the scheduler and client
heartbeat. Each running task has a lease renewed by heartbeat. If a node stops
renewing its lease, the task becomes `lost`; tasks with remaining
`--max-attempts` are returned to the queue. A server cannot terminate a process
after the client connection is lost, so use `--max-attempts` only for commands
that tolerate a repeated or overlapping attempt.

Deployment examples are under `deploy/`. The NFS examples export only
`/home/iaw/soft` and `/DATA` from FED1 to FED2. Both hosts expose the shared
data through `/home/iaw/DATA`, which is a symlink to the local mount point
`/DATA`; on FED2 that mount point is the NFS filesystem. Keep the SQLite
database on FED1's local filesystem; only task inputs, outputs, and logs belong
on the shared data path.

The user-level binaries and services can be installed without root. To finish
machine setup, run `deploy/enable-fed1.sh` as `iaw` on FED1 and
`deploy/enable-fed2.sh` as `iaw` on FED2. Those scripts use `sudo` for the NFS,
firewall, mount, and login-linger steps, then restart the existing user
services. FED1's script also starts a `tri-client` for FED1 itself, so both
hosts appear in `tri_node`.
