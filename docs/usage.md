# Using TriQueue

Start with the [README quick start](../README.md#run-your-first-task) if you do
not yet have a running server and compute client. Commands below assume that
the CLI can connect to your server and that job paths exist on the selected node.

## Submit a command

```bash
tri_sub --name hello --cpu 1 --mem 128M -- bash -c 'echo hello'
```

`tri_sub` prints the task ID to standard output. Capture it for later commands:

```bash
job_id=$(tri_sub --name calculation --cpu 2 --mem 2G -- python3 calculation.py)
tri_stat "$job_id"
tri_log "$job_id" --follow
```

Commands default to one CPU, no GPUs, and no task-specific memory limit.
Use `--mem` to reserve memory and enforce a limit. Sizes such as `512M` and `8G`
use powers of 1024. A task waits until its requested resources are available.
`--node worker-1` restricts it to a named node; otherwise the scheduler chooses.

Pass task environment variables explicitly with repeated `--env KEY=VALUE`
options. Do not assume the submitting shell's environment is copied to the job.

## Working directories and logs

For a command, the default working directory is the submitting directory.
For a directly submitted script, it is the script's directory. Override either
with `--chdir /absolute/job/directory` (`--cwd` is an alias). Prefer absolute
paths across machines; TriQueue does not copy files to the compute node.

Standard output and standard error are merged into `tri-<id>.out` by default.
Choose separate files with `--output` and `--error`:

```bash
tri_sub --chdir "$PWD" --output 'job-%j.out' --error 'job-%j.err' \
  -- python3 calculation.py
```

`%j` expands to the task ID. Relative log paths are resolved from the job
working directory, even when the task runs in scratch. Absolute paths are used
as written. If only `--output` is set, standard error also goes to that file.

```bash
tri_log "$job_id"
tri_log "$job_id" --stderr
tri_log "$job_id" --follow
```

`tri_log` reads the recorded path on the machine running the command. It does
not download output from a compute node, so that path must be accessible there.

## Submit a job script

Save this as `job.sh`:

```bash
#!/usr/bin/env bash
# tri_name: example
# tri_cpu: 1
# tri_mem: 128M
# tri_timeout: 60
# tri_output: tri-%j.out
# tri_error: tri-%j.err

set -Eeuo pipefail
printf 'Task %s is running on %s\n' "$TRI_TASK_ID" "$(hostname)"
```

Submit the script directly so that TriQueue reads its header:

```bash
tri_sub -- ./job.sh
```

Directly submitted scripts are executed with Bash and do not need the
executable bit. Header declarations must precede the first non-comment command.
Passing `bash job.sh` instead submits an ordinary command and does not parse
the script header. The script must be readable on the compute node at the
same absolute path as on the submitting machine.

| Header declaration | Command-line option |
| --- | --- |
| `tri_name` | `--name` |
| `tri_node` | `--node` |
| `tri_cpu` | `--cpu` |
| `tri_mem` | `--mem` |
| `tri_gpu` | `--gpu` |
| `tri_scratch` | `--scratch-cwd` |
| `tri_timeout` | `--timeout` |
| `tri_output` | `-o`, `--output` |
| `tri_error` | `-e`, `--error` |

Command-line options override header declarations. `tri_gpus`,
`tri_scratch_cwd`, `tri_out`, and `tri_err` are accepted header aliases.
Use `--scratch-cwd=false` to override a script that enables scratch.

## GPU jobs

```bash
tri_sub --cpu 4 --mem 8G --gpu 1 -- python3 train.py
```

The scheduler assigns GPU indexes. The client configures systemd device rules
and sets `CUDA_VISIBLE_DEVICES` and `NVIDIA_VISIBLE_DEVICES` for the task.
A GPU count reserves devices; it does not set a GPU memory quota. See
[deployment](deployment.md#compute-node-configuration) for node device configuration.

## Scratch directories

Each task receives a node-local directory through `TRI_SCRATCH`.
`--scratch-cwd` or `# tri_scratch: true` starts the task there. The original
job directory remains available as `TRI_JOB_CWD`.

For example, save this as `scratch-job.sh` beside an `input.txt` file:

```bash
#!/usr/bin/env bash
# tri_name: sort-input
# tri_cpu: 1
# tri_mem: 128M
# tri_scratch: true

set -Eeuo pipefail
scratch=${TRI_SCRATCH:?}
job_dir=${TRI_JOB_CWD:?}
export TMPDIR="$scratch"

cp "$job_dir/input.txt" "$scratch/input.txt"
sort "$scratch/input.txt" > "$scratch/result.txt"
cp "$scratch/result.txt" "$job_dir/result.txt"
```

```bash
tri_sub --chdir "$PWD" -- ./scratch-job.sh
```

Copy final results back before the script exits. The client performs best-effort
scratch cleanup after completion; scratch is temporary, not a result archive.
Logs remain at their configured output paths. Avoid hardcoding node-specific
scratch paths. A retry on the same node reuses the task's directory path;
do not assume it starts with a unique path for every attempt.

## Runtime limits, dependencies, and retries

`--timeout SECONDS` sets a positive runtime limit through systemd's
`RuntimeMaxSec`. A task that reaches the limit ends in `timed_out`.

Use `--after` for prerequisites that must complete successfully:

```bash
first=$(tri_sub --name prepare -- python3 prepare.py)
tri_sub --name analyze --after "$first" -- python3 analyze.py
```

Repeat `--after` to require multiple tasks. A prerequisite in `fail`, `cancel`,
`lost`, `blocked`, or `timed_out` blocks the dependent task. Removing a
prerequisite's record before the dependent task is scheduled also blocks it.

`--max-attempts N` permits another attempt after a node lease expires. The
number includes the initial attempt and defaults to one. It does not retry
ordinary command failures. Lease expiry does not prove the old process has
stopped, so enable retries only for jobs that tolerate repeated execution.

## Inspect and cancel work

```bash
tri_ls
tri_ls --state run
tri_stat "$job_id"
tri_del "$job_id"
tri_node --check
tri_top
```

The `TIME` column in `tri_ls` shows elapsed runtime as `HH:MM:SS`, or
`D-HH:MM:SS` for jobs lasting more than a day. Tasks that have not started show
`--`. In `tri_top`, use `j`/`k` or arrow keys to select a task, `c` to request
cancellation, `r` to refresh, and `q` or Escape to quit.

Cancellation of a running task is asynchronous. Check `cancel_confirmed` before
assuming it has stopped; see [cancellation semantics](reference.md#cancellation-and-resource-reservations).

## Clean up task records

```bash
tri_clean                         # Records older than 30 days
tri_clean --older-than 604800      # Records older than seven days
```

`tri_clean --all` removes all records in terminal states, including recent
ones. Cleanup removes metadata and associated submission receipts, not log or
result files. Keep records still needed by dependent tasks or workflow retries.
Avoid cleaning unconfirmed cancellations; their stop status may still be unresolved.

For scripting, JSON output, and deduplicated submissions, see the
[reference](reference.md#json-output-and-workflow-integration).
