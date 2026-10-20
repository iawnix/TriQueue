# Contributing to TriQueue

Bug reports, documentation fixes, and focused pull requests are welcome.
For a larger change, open an issue first to discuss the intended behavior.

## Build and test

Use a current stable Rust toolchain and a C compiler/linker on Linux:

```bash
cargo build --locked --bins
cargo test --locked
cargo fmt --all -- --check
```

The tests cover queue storage, scheduling, protocol behavior, and client
cancellation. The cancellation integration test uses fake systemd commands;
it does not establish that resource controls work on your host. For runtime
changes, also follow the [quick start](README.md#run-your-first-task) on a host
with a working systemd user manager and cgroup v2. Validate GPU changes on
suitable hardware.

## Find your way around

| Path | Responsibility |
| --- | --- |
| `src/bin/` | CLI entry points, server launcher, and compute client |
| `src/server.rs` | Request handling and scheduling |
| `src/store.rs` | SQLite persistence, attempts, and leases |
| `src/model.rs`, `src/protocol.rs` | Shared types and wire protocol |
| `src/client.rs` | Connections and protocol exchange |
| `src/output.rs`, `src/tui.rs` | Command output and dashboard |
| `tests/` | Integration tests |
| `deploy/user/` | Generic systemd user service templates |
| `docs/` | User, deployment, and reference documentation |

## Submit a change

Keep pull requests focused on one problem. Explain the behavior before and
after the change and how you checked it. Add regression coverage for behavior
changes where practical, and update the relevant documentation when changing
commands, configuration, task states, or protocol fields.

For bug reports, include the revision, Linux and systemd versions, commands
needed to reproduce the issue, and relevant task or service logs. Remove
authentication tokens and private job data before posting them.

The project is licensed under the [MIT License](LICENSE).
