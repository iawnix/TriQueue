# Deployment files

For a new installation, follow the [deployment guide](../docs/deployment.md)
and use the generic systemd user service templates in [`user/`](user/).

## Original FED1/FED2 deployment

The files directly in this directory are site-specific examples from the
original two-node cluster. They contain fixed usernames, private IP addresses,
resource counts, and storage paths. They are not portable installation scripts.

| Files | Purpose |
| --- | --- |
| `tri-server.service`, `tri-client-fed1.service`, `tri-client-fed2.service` | Original system-level service examples using the `iaw` account and `/usr/local/bin` |
| `enable-fed1.sh`, `enable-fed2.sh` | Host setup scripts that configure NFS, mounts, lingering, and user services |
| `exports.example`, `fstab-fed2.example` | NFS exports and mounts for that site |

The original layout exports `/home/iaw/soft` and `/DATA` from FED1 to FED2.
Both hosts expose the data through `/home/iaw/DATA`, a symlink to `/DATA`.
The database stays on FED1's local filesystem; job data and logs use the shared
paths. Both hosts can run compute clients.

The setup scripts assume existing account and service configuration and use
`sudo` to change host settings. In particular, `enable-fed1.sh` expects an
existing `server.env` and user server service, and `enable-fed2.sh` requires
`TRI_AUTH_TOKEN` to match the server. They write user-level services using
`~/.local/bin`, which differs from the system-level examples alongside them.
Read and adapt these files before using any part of them on another host.
