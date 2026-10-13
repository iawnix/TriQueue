#!/usr/bin/env bash
set -euo pipefail

TRI_USER=${TRI_USER:-${SUDO_USER:-$USER}}
TRI_PRIMARY_IP=${TRI_PRIMARY_IP:-192.0.2.10}
TRI_WORKER_IP=${TRI_WORKER_IP:-192.0.2.11}
TRI_SOFTWARE_ROOT=${TRI_SOFTWARE_ROOT:-/opt/tri/software}
TRI_DATA_ROOT=${TRI_DATA_ROOT:-/srv/tri-data}
TRI_SCRATCH_ROOT=${TRI_SCRATCH_ROOT:-/var/tmp/tri-scratch}
TRI_WORKER_CPUS=${TRI_WORKER_CPUS:-8}
TRI_WORKER_MEMORY=${TRI_WORKER_MEMORY:-16G}
TRI_WORKER_GPUS=${TRI_WORKER_GPUS:-0}

sudo install -d -o "$TRI_USER" -g "$TRI_USER" "$TRI_SOFTWARE_ROOT" "$TRI_DATA_ROOT" "$TRI_SCRATCH_ROOT"
shared_link="$HOME/tri-data"
if [[ -L "$shared_link" ]]; then
  [[ "$(readlink "$shared_link")" == "$TRI_DATA_ROOT" ]] || {
    echo "$shared_link must point to $TRI_DATA_ROOT" >&2
    exit 1
  }
elif [[ -e "$shared_link" ]]; then
  echo "$shared_link already exists; move it aside before creating the shared-data link" >&2
  exit 1
else
  ln -s "$TRI_DATA_ROOT" "$shared_link"
fi

for fstab_line in \
  "$TRI_PRIMARY_IP:$TRI_SOFTWARE_ROOT $TRI_SOFTWARE_ROOT nfs4 ro,_netdev,x-systemd.automount 0 0" \
  "$TRI_PRIMARY_IP:$TRI_DATA_ROOT $TRI_DATA_ROOT nfs4 rw,_netdev,x-systemd.automount 0 0"; do
  if ! sudo grep -Fqx "$fstab_line" /etc/fstab 2>/dev/null; then
    printf '%s\n' "$fstab_line" | sudo tee -a /etc/fstab >/dev/null
  fi
done

sudo mount -a
sudo loginctl enable-linger "$TRI_USER"
cat > "$HOME/.config/systemd/user/tri-client.service" <<EOF_UNIT
[Unit]
Description=tri compute client on worker node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=TRI_SERVER_ADDR=$TRI_PRIMARY_IP:7400
ExecStart=%h/.local/bin/tri-client --name worker --address $TRI_WORKER_IP --cpus $TRI_WORKER_CPUS --memory $TRI_WORKER_MEMORY --gpus $TRI_WORKER_GPUS --scratch $TRI_SCRATCH_ROOT
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
EOF_UNIT
systemctl --user daemon-reload
systemctl --user enable --now tri-client.service

echo 'Worker node is ready. Check with: tri_node on the primary node'
