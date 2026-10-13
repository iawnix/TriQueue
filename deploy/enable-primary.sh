#!/usr/bin/env bash
set -euo pipefail

TRI_USER=${TRI_USER:-${SUDO_USER:-$USER}}
TRI_PRIMARY_IP=${TRI_PRIMARY_IP:-192.0.2.10}
TRI_WORKER_IP=${TRI_WORKER_IP:-192.0.2.11}
TRI_SOFTWARE_ROOT=${TRI_SOFTWARE_ROOT:-/opt/tri/software}
TRI_DATA_ROOT=${TRI_DATA_ROOT:-/srv/tri-data}
TRI_SCRATCH_ROOT=${TRI_SCRATCH_ROOT:-/var/tmp/tri-scratch}
TRI_PRIMARY_CPUS=${TRI_PRIMARY_CPUS:-8}
TRI_PRIMARY_MEMORY=${TRI_PRIMARY_MEMORY:-16G}
TRI_PRIMARY_GPUS=${TRI_PRIMARY_GPUS:-0}

sudo install -d -o "$TRI_USER" -g "$TRI_USER" "$TRI_DATA_ROOT/tri" "$TRI_SCRATCH_ROOT"

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

sudo systemctl enable --now nfs-server.service

for export_line in \
  "$TRI_SOFTWARE_ROOT $TRI_WORKER_IP(ro,sync,root_squash)" \
  "$TRI_DATA_ROOT $TRI_WORKER_IP(rw,sync,root_squash)"; do
  if ! sudo grep -Fqx "$export_line" /etc/exports 2>/dev/null; then
    printf '%s\n' "$export_line" | sudo tee -a /etc/exports >/dev/null
  fi
done
sudo exportfs -ra

sudo firewall-cmd --permanent \
  --add-rich-rule="rule family=\"ipv4\" source address=\"$TRI_WORKER_IP\" port port=\"2049\" protocol=\"tcp\" accept"
sudo firewall-cmd --permanent \
  --add-rich-rule="rule family=\"ipv4\" source address=\"$TRI_WORKER_IP\" port port=\"7400\" protocol=\"tcp\" accept"
sudo firewall-cmd --reload
sudo loginctl enable-linger "$TRI_USER"

mkdir -p "$HOME/.config/tri"
if [[ -f "$HOME/.config/tri/server.env" ]]; then
  sed -i "s#^TRI_DATA_DIR=.*#TRI_DATA_DIR=$TRI_DATA_ROOT/tri#" "$HOME/.config/tri/server.env"
fi
cat > "$HOME/.config/tri/client-primary.env" <<EOF_ENV
TRI_SERVER_ADDR=127.0.0.1:7400
TRI_NODE=primary
TRI_NODE_ADDRESS=$TRI_PRIMARY_IP
TRI_NODE_CPUS=$TRI_PRIMARY_CPUS
TRI_NODE_MEMORY=$TRI_PRIMARY_MEMORY
TRI_NODE_GPUS=$TRI_PRIMARY_GPUS
TRI_NODE_SCRATCH=$TRI_SCRATCH_ROOT
EOF_ENV
cat > "$HOME/.config/systemd/user/tri-client-primary.service" <<'EOF_UNIT'
[Unit]
Description=tri compute client on primary node
After=network-online.target tri-server.service
Wants=network-online.target
Requires=tri-server.service

[Service]
Type=simple
EnvironmentFile=%h/.config/tri/client-primary.env
ExecStart=%h/.local/bin/tri-client
Restart=on-failure
RestartSec=2

[Install]
WantedBy=default.target
EOF_UNIT
systemctl --user daemon-reload
systemctl --user enable --now tri-server.service tri-client-primary.service

echo 'Primary node is ready. Check with: tri_node'
