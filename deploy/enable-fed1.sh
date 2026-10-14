#!/usr/bin/env bash
set -euo pipefail

sudo install -d -o iaw -g iaw /DATA/tri

if [[ -L "$HOME/DATA" ]]; then
  [[ "$(readlink "$HOME/DATA")" == "/DATA" ]] || {
    echo "$HOME/DATA must point to /DATA" >&2
    exit 1
  }
elif [[ -e "$HOME/DATA" ]]; then
  echo "$HOME/DATA already exists; move it aside before creating the shared-data link" >&2
  exit 1
else
  ln -s /DATA "$HOME/DATA"
fi

sudo systemctl enable --now nfs-server.service

for export_line in \
  '/home/iaw/soft 192.168.31.18(ro,sync,root_squash)' \
  '/DATA 192.168.31.18(rw,sync,root_squash)'; do
  if ! sudo grep -Fqx "$export_line" /etc/exports 2>/dev/null; then
    printf '%s\n' "$export_line" | sudo tee -a /etc/exports >/dev/null
  fi
done
sudo exportfs -ra

sudo firewall-cmd --permanent \
  --add-rich-rule='rule family="ipv4" source address="192.168.31.18" port port="2049" protocol="tcp" accept'
sudo firewall-cmd --permanent \
  --add-rich-rule='rule family="ipv4" source address="192.168.31.18" port port="7400" protocol="tcp" accept'
sudo firewall-cmd --reload
sudo loginctl enable-linger iaw

mkdir -p "$HOME/.config/tri"
sed -i 's#^TRI_DATA_DIR=.*#TRI_DATA_DIR=/home/iaw/DATA/tri#' "$HOME/.config/tri/server.env"
cat > "$HOME/.config/tri/client-fed1.env" <<'EOF'
TRI_SERVER_ADDR=127.0.0.1:7400
TRI_NODE=fed1
TRI_NODE_ADDRESS=192.168.31.243
TRI_NODE_CPUS=24
TRI_NODE_MEMORY=31G
TRI_NODE_GPUS=1
TRI_NODE_SCRATCH=/home/iaw/scratch
EOF
cat > "$HOME/.config/systemd/user/tri-client-fed1.service" <<'EOF'
[Unit]
Description=tri compute client on FED1
After=network-online.target tri-server.service
Wants=network-online.target
Requires=tri-server.service

[Service]
Type=simple
EnvironmentFile=%h/.config/tri/client-fed1.env
Environment=TRI_CLIENT_UNIT=tri-client-fed1.service
ExecStart=%h/.local/bin/tri-client
Restart=on-failure
RestartSec=2

[Install]
WantedBy=default.target
EOF
mkdir -p "$HOME/scratch"
systemctl --user daemon-reload
systemctl --user enable --now tri-server.service tri-client-fed1.service

echo 'FED1 is ready. Check with: tri_node'
