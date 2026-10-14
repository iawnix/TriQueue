#!/usr/bin/env bash
set -euo pipefail

sudo install -d -o iaw -g iaw /home/iaw/soft /DATA /home/iaw/scratch

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

for fstab_line in \
  '192.168.31.243:/home/iaw/soft /home/iaw/soft nfs4 ro,_netdev,x-systemd.automount 0 0' \
  '192.168.31.243:/DATA /DATA nfs4 rw,_netdev,x-systemd.automount 0 0'; do
  if ! sudo grep -Fqx "$fstab_line" /etc/fstab 2>/dev/null; then
    printf '%s\n' "$fstab_line" | sudo tee -a /etc/fstab >/dev/null
  fi
done

sudo mount -a
sudo loginctl enable-linger iaw
cat > "$HOME/.config/systemd/user/tri-client.service" <<'EOF'
[Unit]
Description=tri compute client on FED2
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=TRI_SERVER_ADDR=192.168.31.243:7400
ExecStart=%h/.local/bin/tri-client --name fed2 --address 192.168.31.18 --cpus 8 --memory 23G --gpus 0 --scratch /home/iaw/scratch
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
EOF
systemctl --user daemon-reload
systemctl --user restart tri-client.service

echo 'FED2 is ready. Check with: tri-client logs and tri_node on FED1'
