#!/usr/bin/env bash
# Install a root-owned client while keeping calculations under a non-root user.
set -euo pipefail

if [[ $# -ne 2 || $EUID -ne 0 ]]; then
  echo 'Usage: sudo bash deploy/install-system-client.sh TASK_USER CLIENT_ENV_FILE' >&2
  exit 1
fi
account=$1
config_source=$(realpath -- "$2")
account_uid=$(id -u -- "$account")
[[ "$account_uid" != 0 ]] || { echo 'Task account must not be root.' >&2; exit 1; }
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
client_binary="$repo_dir/target/release/tri-client"
[[ -x "$client_binary" ]] || { echo 'First run: cargo build --release --locked --bins' >&2; exit 1; }
[[ -f "$config_source" ]] || { echo 'Client environment file not found.' >&2; exit 1; }
command -v runuser >/dev/null

user_manager() {
  runuser -u "$account" -- env "XDG_RUNTIME_DIR=/run/user/$account_uid" \
    "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$account_uid/bus" systemctl --user "$@"
}
# Never replace a client while task processes may still be running.
if user_manager show-environment >/dev/null 2>&1; then
  units=$(user_manager list-units --all --plain --no-legend 'tri-task-*.service')
  [[ -z "$units" ]] || { echo 'Drain user task units before switching execution managers.' >&2; exit 1; }
fi
units=$(systemctl list-units --all --plain --no-legend 'tri-task-*.service')
[[ -z "$units" ]] || { echo 'Drain system task units before installing or upgrading.' >&2; exit 1; }

# A system service cannot execute home-labelled binaries on SELinux hosts.
# Stage the probe at a system executable path before changing installed services.
install -d -m 755 /usr/local/bin
probe_dir=$(mktemp -d /usr/local/bin/.tri-client-check.XXXXXX)
config_tmp=''
cleanup() {
  rm -rf -- "$probe_dir"
  if [[ -n "$config_tmp" ]]; then rm -f -- "$config_tmp"; fi
}
trap cleanup EXIT
chmod 755 "$probe_dir"
install -m 755 "$client_binary" "$probe_dir/tri-client"
if command -v selinuxenabled >/dev/null && selinuxenabled; then
  restorecon -RF -- "$probe_dir"
fi
# Check from a system service, not only the administrator's login context.
probe_unit="tri-client-install-check-$$"
if ! systemd-run --wait --collect --unit "$probe_unit" \
    "$probe_dir/tri-client" --task-user "$account" --check; then
  journalctl -u "$probe_unit.service" -n 40 --no-pager >&2
  exit 1
fi
# An upgrade can keep the previous client running until the new probe passes.
# Recheck after stopping it, since a task might have started during the probe.
if systemctl is-active --quiet tri-client.service; then
  systemctl stop tri-client.service
  units=$(systemctl list-units --all --plain --no-legend 'tri-task-*.service')
  if [[ -n "$units" ]]; then
    systemctl start tri-client.service
    echo 'A task started during installation; previous client resumed. Drain tasks and retry.' >&2
    exit 1
  fi
fi
install -m 755 "$client_binary" /usr/local/bin/tri-client
if command -v selinuxenabled >/dev/null && selinuxenabled; then
  restorecon -F -- /usr/local/bin/tri-client
fi
install -d -m 700 /etc/tri
config_tmp=$(mktemp /etc/tri/client.env.XXXXXX)
# The source uses systemd EnvironmentFile syntax, not executable shell syntax.
sed '/^[[:space:]]*TRI_TASK_USER=/d' "$config_source" > "$config_tmp"
printf '\nTRI_TASK_USER=%s\n' "$account" >> "$config_tmp"
chmod 600 "$config_tmp"
mv -- "$config_tmp" /etc/tri/client.env
install -m 644 "$repo_dir/deploy/system/tri-client.service" /etc/systemd/system/tri-client.service
if command -v selinuxenabled >/dev/null && selinuxenabled; then
  restorecon -RF -- /etc/tri
  restorecon -F -- /etc/systemd/system/tri-client.service
fi
systemctl daemon-reload

# Stop only compute clients. Leave the server, shell, and user manager running.
if user_manager show-environment >/dev/null 2>&1; then
  mapfile -t old_clients < <(user_manager list-units --state=running --plain --no-legend 'tri-client*.service' | awk '{print $1}')
  if ((${#old_clients[@]})); then
    user_manager stop "${old_clients[@]}"
    units=$(user_manager list-units --all --plain --no-legend 'tri-task-*.service')
    if [[ -n "$units" ]]; then
      user_manager start "${old_clients[@]}"
      echo 'A task started during installation; previous clients resumed. Drain tasks and retry.' >&2
      exit 1
    fi
    user_manager disable "${old_clients[@]}"
  fi
fi
systemctl enable --now tri-client.service
echo "System client installed. Tasks run as $account. Check: systemctl status tri-client.service"
