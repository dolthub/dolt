#!/bin/bash

src_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
plist=io.ipfs.ipfs-daemon.plist
dest_dir="$HOME/Library/LaunchAgents"
IPFS_PATH="${IPFS_PATH:-$HOME/.ipfs}"
escaped_ipfs_path=$(echo $IPFS_PATH|sed 's/\//\\\//g')

IPFS_BIN=$(which ipfs || echo ipfs)
escaped_ipfs_bin=$(echo $IPFS_BIN|sed 's/\//\\\//g')

mkdir -p "$dest_dir"

sed -e 's/{{IPFS_PATH}}/'"$escaped_ipfs_path"'/g' \
  -e 's/{{IPFS_BIN}}/'"$escaped_ipfs_bin"'/g' \
  "$src_dir/$plist" \
  > "$dest_dir/$plist"

launchctl list | grep ipfs-daemon >/dev/null
if [ $? ]; then
  echo Unloading existing ipfs-daemon
  launchctl unload "$dest_dir/$plist"
fi

echo Loading ipfs-daemon
if (( `sw_vers -productVersion | cut -d'.' -f2` > 9 )); then
  sudo chown root "$dest_dir/$plist"
  sudo launchctl bootstrap system "$dest_dir/$plist"
else
  launchctl load "$dest_dir/$plist"
fi
launchctl list | grep ipfs-daemon
